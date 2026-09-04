package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type parsedMsg struct {
	natsMsg         jetstream.Msg
	natsSafeSubject string
	tableName       string // routing key for per-table batching; raw (unencoded) name
	scope           string
	columns         []string        // envelope column names, in declaration order
	colSig          string          // columns joined; the within-table batch key
	row             json.RawMessage // one JSONCompactEachRow line, no trailing newline
}

// columnSignature renders a column list as a map key. The separator is a byte
// that cannot appear in a ClickHouse identifier's UTF-8 encoding, so no pair of
// distinct column lists can collide on it.
func columnSignature(cols []string) string {
	return strings.Join(cols, "\x00")
}

type IngestWorker struct {
	js         jetstream.JetStream
	httpClient *http.Client
	cache      cache.Cache
	logger     *slog.Logger
	// target resolves the ClickHouse HTTP wiring per insert
	// (chconn.Manager.Target in production) so a settings reload that
	// re-points ClickHouse applies to the next flush.
	target   func() chconn.Target
	maxBatch int
	maxWait  time.Duration
	// dlqEnabled reports, per table, whether a row that still fails after
	// row-by-row isolation is parked on the DLQ (settings.Store.DLQFor in
	// production; nil means always). Resolved at the moment of the failure, so
	// a settings reload applies to the next poison row without a restart.
	dlqEnabled func(table string) bool

	// wg tracks the dispatch loop; ackWg tracks backgrounded DoubleAck goroutines.
	// Separate so shutdown can drain inserts (wg → tableWg) before waiting on the
	// fsync-bound acks, without an ackWg.Add racing its Wait — see dispatchLoop.
	wg    sync.WaitGroup
	ackWg sync.WaitGroup
}

// poisonDroppedCounter counts envelopes the worker could not read and could not
// park on the DLQ because it is switched off for the table. Every increment is
// a row that no longer exists anywhere; a non-zero rate right after an upgrade
// means the ingest queue was not drained first.
var poisonDroppedCounter, _ = otel.Meter("wavehouse-ingest").Int64Counter(
	"wavehouse_ingest_poison_dropped_total",
	metric.WithDescription("Unreadable ingest envelopes acked and dropped because the DLQ is disabled for the table"),
)

// Batching defaults; overridable on the struct for tests.
// TODO: eventually make this configurable not just in tests
const (
	defaultMaxBatch = 500
	defaultMaxWait  = 5 * time.Second
)

// Consumer/consume tuning, fixed at creation. Invariants: pullMaxMessages ≤
// maxAckPending, and ackWait > defaultMaxWait + CH flush (else in-flight
// messages are redelivered mid-processing → duplicate inserts).
const (
	// Server-side cap on unacked messages; suspends delivery when hit (backpressure).
	maxAckPending = 10_000 // TODO: raise if NATS delivery becomes the bottleneck

	// Client prefetch buffer in front of msgChan (was the implicit jetstream default).
	pullMaxMessages = 500

	// Redelivery timeout. 60s ≈ 5s batch + ~30s HTTP timeout + margin.
	ackWait = 60 * time.Second
)

func StartIngestWorker(
	ctx context.Context, nc *nats.Conn, cache cache.Cache,
	target func() chconn.Target,
	dlqEnabled func(table string) bool,
) (func(context.Context) error, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is nil")
	}
	if cache == nil {
		return nil, fmt.Errorf("cache is nil")
	}
	if target == nil {
		return nil, fmt.Errorf("clickhouse target is nil")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, mq.StreamName(), jetstream.ConsumerConfig{
		Durable:       BufferConsumerName,
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxAckPending: maxAckPending,
	})
	if err != nil {
		return nil, err
	}

	// Tune the HTTP Transport for high-throughput ClickHouse ingestion
	customTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       100,
	}

	worker := &IngestWorker{
		js: js,
		httpClient: &http.Client{
			Transport: customTransport,
			Timeout:   30 * time.Second,
		},
		cache:      cache,
		logger:     slog.Default().With("component", "ingest_worker"),
		target:     target,
		maxBatch:   defaultMaxBatch,
		maxWait:    defaultMaxWait,
		dlqEnabled: dlqEnabled,
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	worker.wg.Add(1)

	// Dispatch loop: one consumer, fanned out to a goroutine per table.
	go worker.dispatchLoop(workerCtx, cons)

	stopFunc := func(shutdownCtx context.Context) error {
		workerCancel()
		return waitOrDeadline(shutdownCtx, &worker.wg)
	}
	return stopFunc, nil
}

// waitOrDeadline returns nil once wg drains, or ctx.Err() if ctx fires first
// (sync.WaitGroup has no context-aware Wait). In-flight goroutines aren't cancelled.
func waitOrDeadline(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// dispatchLoop owns the single JetStream consumer and fans every message out to
// a per-table tableLoop (lazily spawned on first sight of a table). It does no
// batching itself — it parses just enough to route — so a low-volume table can
// never strand another table's rows behind a shared timer. It is the ONLY
// goroutine that watches ctx; tableLoops stop via channel-close, which gives a
// deterministic drain with no abandoned messages.
func (w *IngestWorker) dispatchLoop(ctx context.Context, cons jetstream.Consumer) {
	defer w.wg.Done()

	msgChan := make(chan jetstream.Msg, w.maxBatch*2)

	// Pull consumer with a push-like callback (nats.go prefetches pullMaxMessages).
	// Hand off to msgChan only, so the consume goroutine never blocks on flush work.
	consumeCtx, err := cons.Consume(func(msg jetstream.Msg) {
		msgChan <- msg
	}, jetstream.PullMaxMessages(pullMaxMessages))
	if err != nil {
		w.logger.Error("failed to start consumer", "error", err)
		return
	}
	defer consumeCtx.Stop()

	// flushCtx carries values (trace) but is never cancelled: a started flush must
	// finish so data already in ClickHouse gets acked rather than redelivered. It
	// is bounded by the HTTP client timeout; shutdown waits up to stopFunc's deadline.
	flushCtx := context.WithoutCancel(ctx)

	tableChans := make(map[string]chan parsedMsg)
	var tableWg sync.WaitGroup

	// shutdown drains in order: close every table channel so each tableLoop flushes
	// its remainder and exits (tableWg), then wait for the backgrounded acks (ackWg).
	// This order is what keeps the ack drain race-free: every ackWg.Add happens
	// either inside a tableLoop's lifetime OR on this goroutine (rejectPoison,
	// via parseMsg below), and this goroutine is the one that Waits — so no Add
	// can race the Wait on either path.
	shutdown := func() {
		for _, ch := range tableChans {
			close(ch)
		}
		tableWg.Wait()
		w.ackWg.Wait()
	}

	for {
		select {
		case <-ctx.Done():
			shutdown()
			return
		case m := <-msgChan:
			pm, ok := w.parseMsg(flushCtx, m)
			if !ok {
				continue // unreadable envelope: parked on the DLQ (or acked-and-dropped) in parseMsg
			}
			ch, exists := tableChans[pm.tableName]
			if !exists {
				ch = make(chan parsedMsg, w.maxBatch)
				tableChans[pm.tableName] = ch

				// TODO(#191): tableLoops are spawned per distinct table and never
				// reaped — they live for the process lifetime. Safe while table
				// names are bounded (schema-validated, in-process publishers only,
				// DontListen:true). Add idle-reaping + route/teardown coordination
				// before remote/untrusted publishers can create unbounded cardinality.
				table, in := pm.tableName, ch
				tableWg.Go(func() { w.tableLoop(flushCtx, table, in) })
			}
			// Route to the table's loop, but stay responsive to shutdown if its
			// channel is full (a busy tableLoop must not wedge teardown). A pm
			// dropped here is unacked and simply redelivered.
			select {
			case ch <- pm:
			case <-ctx.Done():
				shutdown()
				return
			}
		}
	}
}

// tableBatcher accumulates one table's rows and flushes them to ClickHouse.
//
// Concurrency: every method runs on a single goroutine (tableLoop), so the
// fields need no locking. The only thing that runs concurrently is the flush
// goroutine started in flushPending — and it operates on a private copy of the
// rows, never on these fields, so there is no shared mutable state.
//
// Coalescing: at most one flush runs per table at a time. A size or timer
// trigger that fires while a flush is in flight is deferred (flushQueued) and
// runs the moment the slot frees. A flush *completing* is not itself a trigger,
// so a partial batch left behind keeps waiting for its own size/timer.
type tableBatcher struct {
	w     *IngestWorker
	table string

	batch []parsedMsg
	timer *time.Timer

	// flushing is closed by the flush goroutine when it finishes, and is nil while
	// no flush is running. Because a receive on a nil channel blocks forever, the
	// "case <-b.flushing" arm in tableLoop is automatically inert while idle.
	flushing chan struct{}

	// flushQueued records that a size/timer trigger fired while a flush was in
	// flight, so the deferred flush runs as soon as that flush completes.
	flushQueued bool
}

func newTableBatcher(w *IngestWorker, table string) *tableBatcher {
	t := time.NewTimer(w.maxWait)
	t.Stop() // created disarmed; armed when the first row of a batch arrives
	return &tableBatcher{w: w, table: table, timer: t}
}

// add appends a row, arming the deadline timer on the first row of a batch and
// requesting a flush once the batch is full.
func (b *tableBatcher) add(ctx context.Context, pm parsedMsg) {
	if len(b.batch) == 0 {
		b.timer.Reset(b.w.maxWait)
	}
	b.batch = append(b.batch, pm)
	if len(b.batch) >= b.w.maxBatch {
		b.requestFlush(ctx)
	}
}

// requestFlush is the single entry point for the size (add) and timer triggers.
// If the table is idle it starts a flush immediately; if a flush is already
// running it records that another is due (flushQueued) so onFlushDone starts it
// when the slot frees. The rows/`b.batch = nil` split hands the goroutine a
// private snapshot that never aliases the live batch.
func (b *tableBatcher) requestFlush(ctx context.Context) {
	if len(b.batch) == 0 {
		return
	}
	if b.flushing != nil {
		b.flushQueued = true
		return
	}
	rows := b.batch
	b.batch = nil
	b.flushQueued = false
	b.timer.Stop() // this batch is leaving, so drop its deadline
	done := make(chan struct{})
	b.flushing = done
	go func() {
		defer close(done)
		b.w.flushTable(ctx, b.table, rows)
	}()
}

// onFlushDone frees the in-flight slot. It starts the next flush only if a
// trigger fired while the previous one was running (the batch overflowed past
// maxBatch, or its deadline elapsed). Otherwise a partial batch keeps waiting for
// its own size or timer trigger — a flush completing is not itself a trigger.
func (b *tableBatcher) onFlushDone(ctx context.Context) {
	b.flushing = nil
	if b.flushQueued {
		b.requestFlush(ctx)
	}
}

// drainAndExit waits for the in-flight flush, then flushes any leftover rows
// synchronously. Called when the input channel is closed (shutdown), so it
// flushes the remainder regardless of size/timer.
func (b *tableBatcher) drainAndExit(ctx context.Context) {
	if b.flushing != nil {
		<-b.flushing
	}
	if len(b.batch) > 0 {
		b.w.flushTable(ctx, b.table, b.batch)
	}
}

// tableLoop drives one table's batcher. Its select has exactly three arms: a new
// row arrives, the batch's deadline fires, or the in-flight flush finishes. It
// stops only when dispatchLoop closes `in` — buffered rows are received first
// (the receive reports closed only once the channel is empty), so the batch is
// fully drained before drainAndExit runs. It deliberately does not watch ctx:
// channel-close is the single stop signal, which avoids a select race that could
// abandon buffered rows.
func (w *IngestWorker) tableLoop(ctx context.Context, table string, in <-chan parsedMsg) {
	b := newTableBatcher(w, table)
	defer b.timer.Stop()

	for {
		select {
		case pm, ok := <-in:
			if !ok {
				b.drainAndExit(ctx)
				return
			}
			b.add(ctx, pm)
		case <-b.timer.C:
			b.requestFlush(ctx)
		case <-b.flushing:
			b.onFlushDone(ctx)
		}
	}
}

// parseMsg unmarshals one envelope into a parsedMsg. An envelope the worker can
// never insert is poison — malformed JSON, a row format it doesn't know (which
// is what a pre-v2 envelope looks like: it carries no `format` at all), or
// columns and a row it can't pair. Poison is parked on the DLQ rather than
// dropped, so an operator who skipped the documented pre-deploy drain finds
// those rows waiting instead of gone; when the DLQ is off for the table it is
// acked-and-dropped with a counted error, because a message that can never
// insert must not redeliver forever. ok is false either way so the caller skips it.
func (w *IngestWorker) parseMsg(ctx context.Context, m jetstream.Msg) (parsedMsg, bool) {
	var envelope EventMessage

	if err := json.Unmarshal(m.Data(), &envelope); err != nil {
		w.logger.ErrorContext(ctx, "failed to parse event envelope", "error", err)
		w.rejectPoison(ctx, m, "", "malformed", err.Error())
		return parsedMsg{}, false
	}
	if envelope.Format != FormatJSONCompactEachRow {
		w.logger.ErrorContext(ctx, "event envelope declares an unknown row format",
			"format", envelope.Format, "table", envelope.TableName)
		w.rejectPoison(ctx, m, envelope.TableName, "unknown_format",
			fmt.Sprintf("unknown row format %q (a pre-v2 envelope carries none); drain the ingest queue before upgrading", envelope.Format))
		return parsedMsg{}, false
	}
	if len(envelope.Columns) == 0 || len(envelope.Row) == 0 {
		w.logger.ErrorContext(ctx, "event envelope carries no columns or no row",
			"table", envelope.TableName, "columns", len(envelope.Columns))
		w.rejectPoison(ctx, m, envelope.TableName, "unpairable",
			"envelope carries no columns or no row, so its values cannot be mapped to columns")
		return parsedMsg{}, false
	}
	// Columns and row are only meaningful together, so check that they pair
	// rather than trusting the producer. Without this the mismatch reaches
	// ClickHouse and fails the INSERT — the row lands in the DLQ either way, but
	// via a batch failure and a row-by-row retry, and with a ClickHouse error
	// instead of one naming the real problem. The stream's pairRow makes the
	// same check; this is the ingest half of the contract AGENTS.md states.
	var cells []json.RawMessage
	if err := json.Unmarshal(envelope.Row, &cells); err != nil || len(cells) != len(envelope.Columns) {
		w.logger.ErrorContext(ctx, "event envelope row does not pair with its columns",
			"table", envelope.TableName, "columns", len(envelope.Columns), "error", err)
		w.rejectPoison(ctx, m, envelope.TableName, "unpairable",
			fmt.Sprintf("row does not pair with its %d column(s), so its values cannot be mapped to columns", len(envelope.Columns)))
		return parsedMsg{}, false
	}

	return parsedMsg{
		natsMsg:         m,
		natsSafeSubject: strings.TrimPrefix(m.Subject(), "ingest."),
		tableName:       envelope.TableName,
		scope:           envelope.Scope,
		columns:         envelope.Columns,
		colSig:          columnSignature(envelope.Columns),
		row:             envelope.Row,
	}, true
}

// flushTable inserts one table's batch into ClickHouse, then (on success) kicks
// off cache invalidation + backgrounded acks via handleSuccess. On bulk failure
// it falls back to 1-by-1 isolation: each row that re-inserts cleanly is acked,
// each that fails again is sent to the DLQ — or, with the DLQ switched off for
// the table, left unacked so NATS redelivers it (the row is never dropped, it
// retries until it inserts or the DLQ is switched on). tableLoop guarantees at
// most one concurrent flushTable per table; different tables may flush
// concurrently.
func (w *IngestWorker) flushTable(ctx context.Context, tableName string, msgs []parsedMsg) {
	if len(msgs) == 0 {
		return
	}

	// One INSERT per distinct column list. The row is positional, so rows
	// written under different column lists — a schema change mid-stream —
	// cannot share a statement. In steady state a table has exactly one
	// signature and this is a single group.
	for _, group := range groupByColumns(msgs) {
		w.flushGroup(ctx, tableName, group)
	}
}

// groupByColumns splits a table's batch by column list. Arrival order is
// preserved WITHIN each group; the groups themselves are ordered by where their
// column list first appeared, so a row can be inserted before an earlier row
// that used a different list. Returns the input as a single group in the common
// case where every row agrees, which is the only case where the batch's arrival
// order survives end to end.
func groupByColumns(msgs []parsedMsg) [][]parsedMsg {
	groups := make([][]parsedMsg, 0, 1)
	index := make(map[string]int, 1)
	for _, pm := range msgs {
		i, ok := index[pm.colSig]
		if !ok {
			index[pm.colSig] = len(groups)
			groups = append(groups, []parsedMsg{pm})
			continue
		}
		groups[i] = append(groups[i], pm)
	}
	return groups
}

// flushGroup inserts one (table, column list) batch, falling back to row-by-row
// isolation on failure. Every message in group shares a column signature, so the
// first one's columns describe them all.
func (w *IngestWorker) flushGroup(ctx context.Context, tableName string, group []parsedMsg) {
	cols := group[0].columns

	err := w.insertToClickHouse(ctx, tableName, cols, group)
	if err == nil {
		w.handleSuccess(ctx, tableName, group)
		return
	}

	w.logger.WarnContext(ctx, "bulk insert failed, falling back to 1-by-1 isolation", "table", tableName, "error", err)

	// ISOLATE & DLQ: re-insert one row at a time so a single poison row can't
	// sink the whole batch.
	// TODO: potentially could try a binary search or something eventually maybe? unclear if faster...
	for _, pm := range group {
		singleErr := w.insertToClickHouse(ctx, tableName, cols, []parsedMsg{pm})
		if singleErr != nil {
			if w.dlqEnabled != nil && !w.dlqEnabled(tableName) {
				w.logger.ErrorContext(ctx, "isolated bad row, DLQ disabled for table — left unacked, NATS will redeliver it until it inserts or dlq is enabled", "table", tableName, "error", singleErr)
				continue
			}
			w.logger.ErrorContext(ctx, "isolated bad row, sending to DLQ", "table", tableName, "error", singleErr)
			w.sendToDLQ(ctx, tableName, pm, singleErr.Error())
		} else {
			w.handleSuccess(ctx, tableName, []parsedMsg{pm})
		}
	}
}

// insertToClickHouse writes one group as a single INSERT naming columns
// explicitly, so the positional rows land in the right slots. Callers group by
// column list first: a batch spanning two lists cannot share one statement.
func (w *IngestWorker) insertToClickHouse(ctx context.Context, tableName string, columns []string, msgs []parsedMsg) error {
	var buf bytes.Buffer
	for _, m := range msgs {
		buf.Write(m.row)
		buf.WriteString("\n")
	}

	// The row is positional, so the statement must name the columns in the same
	// order the envelope did. The table still binds as a server-side Identifier
	// parameter; a column LIST has no such parameter, so each name is quoted
	// client-side by the one helper that renders an identifier as SQL.
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = chsql.QuoteIdent(c)
	}

	t := w.target()
	q := url.Values{}
	q.Set("database", t.Database)
	q.Set("param_target_table", tableName)
	q.Set("query", fmt.Sprintf("INSERT INTO {target_table:Identifier} (%s) FORMAT JSONCompactEachRow", strings.Join(quoted, ", ")))
	q.Set("date_time_input_format", "best_effort")
	// A field the record omitted rides as an explicit null in its column's slot,
	// because a positional row has one value per column and no way to say
	// "absent". For a NON-nullable column with a default this setting turns that
	// null back into the default, matching what omitting the key did under
	// JSONEachRow. It is already the server default (verified on 26.6.3), so
	// this is belt-and-braces for a server configured otherwise.
	//
	// TRANSITIONAL DIVERGENCE, and it is NOT what this setting controls: on a
	// NULLABLE column an explicit null is stored as NULL whatever the setting
	// says — only an ABSENT key ever took the default. So a `Nullable(T) DEFAULT
	// …` column now stores NULL where it previously took its default. Verified
	// on 26.6.3: omitted key → default; explicit null → NULL at both settings.
	q.Set("input_format_null_as_default", "1")

	req, err := http.NewRequestWithContext(ctx, "POST", t.URL+"?"+q.Encode(), &buf)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", t.Username)
	req.Header.Set("X-ClickHouse-Key", t.Password)

	// TODO: future optimization: could build list for cache invalidation here while waiting on the network request
	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (w *IngestWorker) handleSuccess(ctx context.Context, tableName string, msgs []parsedMsg) {
	// Build the minimal set of namespaces to invalidate. Every msg here is for
	// tableName, so a single scopeless write bumps the whole table — which subsumes
	// every scope — and there's nothing more to add. Otherwise invalidate each
	// distinct scope. Doing this here (we already loop the batch once, and know it's
	// one table) keeps Cache.Invalidate a simple one-pass bump.
	encodedTable := query.SafeEncodeNATS(tableName)
	seenScopes := make(map[string]struct{}, len(msgs))
	namespaces := make([]cache.Namespace, 0, len(msgs))

	for _, pm := range msgs {
		if pm.scope == "" {
			namespaces = []cache.Namespace{{Table: encodedTable}}
			break
		}
		if _, exists := seenScopes[pm.scope]; exists {
			continue
		}
		seenScopes[pm.scope] = struct{}{}
		namespaces = append(namespaces, cache.Namespace{
			Table: encodedTable,
			Scope: query.SafeEncodeNATS(pm.scope),
		})
	}

	if len(namespaces) > 0 {
		invCtx := trace.ContextWithSpanContext(context.WithoutCancel(ctx), trace.SpanContextFromContext(ctx))
		_, err := w.cache.Invalidate(invCtx, namespaces)
		if err != nil {
			w.logger.ErrorContext(invCtx, "failed to invalidate cache after insert - your cache is holding stale data now!", "table", tableName, "error", err)
		}
	}

	// Ack in the background, tracked on ackWg. DoubleAck is fsync-bound
	// (SyncAlways) and slow, so it must stay off the insert path. dispatchLoop
	// drains ackWg only after every tableLoop has returned, so each Add here
	// happens-before that Wait — no race, no reliance on synchronous flushing.
	w.ackWg.Go(func() {
		var acks sync.WaitGroup
		for _, pm := range msgs {
			acks.Go(func() {
				if err := pm.natsMsg.DoubleAck(context.WithoutCancel(ctx)); err != nil {
					w.logger.ErrorContext(context.WithoutCancel(ctx), "double ack failed for processed message", "error", err, "table", tableName)
				}
			})
		}
		acks.Wait()
	})
}

// rejectPoison disposes of a message the worker can never insert. The DLQ is
// preferred — the row is preserved for inspection and replay — and dropping is
// the fallback when the DLQ is off for the table, since redelivering a message
// that can never succeed would wedge the consumer behind it forever. A DLQ
// publish that FAILS leaves the message unacked, exactly as the isolation path
// does: that is a transient DLQ outage, and retrying beats destroying the row.
func (w *IngestWorker) rejectPoison(ctx context.Context, m jetstream.Msg, tableName, reason, detail string) {
	if w.dlqEnabled == nil || w.dlqEnabled(tableName) {
		// Backgrounded on ackWg for the same reason handleSuccess backgrounds its
		// acks: parkOnDLQ does a JetStream publish AND an fsync-bound DoubleAck,
		// and parseMsg runs on the dispatchLoop goroutine. The scenario this whole
		// change targets is an operator who skipped the drain, where EVERY backlog
		// message is poison — done inline that is one publish plus one fsync per
		// message in series, with intake stalled behind it. dispatchLoop adds and
		// waits on the same goroutine, so each Add still happens-before the Wait.
		subject := strings.TrimPrefix(m.Subject(), "ingest.")
		w.ackWg.Go(func() { w.parkOnDLQ(ctx, m, subject, tableName, detail) })
		return
	}
	poisonDroppedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("table", tableName),
		attribute.String("reason", reason),
	))
	w.logger.ErrorContext(ctx, "unreadable envelope dropped — the DLQ is disabled for this table, and a message that can never insert must not redeliver forever",
		"table", tableName, "reason", reason, "detail", detail)
	w.ackWg.Go(func() { _ = m.DoubleAck(ctx) })
}

// sendToDLQ parks a row that failed its own isolated INSERT. Distinct from
// rejectPoison, which parks an envelope the worker could not read at all.
func (w *IngestWorker) sendToDLQ(ctx context.Context, tableName string, pm parsedMsg, errMsg string) {
	w.parkOnDLQ(ctx, pm.natsMsg, pm.natsSafeSubject, tableName, errMsg)
}

// parkOnDLQ republishes one message on its dlq.* subject with the failure
// context in headers, then acks the original so NATS stops redelivering it. A
// failed publish deliberately leaves the original unacked.
func (w *IngestWorker) parkOnDLQ(ctx context.Context, natsMsg jetstream.Msg, safeSubject, tableName, errMsg string) {
	subject := "dlq." + safeSubject

	msg := nats.NewMsg(subject)
	msg.Data = natsMsg.Data()
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	msg.Header.Set("X-DLQ-Table", tableName)
	msg.Header.Set("X-DLQ-Error", errMsg)
	msg.Header.Set("X-DLQ-Timestamp", time.Now().UTC().Format(time.RFC3339))

	_, pubErr := w.js.PublishMsg(ctx, msg)
	if pubErr != nil {
		w.logger.ErrorContext(ctx, "NATS DLQ publish failed, this data will continue retrying insertion indefinitely until the DLQ recovers", "table", tableName, "subject", subject, "error", pubErr)
		return
	}

	// DoubleAck original message so NATS doesn't redeliver the corrupt data
	_ = natsMsg.DoubleAck(ctx)
}
