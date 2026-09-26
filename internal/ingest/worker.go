package ingest

import (
	"bytes"
	"context"
	"crypto/tls"
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
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Queue is what the worker needs from the MQ: a durable consumer on the
// ingest queue and somewhere to park what it cannot write. mq.Broker
// satisfies it.
type Queue interface {
	mq.ConsumerManager
	mq.DeadLetterer
}

type parsedMsg struct {
	msg       *mq.Message
	tenant    tenant.ID // whose table it is: the topic the message arrived on names it (#583)
	tableName string    // with tenant, the routing key for per-tenant-table batching; raw (unencoded) name
	scope     string
	columns   []string        // envelope column names, in declaration order
	colSig    string          // columns joined; the within-table batch key
	row       json.RawMessage // one JSONCompactEachRow line, no trailing newline
}

// columnSignature renders a column list as a map key. The separator is a byte
// that cannot appear in a ClickHouse identifier's UTF-8 encoding, so no pair of
// distinct column lists can collide on it.
func columnSignature(cols []string) string {
	return strings.Join(cols, "\x00")
}

type IngestWorker struct {
	dlq mq.DeadLetterer
	// failed carries the one error that ends the worker on its own — the
	// consumer could not start, or delivery ended underneath it. Buffered so
	// the dispatch loop never blocks on a caller that has already gone.
	failed chan error
	// clients follows the target's TLS config: one client per config,
	// replaced when a reload changes it (chconn.HTTPClients).
	clients *chconn.HTTPClients
	cache   cache.Cache
	// target resolves a tenant's ClickHouse HTTP wiring per insert
	// (chconn.Pools.Target in production) so a settings reload that
	// re-points the tenant applies to the next flush; the zero Target is a
	// tenant on no pool, whose batch goes to the dead-letter decision whole
	// (parkBatch).
	target   func(tenant.ID) chconn.Target
	maxBatch int
	maxWait  time.Duration
	// dlqEnabled reports, per tenant table, whether a row that still fails
	// after row-by-row isolation — or every row of a batch with no ClickHouse
	// connection — is parked on the DLQ (settings.Store.DLQFor in production;
	// nil means always). Resolved at the moment of the failure
	// under the row's own tenant — the one its topic names — so a settings
	// reload applies to the next poison row without a restart.
	dlqEnabled func(id tenant.ID, table string) bool

	// backoffs holds one retry backoff per ClickHouse pool: a batch that
	// meets an unavailable ClickHouse is handed back to the MQ for a delayed
	// redelivery, and every table on that pool waits out the same backoff.
	backoffs backoffs
	// now reads the clock for the backoffs; nil is time.Now (tests set it).
	now func() time.Time

	// wg tracks the dispatch loop; ackWg tracks backgrounded DoubleAck goroutines.
	// Separate so shutdown can drain inserts (wg → tableWg) before waiting on the
	// fsync-bound acks, without an ackWg.Add racing its Wait — see dispatchLoop.
	wg    sync.WaitGroup
	ackWg sync.WaitGroup
}

// poisonCounter counts envelopes the worker could not read, by what became of
// each: disposition="parked" was republished to the DLQ and is recoverable,
// disposition="dropped" was acked and discarded because the DLQ is switched off
// for the table and is a row that no longer exists anywhere.
//
// Both dispositions are counted because a non-zero rate right after an upgrade
// means the ingest queue was not drained first, and that is true whichever way
// the switch was set — an operator watching for a missed drain should not have
// to know the table's DLQ setting to see it.
//
// Counted once the envelope is actually parked or actually acked, never once it
// is merely rejected: a DLQ outage and a failed ack both leave the message in
// the stream, unacked and due for redelivery. Counting at rejection would score
// the same envelope again on every retry, and would report it as parked or as
// gone for good while it was still sitting in the queue.
var poisonCounter, _ = otel.Meter("wavehouse-ingest").Int64Counter(
	"wavehouse_ingest_poison_total",
	metric.WithDescription("Ingest envelopes the worker could not read, by disposition: parked on the DLQ, or acked and dropped where the DLQ is disabled for the table"),
)

// retryCounter counts rows handed back to the MQ for a delayed retry because
// ClickHouse could not take them, by reason: the chconn.Class of the failure
// (unavailable, denied, unknown), or backoff for rows turned away without a
// try while their pool was backing off. A sustained rate is an outage that
// is holding rows in the queue — none of them reach the DLQ.
var retryCounter, _ = otel.Meter("wavehouse-ingest").Int64Counter(
	"wavehouse_ingest_retries_total",
	metric.WithDescription("Rows handed back to the ingest queue for a delayed retry because ClickHouse could not take them, by reason"),
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
	// Server-side cap on a tenant's unacked messages; suspends that tenant's
	// delivery when hit (backpressure), and no other tenant's. The worker holds
	// every delivered row until its batch is acked, so while ClickHouse stalls
	// it can hold up to maxAckPending rows per tenant: the in-memory bound
	// grows with the tenants served.
	maxAckPending = 10_000 // TODO: raise if NATS delivery becomes the bottleneck

	// Client prefetch buffer in front of msgChan (was the implicit jetstream
	// default), shared by the tenants' queues (mq.Consumer.Consume).
	pullMaxMessages = 500

	// Redelivery timeout. 60s ≈ 5s batch + ~30s HTTP timeout + margin.
	ackWait = 60 * time.Second
)

// StartIngestWorker starts the batch consumer. stop drains it under the given
// deadline. failed receives at most one error, if the worker ends on its own:
// ingestion has stopped and nothing inside the worker can bring it back, so
// the caller must treat it as fatal (internal/app returns it from Run, which
// stops the process — the next boot recreates the consumer). The worker has
// already flushed and acked what it held by the time failed fires; stop is
// still the caller's to call.
func StartIngestWorker(
	ctx context.Context, queue Queue, cache cache.Cache,
	target func(tenant.ID) chconn.Target,
	dlqEnabled func(id tenant.ID, table string) bool,
) (stop func(context.Context) error, failed <-chan error, err error) {
	if queue == nil {
		return nil, nil, fmt.Errorf("message queue is nil")
	}
	if cache == nil {
		return nil, nil, fmt.Errorf("cache is nil")
	}
	if target == nil {
		return nil, nil, fmt.Errorf("clickhouse target is nil")
	}

	cons, err := queue.CreateConsumer(ctx, mq.ConsumerConfig{
		Durable:       BufferConsumerName,
		AckWait:       ackWait,
		MaxAckPending: maxAckPending,
	})
	if err != nil {
		return nil, nil, err
	}

	worker := &IngestWorker{
		dlq:        queue,
		failed:     make(chan error, 1),
		clients:    chconn.NewHTTPClients(ingestHTTPClient),
		cache:      cache,
		target:     target,
		maxBatch:   defaultMaxBatch,
		maxWait:    defaultMaxWait,
		dlqEnabled: dlqEnabled,
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	worker.wg.Add(1)

	// Dispatch loop: one consumer, fanned out to a goroutine per tenant table.
	go worker.dispatchLoop(workerCtx, cons)

	stopFunc := func(shutdownCtx context.Context) error {
		workerCancel()
		return waitOrDeadline(shutdownCtx, &worker.wg)
	}
	return stopFunc, worker.failed, nil
}

// ingestHTTPClient is the worker's client for the ClickHouse HTTP
// interface: a transport tuned for high-throughput inserts, with the
// target's TLS config for an https target.
func ingestHTTPClient(tlsCfg *tls.Config) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		TLSClientConfig:       tlsCfg,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       100,
	}
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}
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

// dispatchLoop owns the one consumer — held on every tenant's queue — and fans
// every message out to a tableLoop per tenant table (lazily spawned on first
// sight of one). It does no batching itself — it parses just enough to route —
// so a low-volume table can never strand another table's rows behind a shared
// timer. It is the ONLY goroutine that watches ctx; tableLoops stop via
// channel-close, which gives a deterministic drain with no abandoned messages.
func (w *IngestWorker) dispatchLoop(ctx context.Context, cons mq.Consumer) {
	defer w.wg.Done()

	msgChan := make(chan *mq.Message, w.maxBatch*2)

	// Pull consumer with a push-like callback (the client prefetches pullMaxMessages,
	// shared by the tenants' queues). It runs on one delivery goroutine per tenant,
	// so the handoff is a channel send, safe from all of them at once. Hand off to
	// msgChan only, so a consume goroutine never blocks on flush work.
	// The handoff also watches ctx: stop (deferred below) does not wait for a
	// delivery already in the handler, so once this loop has stopped draining
	// msgChan a full channel would otherwise pin a delivery goroutine
	// forever. A message dropped here is unacked and simply redelivered.
	stop, deliveryEnded, err := cons.Consume(func(msg *mq.Message) {
		select {
		case msgChan <- msg:
		case <-ctx.Done():
		}
	}, pullMaxMessages)
	if err != nil {
		slog.ErrorContext(ctx, "failed to start consumer", "error", err)
		w.failed <- fmt.Errorf("ingest worker: start consumer: %w", err)
		return
	}
	defer stop()

	// flushCtx carries values (trace) but is never cancelled: a started flush must
	// finish so data already in ClickHouse gets acked rather than redelivered. It
	// is bounded by the HTTP client timeout; shutdown waits up to stopFunc's deadline.
	flushCtx := context.WithoutCancel(ctx)

	// One loop per tenant table (#583): the dead-letter switch, and in turn
	// the ClickHouse target and the cache namespace (stories 6 and 8), are
	// each resolved for one tenant, so a batch never mixes two — two tenants'
	// tables of one name are two batches.
	type batchKey struct {
		tenant tenant.ID
		table  string
	}
	tableChans := make(map[batchKey]chan parsedMsg)
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
		case err := <-deliveryEnded:
			// The MQ gave up on the consumer: no message will arrive again, so
			// waiting on msgChan would stall ingestion silently. Flush and ack
			// what is already in hand — those rows are delivered and the
			// publish path is not what broke — then fail loud. Messages still
			// in msgChan are unacked and redelivered to the next consumer.
			slog.ErrorContext(ctx, "ingest consumer delivery ended; ingestion has stopped", "error", err)
			shutdown()
			w.failed <- fmt.Errorf("ingest worker: %w", err)
			return
		case m := <-msgChan:
			pm, ok := w.parseMsg(flushCtx, m)
			if !ok {
				continue // unreadable envelope: parked on the DLQ (or acked-and-dropped) in parseMsg
			}
			key := batchKey{tenant: pm.tenant, table: pm.tableName}
			ch, exists := tableChans[key]
			if !exists {
				ch = make(chan parsedMsg, w.maxBatch)
				tableChans[key] = ch

				// TODO(#263): tableLoops are spawned per distinct tenant table and
				// never reaped — they live for the process lifetime. Safe while
				// tenants and table names are bounded (a settings folder per
				// tenant, schema-validated tables, in-process publishers only,
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
// Coalescing: at most one flush runs per tenant table at a time. A size or timer
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
// requesting a flush once the batch is full. A row whose ClickHouse pool is
// inside a backoff window is handed straight back to the MQ instead: holding
// it here would only pin it in memory until a flush that is certain to hand
// it back, so the backlog of an outage stays in the queue, not in the worker.
func (b *tableBatcher) add(ctx context.Context, pm parsedMsg) {
	if wait, ok := b.w.backoffs.waiting(func() chconn.Target { return b.w.target(pm.tenant) }, b.table, b.w.clock()); ok {
		b.w.retryLater(ctx, b.table, []parsedMsg{pm}, wait, "backoff")
		return
	}
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

// firstDuplicate returns the first column name that appears twice, if any.
// ClickHouse rejects a duplicate in an INSERT column list outright (code 15,
// DUPLICATE_COLUMN), so the batch would fail loudly here — but the same
// envelope also reaches the SSE fan-out, where a repeated name silently
// resolves to one value. Refusing it once, at the parse boundary, keeps both
// consumers honest.
func firstDuplicate(cols []string) (string, bool) {
	seen := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		if _, ok := seen[c]; ok {
			return c, true
		}
		seen[c] = struct{}{}
	}
	return "", false
}

// parseMsg unmarshals one envelope into a parsedMsg. An envelope the worker can
// never insert is poison — malformed JSON, a row format it doesn't know, or
// columns and a row it can't pair. Poison is parked on the DLQ rather than
// dropped, so an operator finds those rows waiting instead of gone; when the
// DLQ is off for the table it is acked-and-dropped with a counted error,
// because a message that can never insert must not redeliver forever. ok is
// false either way so the caller skips it.
func (w *IngestWorker) parseMsg(ctx context.Context, m *mq.Message) (parsedMsg, bool) {
	var envelope EventMessage

	// The tenant is the subject's, never the envelope's: an envelope the
	// worker cannot read still has one to be parked under.
	id := m.Topic().Tenant
	if err := json.Unmarshal(m.Data, &envelope); err != nil {
		slog.ErrorContext(ctx, "failed to parse event envelope", "tenant", id, "error", err)
		w.rejectPoison(ctx, m, id, "", "malformed", err.Error())
		return parsedMsg{}, false
	}
	if envelope.Format != FormatJSONCompactEachRow {
		slog.ErrorContext(ctx, "event envelope declares an unknown row format",
			"format", envelope.Format, "tenant", id, "table", envelope.TableName)
		w.rejectPoison(ctx, m, id, envelope.TableName, "unknown_format",
			fmt.Sprintf("unknown row format %q", envelope.Format))
		return parsedMsg{}, false
	}
	if len(envelope.Columns) == 0 || len(envelope.Row) == 0 {
		slog.ErrorContext(ctx, "event envelope carries no columns or no row",
			"tenant", id, "table", envelope.TableName, "columns", len(envelope.Columns))
		w.rejectPoison(ctx, m, id, envelope.TableName, "unpairable",
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
	if dup, ok := firstDuplicate(envelope.Columns); ok {
		slog.ErrorContext(ctx, "unreadable envelope: a column name repeats",
			"tenant", id, "table", envelope.TableName, "column", dup)
		w.rejectPoison(ctx, m, id, envelope.TableName, "unpairable",
			fmt.Sprintf("column %q appears more than once, so its values cannot be mapped to columns", dup))
		return parsedMsg{}, false
	}
	if err := json.Unmarshal(envelope.Row, &cells); err != nil || len(cells) != len(envelope.Columns) {
		slog.ErrorContext(ctx, "event envelope row does not pair with its columns",
			"tenant", id, "table", envelope.TableName, "columns", len(envelope.Columns), "error", err)
		w.rejectPoison(ctx, m, id, envelope.TableName, "unpairable",
			fmt.Sprintf("row does not pair with its %d column(s), so its values cannot be mapped to columns", len(envelope.Columns)))
		return parsedMsg{}, false
	}

	return parsedMsg{
		msg:       m,
		tenant:    id,
		tableName: envelope.TableName,
		scope:     envelope.Scope,
		columns:   envelope.Columns,
		colSig:    columnSignature(envelope.Columns),
		row:       envelope.Row,
	}, true
}

// flushTable inserts one table's batch into ClickHouse, then (on success) kicks
// off cache invalidation + backgrounded acks via handleSuccess. On bulk failure
// it asks what the failure was (chconn.Classify):
//
//   - ClickHouse REJECTED the batch — it read it and refused something in it:
//     row-by-row isolation. Each row that re-inserts cleanly is acked, each
//     that is rejected again goes to the DLQ — or, with the DLQ switched off for
//     the table, is left unacked so NATS redelivers it.
//   - The batch failed in a way a smaller insert may avoid (chconn.Splittable:
//     too many partitions for one INSERT, the memory limit): the same
//     isolation. If the first row fails the same way, the server was the
//     problem after all, and isolation stops there as below.
//   - Anything else — ClickHouse down, unreachable, overloaded, read-only,
//     refusing the credentials, or a failure with no verdict at all: nothing
//     in the batch was judged, so isolating it would only multiply the
//     requests, and dead-lettering it would park good rows. The batch is
//     handed back to the MQ with a delayed redelivery (retryLater), and the
//     pool backs off (backoff), so every table on a down ClickHouse waits
//     together. The same applies to a failure that lands mid-isolation: the
//     rows not yet settled go back, none to the DLQ.
//
// A batch whose tenant has no ClickHouse connection is not tried at all: it
// meets its DLQ switch once, whole, whatever its column lists (parkBatch).
// tableLoop guarantees at most one concurrent flushTable per tenant table;
// different tables — two tenants' tables of one name included — may flush
// concurrently.
func (w *IngestWorker) flushTable(ctx context.Context, tableName string, msgs []parsedMsg) {
	if len(msgs) == 0 {
		return
	}
	id := msgs[0].tenant
	t := w.target(id)
	if t.URL == "" {
		w.parkBatch(ctx, tableName, msgs, noTargetError(id))
		return
	}

	// The table's own backoff first, so a table turned away never claims
	// the pool's probe; a pool that turns it away returns the table's.
	pool, table := w.backoffs.forTarget(t), w.backoffs.forTable(t, tableName)
	wait, ok := table.allow(w.clock())
	if ok {
		if wait, ok = pool.allow(w.clock()); !ok {
			table.release()
		}
	}
	if !ok {
		w.retryLater(ctx, tableName, msgs, wait, "backoff")
		return
	}

	// One INSERT per distinct column list. The row is positional, so rows
	// written under different column lists — a schema change mid-stream —
	// cannot share a statement. In steady state a table has exactly one
	// signature and this is a single group.
	groups := groupByColumns(msgs)
	for i, group := range groups {
		unsettled, err := w.flushGroup(ctx, tableName, group)
		if err == nil {
			continue
		}
		for _, later := range groups[i+1:] {
			unsettled = append(unsettled, later...)
		}
		class := chconn.Classify(err)
		failed, scope := pool, "ClickHouse"
		if chconn.TableScoped(err) {
			// The server answered for the table alone: the pool is up.
			failed, scope = table, "the table"
			w.closeBackoff(ctx, pool, id, tableName, t.URL, "ClickHouse")
		} else {
			table.release()
		}
		wait, first, log := failed.fail(w.clock())
		if log {
			msg := scope + " cannot take inserts, retrying with backoff; no row goes to the DLQ"
			if !first {
				msg = scope + " still cannot take inserts, retrying with backoff"
			}
			slog.WarnContext(ctx, msg, "tenant", id, "table", tableName, "clickhouse", t.URL,
				"class", class.String(), "retry_in", wait, "error", err)
		}
		w.retryLater(ctx, tableName, unsettled, wait, class.String())
		return
	}
	w.closeBackoff(ctx, pool, id, tableName, t.URL, "ClickHouse")
	w.closeBackoff(ctx, table, id, tableName, t.URL, "the table")
}

// closeBackoff closes bo after an answer that was not an outage, logging the
// recovery when it was open.
func (w *IngestWorker) closeBackoff(ctx context.Context, bo *backoff, id tenant.ID, tableName, url, scope string) {
	if recovered, lasted := bo.succeed(w.clock()); recovered {
		slog.InfoContext(ctx, scope+" is taking inserts again", "tenant", id, "table", tableName,
			"clickhouse", url, "outage", lasted)
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
// isolation when ClickHouse rejects it, or refuses it in a way a smaller insert
// may avoid (chconn.Splittable). Every message in group shares a column
// signature, so the first one's columns describe them all.
//
// err is non-nil when ClickHouse could not take a request — the bulk insert or
// any isolated row (see flushTable) — and unsettled is then every row not yet
// acked, dead-lettered or left for redelivery: the whole group, or what
// isolation had not reached. A rejected row is settled; it never makes err.
func (w *IngestWorker) flushGroup(ctx context.Context, tableName string, group []parsedMsg) (unsettled []parsedMsg, err error) {
	cols := group[0].columns

	err = w.insertToClickHouse(ctx, tableName, cols, group)
	if err == nil {
		w.handleSuccess(ctx, tableName, group)
		return nil, nil
	}
	if chconn.Classify(err) != chconn.Rejected && (len(group) == 1 || !chconn.Splittable(err)) {
		return group, err
	}

	slog.WarnContext(ctx, "bulk insert failed, falling back to 1-by-1 isolation", "tenant", group[0].tenant, "table", tableName, "error", err)

	// ISOLATE & DLQ: re-insert one row at a time so a single poison row can't
	// sink the whole batch.
	// TODO: potentially could try a binary search or something eventually maybe? unclear if faster...
	for i, pm := range group {
		singleErr := w.insertToClickHouse(ctx, tableName, cols, []parsedMsg{pm})
		switch {
		case singleErr == nil:
			w.handleSuccess(ctx, tableName, []parsedMsg{pm})
		case chconn.Classify(singleErr) != chconn.Rejected:
			// ClickHouse cannot take this row now — it went away mid-isolation,
			// or a split batch's failure was the server's after all: this row
			// and the rest were never judged, so none of them is dead-lettered.
			return group[i:], singleErr
		case w.dlqEnabled != nil && !w.dlqEnabled(pm.tenant, tableName):
			slog.ErrorContext(ctx, "isolated bad row, DLQ disabled for table — left unacked, NATS will redeliver it until it inserts or dlq is enabled", "tenant", pm.tenant, "table", tableName, "error", singleErr)
		default:
			slog.ErrorContext(ctx, "isolated bad row, sending to DLQ", "tenant", pm.tenant, "table", tableName, "error", singleErr)
			w.sendToDLQ(ctx, tableName, pm, singleErr.Error())
		}
	}
	return nil, nil
}

func (w *IngestWorker) clock() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

// retryLater hands rows ClickHouse could not take back to the MQ, to be
// redelivered no sooner than wait. They are never dead-lettered: nothing in
// them was judged. A nak that fails leaves the row unacked, so the MQ
// redelivers it after the ack wait anyway. reason labels the retry counter.
func (w *IngestWorker) retryLater(ctx context.Context, tableName string, msgs []parsedMsg, wait time.Duration, reason string) {
	for _, pm := range msgs {
		if err := pm.msg.NakWithDelay(wait); err != nil {
			slog.ErrorContext(ctx, "delayed nak failed; the row is redelivered after the ack wait instead", "tenant", pm.tenant, "table", tableName, "error", err)
		}
	}
	retryCounter.Add(ctx, int64(len(msgs)), metric.WithAttributes(
		attribute.String("table", tableName),
		attribute.String("reason", reason),
	))
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

	// A group is one (tenant, table) batch's, so its first row names the
	// tenant whose ClickHouse takes the insert.
	id := msgs[0].tenant
	t := w.target(id)
	if t.URL == "" {
		return noTargetError(id)
	}
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
	// The configured headers first, so WaveHouse's own win over a
	// same-named one.
	for name, value := range t.Headers {
		req.Header.Set(name, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", t.Username)
	req.Header.Set("X-ClickHouse-Key", t.Password)

	// TODO: future optimization: could build list for cache invalidation here while waiting on the network request
	resp, err := w.clients.For(t).Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		return chconn.NewHTTPError(resp)
	}
	return nil
}

// noTargetError is the failure of tenant id's inserts while it is on no pool:
// no longer served, or refused one, such as by the connection ceiling. No
// request is made.
func noTargetError(id tenant.ID) error {
	return fmt.Errorf("no ClickHouse connection is open for tenant %s", id)
}

// parkBatch disposes of a batch whose tenant has no ClickHouse connection in
// one pass: row-by-row isolation would fail every row the same way, logging
// two lines each. The tenant's DLQ switch is asked once for the batch, which
// is parked under its own topic — a tenant no longer served reads as on
// (dlqFor in internal/app) — or, switched off, left unacked for redelivery
// until the tenant has a pool.
func (w *IngestWorker) parkBatch(ctx context.Context, tableName string, msgs []parsedMsg, cause error) {
	id := msgs[0].tenant
	if w.dlqEnabled != nil && !w.dlqEnabled(id, tableName) {
		slog.ErrorContext(ctx, "no ClickHouse connection for tenant, DLQ disabled for table — batch left unacked, NATS will redeliver it until the tenant has one or dlq is enabled", "tenant", id, "table", tableName, "rows", len(msgs))
		return
	}
	slog.ErrorContext(ctx, "no ClickHouse connection for tenant, parking the batch on the DLQ", "tenant", id, "table", tableName, "rows", len(msgs))
	for _, pm := range msgs {
		w.sendToDLQ(ctx, tableName, pm, cause.Error())
	}
}

func (w *IngestWorker) handleSuccess(ctx context.Context, tableName string, msgs []parsedMsg) {
	if len(msgs) == 0 {
		return
	}
	// A batch is one tenant's (dispatchLoop keys its loops by tenant table),
	// so the first row names whose namespaces the insert changed.
	w.invalidate(ctx, msgs[0].tenant, tableName, msgs)

	// Ack in the background, tracked on ackWg. DoubleAck is fsync-bound
	// (SyncAlways) and slow, so it must stay off the insert path. dispatchLoop
	// drains ackWg only after every tableLoop has returned, so each Add here
	// happens-before that Wait — no race, no reliance on synchronous flushing.
	w.ackWg.Go(func() {
		var acks sync.WaitGroup
		for _, pm := range msgs {
			acks.Go(func() {
				if err := pm.msg.DoubleAck(context.WithoutCancel(ctx)); err != nil {
					slog.ErrorContext(context.WithoutCancel(ctx), "double ack failed for processed message", "error", err, "tenant", pm.tenant, "table", tableName)
				}
			})
		}
		acks.Wait()
	})
}

// invalidate bumps the cache namespaces a batch of inserts into tableName
// changed, for tenant id: the namespaces lead with the tenant (#583 story 8),
// so the same table under another tenant keeps its cached results. id is the
// batch's tenant, read off each message's topic (story 5).
//
// The set is the minimal one. Every msg here is for tableName, so a single
// scopeless write bumps the whole table — which subsumes every scope — and
// there's nothing more to add. Otherwise invalidate each distinct scope.
// Doing this here (we already loop the batch once, and know it's one table)
// keeps Cache.Invalidate a simple one-pass bump.
func (w *IngestWorker) invalidate(ctx context.Context, id tenant.ID, tableName string, msgs []parsedMsg) {
	encodedTable := query.SafeEncodeToken(tableName)
	seenScopes := make(map[string]struct{}, len(msgs))
	namespaces := make([]cache.Namespace, 0, len(msgs))

	for _, pm := range msgs {
		if pm.scope == "" {
			namespaces = []cache.Namespace{{Tenant: id, Table: encodedTable}}
			break
		}
		if _, exists := seenScopes[pm.scope]; exists {
			continue
		}
		seenScopes[pm.scope] = struct{}{}
		namespaces = append(namespaces, cache.Namespace{
			Tenant: id,
			Table:  encodedTable,
			Scope:  query.SafeEncodeToken(pm.scope),
		})
	}

	if len(namespaces) == 0 {
		return
	}
	invCtx := trace.ContextWithSpanContext(context.WithoutCancel(ctx), trace.SpanContextFromContext(ctx))
	if _, err := w.cache.Invalidate(invCtx, namespaces); err != nil {
		slog.ErrorContext(invCtx, "failed to invalidate cache after insert - your cache is holding stale data now!", "tenant", id, "table", tableName, "error", err)
	}
}

// rejectPoison disposes of a message the worker can never insert. The DLQ is
// preferred — the row is preserved for inspection and replay — and dropping is
// the fallback when the DLQ is off for the table of tenant id (the topic's),
// since redelivering a message that can never succeed would wedge the
// consumer behind it forever. A DLQ publish that FAILS leaves the message
// unacked, exactly as the isolation path does: that is a transient DLQ
// outage, and retrying beats destroying the row.
func (w *IngestWorker) rejectPoison(ctx context.Context, m *mq.Message, id tenant.ID, tableName, reason, detail string) {
	if w.dlqEnabled == nil || w.dlqEnabled(id, tableName) {
		// Backgrounded on ackWg for the same reason handleSuccess backgrounds its
		// acks: parkOnDLQ does a DLQ publish AND an fsync-bound DoubleAck,
		// and parseMsg runs on the dispatchLoop goroutine. When a whole backlog
		// is poison, done inline that is one publish plus one fsync per message
		// in series, with intake stalled behind it. dispatchLoop adds and
		// waits on the same goroutine, so each Add still happens-before the Wait.
		w.ackWg.Go(func() {
			if w.parkOnDLQ(ctx, m, tableName, detail) {
				countPoison(ctx, tableName, reason, "parked")
			}
		})
		return
	}
	slog.ErrorContext(ctx, "unreadable envelope dropped — the DLQ is disabled for this table, and a message that can never insert must not redeliver forever",
		"tenant", id, "table", tableName, "reason", reason, "detail", detail)
	// Counted only once the ack lands, for the same reason the parked path waits
	// on parkOnDLQ's verdict: a failed ack leaves the message in the stream to be
	// redelivered and refused again, and "dropped" is documented to an operator as
	// a row that no longer exists anywhere. Counting before the ack would report
	// that about a row still sitting in the queue, once per redelivery.
	w.ackWg.Go(func() {
		if err := m.DoubleAck(ctx); err != nil {
			slog.ErrorContext(ctx, "ack of a dropped unreadable envelope failed, so it stays in the stream and will be refused again",
				"tenant", id, "table", tableName, "reason", reason, "error", err)
			return
		}
		countPoison(ctx, tableName, reason, "dropped")
	})
}

// sendToDLQ parks a row that failed its own isolated INSERT. Distinct from
// rejectPoison, which parks an envelope the worker could not read at all.
func (w *IngestWorker) sendToDLQ(ctx context.Context, tableName string, pm parsedMsg, errMsg string) {
	_ = w.parkOnDLQ(ctx, pm.msg, tableName, errMsg)
}

// parkOnDLQ parks one message on the dead-letter queue (under the topic it
// arrived on — the envelope may be unreadable) with the failure context in
// headers, then acks the original so the MQ stops redelivering it.
//
// Reports false in two distinct cases, both meaning "do not count this as a
// parking", and false does NOT imply nothing was published. A failed publish
// leaves the original unacked and parks nothing. A failed ack AFTER a
// successful publish leaves a DLQ copy behind but also leaves the original in
// the stream, so the next redelivery parks a second copy — counting the first
// would overstate the total by one per retry.
func (w *IngestWorker) parkOnDLQ(ctx context.Context, msg *mq.Message, tableName, errMsg string) bool {
	pubErr := w.dlq.DeadLetter(ctx, msg,
		mq.WithHeader("X-DLQ-Table", tableName),
		mq.WithHeader("X-DLQ-Error", errMsg),
		mq.WithHeader("X-DLQ-Timestamp", time.Now().UTC().Format(time.RFC3339)),
	)
	if pubErr != nil {
		slog.ErrorContext(ctx, "DLQ publish failed, this data will continue retrying insertion indefinitely until the DLQ recovers", "table", tableName, "topic", msg.TopicKey(), "error", pubErr)
		return false
	}

	// DoubleAck original message so the MQ doesn't redeliver the corrupt data. A
	// failed ack leaves it in the stream, so the next redelivery publishes a
	// SECOND copy to the DLQ — report false so the caller does not count this
	// parking again on every retry. The duplicate copy is the residual cost:
	// a publish is not idempotent, so it cannot be taken back here.
	if err := msg.DoubleAck(ctx); err != nil {
		slog.ErrorContext(ctx, "parked on the DLQ but the ack failed, so the envelope stays in the stream and will be parked again on redelivery",
			"table", tableName, "topic", msg.TopicKey(), "error", err)
		return false
	}
	return true
}

// countPoison records one unreadable envelope under its disposition.
func countPoison(ctx context.Context, tableName, reason, disposition string) {
	poisonCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("table", tableName),
		attribute.String("reason", reason),
		attribute.String("disposition", disposition),
	))
}
