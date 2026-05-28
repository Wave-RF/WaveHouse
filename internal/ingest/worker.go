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
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

type parsedMsg struct {
	natsMsg         jetstream.Msg
	natsSafeSubject string
	tableName       string // routing key for per-table batchers; raw, not encoded
	scope           string
	rawJSON         []byte
}

type IngestWorker struct {
	js         jetstream.JetStream
	httpClient *http.Client
	cache      cache.Cache
	logger     *slog.Logger
	chURL      string
	user       string
	password   string
	db         string
	maxBatch   int
	maxWait    time.Duration
	wg         sync.WaitGroup
}

// Production defaults; overridable on the struct for tests.
const (
	defaultMaxBatch = 500
	defaultMaxWait  = 5 * time.Second
)

func StartIngestWorker(
	ctx context.Context, nc *nats.Conn, cache cache.Cache,
	chHost, chHTTPPort, chHTTPScheme, chUser, chPassword, chDB string,
) (func(context.Context) error, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is nil")
	}
	if cache == nil {
		return nil, fmt.Errorf("cache is nil")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, mq.StreamName(), jetstream.ConsumerConfig{
		Durable:       BufferConsumerName,
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 10_000, // TODO: testing increasing this to see if NATS is bottlenecking us
	})
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(chHost)
	if err != nil {
		host = chHost // If it fails to split, assume it's just a raw host/IP
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
		cache:    cache,
		logger:   slog.Default().With("component", "ingest_worker"),
		chURL:    fmt.Sprintf("%s://%s:%s", chHTTPScheme, host, chHTTPPort),
		user:     chUser,
		password: chPassword,
		db:       chDB,
		maxBatch: defaultMaxBatch,
		maxWait:  defaultMaxWait,
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	worker.wg.Add(1)

	// Start push-based loop
	go worker.runLoop(workerCtx, cons)

	stopFunc := func(shutdownCtx context.Context) error {
		workerCancel()
		done := make(chan struct{})
		go func() {
			worker.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
	}
	return stopFunc, nil
}

// runLoop owns the JetStream consume side and demultiplexes every incoming
// envelope to a per-table goroutine (lazily spawned on first sight of a
// table). The runLoop itself does no batching — it parses the envelope
// just enough to extract the table name and forwards the parsed message
// to that table's tableLoop, which owns its own batch + timer + at-most-1
// in-flight flush. This means a low-volume table can't strand events from
// a high-volume table behind its 5s maxWait timer: each table's size
// trigger fires on its own row count.
//
// Malformed envelopes are ack'd-and-dropped synchronously here — the same
// poison-pill drop the legacy flush() path did, just earlier in the
// pipeline so the per-table fanout never sees them.
func (w *IngestWorker) runLoop(ctx context.Context, cons jetstream.Consumer) {
	defer w.wg.Done()

	msgChan := make(chan jetstream.Msg, w.maxBatch*2)

	// Push-based consumer (much faster/more efficient than iterators)
	consumeCtx, err := cons.Consume(func(msg jetstream.Msg) {
		msgChan <- msg
	})
	if err != nil {
		w.logger.Error("failed to start consumer", "error", err)
		return
	}
	defer consumeCtx.Stop()

	tableChans := make(map[string]chan parsedMsg)
	var tableWg sync.WaitGroup

	// shutdown drains every per-table goroutine. Closing each tableChan
	// signals the tableLoop to consume any parsedMsgs still buffered in
	// its channel, flush its local batch via a detached context, and
	// exit. Anything sitting in msgChan past ctx.Done is left for NATS
	// to redeliver.
	shutdown := func() {
		for _, ch := range tableChans {
			close(ch)
		}
		tableWg.Wait()
	}

	for {
		select {
		case <-ctx.Done():
			shutdown()
			return
		case m := <-msgChan:
			pm, ok := w.parseMsg(ctx, m)
			if !ok {
				// Malformed — already ack'd-and-dropped inside parseMsg.
				continue
			}
			ch, exists := tableChans[pm.tableName]
			if !exists {
				ch = make(chan parsedMsg, w.maxBatch)
				tableChans[pm.tableName] = ch
				tableWg.Add(1)
				go w.tableLoop(ctx, pm.tableName, ch, &tableWg)
			}
			// The per-table channel is bounded; if a table backs up enough
			// to fill it, propagate that into the consume side via blocking
			// here — NATS' MaxAckPending takes over as the eventual cap.
			select {
			case ch <- pm:
			case <-ctx.Done():
				shutdown()
				return
			}
		}
	}
}

// tableLoop owns one table's batching pipeline. State machine:
//
//   - Accumulate incoming parsedMsgs into `batch`.
//   - On len(batch) == maxBatch OR maxWait elapsed since the first row of
//     the current batch, flush.
//   - At most one in-flight flush per table. If a second batch is ready
//     while a flush is in progress, COALESCE: keep accumulating, and the
//     flush completion path kicks off the next one. This bounds the
//     "too many parts" pressure on ClickHouse (one POST per table at a
//     time) while still letting the next batch fill during the current
//     batch's CH round trip — i.e. pipelined fill + flush.
//
// Shutdown is driven exclusively by `in` being closed (runLoop's shutdown()
// path on ctx.Done). The tableLoop drains any parsedMsgs still buffered in
// the channel into `batch`, waits on any in-flight flush, and does a final
// synchronous flush of whatever remains using a detached context. There is
// deliberately no `case <-ctx.Done()` arm: relying on the channel close
// gives deterministic drain ordering and prevents a select race that would
// otherwise abandon channel-buffered msgs to NATS redelivery.
func (w *IngestWorker) tableLoop(ctx context.Context, tableName string, in <-chan parsedMsg, wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		batch    []parsedMsg
		inFlight chan struct{} // closed when the active flush goroutine exits; nil when idle
	)

	timer := time.NewTimer(w.maxWait)
	if !timer.Stop() {
		<-timer.C
	}

	startFlush := func(toFlush []parsedMsg) chan struct{} {
		done := make(chan struct{})
		// If ctx is already cancelled, runLoop is draining us toward
		// exit — detach so the in-progress POST still completes and acks
		// instead of failing fast and provoking a NATS redelivery.
		flushCtx := ctx
		if ctx.Err() != nil {
			flushCtx = context.WithoutCancel(ctx)
		}
		go func() {
			defer close(done)
			w.flushTable(flushCtx, tableName, toFlush)
		}()
		return done
	}

	tryFlush := func() {
		if len(batch) == 0 || inFlight != nil {
			// Coalesce: if a flush is already running for this table we
			// keep accumulating in `batch`; the inFlight completion arm
			// kicks off the next flush.
			return
		}
		toFlush := batch
		batch = nil
		// Timer was tied to `toFlush`; re-arm it when a new batch starts.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		inFlight = startFlush(toFlush)
	}

	for {
		select {
		case pm, ok := <-in:
			if !ok {
				// runLoop closed our channel — drain any in-flight flush,
				// do a final synchronous flush via a detached ctx, exit.
				if inFlight != nil {
					<-inFlight
				}
				if len(batch) > 0 {
					w.flushTable(context.WithoutCancel(ctx), tableName, batch)
				}
				return
			}
			if len(batch) == 0 {
				// First row of a new batch — arm the deadline timer.
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(w.maxWait)
			}
			batch = append(batch, pm)
			if len(batch) >= w.maxBatch {
				tryFlush()
			}

		case <-timer.C:
			tryFlush()

		case <-inFlight:
			// Active flush completed — clear the slot and, if a batch
			// has accumulated during the flush, kick off the next one.
			inFlight = nil
			if len(batch) > 0 {
				tryFlush()
			}
		}
	}
}

// parseMsg unmarshals one envelope. On a malformed envelope it ack'd-
// and-dropped (poison pill) and returns ok=false so the caller skips it.
// This mirrors the legacy flush() path; moving it into the dispatch
// layer means per-table goroutines never see malformed input.
func (w *IngestWorker) parseMsg(ctx context.Context, m jetstream.Msg) (parsedMsg, bool) {
	natsSafeSubject := strings.TrimPrefix(m.Subject(), "ingest.")

	var envelope struct {
		UnsafeTableName   string          `json:"table_name"`
		UnsafeScope       string          `json:"scope"`
		ReceivedTimestamp string          `json:"received_timestamp"`
		Data              json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(m.Data(), &envelope); err != nil {
		w.logger.ErrorContext(ctx, "failed to parse event envelope", "error", err)
		_ = m.DoubleAck(ctx)
		return parsedMsg{}, false
	}

	return parsedMsg{
		natsMsg:         m,
		natsSafeSubject: natsSafeSubject,
		tableName:       envelope.UnsafeTableName,
		scope:           envelope.UnsafeScope,
		rawJSON:         envelope.Data,
	}, true
}

// flush parses every msg in `batch`, groups them by table, and dispatches
// each group to flushTable in parallel. It is no longer invoked from
// runLoop (which routes per-table via tableLoop) — it survives as the
// unit-test surface for the parse → group → insert → ack pipeline.
//
// The runLoop / tableLoop path parses earlier (in runLoop's intake), so
// per-table goroutines never see malformed input. flush() still has to
// do that parse here, with the same poison-pill drop semantics.
func (w *IngestWorker) flush(ctx context.Context, batch []jetstream.Msg) {
	if len(batch) == 0 {
		return
	}

	groups := make(map[string][]parsedMsg)
	for _, m := range batch {
		pm, ok := w.parseMsg(ctx, m)
		if !ok {
			continue
		}
		groups[pm.tableName] = append(groups[pm.tableName], pm)
	}

	var tableWg sync.WaitGroup
	for tableName, msgs := range groups {
		tableWg.Add(1)
		go func(tableName string, msgs []parsedMsg) {
			defer tableWg.Done()
			w.flushTable(ctx, tableName, msgs)
		}(tableName, msgs)
	}
	tableWg.Wait()
}

// flushTable POSTs one table's batch to ClickHouse. On bulk failure, falls
// back to 1-by-1 isolation: each row that re-inserts successfully gets the
// normal handleSuccess path; each row that fails again is DLQ'd.
//
// Called from both the legacy flush() (which groups by table) and from
// tableLoop (which already has a per-table batch). Safe to call concurrently
// for different tables; tableLoop ensures only one concurrent call per
// (worker, table).
func (w *IngestWorker) flushTable(ctx context.Context, tableName string, msgs []parsedMsg) {
	if len(msgs) == 0 {
		return
	}

	err := w.insertToClickHouse(ctx, tableName, msgs)
	if err == nil {
		w.handleSuccess(ctx, tableName, msgs)
		return
	}
	w.logger.WarnContext(ctx, "bulk insert failed, falling back to 1-by-1 isolation", "table", tableName, "error", err)

	// ISOLATE & DLQ: re-insert one row at a time so a single poison row
	// can't sink the whole batch.
	// TODO: potentially could try a binary search or something eventually maybe? unclear if faster...
	for _, pm := range msgs {
		if singleErr := w.insertToClickHouse(ctx, tableName, []parsedMsg{pm}); singleErr != nil {
			w.logger.ErrorContext(ctx, "isolated bad row, sending to DLQ", "table", tableName, "error", singleErr)
			w.sendToDLQ(ctx, tableName, pm, singleErr.Error())
		} else {
			w.handleSuccess(ctx, tableName, []parsedMsg{pm})
		}
	}
}

func (w *IngestWorker) insertToClickHouse(ctx context.Context, tableName string, msgs []parsedMsg) error {
	var buf bytes.Buffer
	for _, m := range msgs {
		buf.Write(m.rawJSON)
		buf.WriteString("\n")
	}

	q := url.Values{}
	q.Set("database", w.db)
	q.Set("param_target_table", tableName)
	q.Set("query", "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow")

	req, err := http.NewRequestWithContext(ctx, "POST", w.chURL+"?"+q.Encode(), &buf)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", w.user)
	req.Header.Set("X-ClickHouse-Key", w.password)

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
	// Pre-allocate the set using the length of msgs (+1 for tableName itself) to prevent expensive rehashing
	seenSubjects := make(map[string]struct{}, len(msgs)+1)
	versionKeys := make([]string, 0, len(msgs)+1)

	encodedTable := query.SafeEncodeNATS(tableName)
	seenSubjects[encodedTable] = struct{}{}
	versionKeys = append(versionKeys, encodedTable)

	for _, pm := range msgs {
		if _, exists := seenSubjects[pm.natsSafeSubject]; !exists {
			seenSubjects[pm.natsSafeSubject] = struct{}{}
			versionKeys = append(versionKeys, pm.natsSafeSubject)
		}
	}

	if len(versionKeys) > 0 {
		invCtx := trace.ContextWithSpanContext(context.WithoutCancel(ctx), trace.SpanContextFromContext(ctx))
		_, err := w.cache.InvalidateCache(invCtx, versionKeys)
		if err != nil {
			w.logger.ErrorContext(invCtx, "failed to invalidate cache after insert - your cache is holding stale data now!", "table", tableName, "error", err)
		}
	}

	w.wg.Add(1) // Tell the main worker we have a background task running

	go func() {
		defer w.wg.Done()

		var ackWg sync.WaitGroup
		for _, pm := range msgs {
			ackWg.Add(1)

			go func(m jetstream.Msg) {
				defer ackWg.Done()
				if err := m.DoubleAck(context.WithoutCancel(ctx)); err != nil {
					w.logger.ErrorContext(context.WithoutCancel(ctx), "double ack failed for processed message", "error", err, "table", tableName)
				}
			}(pm.natsMsg)
		}

		ackWg.Wait()
	}()
}

func (w *IngestWorker) sendToDLQ(ctx context.Context, tableName string, pm parsedMsg, errMsg string) {
	subject := "dlq." + pm.natsSafeSubject

	msg := nats.NewMsg(subject)
	msg.Data = pm.natsMsg.Data()
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
	_ = pm.natsMsg.DoubleAck(ctx)
}
