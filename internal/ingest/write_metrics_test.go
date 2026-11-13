package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// writeReading is what one collection of the worker's write metrics shows,
// each instrument summed by the attribute that tells its series apart.
type writeReading struct {
	rows            map[string]int64 // by outcome
	retries         int64
	poison          int64
	isolations      int64
	inserts         map[string]uint64 // by "mode/result"
	batchRows       int64             // rows summed over every bulk INSERT
	batchCount      uint64
	batchBytes      int64 // bytes summed over every bulk INSERT
	batchBytesCount uint64
	bytes           map[string]int64 // by outcome
	flushes         map[string]int64 // by trigger, the flushes that did not wait
	queuedFlushes   map[string]int64 // by trigger, those that waited behind the table's previous flush
	// untenanted and untenantedSamples sum what was recorded with no tenant
	// label, because the batch's tenant resolved to no ClickHouse target
	// (#891): the counters' counts, and the histograms' samples.
	untenanted        int64
	untenantedSamples uint64
}

// swapWriteMetrics points the worker's write, retry and poison instruments at
// a meter of the test's own for the rest of the test. They bind to the first
// global provider, so a later run in the same process would otherwise record
// into a dead reader.
func swapWriteMetrics(t *testing.T) func() writeReading {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	m := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	wm, err := newWriteMetrics(m)
	require.NoError(t, err)
	retries, err := newRetryCounter(m)
	require.NoError(t, err)
	poison, err := newPoisonCounter(m)
	require.NoError(t, err)
	savedWM, savedRetries, savedPoison := workerMetrics, retryCounter, poisonCounter
	workerMetrics, retryCounter, poisonCounter = wm, retries, poison
	t.Cleanup(func() { workerMetrics, retryCounter, poisonCounter = savedWM, savedRetries, savedPoison })

	return func() writeReading {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		r := writeReading{rows: map[string]int64{}, inserts: map[string]uint64{}, bytes: map[string]int64{}, flushes: map[string]int64{}, queuedFlushes: map[string]int64{}}
		attr := func(set attribute.Set, key string) string {
			v, _ := set.Value(attribute.Key(key))
			return v.AsString()
		}
		for _, sm := range rm.ScopeMetrics {
			for _, md := range sm.Metrics {
				switch data := md.Data.(type) {
				case metricdata.Sum[int64]:
					for _, dp := range data.DataPoints {
						switch md.Name {
						case "wavehouse_ingest_bytes_total":
							tenantOf(t, md.Name, dp.Attributes)
							assert.Equal(t, "events", attr(dp.Attributes, "table"))
							r.bytes[attr(dp.Attributes, "outcome")] += dp.Value
							continue
						case "wavehouse_ingest_flushes_total":
							tenantOf(t, md.Name, dp.Attributes)
							assert.Equal(t, "events", attr(dp.Attributes, "table"))
							switch attr(dp.Attributes, "queued") {
							case "false":
								r.flushes[attr(dp.Attributes, "trigger")] += dp.Value
							case "true":
								r.queuedFlushes[attr(dp.Attributes, "trigger")] += dp.Value
							default:
								t.Errorf("%s: queued label %q", md.Name, attr(dp.Attributes, "queued"))
							}
							continue
						}
						// Every delivery's measurement is about its batch's
						// tenant, unless that tenant has no ClickHouse target
						// (#891); untenanted sums the delivery counts alone.
						if !tenantOf(t, md.Name, dp.Attributes) {
							r.untenanted += dp.Value
						}
						switch md.Name {
						case "wavehouse_ingest_rows_total":
							assert.Equal(t, "events", attr(dp.Attributes, "table"))
							r.rows[attr(dp.Attributes, "outcome")] += dp.Value
						case "wavehouse_ingest_retries_total":
							r.retries += dp.Value
						case "wavehouse_ingest_poison_total":
							r.poison += dp.Value
						case "wavehouse_ingest_isolations_total":
							r.isolations += dp.Value
						}
					}
				case metricdata.Histogram[float64]:
					for _, dp := range data.DataPoints {
						if !tenantOf(t, md.Name, dp.Attributes) {
							r.untenantedSamples += dp.Count
						}
						r.inserts[attr(dp.Attributes, "mode")+"/"+attr(dp.Attributes, "result")] += dp.Count
					}
				case metricdata.Histogram[int64]:
					for _, dp := range data.DataPoints {
						if !tenantOf(t, md.Name, dp.Attributes) {
							r.untenantedSamples += dp.Count
						}
						switch md.Name {
						case "wavehouse_ingest_batch_rows":
							r.batchRows += dp.Sum
							r.batchCount += dp.Count
						case "wavehouse_ingest_batch_bytes":
							r.batchBytes += dp.Sum
							r.batchBytesCount += dp.Count
						}
					}
				}
			}
		}
		return r
	}
}

// tenantOf reports whether a data point carries a tenant label, and asserts
// that any it carries is the default tenant every test batch is written for.
func tenantOf(t *testing.T, name string, set attribute.Set) bool {
	t.Helper()
	v, ok := set.Value("tenant")
	if ok {
		assert.Equal(t, tenant.Default.String(), v.AsString(), name)
	}
	return ok
}

// poisonRowRT fails every bulk INSERT as rejected, and every single-row INSERT
// whose row carries true.
func poisonRowRT() *testutil.MockRoundTripper {
	return &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			s := string(body)
			if strings.Count(s, "\n") > 1 || strings.Contains(s, "true") {
				return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewBufferString("Code: 60. bad row"))}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
}

// TestWriteMetrics_RecordedAndReconciled drives each way the worker settles a
// delivery and checks the write metrics record it (#895). Every delivery is
// counted exactly once across wavehouse_ingest_rows_total,
// wavehouse_ingest_retries_total and wavehouse_ingest_poison_total, so the
// three reconcile against what the worker received, and the bytes of every
// settled row land in wavehouse_ingest_bytes_total under its outcome (#933).
func TestWriteMetrics_RecordedAndReconciled(t *testing.T) {
	read := swapWriteMetrics(t)
	ctx := context.Background()
	delivered := 0
	msg := func(row map[string]any) *testutil.MockMessage {
		delivered++
		return newIngestMsg(t, "events", "", row)
	}
	settledBytes := 0 // of every row rows_total counts

	// A successful batch: one bulk INSERT of two rows, both inserted.
	rt := okRoundTripper()
	w, _, _, wait := newTestWorker(rt)
	first := parseAll(t, w, msg(map[string]any{"id": 1}), msg(map[string]any{"id": 2}))
	settledBytes += bodyBytes(first)
	flush(ctx, w, "events", first)
	wait()
	got := read()
	assert.Equal(t, map[string]int64{rowsInserted: 2}, got.rows)
	assert.Equal(t, map[string]uint64{"batch/ok": 1}, got.inserts)
	assert.Equal(t, uint64(1), got.batchCount)
	assert.Equal(t, int64(2), got.batchRows)
	// The bulk INSERT's body as ClickHouse received it, and the same bytes
	// settled as inserted.
	require.Len(t, rt.Captured(), 1)
	body := int64(len(rt.Captured()[0].Body))
	assert.Equal(t, uint64(1), got.batchBytesCount)
	assert.Equal(t, body, got.batchBytes, "the bytes sent")
	assert.Equal(t, map[string]int64{rowsInserted: body}, got.bytes)
	assert.Equal(t, map[string]int64{triggerSize: 1}, got.flushes, "flush names a batch at its target")

	// An isolation fallback that parks one row: the bulk INSERT is rejected,
	// two rows insert alone and the third is dead-lettered.
	w, _, _, wait = newTestWorker(poisonRowRT())
	isolated := parseAll(t, w,
		msg(map[string]any{"id": 3, "poison": false}),
		msg(map[string]any{"id": 4, "poison": true}),
		msg(map[string]any{"id": 5, "poison": false}))
	settledBytes += bodyBytes(isolated)
	flush(ctx, w, "events", isolated)
	wait()
	got = read()
	assert.Equal(t, int64(1), got.isolations)
	assert.Equal(t, map[string]int64{rowsInserted: 4, rowsDeadLettered: 1}, got.rows)
	assert.Equal(t, map[string]uint64{"batch/ok": 1, "batch/rejected": 1, "isolated/ok": 2, "isolated/rejected": 1}, got.inserts)
	assert.Equal(t, uint64(2), got.batchCount, "an isolated INSERT is not a batch")
	assert.Equal(t, int64(5), got.batchRows)
	assert.Equal(t, uint64(2), got.batchBytesCount, "nor is it sized as one")
	assert.Equal(t, int64(lineBytes(isolated[1])), got.bytes[rowsDeadLettered], "the parked row's bytes, under its outcome")

	// A rejected row with the DLQ off for the table: left unacked.
	w, _, _, wait = newTestWorker(poisonRowRT())
	w.dlqEnabled = func(tenant.ID, string) bool { return false }
	unacked := parseAll(t, w,
		msg(map[string]any{"id": 6, "poison": false}),
		msg(map[string]any{"id": 7, "poison": true}))
	settledBytes += bodyBytes(unacked)
	flush(ctx, w, "events", unacked)
	wait()
	got = read()
	assert.Equal(t, int64(2), got.isolations)
	assert.Equal(t, map[string]int64{rowsInserted: 5, rowsDeadLettered: 1, rowsLeftUnacked: 1}, got.rows)

	// An unavailable ClickHouse: the batch goes back for a delayed retry, and
	// the INSERT is recorded under the failure's class.
	refused := &testutil.MockRoundTripper{Fn: func(*http.Request) (*http.Response, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
	}}
	w, _, _, wait = newTestWorker(refused)
	flush(ctx, w, "events", parseAll(t, w,
		msg(map[string]any{"id": 8}), msg(map[string]any{"id": 9}), msg(map[string]any{"id": 10})))
	wait()
	got = read()
	assert.Equal(t, int64(3), got.retries)
	assert.Equal(t, uint64(1), got.inserts["batch/unavailable"])
	assert.Equal(t, map[string]int64{rowsInserted: 5, rowsDeadLettered: 1, rowsLeftUnacked: 1}, got.rows,
		"a row handed back for a retry is counted there, not here")

	// A served tenant whose limits were refused with none to keep: the
	// batcher hands its row back for one ack wait, labeled with the tenant
	// like every other hand-back (#891).
	b, w, wait := newTestBatcher(t, okRoundTripper())
	w.limits = func(tenant.ID) (Limits, bool) { return Limits{}, false }
	b.add(ctx, parseAll(t, w, msg(map[string]any{"id": 11}))[0])
	wait()
	w.ackWg.Wait()
	got = read()
	assert.Equal(t, int64(4), got.retries, "the unconfigured hand-back")
	assert.Zero(t, got.untenanted, "labeled with its tenant")

	// An envelope the worker cannot read: parked and counted as poison.
	w, _, _, _ = newTestWorker(okRoundTripper())
	delivered++
	_, ok := w.parseMsg(ctx, (&testutil.MockMessage{
		MsgTopic: mq.Topic{Tenant: tenant.Default, Table: "events"},
		MsgData:  v1Envelope(t, "events", map[string]any{"id": 11}),
	}).Message())
	require.False(t, ok)
	w.ackWg.Wait()

	got = read()
	var settled, settledSize int64
	for _, n := range got.rows {
		settled += n
	}
	for _, n := range got.bytes {
		settledSize += n
	}
	assert.Equal(t, int64(delivered), settled+got.retries+got.poison,
		"every delivery is counted once across rows, retries and poison")
	assert.Equal(t, int64(settledBytes), settledSize, "and every settled row's bytes once across the outcomes")
	assert.Equal(t, int64(4), got.flushes[triggerSize], "one flush per batch, the retried one included")
	assert.Empty(t, got.queuedFlushes, "none waited behind another")
	// Every tenant here has a target, so every measurement is labeled.
	assert.Zero(t, got.untenanted)
	assert.Zero(t, got.untenantedSamples)
}

// TestWriteMetrics_NoConnectionAndFailedPark covers the outcomes no INSERT
// succeeds for: a tenant with no ClickHouse connection, whose batch is parked
// or left by its DLQ switch and which sends and times nothing, and a rejected
// row whose DLQ publish fails, which stays in the stream.
func TestWriteMetrics_NoConnectionAndFailedPark(t *testing.T) {
	read := swapWriteMetrics(t)
	ctx := context.Background()
	noTarget := func(tenant.ID) chconn.Target { return chconn.Target{} }
	rows := func() []*testutil.MockMessage {
		return []*testutil.MockMessage{
			newIngestMsg(t, "events", "", map[string]any{"id": 1}),
			newIngestMsg(t, "events", "", map[string]any{"id": 2}),
		}
	}

	// No connection, DLQ on: the batch is parked whole.
	w, _, _, wait := newTestWorker(okRoundTripper())
	w.target = noTarget
	flush(ctx, w, "events", parseAll(t, w, rows()...))
	wait()
	got := read()
	assert.Equal(t, map[string]int64{rowsDeadLettered: 2}, got.rows)
	// A tenant with no connection is not one the worker serves, so its
	// rows take no tenant label (#891).
	assert.Equal(t, int64(2), got.untenanted)

	// No connection, DLQ off: the batch is left for redelivery.
	w, _, _, wait = newTestWorker(okRoundTripper())
	w.target = noTarget
	w.dlqEnabled = func(tenant.ID, string) bool { return false }
	flush(ctx, w, "events", parseAll(t, w, rows()...))
	wait()
	assert.Equal(t, map[string]int64{rowsDeadLettered: 2, rowsLeftUnacked: 2}, read().rows)

	// An INSERT with no connection makes no request, so it records no sample.
	w, _, _, _ = newTestWorker(okRoundTripper())
	w.target = noTarget
	msgs := parseAll(t, w, rows()...)
	require.ErrorIs(t, w.insert(ctx, "events", msgs[0].columns, msgs, modeBatch, 0), errNoTarget)
	got = read()
	assert.Empty(t, got.inserts)
	assert.Zero(t, got.batchCount)

	// A rejected row whose DLQ publish fails is left unacked, not dead-lettered.
	w, pub, _, wait := newTestWorker(poisonRowRT())
	pub.Err = errors.New("jetstream unavailable")
	flush(ctx, w, "events", parseAll(t, w,
		newIngestMsg(t, "events", "", map[string]any{"id": 3, "poison": false}),
		newIngestMsg(t, "events", "", map[string]any{"id": 4, "poison": true})))
	wait()
	got = read()
	assert.Equal(t, map[string]int64{rowsInserted: 1, rowsDeadLettered: 2, rowsLeftUnacked: 3}, got.rows)
	// Only the four rows of the two batches with no connection went unlabeled.
	assert.Equal(t, int64(4), got.untenanted)

	// A poison message on the topic of a tenant with no connection is parked
	// and counted unlabeled too: a topic can name any tenant, served or not.
	w, _, _, _ = newTestWorker(okRoundTripper())
	w.target = noTarget
	m := &testutil.MockMessage{MsgTopic: mq.Topic{Tenant: tenant.Default, Table: "events"}, MsgData: []byte(`{"table_name":`)}
	_, ok := w.parseMsg(ctx, m.Message())
	w.ackWg.Wait()
	require.False(t, ok)
	got = read()
	assert.Equal(t, int64(1), got.poison)
	assert.Equal(t, int64(4+1), got.untenanted, "the poison count")
	assert.Zero(t, got.untenantedSamples, "no INSERT was sent, so none was timed")
}

// TestWriteMetrics_InsertedBeforeAck pins that a row ClickHouse took is
// counted inserted even when its ack then fails: the row is in ClickHouse,
// and a redelivery is written and counted again (deployment.md, Ingest writes).
func TestWriteMetrics_InsertedBeforeAck(t *testing.T) {
	read := swapWriteMetrics(t)
	w, _, _, wait := newTestWorker(okRoundTripper())
	m := newIngestMsg(t, "events", "", map[string]any{"id": 1})
	m.DoubleAckErr = errors.New("ack timed out")
	flush(context.Background(), w, "events", parseAll(t, w, m))
	wait()
	got := read()
	assert.Equal(t, map[string]int64{rowsInserted: 1}, got.rows)
	assert.Zero(t, got.untenanted)
	assert.Zero(t, got.untenantedSamples)
}

// TestWriteMetrics_FailedDLQAck: a rejected row whose DLQ publish succeeds
// but whose own ack fails is left unacked, not dead-lettered, so a
// redelivery that parks it is the one dead_lettered counts.
func TestWriteMetrics_FailedDLQAck(t *testing.T) {
	read := swapWriteMetrics(t)
	ctx := context.Background()
	w, pub, _, wait := newTestWorker(poisonRowRT())
	m := newIngestMsg(t, "events", "", map[string]any{"id": 1, "poison": true})
	m.DoubleAckErr = errors.New("ack timed out")
	flush(ctx, w, "events", parseAll(t, w, m))
	wait()
	require.Len(t, pub.Published(), 1, "parked, though the ack after it failed")
	assert.Equal(t, map[string]int64{rowsLeftUnacked: 1}, read().rows)

	// The redelivery parks again, and this time its ack succeeds.
	m.DoubleAckErr = nil
	flush(ctx, w, "events", parseAll(t, w, m))
	wait()
	require.Len(t, pub.Published(), 2)
	got := read()
	assert.Equal(t, map[string]int64{rowsLeftUnacked: 1, rowsDeadLettered: 1}, got.rows)
	assert.Zero(t, got.untenanted)
	assert.Zero(t, got.untenantedSamples)
}

// TestWriteMetrics_FlushTriggers drives each way a batch leaves the batcher
// and checks wavehouse_ingest_flushes_total names what asked for it (#960):
// the size target, the deadline, the shutdown drain, and the batch of a
// tenant no longer served, which no trigger sized and must not read as a
// size flush. A flush asked for while another is in flight keeps the trigger
// that fired first.
func TestWriteMetrics_FlushTriggers(t *testing.T) {
	read := swapWriteMetrics(t)
	ctx := context.Background()
	row := func(id int) *testutil.MockMessage { return newIngestMsg(t, "events", "", map[string]any{"id": id}) }

	// Size: the third row reaches the target newTestBatcher sets.
	b, w, wait := newTestBatcher(t, okRoundTripper())
	for _, pm := range parseAll(t, w, row(1), row(2), row(3)) {
		b.add(ctx, pm)
	}
	require.NotNil(t, b.flushing, "the size trigger started a flush")
	<-b.flushing
	wait()
	assert.Equal(t, map[string]int64{triggerSize: 1}, read().flushes)

	// Timer: one row under a short wait, driven by the table loop as the
	// worker drives it, is written when its deadline passes.
	w, _, _, wait = newTestWorker(okRoundTripper())
	w.limits = limitsOf(Limits{MaxBatchBytes: 1 << 20, MaxBatchWait: 20 * time.Millisecond})
	in := make(chan parsedMsg, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.tableLoop(ctx, "events", in)
	}()
	in <- parseAll(t, w, row(4))[0]
	require.Eventually(t, func() bool { return read().flushes[triggerTimer] == 1 }, 2*time.Second, 5*time.Millisecond, "the deadline flushed the row")
	close(in)
	<-done
	wait()

	// Shutdown: a row left in a batch is flushed by the drain.
	b, w, wait = newTestBatcher(t, okRoundTripper())
	b.batch = parseAll(t, w, row(5))
	b.drainAndExit(ctx)
	wait()
	assert.Equal(t, int64(1), read().flushes[triggerShutdown])

	// Latched: the size trigger fires behind an in-flight flush, then the
	// deadline; the flush onFlushDone starts is counted as the size one, and
	// as one that waited.
	b, w, wait = newTestBatcher(t, okRoundTripper())
	inflight := make(chan struct{})
	b.flushing = inflight
	for _, pm := range parseAll(t, w, row(6), row(7), row(8)) {
		b.add(ctx, pm)
	}
	require.Equal(t, triggerSize, b.queued)
	b.requestFlush(ctx, triggerTimer, false)
	assert.Equal(t, triggerSize, b.queued, "the first trigger to fire is kept")
	close(inflight)
	b.onFlushDone(ctx)
	require.NotNil(t, b.flushing)
	<-b.flushing
	wait()
	got := read()
	assert.Equal(t, int64(1), got.flushes[triggerSize], "the direct size flush alone")
	assert.Equal(t, map[string]int64{triggerSize: 1}, got.queuedFlushes, "the latched one waited behind the in-flight flush")

	// Latched at shutdown: the size trigger fires behind an in-flight flush
	// and the drain runs before the slot frees; the remainder keeps its
	// trigger and waited, as the flush onFlushDone would have started.
	b, w, wait = newTestBatcher(t, okRoundTripper())
	inflight = make(chan struct{})
	b.flushing = inflight
	for _, pm := range parseAll(t, w, row(10), row(11), row(12)) {
		b.add(ctx, pm)
	}
	require.Equal(t, triggerSize, b.queued)
	close(inflight)
	b.drainAndExit(ctx)
	wait()
	assert.Equal(t, int64(2), read().queuedFlushes[triggerSize], "a size flush that waited, not a shutdown one")

	// Drained behind a flush: a row under the target, with nothing latched,
	// left when the worker stops while a flush is in flight is a shutdown
	// flush that waited.
	b, w, wait = newTestBatcher(t, okRoundTripper())
	inflight = make(chan struct{})
	b.flushing = inflight
	b.add(ctx, parseAll(t, w, row(13))[0])
	require.Empty(t, b.queued, "under the target, nothing latched")
	close(inflight)
	b.drainAndExit(ctx)
	wait()
	assert.Equal(t, int64(1), read().queuedFlushes[triggerShutdown], "a shutdown flush that waited")

	// None: a tenant with no limits and no pool has its row flushed at once
	// to be parked, through a target of 0 that is no size trigger.
	b, w, wait = newTestBatcher(t, okRoundTripper())
	w.limits = func(tenant.ID) (Limits, bool) { return Limits{}, false }
	w.target = func(tenant.ID) chconn.Target { return chconn.Target{} }
	b.add(ctx, parseAll(t, w, row(9))[0])
	require.NotNil(t, b.flushing, "flushed at its first row")
	<-b.flushing
	wait()
	got = read()
	assert.Equal(t, map[string]int64{triggerSize: 1, triggerTimer: 1, triggerShutdown: 1, triggerNone: 1}, got.flushes)
	assert.Equal(t, map[string]int64{triggerSize: 2, triggerShutdown: 1}, got.queuedFlushes)
	assert.Equal(t, int64(1), got.rows[rowsDeadLettered], "parked")
}
