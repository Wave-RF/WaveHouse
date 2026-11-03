package ingest

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// writeMetrics are what the worker reports about its writes to ClickHouse
// (#895): how long each INSERT took, how full each bulk INSERT was, in rows
// and in bytes (#933), what asked for each batch's flush (#960), how often a
// bulk INSERT fell back to row-by-row isolation, and how each row it took
// off the queue was settled, counted and sized. Together with
// wavehouse_ingest_retries_total (rows handed back for a delayed retry) and
// wavehouse_ingest_poison_total (envelopes it could not read), every delivery
// the worker settles is counted exactly once. Each is labeled with the
// batch's tenant (#891).
type writeMetrics struct {
	insertSeconds metric.Float64Histogram
	batchRows     metric.Int64Histogram
	batchBytes    metric.Int64Histogram
	flushes       metric.Int64Counter
	isolations    metric.Int64Counter
	rows          metric.Int64Counter
	bytes         metric.Int64Counter
}

// insertBuckets match the API's request and query buckets (latencyBuckets in
// internal/api), which end at the seed's 30 s clickhouse.insert_timeout.
var insertBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// batchRowBuckets top out at the default unacked budget
// (mq.DefaultMaxAckPending), the most rows a batch holds at the defaults, so
// a batch that fills it still lands in a bucket of its own.
var batchRowBuckets = []float64{1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000}

// batchByteBuckets run in powers of four from 1 KiB to 64 MiB: the seed's
// 256 KiB ingest.max_batch_bytes is a boundary, so a batch at the target and
// one short of it fall either side of it, and 64 MiB is well past a batch of
// ordinary rows at the default budget (100,000 rows of 200 bytes is 20 MiB).
var batchByteBuckets = []float64{1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20, 1 << 22, 1 << 24, 1 << 26}

// Insert modes: a whole (table, column list) batch, or one row of a batch
// that fell back to isolation.
const (
	modeBatch    = "batch"
	modeIsolated = "isolated"
)

// Flush triggers for wavehouse_ingest_flushes_total: what asked for a
// batch's flush. A batch latched behind the table's in-flight flush keeps
// the trigger that fired first (tableBatcher.queued), and the counter's
// queued label says whether the batch waited so (#960): that wait puts the
// previous flush's INSERT on the row's way to the ack wait.
const (
	// triggerSize: the batch's rows' JSON reached the tenant's byte target.
	triggerSize = "size"
	// triggerTimer: the tenant's batch wait passed since the batch's first row.
	triggerTimer = "timer"
	// triggerShutdown: what the table held when the worker stopped, with no
	// trigger having asked for it yet (one that had keeps its trigger, queued).
	triggerShutdown = "shutdown"
	// triggerNone: no trigger sized or timed the batch. Its tenant had no
	// limits and no pool, so it was flushed at its first row to be parked
	// (tableBatcher.add); named apart so a target of 0 never reads as a
	// size flush.
	triggerNone = "none"
)

// Row outcomes for wavehouse_ingest_rows_total and wavehouse_ingest_bytes_total.
const (
	rowsInserted     = "inserted"
	rowsDeadLettered = "dead_lettered"
	rowsLeftUnacked  = "left_unacked"
)

var workerMetrics, _ = newWriteMetrics(otel.Meter("wavehouse-ingest"))

// newWriteMetrics builds the instruments on m (a test builds them on its own
// meter, as newPoisonCounter's does).
func newWriteMetrics(m metric.Meter) (writeMetrics, error) {
	insertSeconds, err1 := m.Float64Histogram(
		"wavehouse_ingest_insert_duration_seconds",
		metric.WithDescription("Duration of each INSERT the ingest worker sent to ClickHouse, by mode (batch, or isolated for one row of a batch that fell back) and result (ok, or the failure's class: rejected, unavailable, denied, unknown)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(insertBuckets...),
	)
	batchRows, err2 := m.Int64Histogram(
		"wavehouse_ingest_batch_rows",
		metric.WithDescription("Rows in each bulk INSERT the ingest worker sent to ClickHouse"),
		metric.WithExplicitBucketBoundaries(batchRowBuckets...),
	)
	batchBytes, err3 := m.Int64Histogram(
		"wavehouse_ingest_batch_bytes",
		metric.WithDescription("Bytes in the body of each bulk INSERT the ingest worker sent to ClickHouse: its rows' JSON lines, newline included"),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(batchByteBuckets...),
	)
	flushes, err4 := m.Int64Counter(
		"wavehouse_ingest_flushes_total",
		metric.WithDescription("Batches the ingest worker flushed, by what asked for the flush: size (the rows' JSON reached ingest.max_batch_bytes), timer (ingest.max_batch_wait_ms passed since the first row), shutdown (what the table held when the worker stopped), or none (the batch of a tenant no longer served, flushed at its first row to be parked); and by queued, whether the batch waited behind the table's previous flush before its own started"),
	)
	isolations, err5 := m.Int64Counter(
		"wavehouse_ingest_isolations_total",
		metric.WithDescription("Bulk INSERTs ClickHouse refused that the ingest worker retried row by row to find the rows it rejects"),
	)
	rows, err6 := m.Int64Counter(
		"wavehouse_ingest_rows_total",
		metric.WithDescription("Rows the ingest worker settled, by outcome: inserted into ClickHouse (a row whose ack then fails can come back and is counted again), dead_lettered on the DLQ, or left_unacked to come back on redelivery (the DLQ is off for the table, or the DLQ publish or ack failed); a row left unacked is counted again on every attempt"),
	)
	byteCount, err7 := m.Int64Counter(
		"wavehouse_ingest_bytes_total",
		metric.WithDescription("Bytes of the rows the ingest worker settled, each row's JSON line with its newline, by the outcome wavehouse_ingest_rows_total counts the row under"),
		metric.WithUnit("By"),
	)
	return writeMetrics{insertSeconds, batchRows, batchBytes, flushes, isolations, rows, byteCount}, errors.Join(err1, err2, err3, err4, err5, err6, err7)
}

// zeroWorkerCounters exports the worker's counters at 0 for every closed
// label set, so the first poison row, retry or isolation is an increase a
// rate() or increase() alert sees (#905). They carry table and tenant, so
// observability.ZeroCounter does this only where both labels are dropped.
func zeroWorkerCounters() {
	const carries = observability.CarriesTable | observability.CarriesTenant
	observability.ZeroCounter(workerMetrics.isolations, carries)
	outcomes := observability.LabelSets(observability.Values("outcome", rowsInserted, rowsDeadLettered, rowsLeftUnacked))
	observability.ZeroCounter(workerMetrics.rows, carries, outcomes...)
	observability.ZeroCounter(workerMetrics.bytes, carries, outcomes...)
	observability.ZeroCounter(workerMetrics.flushes, carries, observability.LabelSets(
		observability.Values("trigger", triggerSize, triggerTimer, triggerShutdown, triggerNone),
		observability.Values("queued", "false", "true"))...)
	observability.ZeroCounter(poisonCounter, carries, observability.LabelSets(
		observability.Values("reason", "malformed", "unknown_format", "unpairable"),
		observability.Values("disposition", "parked", "dropped"))...)
	observability.ZeroCounter(retryCounter, carries, observability.LabelSets(
		observability.Values("reason", "backoff", "unconfigured", chconn.Unavailable.String(), chconn.Denied.String(), chconn.Unknown.String()))...)
}

// lineBytes is the size of pm's line in an INSERT body: its JSON and the
// newline after it. The batcher's byte target counts the JSON alone, so a
// batch at the target measures one byte per row more here.
func lineBytes(pm parsedMsg) int { return len(pm.row) + 1 }

// bodyBytes is the size of the INSERT body msgs make.
func bodyBytes(msgs []parsedMsg) int {
	n := 0
	for _, pm := range msgs {
		n += lineBytes(pm)
	}
	return n
}

// insert sends one INSERT under the tenant's insert deadline and records it:
// its duration and result, and for a bulk INSERT its row count and its
// body's bytes. A tenant with no ClickHouse connection makes no request, so
// it records nothing.
func (w *IngestWorker) insert(ctx context.Context, tableName string, columns []string, msgs []parsedMsg, mode string, timeout time.Duration) error {
	start := time.Now()
	err := w.insertToClickHouse(ctx, tableName, columns, msgs, timeout)
	if errors.Is(err, errNoTarget) {
		return err
	}
	result := "ok"
	if err != nil {
		result = chconn.Classify(err).String()
	}
	workerMetrics.insertSeconds.Record(ctx, time.Since(start).Seconds(), observability.MetricAttrs(ctx,
		attribute.String("mode", mode),
		attribute.String("result", result),
	))
	if mode == modeBatch {
		workerMetrics.batchRows.Record(ctx, int64(len(msgs)), observability.MetricAttrs(ctx))
		workerMetrics.batchBytes.Record(ctx, int64(bodyBytes(msgs)), observability.MetricAttrs(ctx))
	}
	return err
}

// countFlush records one batch of tableName flushed, by the trigger that
// asked for it and by whether the batch waited behind the table's previous
// flush (the queued label, "true" or "false").
func countFlush(ctx context.Context, tableName, trigger string, waited bool) {
	workerMetrics.flushes.Add(ctx, 1, observability.MetricAttrs(ctx,
		attribute.String("table", tableName),
		attribute.String("trigger", trigger),
		attribute.String("queued", strconv.FormatBool(waited)),
	))
}

// countRows records the rows of msgs settled with outcome, and their bytes.
func countRows(ctx context.Context, tableName, outcome string, msgs ...parsedMsg) {
	attrs := observability.MetricAttrs(ctx,
		attribute.String("table", tableName),
		attribute.String("outcome", outcome),
	)
	workerMetrics.rows.Add(ctx, int64(len(msgs)), attrs)
	workerMetrics.bytes.Add(ctx, int64(bodyBytes(msgs)), attrs)
}
