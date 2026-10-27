package ingest

import (
	"context"
	"errors"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// writeMetrics are what the worker reports about its writes to ClickHouse
// (#895): how long each INSERT took, how full each bulk INSERT was, how often
// a bulk INSERT fell back to row-by-row isolation, and how each row it took
// off the queue was settled. Together with wavehouse_ingest_retries_total
// (rows handed back for a delayed retry) and wavehouse_ingest_poison_total
// (envelopes it could not read), every delivery the worker settles is counted
// exactly once. No instrument carries a tenant (#891).
type writeMetrics struct {
	insertSeconds metric.Float64Histogram
	batchRows     metric.Int64Histogram
	isolations    metric.Int64Counter
	rows          metric.Int64Counter
}

// insertBuckets match the API's request and query buckets (latencyBuckets in
// internal/api), which end at the insert client's 30 s timeout.
var insertBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// batchRowBuckets top out above defaultMaxBatch, so a later change to it
// stays readable.
var batchRowBuckets = []float64{1, 5, 10, 50, 100, 250, 500, 1000}

// Insert modes: a whole (table, column list) batch, or one row of a batch
// that fell back to isolation.
const (
	modeBatch    = "batch"
	modeIsolated = "isolated"
)

// Row outcomes for wavehouse_ingest_rows_total.
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
	isolations, err3 := m.Int64Counter(
		"wavehouse_ingest_isolations_total",
		metric.WithDescription("Bulk INSERTs ClickHouse refused that the ingest worker retried row by row to find the rows it rejects"),
	)
	rows, err4 := m.Int64Counter(
		"wavehouse_ingest_rows_total",
		metric.WithDescription("Rows the ingest worker settled, by outcome: inserted into ClickHouse (a row whose ack then fails can come back and is counted again), dead_lettered on the DLQ, or left_unacked to come back on redelivery (the DLQ is off for the table, or the DLQ publish or ack failed); a row left unacked is counted again on every attempt"),
	)
	return writeMetrics{insertSeconds, batchRows, isolations, rows}, errors.Join(err1, err2, err3, err4)
}

// insert sends one INSERT and records it: its duration and result, and for a
// bulk INSERT its row count. A tenant with no ClickHouse connection makes no
// request, so it records nothing.
func (w *IngestWorker) insert(ctx context.Context, tableName string, columns []string, msgs []parsedMsg, mode string) error {
	start := time.Now()
	err := w.insertToClickHouse(ctx, tableName, columns, msgs)
	if errors.Is(err, errNoTarget) {
		return err
	}
	result := "ok"
	if err != nil {
		result = chconn.Classify(err).String()
	}
	workerMetrics.insertSeconds.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
		attribute.String("mode", mode),
		attribute.String("result", result),
	))
	if mode == modeBatch {
		workerMetrics.batchRows.Record(ctx, int64(len(msgs)))
	}
	return err
}

// countRows records n rows of tableName settled with outcome.
func countRows(ctx context.Context, tableName, outcome string, n int) {
	workerMetrics.rows.Add(ctx, int64(n), metric.WithAttributes(
		attribute.String("table", tableName),
		attribute.String("outcome", outcome),
	))
}
