package api

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// Results on wavehouse_ingest_records_total: what became of a record the
// ingest handler read (#896).
const (
	resultQueued            = "queued"
	resultDuplicate         = "duplicate"
	resultInFlight          = "in_flight"
	resultInvalidBody       = "invalid_body"
	resultSchema            = "schema"
	resultForbidden         = "forbidden"
	resultMissingID         = "missing_id"
	resultDedupeUnavailable = "dedupe_unavailable"
	resultQueueFull         = "queue_full"
	resultQueueUnavailable  = "queue_unavailable"
	resultPublishFailed     = "publish_failed"
	resultRecordsCanceled   = "canceled"
)

// ingestRecords counts every record the ingest handler read, by table and
// result, in single and batch bodies alike. A refusal before any record is
// read (no table, an unknown one, the content type, an empty body, the
// insert grant) counts nothing here and is left to the HTTP histogram.
var ingestRecords, _ = newIngestRecordsCounter(otel.Meter("wavehouse-ingest"))

// newIngestRecordsCounter builds the counter on m (a test builds it on its
// own meter).
func newIngestRecordsCounter(m metric.Meter) (metric.Int64Counter, error) {
	return m.Int64Counter(
		"wavehouse_ingest_records_total",
		metric.WithDescription("Records POST /v1/ingest read, by result: queued, duplicate, in_flight, a rejection (invalid_body, schema, forbidden, missing_id), a failure (dedupe_unavailable, queue_full, queue_unavailable, publish_failed), or canceled when the client went away first"),
	)
}

// countRecords counts recs under their results. A record that was neither
// rejected, a duplicate nor published takes the result of abort, the
// whole-request failure that stopped it, or canceled when the client went
// away first (its request context canceled, not a deadline the server set):
// that is not the dedupe store's or the queue's failure. A Final abort was
// classified when its call failed, so a client that left only after that
// leaves its Result. With no abort, every such record was published.
func countRecords(ctx context.Context, table string, recs []pendingRecord, abort *requestAbort) {
	stopped := ""
	if abort != nil {
		stopped = abort.Result
		if !abort.Final && clientGone(ctx) {
			stopped = resultRecordsCanceled
		}
	}
	counts := map[string]int64{}
	for i := range recs {
		rec := &recs[i]
		switch {
		case rec.reject != nil:
			counts[rec.reject.Result]++
		case rec.duplicate:
			counts[resultDuplicate]++
		case rec.queued || abort == nil:
			counts[resultQueued]++
		default:
			counts[stopped]++
		}
	}
	for result, n := range counts {
		ingestRecords.Add(ctx, n, observability.MetricAttrs(ctx,
			attribute.String("table", table),
			attribute.String("result", result),
		))
	}
}
