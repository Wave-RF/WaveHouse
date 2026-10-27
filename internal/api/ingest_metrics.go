package api

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
		metric.WithDescription("Records POST /v1/ingest read, by result: queued, duplicate, in_flight, a rejection (invalid_body, schema, forbidden, missing_id) or a failure (dedupe_unavailable, queue_full, queue_unavailable, publish_failed)"),
	)
}

// countRecords counts recs under their results. A record that was neither
// rejected, a duplicate nor published takes the result of abort, the
// whole-request failure that stopped it; with no abort, every such record was
// published.
func countRecords(ctx context.Context, table string, recs []pendingRecord, abort *requestAbort) {
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
			counts[abort.Result]++
		}
	}
	for result, n := range counts {
		ingestRecords.Add(ctx, n, metric.WithAttributes(
			attribute.String("table", table),
			attribute.String("result", result),
		))
	}
}
