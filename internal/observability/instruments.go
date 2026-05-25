package observability

import (
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// InstrumentScope is the OTel instrumentation-scope name for every WaveHouse
// span and metric. Per-component breakdown lives in metric names and
// span/log attributes (`component=...`), not scope.
const InstrumentScope = "github.com/Wave-RF/WaveHouse"

func Tracer() trace.Tracer { return otel.Tracer(InstrumentScope) }
func Meter() metric.Meter  { return otel.Meter(InstrumentScope) }

// must* panic on registration error. The only failure mode is invalid
// names/descriptions — caller-controlled at init — so a panic surfaces typos
// immediately rather than producing silent no-op instruments.
func mustFloat64Histogram(name string, opts ...metric.Float64HistogramOption) metric.Float64Histogram {
	h, err := Meter().Float64Histogram(name, opts...)
	if err != nil {
		slog.Error("instrument registration failed", "kind", "Float64Histogram", "name", name, "error", err)
		panic(err)
	}
	return h
}

func mustInt64Counter(name string, opts ...metric.Int64CounterOption) metric.Int64Counter {
	c, err := Meter().Int64Counter(name, opts...)
	if err != nil {
		slog.Error("instrument registration failed", "kind", "Int64Counter", "name", name, "error", err)
		panic(err)
	}
	return c
}

var (
	// IngestDuration: end-to-end ingest latency, one record per row.
	// Labels: table, outcome=committed|dlq|dropped.
	IngestDuration = mustFloat64Histogram(
		"wavehouse_ingest_duration_seconds",
		metric.WithDescription("End-to-end ingest latency from HTTP receive to ClickHouse commit, per row"),
		metric.WithUnit("s"),
	)

	// ClickHouseDuration: per-operation ClickHouse latency.
	// Labels: operation=insert|query|admin_query|pipes|structured_query.
	ClickHouseDuration = mustFloat64Histogram(
		"wavehouse_clickhouse_duration_seconds",
		metric.WithDescription("ClickHouse operation latency, by operation"),
		metric.WithUnit("s"),
	)

	// ClickHouseErrors: non-2xx ClickHouse responses.
	// Labels: operation, clickhouse_code (0 when no server-side code).
	ClickHouseErrors = mustInt64Counter(
		"wavehouse_clickhouse_errors_total",
		metric.WithDescription("ClickHouse non-2xx responses, by operation and parsed error code"),
	)

	// HTTPRequestDuration: server-side HTTP latency.
	// Labels: route (chi pattern), method, status_class.
	HTTPRequestDuration = mustFloat64Histogram(
		"wavehouse_http_request_duration_seconds",
		metric.WithDescription("Server-side HTTP request latency, by route template and status class"),
		metric.WithUnit("s"),
	)

	// SchemaRejected: ingest payloads rejected by schema validation.
	// Labels: table, reason.
	SchemaRejected = mustInt64Counter(
		"wavehouse_schema_validation_rejected_total",
		metric.WithDescription("Ingest payloads rejected by schema validation, by table and reason"),
	)

	// AuthFailures: JWT verify failures. Labels: reason.
	AuthFailures = mustInt64Counter(
		"wavehouse_auth_failures_total",
		metric.WithDescription("JWT authentication failures, by reason"),
	)

	// CacheHits/CacheMisses: cache outcomes by tier. Today only L1 emits;
	// the `tier` label is forward-compat for a future L2.
	CacheHits = mustInt64Counter(
		"wavehouse_cache_hits_total",
		metric.WithDescription("Cache hits, by tier"),
	)
	CacheMisses = mustInt64Counter(
		"wavehouse_cache_misses_total",
		metric.WithDescription("Cache misses, by tier"),
	)

	// QuerySingleflightShared: requests collapsed into a concurrent caller's
	// in-flight execution (singleflight sf.Do returned shared=true).
	// Labels: surface=structured_query|pipes.
	QuerySingleflightShared = mustInt64Counter(
		"wavehouse_query_singleflight_shared_total",
		metric.WithDescription("Queries whose fill execution was coalesced via singleflight (sf.Do shared=true), by surface"),
	)

	// DedupeLookups: dedupe Has/Put outcomes.
	// Labels: table, outcome=hit|miss|err.
	DedupeLookups = mustInt64Counter(
		"wavehouse_dedupe_lookups_total",
		metric.WithDescription("Dedupe Has/Put lookups, by table and outcome"),
	)

	// IngestPublishThrottled: ingest requests rejected with 503 because
	// JetStream is at capacity. Labels: table.
	IngestPublishThrottled = mustInt64Counter(
		"wavehouse_ingest_publish_throttled_total",
		metric.WithDescription("Ingest publish requests rejected with 503 because JetStream is at capacity"),
	)
)
