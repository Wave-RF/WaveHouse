package observability

import (
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// InstrumentScope is the single OTel instrumentation-scope name used by every
// WaveHouse-produced span and metric. The OTel convention is one scope per
// library — WaveHouse is the library here, and the per-component breakdown
// already lives in metric names (`wavehouse_ingest_*`, `wavehouse_clickhouse_*`)
// and span/log attributes (`component=api/ingest_handler`).
const InstrumentScope = "github.com/Wave-RF/WaveHouse"

// Tracer returns the WaveHouse tracer. Spans produced here flow through the
// globally-installed TracerProvider, which is no-op until InitProvider runs
// and a real provider is wired.
func Tracer() trace.Tracer { return otel.Tracer(InstrumentScope) }

// Meter returns the WaveHouse meter. Same global-resolution semantics as
// Tracer — instruments created against this meter are safe to declare at
// package load time; the global OTel proxy re-resolves the underlying
// provider on each Record/Add call.
func Meter() metric.Meter { return otel.Meter(InstrumentScope) }

// mustFloat64Histogram + mustInt64Counter are package-private constructors that
// panic on registration error. The only failure mode is invalid names /
// descriptions — all caller-controlled — so panicking at init surfaces typos
// immediately instead of letting them propagate as silent no-op instruments.
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

// Custom instruments — declared once at package load, used everywhere.
//
// Naming follows the convention from issue #94:
//   - `wavehouse_<component>_duration_seconds` for latency histograms
//   - `wavehouse_<component>_<noun>_total` for monotonic counters
//   - `wavehouse_<component>_<noun>` for gauges
//
// Histogram bucket boundaries inherit the OTel SDK defaults (Prometheus-style
// fixed buckets via the Prometheus exporter, exponential via OTLP push). If
// future dashboards reveal poor resolution at the long tail we'll add explicit
// View definitions in InitProvider.
var (
	// IngestDuration is the end-to-end ingest latency (HTTP receive →
	// ClickHouse commit), recorded once per row from the bento worker.
	// Labels: table, outcome=committed|dlq|dropped.
	IngestDuration = mustFloat64Histogram(
		"wavehouse_ingest_duration_seconds",
		metric.WithDescription("End-to-end ingest latency from HTTP receive to ClickHouse commit, per row"),
		metric.WithUnit("s"),
	)

	// ClickHouseDuration is the per-operation latency for any ClickHouse
	// HTTP request — INSERT batches, /v1/query proxy, /v1/admin/query proxy,
	// pipes execution, structured-query execution.
	// Labels: operation=insert|query|admin_query|pipes|structured.
	ClickHouseDuration = mustFloat64Histogram(
		"wavehouse_clickhouse_duration_seconds",
		metric.WithDescription("ClickHouse operation latency, by operation"),
		metric.WithUnit("s"),
	)

	// ClickHouseErrors counts non-2xx ClickHouse responses by operation and
	// ClickHouse error code (parsed from response body; 0 if unparsed).
	// Pairs with ClickHouseDuration for SLO error-rate views.
	ClickHouseErrors = mustInt64Counter(
		"wavehouse_clickhouse_errors_total",
		metric.WithDescription("ClickHouse non-2xx responses, by operation and parsed error code"),
	)

	// HTTPRequestDuration is the HTTP server-side latency, labeled by route
	// template (chi pattern) and status class. Complements the otelhttp
	// instrumentation which records spans but no per-route latency.
	HTTPRequestDuration = mustFloat64Histogram(
		"wavehouse_http_request_duration_seconds",
		metric.WithDescription("Server-side HTTP request latency, by route template and status class"),
		metric.WithUnit("s"),
	)

	// SchemaRejected counts ingest payloads rejected during schema validation.
	// Labels: table, reason=unknown_field|type_mismatch|null_violation|empty.
	SchemaRejected = mustInt64Counter(
		"wavehouse_schema_validation_rejected_total",
		metric.WithDescription("Ingest payloads rejected by schema validation, by table and reason"),
	)

	// AuthFailures counts JWT verification failures.
	// Labels: reason=no_token|bad_signature|expired|jwks_fetch_failed|missing_role_claim|malformed.
	AuthFailures = mustInt64Counter(
		"wavehouse_auth_failures_total",
		metric.WithDescription("JWT authentication failures, by reason"),
	)

	// CacheHits and CacheMisses split the cache outcomes by tier. The L2 tier
	// is reserved for a future shared cache; today only L1 (Ristretto) emits.
	CacheHits = mustInt64Counter(
		"wavehouse_cache_hits_total",
		metric.WithDescription("Cache hits, by tier"),
	)
	CacheMisses = mustInt64Counter(
		"wavehouse_cache_misses_total",
		metric.WithDescription("Cache misses, by tier"),
	)

	// QuerySingleflightShared counts requests whose fill function call was
	// COLLAPSED into a concurrent caller's in-flight execution by
	// golang.org/x/sync/singleflight. shared=true on the sf.Do return value
	// means "you shared this result with at least one other caller" — i.e.
	// the work was done once and N callers got the same value. Pairs with
	// CacheHits/CacheMisses for the structured-query and pipes handlers'
	// effectiveness story.
	QuerySingleflightShared = mustInt64Counter(
		"wavehouse_query_singleflight_shared_total",
		metric.WithDescription("Queries whose fill execution was coalesced via singleflight (sf.Do shared=true)"),
	)

	// DedupeLookups counts dedupe lookups by outcome.
	// Labels: table, outcome=hit|miss|err.
	DedupeLookups = mustInt64Counter(
		"wavehouse_dedupe_lookups_total",
		metric.WithDescription("Dedupe Has/Put lookups, by table and outcome"),
	)

	// IngestPublishThrottled counts NATS-publish 503s — JetStream stream full,
	// API returned Retry-After. Backed by the same path that emits the 503.
	IngestPublishThrottled = mustInt64Counter(
		"wavehouse_ingest_publish_throttled_total",
		metric.WithDescription("Ingest publish requests rejected with 503 because JetStream is at capacity"),
	)
)
