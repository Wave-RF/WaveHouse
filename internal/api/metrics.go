package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// latencyBuckets are the boundaries, in seconds, of the request and query
// latency histograms: Prometheus' default buckets, plus 30s for a query that
// runs up to a long query_timeout.
var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// routeUnmatched is the route label of a request that reached no route or
// mount: an unknown top-level path, a method chi does not route, or a CORS
// preflight answered before routing. One answered inside a mount short of a
// route (an unknown path or an unsupported method under /v1) carries the
// mount's pattern, /v1/*, as does one the /v1/ops gates refuse, which run
// before its routes (/v1/ops/*); one a group middleware refuses (a tenant
// not served) has matched its route already and carries that.
const routeUnmatched = "unmatched"

// httpMetrics records each request's latency by route, method and status
// class. The zero value (a nil *httpMetrics) is a no-op.
type httpMetrics struct {
	duration metric.Float64Histogram
}

// meterName is the meter the api package's instruments are built on.
const meterName = "wavehouse-api"

// newHTTPMetrics builds the HTTP instruments on the global meter provider.
// Call it after observability.InitProvider, like every other instrument.
func newHTTPMetrics() *httpMetrics { return httpMetricsOn(otel.Meter(meterName)) }

// httpMetricsOn builds the HTTP instruments on meter.
func httpMetricsOn(meter metric.Meter) *httpMetrics {
	duration, _ := meter.Float64Histogram("wavehouse_http_request_duration_seconds",
		metric.WithDescription("HTTP request latency by route (the matched route pattern), method and status_class (2xx, 3xx, 4xx, 5xx); SSE streams on /v1/stream are not counted"),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(latencyBuckets...))
	return &httpMetrics{duration: duration}
}

// middleware times every request but the SSE streams, whose lifetime is
// wavehouse_sse_stream_duration_seconds, not a latency. It runs outside
// jsonRecoverer, so a recovered panic counts as the 500 it answers.
func (m *httpMetrics) middleware(next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/stream") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		// The route pattern is complete only once routing has run, so it is
		// read after the handler, never before.
		route := routeUnmatched
		if rctx := chi.RouteContext(r.Context()); rctx != nil {
			if p := rctx.RoutePattern(); p != "" {
				route = p
			}
		}
		// The request's tenant, once a served one is resolved, is read
		// after the handler too: the scope it lives on is filled in place.
		ctx := r.Context()
		m.duration.Record(ctx, time.Since(start).Seconds(), observability.MetricAttrs(ctx,
			attribute.String("route", route),
			attribute.String("method", methodLabel(r.Method)),
			attribute.String("status_class", statusClass(ww.Status())),
		))
	})
}

// methodLabel is method when it is one of net/http's, and "other" otherwise:
// the method is the client's to choose, and a label must stay a closed set.
func methodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	}
	return "other"
}

// statusClass is status's class, "2xx" for a handler that wrote nothing
// (net/http answers it 200).
func statusClass(status int) string {
	switch {
	case status == 0 || status >= 200 && status < 300:
		return "2xx"
	case status < 200:
		return "1xx"
	case status < 400:
		return "3xx"
	case status < 500:
		return "4xx"
	}
	return "5xx"
}

// Operations for the "operation" attribute on
// wavehouse_clickhouse_query_duration_seconds: which query path ran the
// statement.
const (
	opStructuredQuery = "query"      // POST /v1/query
	opPipe            = "pipe"       // a pipe that reads, /v1/pipes/{name}
	opPipeWrite       = "pipe_write" // a pipe that writes, /v1/pipes/{name}
	opOpsQuery        = "ops_query"  // raw SQL, POST /v1/ops/query
)

// Results that are not an error code: a query ClickHouse answered, and one
// cut short because its client went away.
const (
	resultOK       = "ok"
	resultCanceled = "canceled"
)

// queryMetrics records each ClickHouse call the query paths make: one per
// statement sent, so a cache hit records none and callers coalesced onto one
// call record it once. The zero value (a nil *queryMetrics) is a no-op.
type queryMetrics struct {
	duration metric.Float64Histogram
}

// newQueryMetrics builds the query-path instruments on the global meter
// provider. Call it after observability.InitProvider.
func newQueryMetrics() *queryMetrics { return queryMetricsOn(otel.Meter(meterName)) }

// queryMetricsOn builds the query-path instruments on meter.
func queryMetricsOn(meter metric.Meter) *queryMetrics {
	duration, _ := meter.Float64Histogram("wavehouse_clickhouse_query_duration_seconds",
		metric.WithDescription("ClickHouse calls on the query paths by operation (query, pipe, pipe_write, ops_query) and result: ok, canceled (the client went away), or the error code the response carries without its clickhouse. prefix (rejected, limit_exceeded, access_denied, misconfigured, unavailable, response_too_large, unknown)"),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(latencyBuckets...))
	return &queryMetrics{duration: duration}
}

// observe records one call of operation that took d and ended in err,
// classed as the response classes it (chFailureOf, under caps); reqCtx is the
// request's own context. A call cut short because the client went away is
// resultCanceled, not the unavailable its response would read as: nobody
// reads that response, and ClickHouse was not the one that failed. It returns
// the result recorded, for the call's span.
func (m *queryMetrics) observe(reqCtx context.Context, operation string, d time.Duration, err error, caps queryCaps) string {
	result := resultOK
	switch {
	case err == nil:
	case errors.Is(err, context.Canceled) && reqCtx.Err() != nil:
		result = resultCanceled
	default:
		result = chResult(chFailureOf(err, http.StatusInternalServerError, caps).code)
	}
	m.record(reqCtx, operation, d, result)
	return result
}

// record records one call of operation that took d, with result as given,
// labeled with the request's tenant (#891).
func (m *queryMetrics) record(reqCtx context.Context, operation string, d time.Duration, result string) {
	if m == nil {
		return
	}
	m.duration.Record(reqCtx, d.Seconds(), observability.MetricAttrs(reqCtx,
		attribute.String("operation", operation), attribute.String("result", result)))
}

// chResult is the "result" label of a failure answered with code.
func chResult(code string) string {
	return strings.TrimPrefix(code, "clickhouse.")
}
