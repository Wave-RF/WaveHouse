package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// httpMetricsMiddleware records server-side request latency as a histogram
// labeled by route template, method, and status class (1xx..5xx). Skip set
// matches the otelhttp tracer (`router.go`): SSE/WS streams, /health, /ready,
// and the Prometheus scrape path. Same rationale — long-lived connections
// produce one record per stream and probe-path cardinality drowns out
// user-traffic signal.
//
// Route label uses chi's RoutePattern (e.g. "/v1/pipes/{name}") rather than
// the raw URL, so {name} doesn't blow cardinality. Pre-routing failures
// (404 fallthrough) get "unmatched".
func httpMetricsMiddleware(metricsPath string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := r.URL.Path
			if strings.HasPrefix(p, "/v1/stream/") || p == "/health" || p == "/ready" ||
				(metricsPath != "" && p == metricsPath) {
				next.ServeHTTP(w, r)
				return
			}

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)

			pattern := chi.RouteContext(r.Context()).RoutePattern()
			if pattern == "" {
				pattern = "unmatched"
			}
			status := ww.Status()
			if status == 0 {
				// Handler never wrote a status — net/http would emit 200, mirror that.
				status = http.StatusOK
			}
			observability.HTTPRequestDuration.Record(r.Context(), time.Since(start).Seconds(),
				metric.WithAttributes(
					attribute.String("route", pattern),
					attribute.String("method", r.Method),
					attribute.String("status_class", fmt.Sprintf("%dxx", status/100)),
				))
		})
	}
}
