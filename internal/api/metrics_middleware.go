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

// httpMetricsMiddleware records server-side request latency by route, method,
// and status_class. Route uses chi's RoutePattern so URL params collapse into
// one bucket. Skip set matches the otelhttp tracer (router.go) — same
// rationale: long-lived streams and probe paths swamp user-traffic signal.
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
				// net/http emits 200 when nothing wrote a status.
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
