package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestHTTPMetricsMiddleware_RecordsLabeledHistogram pins the middleware's
// contract: every non-skipped request produces a histogram sample with
// route/method/status_class labels. Without this, a refactor that broke the
// chi RoutePattern lookup would silently lose all per-route latency data.
func TestHTTPMetricsMiddleware_RecordsLabeledHistogram(t *testing.T) {
	// Not parallel — swaps the global MeterProvider.
	savedMP := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	r := chi.NewRouter()
	r.Use(httpMetricsMiddleware(""))
	r.Get("/v1/echo/{name}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot) // 4xx
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/echo/alice", nil)
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusTeapot, rec.Code)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	var hist *metricdata.Histogram[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "wavehouse_http_request_duration_seconds" {
				h, ok := m.Data.(metricdata.Histogram[float64])
				require.True(t, ok, "expected Histogram[float64], got %T", m.Data)
				hist = &h
			}
		}
	}
	require.NotNil(t, hist, "wavehouse_http_request_duration_seconds histogram was not recorded")
	require.Len(t, hist.DataPoints, 1)
	attrs := hist.DataPoints[0].Attributes
	route, _ := attrs.Value("route")
	method, _ := attrs.Value("method")
	statusClass, _ := attrs.Value("status_class")
	// The {name} parameter must collapse into the chi RoutePattern, not the
	// raw URL — otherwise cardinality explodes on unique user values.
	assert.Equal(t, "/v1/echo/{name}", route.AsString())
	assert.Equal(t, http.MethodGet, method.AsString())
	assert.Equal(t, "4xx", statusClass.AsString())
}

// TestHTTPMetricsMiddleware_SkipsProbeAndStreamPaths confirms the skip set
// (/health, /ready, /v1/stream/*, metrics scrape path). Cardinality control:
// scrape-path metrics would create a self-loop; long-lived stream metrics
// would record one sample per disconnect with skewed latency.
func TestHTTPMetricsMiddleware_SkipsProbeAndStreamPaths(t *testing.T) {
	savedMP := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	r := chi.NewRouter()
	r.Use(httpMetricsMiddleware("/metrics"))
	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/metrics", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/v1/stream/sse", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	for _, path := range []string{"/health", "/ready", "/metrics", "/v1/stream/sse"} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "wavehouse_http_request_duration_seconds" {
				if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
					assert.Empty(t, h.DataPoints, "no samples should be recorded for skipped paths")
				}
			}
		}
	}
}
