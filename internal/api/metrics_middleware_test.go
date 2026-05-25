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

// Pins: non-skipped requests record a histogram sample with route/method/
// status_class. Without this, a broken RoutePattern lookup silently drops
// all per-route latency data.
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
	// {name} must collapse into the RoutePattern, not blow cardinality.
	assert.Equal(t, "/v1/echo/{name}", route.AsString())
	assert.Equal(t, http.MethodGet, method.AsString())
	assert.Equal(t, "4xx", statusClass.AsString())
}

// /health, /ready, /v1/stream/*, and the scrape path must all skip the
// histogram. Scrape-path samples create a self-loop; long-lived streams
// record one skewed sample per disconnect.
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

	cases := []struct {
		name string
		path string
	}{
		{"health", "/health"},
		{"ready", "/ready"},
		{"metrics", "/metrics"},
		{"stream_sse", "/v1/stream/sse"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, tc.path, nil))
		})
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
