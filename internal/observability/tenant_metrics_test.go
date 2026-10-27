package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

func TestTenantAttr_IsTheLogAndSpanKey(t *testing.T) {
	t.Parallel()
	assert.Equal(t, attribute.Key("tenant"), TenantAttr)
}

// TestTenantLabels_CapsAndWarnsOnce captures the default logger, so it is
// serial.
func TestTenantLabels_CapsAndWarnsOnce(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelWarn)
	l := newTenantLabels(2)
	assert.Equal(t, "acme", l.value("acme"))
	assert.Equal(t, "globex", l.value("globex"))
	assert.Equal(t, "acme", l.value("acme"), "a repeat is never a new tenant")
	assert.Empty(t, logs.String(), "two tenants at a limit of two must not warn")

	assert.Equal(t, TenantOverflow, l.value("initech"))
	assert.Equal(t, TenantOverflow, l.value("umbrella"))
	assert.Equal(t, "globex", l.value("globex"), "a labeled tenant keeps its label past the cap")
	out := logs.String()
	assert.Equal(t, 1, strings.Count(out, "\n"), "one warning, however far past the limit: %s", out)
	assert.Contains(t, out, "WH_METRICS_PER_TENANT=false")
	assert.Contains(t, out, `"limit":2`)
}

// TestTenantLabels_OffNeitherCapsNorWarns: with the label dropped by the view
// (per_tenant_metrics: false) a value is never exported, so the setting the
// warning names is already set. Captures the default logger, so it is serial.
func TestTenantLabels_OffNeitherCapsNorWarns(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelWarn)
	l := newTenantLabels(1)
	l.off.Store(true)
	assert.Equal(t, "acme", l.value("acme"))
	assert.Equal(t, "globex", l.value("globex"))
	assert.Empty(t, logs.String())
}

// TestProcessTenants_OffUntilAProviderExports: a process that exports no
// metrics (the default config never calls InitProvider) must not warn about a
// label nobody sees.
func TestProcessTenants_OffUntilAProviderExports(t *testing.T) {
	t.Parallel()
	assert.True(t, newProcessTenants().off.Load())
}

func TestMetricAttrs(t *testing.T) {
	t.Parallel()
	role := attribute.String("role", "viewer")
	tenantOf := func(ctx context.Context) (string, bool) {
		set := metric.NewAddConfig([]metric.AddOption{MetricAttrs(ctx, role)}).Attributes()
		assert.True(t, set.HasValue("role"), "the measurement's own attributes are kept")
		v, ok := set.Value(TenantAttr)
		return v.AsString(), ok
	}

	_, ok := tenantOf(t.Context())
	assert.False(t, ok, "no tenant outside a request or a WithMetricTenant")

	got, ok := tenantOf(WithMetricTenant(t.Context(), "acme"))
	assert.True(t, ok)
	assert.Equal(t, "acme", got)

	req := WithRequest(t.Context(), "req-1")
	SetRequestTenant(req, "acme")
	_, ok = tenantOf(req)
	assert.False(t, ok, "a request's tenant is not a label until it is served: the client names it")
	SetRequestServed(req)
	got, ok = tenantOf(req)
	assert.True(t, ok)
	assert.Equal(t, "acme", got)
	SetRequestTenant(req, "globex")
	_, ok = tenantOf(req)
	assert.False(t, ok, "naming another tenant takes the served mark back")
}

// collectAll collects every data point the view exports for one instrument
// of each kind recorded through record, by metric name.
func collectAll(t *testing.T, view sdkmetric.View, scope string, record func(metric.Meter)) map[string][]attribute.Set {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(view))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	record(mp.Meter(scope))
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	got := map[string][]attribute.Set{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					got[m.Name] = append(got[m.Name], dp.Attributes)
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					got[m.Name] = append(got[m.Name], dp.Attributes)
				}
			case metricdata.Histogram[int64]:
				for _, dp := range d.DataPoints {
					got[m.Name] = append(got[m.Name], dp.Attributes)
				}
			}
		}
	}
	return got
}

func TestMetricsView_Tenant(t *testing.T) {
	t.Parallel()
	record := func(m metric.Meter) {
		c, err := m.Int64Counter("wavehouse_test_total")
		require.NoError(t, err)
		c.Add(t.Context(), 1, metric.WithAttributes(TenantAttr.String("acme"), attribute.String("result", "ok")))
		c.Add(t.Context(), 1, metric.WithAttributes(TenantAttr.String("globex"), attribute.String("result", "ok")))
	}
	kept := collectAll(t, metricsView(viewOptions{}), "test", record)
	assert.Len(t, kept["wavehouse_test_total"], 2, "per tenant by default")

	summed := collectAll(t, metricsView(viewOptions{aggregateTenants: true}), "test", record)
	require.Len(t, summed["wavehouse_test_total"], 1, "one series summed over every tenant")
	assert.Equal(t, attribute.NewSet(attribute.String("result", "ok")), summed["wavehouse_test_total"][0])
}

func TestMetricsView_HTTPServer(t *testing.T) {
	t.Parallel()
	attrs := metric.WithAttributes(
		semconv.ServerAddress("acme.example.com"), semconv.ServerPort(8443),
		attribute.String("http.request.method", "GET"), attribute.String("http.route", "/v1/query"))
	record := func(m metric.Meter) {
		d, err := m.Float64Histogram("http.server.request.duration")
		require.NoError(t, err)
		d.Record(t.Context(), .1, attrs)
		for _, name := range []string{"http.server.request.body.size", "http.server.response.body.size"} {
			b, err := m.Int64Histogram(name)
			require.NoError(t, err)
			b.Record(t.Context(), 10, attrs)
		}
	}
	want := attribute.NewSet(attribute.String("http.request.method", "GET"), attribute.String("http.route", "/v1/query"))

	got := collectAll(t, metricsView(viewOptions{}), otelhttpScope, record)
	assert.Equal(t, []attribute.Set{want}, got["http.server.request.duration"], "server.address and server.port are dropped")
	assert.NotContains(t, got, "http.server.request.body.size", "body sizes are opt-in")
	assert.NotContains(t, got, "http.server.response.body.size", "body sizes are opt-in")

	got = collectAll(t, metricsView(viewOptions{httpBodySize: true}), otelhttpScope, record)
	assert.Equal(t, []attribute.Set{want}, got["http.server.request.body.size"])
	assert.Equal(t, []attribute.Set{want}, got["http.server.response.body.size"])

	got = collectAll(t, metricsView(viewOptions{}), "another-library", record)
	assert.Len(t, got["http.server.request.body.size"], 1, "only otelhttp's instruments are touched")
}

// TestInitProvider_TenantLabel scrapes the Prometheus handler: the cap and the
// setting reach the provider every global-meter instrument binds to.
func TestInitProvider_TenantLabel(t *testing.T) {
	// No t.Parallel(): InitProvider mutates global OTel state.
	savedProp := otel.GetTextMapPropagator()
	savedMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTextMapPropagator(savedProp)
		otel.SetMeterProvider(savedMP)
	})

	for _, aggregate := range []bool{false, true} {
		shutdown, promHandler, err := InitProvider(t.Context(), testIdentity, ProviderConfig{PrometheusEnabled: true, AggregateTenants: aggregate})
		require.NoError(t, err)
		counter, err := otel.Meter("test").Int64Counter("wavehouse_tenant_label_test_total")
		require.NoError(t, err)
		for _, id := range []string{"acme", "globex"} {
			counter.Add(t.Context(), 1, MetricAttrs(WithMetricTenant(t.Context(), id)))
		}

		rec := httptest.NewRecorder()
		promHandler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		var lines []string
		for line := range strings.SplitSeq(string(body), "\n") {
			if strings.HasPrefix(line, "wavehouse_tenant_label_test_total{") {
				lines = append(lines, line)
			}
		}
		if aggregate {
			require.Len(t, lines, 1, "%s", body)
			assert.NotContains(t, lines[0], `tenant=`)
			assert.True(t, strings.HasSuffix(lines[0], " 2"), lines[0])
		} else {
			require.Len(t, lines, 2, "%s", body)
			for i, id := range []string{"acme", "globex"} {
				assert.Contains(t, lines[i], fmt.Sprintf(`tenant=%q`, id))
			}
		}
		require.NoError(t, shutdown(t.Context()))
	}
}
