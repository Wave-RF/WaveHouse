package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// dropLabels sets which labels ZeroCounter treats as dropped for the test,
// as InitProvider would.
func dropLabels(t *testing.T, dropped Carries) {
	t.Helper()
	saved := labelsDropped.Load()
	labelsDropped.Store(uint32(dropped))
	t.Cleanup(func() { labelsDropped.Store(saved) })
}

// zeroSums builds a counter on a ManualReader, zero-publishes sets on it and
// returns the exported points as attribute set → value.
func zeroSums(t *testing.T, carries Carries, sets ...attribute.Set) map[string]int64 {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	counter, err := mp.Meter("test").Int64Counter("wavehouse_zero_test_total")
	require.NoError(t, err)
	ZeroCounter(counter, carries, sets...)
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				got[dp.Attributes.Encoded(attribute.DefaultEncoder())] = dp.Value
			}
		}
	}
	return got
}

func TestZeroCounter_PublishesEachSetAtZero(t *testing.T) {
	dropLabels(t, 0)
	got := zeroSums(t, 0, LabelSets(Values("op", "get", "put"), Values("outcome", "ok", "error"))...)
	assert.Equal(t, map[string]int64{
		"op=get,outcome=ok": 0, "op=get,outcome=error": 0,
		"op=put,outcome=ok": 0, "op=put,outcome=error": 0,
	}, got)
}

func TestZeroCounter_NoSetsIsTheUnlabeledSeries(t *testing.T) {
	dropLabels(t, 0)
	assert.Equal(t, map[string]int64{"": 0}, zeroSums(t, 0))
}

func TestZeroCounter_OnlyWhereEveryCarriedLabelIsDropped(t *testing.T) {
	sets := LabelSets(Values("reason", "a", "b"))
	for _, tc := range []struct {
		name    string
		dropped Carries
		carries Carries
		publish bool
	}{
		{"tenant carried, kept", 0, CarriesTenant, false},
		{"tenant carried, dropped", CarriesTenant, CarriesTenant, true},
		{"both carried, only tenant dropped", CarriesTenant, CarriesTable | CarriesTenant, false},
		{"both carried, only table dropped", CarriesTable, CarriesTable | CarriesTenant, false},
		{"both carried, both dropped", CarriesTable | CarriesTenant, CarriesTable | CarriesTenant, true},
		{"none carried", 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dropLabels(t, tc.dropped)
			got := zeroSums(t, tc.carries, sets...)
			if tc.publish {
				assert.Equal(t, map[string]int64{"reason=a": 0, "reason=b": 0}, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

func TestZeroCounter_NilCounter(t *testing.T) {
	dropLabels(t, 0)
	assert.NotPanics(t, func() { ZeroCounter(nil, 0) })
}

func TestLabelSets(t *testing.T) {
	assert.Len(t, LabelSets(Values("a", "1", "2", "3"), Values("b", "x", "y")), 6)
	assert.Equal(t, []attribute.Set{*attribute.EmptySet()}, LabelSets())
	assert.Empty(t, LabelSets(Values("a")), "a dimension with no values has no combinations")
}

// TestInitProvider_ZeroSeriesIsTheExportedSeries is the end to end case: with
// table and tenant aggregated, a zero-published series is the one a later
// measurement carrying both labels increments, so the scrape goes 0 → 1 on
// one series rather than showing a second one. With tenant kept, nothing is
// zero-published.
func TestInitProvider_ZeroSeriesIsTheExportedSeries(t *testing.T) {
	// No t.Parallel(): InitProvider mutates global OTel state.
	savedProp := otel.GetTextMapPropagator()
	savedMP := otel.GetMeterProvider()
	savedDropped := labelsDropped.Load()
	savedTenantsOff := processTenants.off.Load()
	t.Cleanup(func() {
		otel.SetTextMapPropagator(savedProp)
		otel.SetMeterProvider(savedMP)
		labelsDropped.Store(savedDropped)
		processTenants.off.Store(savedTenantsOff)
	})

	scrape := func(h http.Handler) string {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		return string(body)
	}
	const series = `(?m)^wavehouse_zero_e2e_total\{otel_scope_name="test"[^}]*reason="a"[^}]*\} `

	for _, aggregate := range []bool{true, false} {
		shutdown, promHandler, err := InitProvider(t.Context(), testIdentity, ProviderConfig{
			PrometheusEnabled: true, AggregateTables: aggregate, AggregateTenants: aggregate,
		})
		require.NoError(t, err)
		counter, err := otel.Meter("test").Int64Counter("wavehouse_zero_e2e_total")
		require.NoError(t, err)
		ZeroCounter(counter, CarriesTable|CarriesTenant, LabelSets(Values("reason", "a"))...)

		before := scrape(promHandler)
		counter.Add(t.Context(), 1, metric.WithAttributes(
			attribute.String("reason", "a"), TableAttr.String("clicks"), TenantLabel("acme")))
		after := scrape(promHandler)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = shutdown(ctx)
		cancel()

		if aggregate {
			assert.Regexp(t, series+`0$`, before)
			assert.Regexp(t, series+`1$`, after)
			assert.NotContains(t, after, `table="`)
			assert.NotContains(t, after, `tenant="`)
		} else {
			assert.NotContains(t, before, "wavehouse_zero_e2e_total{")
			assert.Regexp(t, `(?m)^wavehouse_zero_e2e_total\{[^}]*tenant="acme"[^}]*\} 1$`, after)
		}
	}
}
