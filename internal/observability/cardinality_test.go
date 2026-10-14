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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// collectSums records each attribute set once on a counter named name, behind
// tableView, and returns the exported data points as attribute set → value.
func collectSums(t *testing.T, view sdkmetric.View, name string, sets ...[]attribute.KeyValue) map[string]int64 {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(view))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	counter, err := mp.Meter("test").Int64Counter(name)
	require.NoError(t, err)
	for _, attrs := range sets {
		counter.Add(t.Context(), 1, metric.WithAttributes(attrs...))
	}
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

// TestTableAttr_MatchesTheRecorders pins the key: the seven recorders spell
// "table" as a literal, so a rename here would silently leave them unbounded.
func TestTableAttr_MatchesTheRecorders(t *testing.T) {
	t.Parallel()
	assert.Equal(t, attribute.Key("table"), TableAttr)
}

func TestTableView(t *testing.T) {
	t.Parallel()
	sets := [][]attribute.KeyValue{
		{TableAttr.String("clicks"), attribute.String("role", "viewer")},
		{TableAttr.String("users"), attribute.String("role", "viewer")},
		{TableAttr.String("clicks"), attribute.String("role", "admin")},
	}
	tests := []struct {
		name      string
		aggregate bool
		metric    string
		want      map[string]int64
	}{
		{"per table keeps the label", false, "wavehouse_test_total", map[string]int64{
			"role=viewer,table=clicks": 1,
			"role=viewer,table=users":  1,
			"role=admin,table=clicks":  1,
		}},
		{"aggregate sums over every table", true, "wavehouse_test_total", map[string]int64{
			"role=viewer": 2,
			"role=admin":  1,
		}},
		{"aggregate leaves a library instrument alone", true, "http.server.test", map[string]int64{
			"role=viewer,table=clicks": 1,
			"role=viewer,table=users":  1,
			"role=admin,table=clicks":  1,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, collectSums(t, tableView(tt.aggregate, TableWarnThreshold), tt.metric, sets...))
		})
	}
}

// TestTableView_SumsAnObservableUpDownCounter pins "summed" for the
// asynchronous kind a per-table size metric would use.
func TestTableView_SumsAnObservableUpDownCounter(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(tableView(true, TableWarnThreshold)))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	_, err := mp.Meter("test").Int64ObservableUpDownCounter("wavehouse_test_bytes",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(10, metric.WithAttributes(TableAttr.String("clicks")))
			o.Observe(32, metric.WithAttributes(TableAttr.String("users")))
			return nil
		}))
	require.NoError(t, err)
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	dps := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64]).DataPoints
	require.Len(t, dps, 1)
	assert.Equal(t, int64(42), dps[0].Value)
	assert.Equal(t, 0, dps[0].Attributes.Len())
}

// TestTableView_WarnsOncePastThreshold captures the default logger, so it is
// serial.
func TestTableView_WarnsOncePastThreshold(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelWarn)
	var sets [][]attribute.KeyValue
	for i := range 3 {
		sets = append(sets, []attribute.KeyValue{TableAttr.String(fmt.Sprintf("t%d", i))})
	}
	// The same tables again: a repeat is never a new table.
	collectSums(t, tableView(false, 3), "wavehouse_test_total", append(sets, sets...)...)
	assert.Empty(t, logs.String(), "three tables at a threshold of three must not warn")

	logs = logtest.Capture(t, slog.LevelWarn)
	for i := 3; i < 6; i++ {
		sets = append(sets, []attribute.KeyValue{TableAttr.String(fmt.Sprintf("t%d", i))})
	}
	got := collectSums(t, tableView(false, 3), "wavehouse_test_total", sets...)
	assert.Len(t, got, 6, "the warning must not drop the label")
	out := logs.String()
	assert.Equal(t, 1, strings.Count(out, "\n"), "one warning, however far past the threshold: %s", out)
	assert.Contains(t, out, "WH_METRICS_PER_TABLE=false")
	assert.Contains(t, out, `"threshold":3`)
}

// TestInitProvider_AggregateTables scrapes the Prometheus handler: the view is
// on the provider every global-meter instrument binds to.
func TestInitProvider_AggregateTables(t *testing.T) {
	// No t.Parallel(): InitProvider mutates global OTel state.
	savedProp := otel.GetTextMapPropagator()
	savedMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTextMapPropagator(savedProp)
		otel.SetMeterProvider(savedMP)
	})

	for _, aggregate := range []bool{false, true} {
		shutdown, promHandler, err := InitProvider(t.Context(), testIdentity, ProviderConfig{PrometheusEnabled: true, AggregateTables: aggregate})
		require.NoError(t, err)
		counter, err := otel.Meter("test").Int64Counter("wavehouse_cardinality_test_total")
		require.NoError(t, err)
		counter.Add(t.Context(), 1, metric.WithAttributes(TableAttr.String("clicks")))
		counter.Add(t.Context(), 1, metric.WithAttributes(TableAttr.String("users")))

		rec := httptest.NewRecorder()
		promHandler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = shutdown(ctx)
		cancel()

		if aggregate {
			assert.Regexp(t, `(?m)^wavehouse_cardinality_test_total\{otel_scope_name="test"[^}]*\} 2$`, string(body))
			assert.NotContains(t, string(body), `table="`)
		} else {
			assert.Regexp(t, `(?m)^wavehouse_cardinality_test_total\{[^}]*table="clicks"[^}]*\} 1$`, string(body))
			assert.Regexp(t, `(?m)^wavehouse_cardinality_test_total\{[^}]*table="users"[^}]*\} 1$`, string(body))
		}
	}
}
