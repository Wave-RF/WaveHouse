package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRegisterSystemMetrics_NilInputs(t *testing.T) {
	// No t.Parallel(): all three TestRegisterSystemMetrics_* tests mutate the
	// global meter provider via otel.SetMeterProvider, and RegisterSystemMetrics
	// reads it via otel.Meter — running them in parallel races on the global.
	// They serialize against TestInitProvider_Shutdown (also non-parallel) for
	// the same reason.

	// Use an SDK meter provider so RegisterCallback actually runs. Save and
	// restore the prior global so this test doesn't leave the package's global
	// pointing at a shut-down provider after cleanup. Same pattern as
	// TestInitProvider_Shutdown in provider_test.go.
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	err := RegisterSystemMetrics(nil, nil)
	require.NoError(t, err)

	// Collect — the callback should run without panicking even when both
	// dependencies are nil.
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
}

func TestRegisterSystemMetrics_WithDedup(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	pebbleStats := func() map[string]int64 {
		return map[string]int64{"pebble_wal_size": 1024, "pebble_table_count": 7}
	}
	require.NoError(t, RegisterSystemMetrics(nil, pebbleStats))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	// We should see the four gauges registered by RegisterSystemMetrics.
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	require.Contains(t, names, "wavehouse_pebble_wal_size")
	require.Contains(t, names, "wavehouse_pebble_table_count")
}

// useManualReader points the global meter provider at a fresh manual reader
// for the test, restoring the previous provider afterwards.
func useManualReader(t *testing.T) *metric.ManualReader {
	t.Helper()
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})
	return reader
}

// int64Points collects every int64 gauge and sum point, keyed by the metric
// name, plus "/<direction>" when the point has one.
func int64Points(t *testing.T, reader *metric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	add := func(name string, dp metricdata.DataPoint[int64]) {
		if d, ok := dp.Attributes.Value("direction"); ok {
			name += "/" + d.AsString()
		}
		got[name] = dp.Value
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					add(m.Name, dp)
				}
			case metricdata.Sum[int64]:
				require.True(t, data.IsMonotonic, "%s is a counter", m.Name)
				for _, dp := range data.DataPoints {
					add(m.Name, dp)
				}
			}
		}
	}
	return got
}

func TestRegisterSystemMetrics_WithMQStats(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	reader := useManualReader(t)

	calls := 0
	mqStats := func() (MQStats, error) {
		calls++
		return MQStats{Connections: 1, InMsgs: 42, OutMsgs: 7, InBytes: 4200, OutBytes: 700, Reconnects: 2}, nil
	}
	require.NoError(t, RegisterSystemMetrics(mqStats, nil))

	got := int64Points(t, reader)
	require.Equal(t, 1, calls, "stats are read once per scrape")
	require.Equal(t, map[string]int64{
		"wavehouse_mq_client_messages_total/in":  42,
		"wavehouse_mq_client_messages_total/out": 7,
		"wavehouse_mq_client_bytes_total/in":     4200,
		"wavehouse_mq_client_bytes_total/out":    700,
		"wavehouse_mq_client_reconnects_total":   2,
		// Deprecated, unchanged.
		"wavehouse_nats_connections":   1,
		"wavehouse_nats_in_msgs_total": 42,
	}, got, "an external connection reports only the client metrics")
}

func TestRegisterSystemMetrics_WithEmbeddedMQStats(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	reader := useManualReader(t)

	mqStats := func() (MQStats, error) {
		return MQStats{Embedded: true, Connections: 3, InMsgs: 42, OutMsgs: 9}, nil
	}
	require.NoError(t, RegisterSystemMetrics(mqStats, nil))

	require.Equal(t, map[string]int64{
		"wavehouse_mq_embedded_connections":        3,
		"wavehouse_mq_embedded_messages_total/in":  42,
		"wavehouse_mq_embedded_messages_total/out": 9,
		// Deprecated, unchanged.
		"wavehouse_nats_connections":   3,
		"wavehouse_nats_in_msgs_total": 42,
	}, int64Points(t, reader), "the embedded server reports only the embedded metrics")
}

// The new cumulative instruments reach Prometheus as counters, under the
// names the docs give (#900).
func TestRegisterSystemMetrics_MQCountersArePrometheusCounters(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	savedMP := otel.GetMeterProvider()
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo())
	require.NoError(t, err)
	mp := metric.NewMeterProvider(metric.WithReader(exporter))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	embedded := false
	require.NoError(t, RegisterSystemMetrics(func() (MQStats, error) {
		return MQStats{Embedded: embedded, Connections: 1, InMsgs: 1, OutMsgs: 1, InBytes: 1, OutBytes: 1, Reconnects: 1}, nil
	}, nil))
	scrape := func() string {
		rec := httptest.NewRecorder()
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
		return rec.Body.String()
	}

	body := scrape()
	for _, name := range []string{
		"wavehouse_mq_client_messages_total",
		"wavehouse_mq_client_bytes_total",
		"wavehouse_mq_client_reconnects_total",
	} {
		require.Contains(t, body, "# TYPE "+name+" counter\n")
	}
	require.NotContains(t, body, "# TYPE wavehouse_mq_embedded_", "an external connection reports no embedded metric")

	embedded = true
	body = scrape()
	require.Contains(t, body, "# TYPE wavehouse_mq_embedded_messages_total counter\n")
	require.Contains(t, body, "# TYPE wavehouse_mq_embedded_connections gauge\n")
}

func TestRegisterSystemMetrics_MQStatsErrorSkipsGauges(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	// A failed read must not panic or observe a zero — the gauges simply
	// carry no data point for that scrape.
	mqStats := func() (MQStats, error) { return MQStats{}, errors.New("varz unavailable") }
	require.NoError(t, RegisterSystemMetrics(mqStats, nil))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				require.Empty(t, data.DataPoints, "%s must not be observed on a failed read", m.Name)
			case metricdata.Sum[int64]:
				require.Empty(t, data.DataPoints, "%s must not be observed on a failed read", m.Name)
			}
		}
	}
}

func TestRegisterSystemMetrics_NilDedupStats(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	// A nil map — no store open — must not panic inside the callback, and
	// observes nothing.
	require.NoError(t, RegisterSystemMetrics(nil, func() map[string]int64 { return nil }))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok {
				require.Empty(t, g.DataPoints, "%s must not be observed while no store is open", m.Name)
			}
		}
	}
}
