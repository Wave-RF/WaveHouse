package observability

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
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

func TestRegisterSystemMetrics_WithMQStats(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	calls := 0
	mqStats := func() (MQStats, error) {
		calls++
		return MQStats{Connections: 3, InMsgs: 42}, nil
	}
	require.NoError(t, RegisterSystemMetrics(mqStats, nil))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.Equal(t, 1, calls, "stats are read once per scrape")

	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok && len(g.DataPoints) == 1 {
				got[m.Name] = g.DataPoints[0].Value
			}
		}
	}
	require.Equal(t, int64(3), got["wavehouse_nats_connections"])
	require.Equal(t, int64(42), got["wavehouse_nats_in_msgs_total"])
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
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok {
				require.Empty(t, g.DataPoints, "%s must not be observed on a failed read", m.Name)
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
