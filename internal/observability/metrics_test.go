package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// stubDeduplicator is a no-op dedupe.Deduplicator implementation for tests.
// We use a local stub rather than testutil.MockDeduplicator because
// testutil → mq → observability is an import cycle: observability cannot
// pull in testutil even from test files (they live in package observability,
// not observability_test). Keep the stub minimal — anything richer should
// move into the dedupe package itself.
type stubDeduplicator struct {
	stats map[string]int64
}

func (s *stubDeduplicator) CheckAndMark(context.Context, string) (bool, error) { return false, nil }
func (s *stubDeduplicator) Stats() map[string]int64                            { return s.stats }
func (s *stubDeduplicator) Close() error                                       { return nil }

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

	dedup := &stubDeduplicator{stats: map[string]int64{
		"pebble_wal_size":    1024,
		"pebble_table_count": 7,
	}}
	require.NoError(t, RegisterSystemMetrics(nil, dedup))

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

	// Stats() returning nil must not panic inside the callback.
	dedup := &stubDeduplicator{stats: nil}
	require.NoError(t, RegisterSystemMetrics(nil, dedup))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
}
