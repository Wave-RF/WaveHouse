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
// We can't import internal/dedupe here because that would create an import
// cycle (dedupe already depends on nothing; observability depends on dedupe
// via the Deduplicator interface it uses in RegisterSystemMetrics).
// Actually dedupe doesn't import observability, so a direct dependency is
// fine — but we keep this file free of dedupe-external behavior.
type stubDeduplicator struct {
	stats map[string]int64
}

func (s *stubDeduplicator) CheckAndMark(context.Context, string) (bool, error) { return false, nil }
func (s *stubDeduplicator) Stats() map[string]int64                            { return s.stats }
func (s *stubDeduplicator) Close() error                                       { return nil }

func TestRegisterSystemMetrics_NilInputs(t *testing.T) {
	t.Parallel()

	// Use an SDK meter provider so RegisterCallback actually runs.
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	err := RegisterSystemMetrics(nil, nil)
	require.NoError(t, err)

	// Collect — the callback should run without panicking even when both
	// dependencies are nil.
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
}

func TestRegisterSystemMetrics_WithDedup(t *testing.T) {
	t.Parallel()

	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

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
	t.Parallel()

	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	// Stats() returning nil must not panic inside the callback.
	dedup := &stubDeduplicator{stats: nil}
	require.NoError(t, RegisterSystemMetrics(nil, dedup))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
}
