package observability

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

	err := RegisterSystemMetrics(nil, nil, nil, "")
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
	require.NoError(t, RegisterSystemMetrics(nil, dedup, nil, ""))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	// We should see the Pebble gauges registered by RegisterSystemMetrics.
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
	require.NoError(t, RegisterSystemMetrics(nil, dedup, nil, ""))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
}

// stubCHConn returns canned system.parts rows (or an error) for the storage
// scraper. The embedded nil driver.Conn keeps every method we don't stub out
// of the way — calling one panics, which is what we want in a test.
type stubCHConn struct {
	driver.Conn
	rows     driver.Rows
	queryErr error
}

func (c *stubCHConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return c.rows, c.queryErr
}

type partsRow struct {
	table                string
	uncompressed, onDisk uint64
}

// stubPartsRows implements driver.Rows for the (table, uncompressed, on_disk)
// tuples the storage scraper selects.
type stubPartsRows struct {
	driver.Rows
	rows []partsRow
	i    int
}

func (r *stubPartsRows) Next() bool { r.i++; return r.i <= len(r.rows) }

func (r *stubPartsRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	*(dest[0].(*string)) = row.table
	*(dest[1].(*uint64)) = row.uncompressed
	*(dest[2].(*uint64)) = row.onDisk
	return nil
}

func (*stubPartsRows) Close() error { return nil }

func TestRegisterSystemMetrics_ClickHouseStorage(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	conn := &stubCHConn{rows: &stubPartsRows{rows: []partsRow{
		{table: "events", uncompressed: 75_000_000_000, onDisk: 1_400_000_000},
		{table: "clicks", uncompressed: 10, onDisk: 3},
	}}}
	require.NoError(t, RegisterSystemMetrics(nil, nil, conn, "wavehouse"))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	// Both gauges must carry one data point per table, keyed by the `table`
	// attribute.
	uncompressed := map[string]int64{}
	onDisk := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				continue
			}
			for _, dp := range g.DataPoints {
				table, _ := dp.Attributes.Value("table")
				switch m.Name {
				case "wavehouse_clickhouse_uncompressed_bytes":
					uncompressed[table.AsString()] = dp.Value
				case "wavehouse_clickhouse_bytes_on_disk":
					onDisk[table.AsString()] = dp.Value
				}
			}
		}
	}
	require.Equal(t, map[string]int64{"events": 75_000_000_000, "clicks": 10}, uncompressed)
	require.Equal(t, map[string]int64{"events": 1_400_000_000, "clicks": 3}, onDisk)
}

func TestRegisterSystemMetrics_ClickHouseQueryError(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	savedMP := otel.GetMeterProvider()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	// A failing query must not panic — the storage gauges just go unobserved
	// for the cycle.
	conn := &stubCHConn{queryErr: errors.New("clickhouse down")}
	require.NoError(t, RegisterSystemMetrics(nil, nil, conn, "wavehouse"))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			require.NotContains(t, m.Name, "wavehouse_clickhouse_", "no storage gauge should be observed on query error")
		}
	}
}
