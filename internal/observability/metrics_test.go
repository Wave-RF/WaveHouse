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
// scraper, capturing the query and bind args so tests can assert the active
// filter and database bind. The embedded nil driver.Conn keeps every method
// we don't stub out of the way — calling one panics, which is what we want
// in a test. Local rather than in testutil: testutil → mq → observability is
// an import cycle (see stubDeduplicator above).
type stubCHConn struct {
	driver.Conn
	rows     driver.Rows
	queryErr error
	gotQuery string
	gotArgs  []any
}

func (c *stubCHConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.gotQuery = query
	c.gotArgs = args
	return c.rows, c.queryErr
}

type partsRow struct {
	table                string
	uncompressed, onDisk uint64
}

// stubPartsRows implements driver.Rows for the (table, uncompressed, on_disk)
// tuples the storage scraper selects. A non-nil err surfaces via Err() after
// iteration, like a mid-stream failure in the real driver.
type stubPartsRows struct {
	driver.Rows
	rows []partsRow
	err  error
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

func (r *stubPartsRows) Err() error { return r.err }

func (*stubPartsRows) Close() error { return nil }

func TestRegisterSystemMetrics_ClickHouseStorage(t *testing.T) {
	// No t.Parallel(): see TestRegisterSystemMetrics_NilInputs.
	tests := []struct {
		name             string
		conn             *stubCHConn
		wantUncompressed map[string]int64
		wantOnDisk       map[string]int64
	}{
		{
			name: "per-table gauges observed",
			conn: &stubCHConn{rows: &stubPartsRows{rows: []partsRow{
				{table: "events", uncompressed: 75_000_000_000, onDisk: 1_400_000_000},
				{table: "clicks", uncompressed: 10, onDisk: 3},
			}}},
			wantUncompressed: map[string]int64{"events": 75_000_000_000, "clicks": 10},
			wantOnDisk:       map[string]int64{"events": 1_400_000_000, "clicks": 3},
		},
		{
			// A failing query must not panic — the storage gauges just go
			// unobserved for the cycle.
			name:             "query error observes nothing",
			conn:             &stubCHConn{queryErr: errors.New("clickhouse down")},
			wantUncompressed: map[string]int64{},
			wantOnDisk:       map[string]int64{},
		},
		{
			// A mid-stream failure must discard the whole scrape: a partial
			// per-table set would skew dashboard-layer totals.
			name: "mid-stream error discards the partial scrape",
			conn: &stubCHConn{rows: &stubPartsRows{
				rows: []partsRow{{table: "events", uncompressed: 1, onDisk: 1}},
				err:  errors.New("stream aborted"),
			}},
			wantUncompressed: map[string]int64{},
			wantOnDisk:       map[string]int64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			savedMP := otel.GetMeterProvider()
			reader := metric.NewManualReader()
			mp := metric.NewMeterProvider(metric.WithReader(reader))
			otel.SetMeterProvider(mp)
			t.Cleanup(func() {
				_ = mp.Shutdown(context.Background())
				otel.SetMeterProvider(savedMP)
			})

			require.NoError(t, RegisterSystemMetrics(nil, nil, tt.conn, "wavehouse"))

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))

			// The scraper must aggregate only active parts of the configured
			// database.
			require.Contains(t, tt.conn.gotQuery, "system.parts")
			require.Contains(t, tt.conn.gotQuery, "active")
			require.Equal(t, []any{"wavehouse"}, tt.conn.gotArgs)

			// Gather both gauges' data points, keyed by the `table` attribute.
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
			require.Equal(t, tt.wantUncompressed, uncompressed)
			require.Equal(t, tt.wantOnDisk, onDisk)
		})
	}
}
