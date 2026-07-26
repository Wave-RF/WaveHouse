//go:build integration

// Storage-gauge integration test.
//
// Runs the observability storage scraper's system.parts aggregate against the
// shared ClickHouse container. The unit tests in internal/observability stub
// driver.Conn and assert only on the query string, so a misspelled column, a
// syntax error, or an aggregate whose type no longer matches the Scan targets
// would pass them — and the scraper's error path is deliberately silent, so
// in production such a regression presents as "the gauges are simply never
// there". Only a real round-trip catches it.

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

func TestSystemMetrics_StorageGauges_RealClickHouse(t *testing.T) {
	// No t.Parallel: RegisterSystemMetrics reads the global meter provider,
	// which this test swaps via otel.SetMeterProvider (restored by the guard).
	guardOTelGlobals(t)

	table := createTable(t, "user_id String, value Float64", "ORDER BY user_id")

	// Insert synchronously so the table has at least one active part — the
	// scraper reports only tables with active parts.
	ctx := context.Background()
	require.NoError(t, env(t).chConn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s VALUES ('u1', 1.5), ('u2', 2.5)", table)))

	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	require.NoError(t, observability.RegisterSystemMetrics(nil, nil, env(t).chConn, testCHDatabase))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	// Gather both storage gauges' data points for our table, keyed by
	// metric name.
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				continue
			}
			for _, dp := range g.DataPoints {
				if attr, _ := dp.Attributes.Value("table"); attr.AsString() == table {
					got[m.Name] = dp.Value
				}
			}
		}
	}
	require.Contains(t, got, "wavehouse_clickhouse_uncompressed_bytes", "no data point for table %s", table)
	require.Contains(t, got, "wavehouse_clickhouse_bytes_on_disk", "no data point for table %s", table)
	require.Positive(t, got["wavehouse_clickhouse_uncompressed_bytes"])
	require.Positive(t, got["wavehouse_clickhouse_bytes_on_disk"])
}
