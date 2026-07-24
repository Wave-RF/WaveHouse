package observability

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/nats-io/nats-server/v2/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RegisterSystemMetrics creates asynchronous gauges that periodically pull
// stats from embedded systems (NATS, Pebble) and ClickHouse storage
// (system.parts) and push them to OpenTelemetry.
func RegisterSystemMetrics(natsServer *server.Server, dedup dedupe.Deduplicator, chConn driver.Conn, chDatabase string) error {
	meter := otel.Meter("wavehouse-system")

	// NATS Instruments
	natsConnections, _ := meter.Int64ObservableGauge("wavehouse_nats_connections", metric.WithDescription("Active NATS client connections"))
	natsInMsgs, _ := meter.Int64ObservableGauge("wavehouse_nats_in_msgs_total", metric.WithDescription("Total NATS messages received"))

	// Pebble Instruments
	pebbleWalSize, _ := meter.Int64ObservableGauge("wavehouse_pebble_wal_size", metric.WithDescription("Size of Pebble WAL in bytes"))
	pebbleTableCount, _ := meter.Int64ObservableGauge("wavehouse_pebble_table_count", metric.WithDescription("Total Pebble SSTables"))

	// ClickHouse storage Instruments
	chUncompressedBytes, _ := meter.Int64ObservableGauge("wavehouse_clickhouse_uncompressed_bytes", metric.WithDescription("Uncompressed bytes of live data per table (active parts) — the ingested size"))
	chBytesOnDisk, _ := meter.Int64ObservableGauge("wavehouse_clickhouse_bytes_on_disk", metric.WithDescription("Bytes on disk per table (active parts): compressed data plus marks/index files"))

	// Register the scraper callback (runs every 15 seconds)
	_, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		// Scrape NATS
		if natsServer != nil {
			if varz, err := natsServer.Varz(nil); err == nil {
				o.ObserveInt64(natsConnections, int64(varz.Connections))
				o.ObserveInt64(natsInMsgs, varz.InMsgs)
			}
		}

		// Scrape Pebble (if using embedded dedupe)
		if dedup != nil {
			stats := dedup.Stats()
			if stats != nil {
				o.ObserveInt64(pebbleWalSize, stats["pebble_wal_size"])
				o.ObserveInt64(pebbleTableCount, stats["pebble_table_count"])
			}
		}

		// Scrape ClickHouse storage: live data before vs after compression,
		// per table. Active parts only, so TTL expiry and merges shrink these —
		// "currently stored", not a lifetime ingest counter.
		if chConn != nil {
			rows, err := chConn.Query(ctx,
				`SELECT table, sum(data_uncompressed_bytes), sum(bytes_on_disk)
				 FROM system.parts
				 WHERE active AND database = ?
				 GROUP BY table`,
				chDatabase,
			)
			if err == nil {
				for rows.Next() {
					var table string
					var uncompressed, onDisk uint64
					if err := rows.Scan(&table, &uncompressed, &onDisk); err == nil {
						attrs := metric.WithAttributes(attribute.String("table", table))
						o.ObserveInt64(chUncompressedBytes, int64(uncompressed), attrs)
						o.ObserveInt64(chBytesOnDisk, int64(onDisk), attrs)
					}
				}
				_ = rows.Close()
			}
		}

		return nil
	}, natsConnections, natsInMsgs, pebbleWalSize, pebbleTableCount, chUncompressedBytes, chBytesOnDisk)

	return err
}
