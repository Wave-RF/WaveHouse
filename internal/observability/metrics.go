package observability

import (
	"context"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/nats-io/nats-server/v2/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// RegisterSystemMetrics creates asynchronous gauges that periodically pull
// stats from embedded systems (NATS, Pebble) and push them to OpenTelemetry.
func RegisterSystemMetrics(natsServer *server.Server, dedup dedupe.Deduplicator) error {
	meter := otel.Meter("wavehouse-system")

	// NATS Instruments
	natsConnections, _ := meter.Int64ObservableGauge("wavehouse_nats_connections", metric.WithDescription("Active NATS client connections"))
	natsInMsgs, _ := meter.Int64ObservableGauge("wavehouse_nats_in_msgs_total", metric.WithDescription("Total NATS messages received"))

	// Pebble Instruments
	pebbleWalSize, _ := meter.Int64ObservableGauge("wavehouse_pebble_wal_size", metric.WithDescription("Size of Pebble WAL in bytes"))
	pebbleTableCount, _ := meter.Int64ObservableGauge("wavehouse_pebble_table_count", metric.WithDescription("Total Pebble SSTables"))

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

		return nil
	}, natsConnections, natsInMsgs, pebbleWalSize, pebbleTableCount)

	return err
}