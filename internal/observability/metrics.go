package observability

import (
	"context"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// MQStats is the point-in-time snapshot of the embedded message queue the
// system gauges observe. Reported by the mq package (mq.Broker.Stats);
// defined here because mq imports observability, not the other way round.
type MQStats struct {
	Connections int64 // active client connections
	InMsgs      int64 // messages received, cumulative
}

// RegisterSystemMetrics creates asynchronous gauges that periodically pull
// stats from embedded systems (the MQ, Pebble) and push them to OpenTelemetry.
// mqStats is read on every scrape; nil skips the MQ gauges, as a nil dedup
// skips the Pebble ones.
func RegisterSystemMetrics(mqStats func() (MQStats, error), dedup dedupe.Deduplicator) error {
	meter := otel.Meter("wavehouse-system")

	// NATS Instruments
	natsConnections, _ := meter.Int64ObservableGauge("wavehouse_nats_connections", metric.WithDescription("Active NATS client connections"))
	natsInMsgs, _ := meter.Int64ObservableGauge("wavehouse_nats_in_msgs_total", metric.WithDescription("Total NATS messages received"))

	// Pebble Instruments
	pebbleWalSize, _ := meter.Int64ObservableGauge("wavehouse_pebble_wal_size", metric.WithDescription("Size of Pebble WAL in bytes"))
	pebbleTableCount, _ := meter.Int64ObservableGauge("wavehouse_pebble_table_count", metric.WithDescription("Total Pebble SSTables"))

	// Register the scraper callback (runs every 15 seconds)
	_, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		// Scrape the MQ
		if mqStats != nil {
			if stats, err := mqStats(); err == nil {
				o.ObserveInt64(natsConnections, stats.Connections)
				o.ObserveInt64(natsInMsgs, stats.InMsgs)
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
