package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MQStats is a point-in-time snapshot of the message queue the system metrics
// observe. Reported by the mq package (mq.Broker.Stats); defined here because
// mq imports observability, not the other way round. Every count is
// cumulative and counts every message on the wire, control traffic (JetStream
// API replies, KV reads, consumer heartbeats) included, so none of them is a
// count of events.
type MQStats struct {
	// Embedded says the figures are the embedded server's (its varz, covering
	// every client connected to it); otherwise they are this process's own
	// client connection to an external server.
	Embedded bool
	// Connections is the embedded server's client connections, or 1 while
	// this process's client is connected and 0 while it is not.
	Connections int64
	InMsgs      int64
	OutMsgs     int64
	InBytes     int64 // this process's client only
	OutBytes    int64 // this process's client only
	Reconnects  int64 // this process's client only
}

// RegisterSystemMetrics creates asynchronous instruments that pull stats from
// the MQ and the embedded Pebble store on every collection.
// Both are read on every scrape; nil skips the MQ gauges, as a nil
// pebbleStats — or a nil map from it, no store being open — skips the Pebble
// ones. The Pebble figures are the one instance's that every tenant's seen
// ids share (dedupe.Embedded.Stats in production).
func RegisterSystemMetrics(mqStats func() (MQStats, error), pebbleStats func() map[string]int64) error {
	meter := otel.Meter("wavehouse-system")

	// MQ instruments. Each backend observes only its own set, so no name
	// changes meaning with mq.backend.
	clientMsgs, _ := meter.Int64ObservableCounter("wavehouse_mq_client_messages_total",
		metric.WithDescription("Messages this process's NATS client connection has received (direction=in) and sent (direction=out), control traffic included: not a count of events. Under mq.backend: nats only"))
	clientBytes, _ := meter.Int64ObservableCounter("wavehouse_mq_client_bytes_total",
		metric.WithDescription("Bytes this process's NATS client connection has received (direction=in) and sent (direction=out), control traffic included. Under mq.backend: nats only"),
		metric.WithUnit("By"))
	clientReconnects, _ := meter.Int64ObservableCounter("wavehouse_mq_client_reconnects_total",
		metric.WithDescription("Times this process's NATS client connection has reconnected. Under mq.backend: nats only"))
	embeddedConns, _ := meter.Int64ObservableGauge("wavehouse_mq_embedded_connections",
		metric.WithDescription("Client connections to the embedded NATS server. Under mq.backend: embedded only"))
	embeddedMsgs, _ := meter.Int64ObservableCounter("wavehouse_mq_embedded_messages_total",
		metric.WithDescription("Messages the embedded NATS server has received from (direction=in) and sent to (direction=out) its clients, control traffic included: not a count of events. Under mq.backend: embedded only"))
	// Deprecated: replaced by the instruments above, removed in the next minor
	// release. Kept as they were, meaning included.
	natsConnections, _ := meter.Int64ObservableGauge("wavehouse_nats_connections",
		metric.WithDescription("Deprecated: use wavehouse_mq_connected (nats) or wavehouse_mq_embedded_connections (embedded). Active NATS client connections"))
	natsInMsgs, _ := meter.Int64ObservableGauge("wavehouse_nats_in_msgs_total",
		metric.WithDescription("Deprecated: use wavehouse_mq_client_messages_total or wavehouse_mq_embedded_messages_total with direction=in. Total NATS messages received"))
	in := metric.WithAttributes(attribute.String("direction", "in"))
	out := metric.WithAttributes(attribute.String("direction", "out"))

	// Pebble Instruments
	pebbleWalSize, _ := meter.Int64ObservableGauge("wavehouse_pebble_wal_size", metric.WithDescription("Size of Pebble WAL in bytes"))
	pebbleTableCount, _ := meter.Int64ObservableGauge("wavehouse_pebble_table_count", metric.WithDescription("Total Pebble SSTables"))

	// Register the scraper callback (runs every 15 seconds)
	_, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		// Scrape the MQ
		if mqStats != nil {
			if stats, err := mqStats(); err == nil {
				if stats.Embedded {
					o.ObserveInt64(embeddedConns, stats.Connections)
					o.ObserveInt64(embeddedMsgs, stats.InMsgs, in)
					o.ObserveInt64(embeddedMsgs, stats.OutMsgs, out)
				} else {
					o.ObserveInt64(clientMsgs, stats.InMsgs, in)
					o.ObserveInt64(clientMsgs, stats.OutMsgs, out)
					o.ObserveInt64(clientBytes, stats.InBytes, in)
					o.ObserveInt64(clientBytes, stats.OutBytes, out)
					o.ObserveInt64(clientReconnects, stats.Reconnects)
				}
				o.ObserveInt64(natsConnections, stats.Connections)
				o.ObserveInt64(natsInMsgs, stats.InMsgs)
			}
		}

		// Scrape Pebble (if using embedded dedupe)
		if pebbleStats != nil {
			if stats := pebbleStats(); stats != nil {
				o.ObserveInt64(pebbleWalSize, stats["pebble_wal_size"])
				o.ObserveInt64(pebbleTableCount, stats["pebble_table_count"])
			}
		}

		return nil
	}, clientMsgs, clientBytes, clientReconnects, embeddedConns, embeddedMsgs,
		natsConnections, natsInMsgs, pebbleWalSize, pebbleTableCount)

	return err
}
