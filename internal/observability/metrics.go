package observability

import (
	"context"
	"errors"
	"log/slog"
	"math"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric"
)

// clampUint64 saturates to int64's positive range. JetStream counts are
// uint64 but never realistically exceed int64; clamp rather than wrap.
func clampUint64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// SystemMetricSources collects live runtime components for the scraper.
// Each field is independently optional — nil sources are skipped at scrape
// time, not refused at registration.
type SystemMetricSources struct {
	NATS         *server.Server
	JS           jetstream.JetStream
	Dedup        dedupe.Deduplicator
	StreamName   string // primary ingest stream — empty disables consumer-pending probe
	DLQStream    string // dead-letter stream — empty disables DLQ-depth probe
	ConsumerName string // durable consumer on StreamName — empty disables consumer-pending probe
}

// RegisterSystemMetrics wires asynchronous gauges that pull from NATS,
// JetStream, and Pebble. The callback fires at the MeterProvider's read
// interval (15s for OTLP push; on-demand for Prometheus).
func RegisterSystemMetrics(src SystemMetricSources) error {
	m := Meter()

	natsConnections, err := m.Int64ObservableGauge("wavehouse_nats_connections", metric.WithDescription("Active NATS client connections"))
	if err != nil {
		return err
	}
	natsInMsgs, err := m.Int64ObservableGauge("wavehouse_nats_in_msgs_total", metric.WithDescription("Total NATS messages received"))
	if err != nil {
		return err
	}
	pebbleWalSize, err := m.Int64ObservableGauge("wavehouse_pebble_wal_size", metric.WithDescription("Size of Pebble WAL in bytes"))
	if err != nil {
		return err
	}
	pebbleTableCount, err := m.Int64ObservableGauge("wavehouse_pebble_table_count", metric.WithDescription("Total Pebble SSTables"))
	if err != nil {
		return err
	}
	// Consumer lag is the leading indicator for ingest backpressure — shows
	// up here long before the stream fills and starts returning 503s.
	consumerPending, err := m.Int64ObservableGauge(
		"wavehouse_jetstream_consumer_pending",
		metric.WithDescription("JetStream consumer pending message count (queue depth waiting on the ingest worker)"),
	)
	if err != nil {
		return err
	}
	dlqDepth, err := m.Int64ObservableGauge(
		"wavehouse_dlq_depth",
		metric.WithDescription("Number of messages currently in the DLQ stream"),
	)
	if err != nil {
		return err
	}

	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if src.NATS != nil {
			if varz, err := src.NATS.Varz(nil); err == nil {
				o.ObserveInt64(natsConnections, int64(varz.Connections))
				o.ObserveInt64(natsInMsgs, varz.InMsgs)
			}
		}

		if src.Dedup != nil {
			stats := src.Dedup.Stats()
			if stats != nil {
				o.ObserveInt64(pebbleWalSize, stats["pebble_wal_size"])
				o.ObserveInt64(pebbleTableCount, stats["pebble_table_count"])
			}
		}

		// JetStream probes are best-effort. Non-NotFound errors log at
		// DEBUG so transient failures don't spam but persistent
		// misconfiguration stays debuggable.
		if src.JS != nil && src.StreamName != "" && src.ConsumerName != "" {
			if cons, err := src.JS.Consumer(ctx, src.StreamName, src.ConsumerName); err == nil {
				if info, err := cons.Info(ctx); err == nil {
					o.ObserveInt64(consumerPending, clampUint64(info.NumPending))
				} else if !errors.Is(err, jetstream.ErrConsumerNotFound) {
					slog.DebugContext(ctx, "consumer pending probe failed", "consumer", src.ConsumerName, "error", err)
				}
			}
		}
		if src.JS != nil && src.DLQStream != "" {
			if st, err := src.JS.Stream(ctx, src.DLQStream); err == nil {
				if info, err := st.Info(ctx); err == nil {
					o.ObserveInt64(dlqDepth, clampUint64(info.State.Msgs))
				} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
					slog.DebugContext(ctx, "dlq depth probe failed", "stream", src.DLQStream, "error", err)
				}
			}
		}

		return nil
	}, natsConnections, natsInMsgs, pebbleWalSize, pebbleTableCount, consumerPending, dlqDepth)

	return err
}
