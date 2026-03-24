package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/BeachHouse/internal/mq"
)

// BufferConsumerName is the durable consumer name used by BufferConsumer.
// The Active Sweeper references this to read the AckFloor.
const BufferConsumerName = "buffer-consumer"

// EventMessage is the wire format published to the MQ.
type EventMessage struct {
	TenantID  string   `json:"tenant_id"`
	EventID   string   `json:"event_id"`
	Timestamp string   `json:"timestamp"`
	EventType string   `json:"type"`
	MapKeys   []string `json:"map_keys"`
	MapValues []string `json:"map_values"`
}

// BufferConsumer subscribes to the ingest stream and batch-inserts into ClickHouse.
type BufferConsumer struct {
	sub           mq.Subscriber
	conn          driver.Conn
	batchSize     int
	flushInterval time.Duration
	logger        *slog.Logger
}

// NewBufferConsumer creates a new batch consumer.
func NewBufferConsumer(sub mq.Subscriber, conn driver.Conn, batchSize int, flushInterval time.Duration, logger *slog.Logger) *BufferConsumer {
	return &BufferConsumer{
		sub:           sub,
		conn:          conn,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
	}
}

// Start begins consuming messages and batching inserts.
func (b *BufferConsumer) Start(ctx context.Context) error {
	var (
		mu    sync.Mutex
		batch []EventMessage
		acks  []func()
	)

	flush := func() {
		mu.Lock()
		if len(batch) == 0 {
			mu.Unlock()
			return
		}
		toInsert := batch
		toAck := acks
		batch = nil
		acks = nil
		mu.Unlock()

		if err := b.insertBatch(ctx, toInsert); err != nil {
			b.logger.Error("batch insert failed", "error", err, "count", len(toInsert))
			return
		}
		for _, ack := range toAck {
			ack()
		}
		b.logger.Info("batch inserted", "count", len(toInsert))
	}

	// Periodic flush.
	go func() {
		ticker := time.NewTicker(b.flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				flush()
			}
		}
	}()

	return b.sub.Subscribe(ctx, "ingest.>", BufferConsumerName, func(msg *mq.Message) error {
		var evt EventMessage
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			b.logger.Error("unmarshal event", "error", err)
			msg.Ack() // Drop malformed messages.
			return nil
		}

		mu.Lock()
		batch = append(batch, evt)
		acks = append(acks, msg.Ack)
		shouldFlush := len(batch) >= b.batchSize
		mu.Unlock()

		if shouldFlush {
			flush()
		}
		return nil
	})
}

func (b *BufferConsumer) insertBatch(ctx context.Context, events []EventMessage) error {
	chBatch, err := b.conn.PrepareBatch(ctx, `INSERT INTO events (tenant_id, event_id, timestamp, type, map_keys, map_values)`)
	if err != nil {
		return err
	}
	for _, evt := range events {
		ts, _ := time.Parse(time.RFC3339, evt.Timestamp)
		if err := chBatch.Append(evt.TenantID, evt.EventID, ts, evt.EventType, evt.MapKeys, evt.MapValues); err != nil {
			return err
		}
	}
	return chBatch.Send()
}
