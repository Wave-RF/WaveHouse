package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/BeachHouse/internal/discovery"
	"github.com/Wave-RF/BeachHouse/internal/mq"
)

// BufferConsumerName is the durable consumer name used by BufferConsumer.
// The Active Sweeper references this to read the AckFloor.
const BufferConsumerName = "buffer-consumer"

// EventMessage is the wire format published to the MQ.
type EventMessage struct {
	TableName         string         `json:"table_name"`
	ReceivedTimestamp string         `json:"received_timestamp"`
	Data              map[string]any `json:"data"`
}

// BufferConsumer subscribes to the ingest stream and batch-inserts into ClickHouse.
type BufferConsumer struct {
	sub           mq.Subscriber
	conn          driver.Conn
	registry      *discovery.SchemaRegistry
	dlqPublisher  mq.Publisher
	dlqEnabled    bool
	batchSize     int
	flushInterval time.Duration
	logger        *slog.Logger
}

// NewBufferConsumer creates a new batch consumer.
func NewBufferConsumer(sub mq.Subscriber, conn driver.Conn, registry *discovery.SchemaRegistry, batchSize int, flushInterval time.Duration, logger *slog.Logger) *BufferConsumer {
	return &BufferConsumer{
		sub:           sub,
		conn:          conn,
		registry:      registry,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
	}
}

// SetDLQ enables the Dead Letter Queue for failed batch inserts.
func (b *BufferConsumer) SetDLQ(pub mq.Publisher) {
	b.dlqPublisher = pub
	b.dlqEnabled = true
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

		b.flushBatch(ctx, toInsert, toAck)
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

// flushBatch groups events by table and inserts each group into ClickHouse.
// Failed groups are routed to the DLQ if enabled.
func (b *BufferConsumer) flushBatch(ctx context.Context, events []EventMessage, acks []func()) {
	// Group events and their ack callbacks by table.
	type tableGroup struct {
		events []EventMessage
		acks   []func()
	}
	groups := make(map[string]*tableGroup)
	for i, evt := range events {
		g, ok := groups[evt.TableName]
		if !ok {
			g = &tableGroup{}
			groups[evt.TableName] = g
		}
		g.events = append(g.events, evt)
		g.acks = append(g.acks, acks[i])
	}

	for table, g := range groups {
		if err := b.insertTableBatch(ctx, table, g.events); err != nil {
			b.logger.Error("batch insert failed", "error", err, "table", table, "count", len(g.events))
			if b.dlqEnabled {
				b.sendToDLQ(ctx, table, g.events)
				// ACK originals so they don't retry forever.
				for _, ack := range g.acks {
					ack()
				}
			}
			// If DLQ disabled, don't ACK — messages stay in NATS for retry.
			continue
		}
		for _, ack := range g.acks {
			ack()
		}
		b.logger.Info("batch inserted", "table", table, "count", len(g.events))
	}
}

func (b *BufferConsumer) insertTableBatch(ctx context.Context, table string, events []EventMessage) error {
	schema := b.registry.Get(table)
	if schema == nil {
		return fmt.Errorf("unknown table %q in schema registry", table)
	}

	// Build the column list from the union of all provided keys across events.
	// Only include columns that at least one event provides, so ClickHouse uses
	// defaults for columns that are omitted entirely.
	provided := make(map[string]bool)
	for _, evt := range events {
		for key := range evt.Data {
			provided[key] = true
		}
	}

	// Preserve schema column order but only include provided columns.
	var cols []discovery.Column
	for _, col := range schema.Columns {
		if provided[col.Name] {
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		return fmt.Errorf("no columns provided for table %q", table)
	}

	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(colNames, ", "))
	chBatch, err := b.conn.PrepareBatch(ctx, sql)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, evt := range events {
		row := make([]any, len(cols))
		for i, col := range cols {
			val, ok := evt.Data[col.Name]
			if !ok || val == nil {
				row[i] = nil
			} else {
				row[i] = coerceValue(col.Type, val)
			}
		}
		if err := chBatch.Append(row...); err != nil {
			return fmt.Errorf("append row: %w", err)
		}
	}
	return chBatch.Send()
}

// coerceValue converts JSON-unmarshaled values (float64 for all numbers) to the
// Go types that the clickhouse-go driver expects for the given ClickHouse column type.
func coerceValue(chType string, val any) any {
	// Unwrap Nullable / LowCardinality wrappers.
	for {
		if strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")") {
			chType = chType[9 : len(chType)-1]
			continue
		}
		if strings.HasPrefix(chType, "LowCardinality(") && strings.HasSuffix(chType, ")") {
			chType = chType[15 : len(chType)-1]
			continue
		}
		break
	}

	f, isFloat := val.(float64)
	if !isFloat {
		return val
	}

	switch chType {
	case "Int8":
		return int8(f)
	case "Int16":
		return int16(f)
	case "Int32":
		return int32(f)
	case "Int64":
		return int64(f)
	case "Int128", "Int256":
		return int64(f) // best-effort; very large values need string-based ingest
	case "UInt8":
		return uint8(f)
	case "UInt16":
		return uint16(f)
	case "UInt32":
		return uint32(f)
	case "UInt64":
		return uint64(f)
	case "UInt128", "UInt256":
		return uint64(f)
	case "Float32":
		return float32(f)
	case "Float64":
		return f
	case "Bool":
		return f != 0
	default:
		return val
	}
}

func (b *BufferConsumer) sendToDLQ(ctx context.Context, table string, events []EventMessage) {
	for _, evt := range events {
		data, err := json.Marshal(evt)
		if err != nil {
			b.logger.Error("dlq marshal failed", "error", err)
			continue
		}
		subject := "dlq." + table
		if err := b.dlqPublisher.Publish(ctx, subject, data); err != nil {
			b.logger.Error("dlq publish failed", "error", err, "table", table)
		}
	}
}
