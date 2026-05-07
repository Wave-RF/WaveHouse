package mq

import (
	"context"
	"time"
)

// Message represents a message received from the queue.
type Message struct {
	Ctx         context.Context
	Subject     string
	Data        []byte
	Timestamp   time.Time
	doubleAckFn func(ctx context.Context) error
	ackFn       func() error
	nakFn       func() error
}

// NewMessage constructs a Message with ack/nak callbacks.
func NewMessage(ctx context.Context, subject string, data []byte, ts time.Time, doubleAck func(context.Context) error, ack func() error, nak func() error) *Message {
	return &Message{Ctx: ctx, Subject: subject, Data: data, Timestamp: ts, doubleAckFn: doubleAck, ackFn: ack, nakFn: nak}
}

// DoubleAck acknowledges the message synchronously, blocking until the NATS
// server confirms receipt. Use this for critical ingest paths (ClickHouse writes).
func (m *Message) DoubleAck(ctx context.Context) error {
	if m.doubleAckFn != nil {
		return m.doubleAckFn(ctx)
	}
	return nil
}

// Ack acknowledges the message asynchronously (fire-and-forget).
// Use for low-criticality consumers or high-throughput scenarios where
// latency is more important than a "received" confirmation from the server.
func (m *Message) Ack() error {
	if m.ackFn != nil {
		return m.ackFn()
	}
	return nil
}

// Nak negatively acknowledges the message asynchronously for redelivery.
// This is fire-and-forget and does not require a context.
func (m *Message) Nak() error {
	if m.nakFn != nil {
		return m.nakFn()
	}
	return nil
}

// Publisher publishes messages to a subject.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
	Close() error
}

// Subscriber subscribes to messages on a subject.
type Subscriber interface {
	// Subscribe registers a handler for incoming messages.
	// CONTRACT: If the handler intends to return an error to trigger automatic
	// redelivery (Nak), it MUST NOT manually call msg.Ack(ctx), msg.AsyncAck(),
	// or msg.Nak(ctx) beforehand.
	// CONTRACT: Symmetrically, if you explicitly call msg.Nak(ctx), do NOT also
	// return an error — the consume loop will call m.Nak(ctx) again on any
	// non-nil error return. Double-Nak is harmless in NATS JetStream but
	// indicates a logic error in the handler.
	Subscribe(ctx context.Context, subject, consumerName string, handler func(msg *Message) error) error
	Close() error
}

// StreamName returns the JetStream stream name used by all MQ implementations.
func StreamName() string {
	return "WAVEHOUSE"
}
