package mq

import (
	"context"
	"time"
)

// Message represents a message received from the queue.
type Message struct {
	Ctx        context.Context
	Subject    string
	Data       []byte
	Timestamp  time.Time
	ack        func(ctx context.Context) error
	asyncAckFn func() error
	nak        func(ctx context.Context) error
}

// NewMessage constructs a Message with ack/nak callbacks.
func NewMessage(ctx context.Context, subject string, data []byte, ts time.Time, ack func(context.Context) error, asyncAckFn func() error, nak func(ctx context.Context) error) *Message {
	return &Message{Ctx: ctx, Subject: subject, Data: data, Timestamp: ts, ack: ack, asyncAckFn: asyncAckFn, nak: nak}
}

// AsyncAck fires a non-blocking Ack without waiting for server confirmation.
// Use for low-criticality consumers (e.g. SSE fan-out hub bridge) where
// DoubleAck latency is not worth the guarantee. Use Ack(ctx) for ingest paths.
func (m *Message) AsyncAck() error {
	if m.asyncAckFn != nil {
		return m.asyncAckFn()
	}
	return nil
}

// Ack acknowledges successful processing.
func (m *Message) Ack(ctx context.Context) error {
	if m.ack != nil {
		return m.ack(ctx)
	}
	return nil
}

// Nak signals processing failure for redelivery.
//
// NOTE: While the underlying NATS Nak is fire-and-forget, this wrapper
// honors context cancellation. If the provided context is cancelled
// (e.g., during graceful shutdown), the network call is suppressed and
// the message will silently wait for AckWait to expire before redelivery.
func (m *Message) Nak(ctx context.Context) error {
	if m.nak != nil {
		return m.nak(ctx)
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
