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
// Note: The underlying NATS implementation for Nak is a fire-and-forget
// network call. It does not actively block or honor the provided context cancellation.
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
	// redelivery (Nak), it MUST NOT manually call msg.Ack() or msg.Nak() beforehand.
	Subscribe(ctx context.Context, subject, consumerName string, handler func(msg *Message) error) error
	Close() error
}

// StreamName returns the JetStream stream name used by all MQ implementations.
func StreamName() string {
	return "WAVEHOUSE"
}
