package mq

import (
	"context"
	"time"
)

// Message represents a message received from the queue.
type Message struct {
	Ctx       context.Context
	Subject   string
	Data      []byte
	Timestamp time.Time
	ack       func()
	nak       func()
}

// NewMessage constructs a Message with ack/nak callbacks.
func NewMessage(ctx context.Context, subject string, data []byte, ts time.Time, ack, nak func()) *Message {
	return &Message{Ctx: ctx, Subject: subject, Data: data, Timestamp: ts, ack: ack, nak: nak}
}

// Ack acknowledges successful processing.
func (m *Message) Ack() {
	if m.ack != nil {
		m.ack()
	}
}

// Nak signals processing failure for redelivery.
func (m *Message) Nak() {
	if m.nak != nil {
		m.nak()
	}
}

// Publisher publishes messages to a subject.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
	Close() error
}

// Subscriber subscribes to messages on a subject.
type Subscriber interface {
	Subscribe(ctx context.Context, subject, consumerName string, handler func(msg *Message) error) error
	Close() error
}

// StreamName returns the primary JetStream stream name. Hardcoded — the
// embedded NATS server is private to the WaveHouse process, so there's no
// multi-tenant case to namespace against. Kept as a function (rather than
// a const) so all callers go through one symbol; future migrations could
// swap the constant out without breaking the API surface.
func StreamName() string {
	return "WAVEHOUSE"
}

// DLQStreamName returns the dead-letter-queue JetStream stream name. Used
// for failed ClickHouse batch inserts (see `internal/api/dlq.go`). Same
// rationale as StreamName for being hardcoded.
func DLQStreamName() string {
	return "WAVEHOUSE_DLQ"
}
