package mq

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
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

// PublishOpt defines a functional option for modifying a NATS message before publishing.
type PublishOpt func(*nats.Msg)

// WithHeader adds a key-value pair to the NATS message headers.
func WithHeader(key, value string) PublishOpt {
	return func(msg *nats.Msg) {
		if msg.Header == nil {
			msg.Header = make(nats.Header)
		}
		msg.Header.Add(key, value)
	}
}

// Publisher publishes messages to a subject.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte, opts ...PublishOpt) error
	Close() error
}

// Subscriber subscribes to messages on a subject.
type Subscriber interface {
	// Subscribe registers a handler for incoming messages.
	//
	// CONTRACT: If the handler intends to return an error to trigger automatic
	// redelivery, it MUST NOT manually call msg.Ack() or msg.Nak() beforehand.
	//
	// CONTRACT: Symmetrically, if you explicitly call msg.Nak(), do NOT also
	// return an error — the consume loop will call Nak() again on any non-nil
	// error return.
	//
	// CONTRACT: Calling msg.DoubleAck(ctx) and then returning a non-nil error is
	// undefined behaviour — the consume loop will Nak() after a successful
	// server-confirmed Ack. Call DoubleAck, then return nil on success.
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
