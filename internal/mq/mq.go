// Package mq is the message-queue boundary: the only package that imports
// NATS/JetStream (enforced by the depguard rule in .golangci.yml, Key Design
// Decision #20). Everything the rest of the process needs from the broker —
// messages, headers, consumers, stream state, replay — is expressed in the
// types below, so a broker change lands here once.
package mq

import (
	"context"
	"errors"
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

// Headers carries a message's headers. It has the same map[string][]string
// shape as NATS and HTTP headers, so it converts to either without a copy.
// Keys are exact (case-sensitive, no canonicalization), matching nats.Header.
type Headers map[string][]string

// Add appends value to the values associated with key.
func (h Headers) Add(key, value string) {
	h[key] = append(h[key], value)
}

// Set replaces the values associated with key with the single value.
func (h Headers) Set(key, value string) {
	h[key] = []string{value}
}

// Get returns the first value associated with key, or "" if there is none.
func (h Headers) Get(key string) string {
	if v := h[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}

// PublishOpt defines a functional option for modifying a message's headers
// before publishing. The map is always non-nil when an option runs.
type PublishOpt func(Headers)

// WithHeader adds a key-value pair to the message headers.
func WithHeader(key, value string) PublishOpt {
	return func(h Headers) {
		h.Add(key, value)
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

// ConsumerConfig describes a durable, explicit-ack pull consumer on the ingest
// stream. Zero values take the broker's defaults.
type ConsumerConfig struct {
	Durable       string
	FilterSubject string
	// AckWait is the redelivery timeout: a message not acked within it is
	// delivered again.
	AckWait time.Duration
	// MaxAckPending caps unacked messages server-side; delivery pauses when
	// hit (backpressure).
	MaxAckPending int
}

// Consumer is a live durable consumer created by ConsumerManager.
type Consumer interface {
	// Consume delivers each message to handler on the client's delivery
	// goroutine, so a handler that blocks holds delivery back — that is the
	// backpressure the ingest worker relies on. Up to prefetch messages are
	// fetched ahead (0 = the client default). The returned stop asks delivery
	// to end and returns without waiting: a handler invocation already in
	// flight, or one for a message already queued client-side, may still run
	// after stop returns, so a handler must not write to anything the caller
	// tears down right after stopping.
	Consume(handler func(msg *Message), prefetch int) (stop func(), err error)
}

// ConsumerManager creates durable consumers on the ingest stream. A delivered
// Message.Ctx is the ctx given to CreateConsumer: unlike Subscriber, the
// consumer path does not extract the trace context carried in the message
// headers, because its one consumer (the ingest worker) batches across
// messages and never reads a per-message context.
type ConsumerManager interface {
	CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error)
}

// StreamState is the slice of a stream's live state WaveHouse reads.
type StreamState struct {
	FirstSeq uint64 // oldest stored sequence, 0 when empty
	LastSeq  uint64 // newest stored sequence, 0 when empty
	Msgs     uint64 // messages stored, across every subject
	// Subjects maps subject → message count for the subjects matching the
	// filter passed to Stream.State; nil when no filter was given.
	Subjects map[string]uint64
}

// ErrConsumerNotFound is returned by Stream.ConsumerAckFloor when the named
// consumer does not exist on the stream (yet).
var ErrConsumerNotFound = errors.New("consumer not found")

// Stream is a handle on one stream, resolved once by StreamManager.Stream so
// the per-call methods don't re-resolve it.
type Stream interface {
	// State reports the stream's sequence bounds and message count. A
	// non-empty subjectFilter (e.g. ">" or "dlq.events") also fills
	// StreamState.Subjects with per-subject counts for the matching subjects.
	State(ctx context.Context, subjectFilter string) (StreamState, error)
	// MessageTime returns the stored timestamp of the message at seq. A purged
	// or never-stored sequence is an error.
	MessageTime(ctx context.Context, seq uint64) (time.Time, error)
	// PurgeBelow removes every message with a sequence lower than seq.
	PurgeBelow(ctx context.Context, seq uint64) error
	// ConsumerAckFloor returns the named consumer's ack floor: the highest
	// stream sequence below which every message has been acked.
	// ErrConsumerNotFound when the consumer has not been created.
	ConsumerAckFloor(ctx context.Context, consumer string) (uint64, error)
}

// StreamManager looks up streams by name (StreamName, DLQStreamName).
type StreamManager interface {
	Stream(ctx context.Context, name string) (Stream, error)
}

// Replayer re-delivers stored messages for SSE gap-fill.
type Replayer interface {
	// ReplaySince sends the data of every message on subject stored at or
	// after since, in stream order, until send returns false or the stream is
	// caught up. Only failing to start the replay is an error; running out of
	// messages is the normal end.
	ReplaySince(ctx context.Context, subject string, since time.Time, send func(data []byte) bool) error
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
