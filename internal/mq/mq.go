// Package mq is the message-queue boundary: the only package that imports
// NATS/JetStream (enforced by the depguard rule in .golangci.yml, Key Design
// Decision #20), and the only one that knows how the broker works. The rest of
// the process says what it wants — publish an event for a table, consume the
// ingest queue, park a message on the dead-letter queue, replay since a time,
// drop what is both written and expired — in the types below. How that maps to
// subjects, streams, sequences, and consumers is the implementation's
// (EmbeddedNATS), so a broker change lands here once.
package mq

import (
	"context"
	"errors"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// Topic addresses the events of one table, optionally narrowed to a scope
// within it. It is the only address the rest of the process handles; the
// broker's own naming (subjects, prefixes, wildcards, token encoding) is
// derived from it inside the implementation. Table and Scope are raw names —
// never pre-encoded.
type Topic struct {
	Table string
	Scope string
}

// Key is an injective string form of the topic, for use as a map key (the SSE
// hub's subscription index). Opaque: not a broker subject, and not parseable.
func (t Topic) Key() string {
	if t.Scope == "" {
		return encodeToken(t.Table)
	}
	return encodeToken(t.Table) + "." + encodeToken(t.Scope)
}

// Message represents a message received from the queue.
type Message struct {
	Ctx       context.Context
	Data      []byte
	Timestamp time.Time
	// topicKey is the key of the topic the message was published on, kept in
	// the form the broker delivered it so the per-message path never decodes
	// or re-encodes a name (see TopicKey / Topic).
	topicKey    string
	doubleAckFn func(ctx context.Context) error
	ackFn       func() error
	nakFn       func() error
}

// NewMessage constructs a Message with ack/nak callbacks.
func NewMessage(ctx context.Context, topic Topic, data []byte, ts time.Time, doubleAck func(context.Context) error, ack func() error, nak func() error) *Message {
	return newMessage(ctx, topic.Key(), data, ts, doubleAck, ack, nak)
}

func newMessage(ctx context.Context, topicKey string, data []byte, ts time.Time, doubleAck func(context.Context) error, ack func() error, nak func() error) *Message {
	return &Message{Ctx: ctx, topicKey: topicKey, Data: data, Timestamp: ts, doubleAckFn: doubleAck, ackFn: ack, nakFn: nak}
}

// TopicKey is Topic().Key() for the topic the message was published on,
// without decoding it: free, so it is what per-message paths (the SSE hub
// bridge) use.
func (m *Message) TopicKey() string { return m.topicKey }

// Topic is the topic the message was published on. It decodes the names on
// every call, so it is for failure paths and tests, not the per-message path.
func (m *Message) Topic() Topic { return parseTopicKey(m.topicKey) }

// DoubleAck acknowledges the message synchronously, blocking until the
// broker confirms receipt. Use this for critical ingest paths (ClickHouse writes).
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

// ErrQueueFull is returned by Publisher.Publish when the ingest queue is at
// its byte budget and refuses new events — the backpressure signal the API
// turns into a 503 with Retry-After.
var ErrQueueFull = errors.New("ingest queue is full")

// Publisher appends events to the ingest queue.
type Publisher interface {
	// Publish stores data as one event on topic. ErrQueueFull when the queue
	// is at its byte budget.
	Publish(ctx context.Context, topic Topic, data []byte, opts ...PublishOpt) error
	Close() error
}

// Subscriber delivers every event on the ingest queue, across all topics.
type Subscriber interface {
	// Subscribe registers a handler for incoming events under a durable
	// consumer named consumerName.
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
	// broker-confirmed Ack. Call DoubleAck, then return nil on success.
	Subscribe(ctx context.Context, consumerName string, handler func(msg *Message) error) error
	Close() error
}

// ConsumerConfig describes a durable, explicit-ack consumer of every event on
// the ingest queue. Zero values take the broker's defaults.
type ConsumerConfig struct {
	Durable string
	// AckWait is the redelivery timeout: a message not acked within it is
	// delivered again.
	AckWait time.Duration
	// MaxAckPending caps unacked messages broker-side; delivery pauses when
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

// ConsumerManager creates durable consumers on the ingest queue. A delivered
// Message.Ctx is the ctx given to CreateConsumer: unlike Subscriber, the
// consumer path does not extract the trace context carried in the message
// headers, because its one consumer (the ingest worker) batches across
// messages and never reads a per-message context.
type ConsumerManager interface {
	CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error)
}

// DeadLetterer parks messages on the dead-letter queue.
type DeadLetterer interface {
	// DeadLetter stores msg's data on the dead-letter queue under msg's topic,
	// with the headers the options set. It does not ack msg: the caller acks
	// once the parking is confirmed, so a failure here leaves the original to
	// be redelivered.
	DeadLetter(ctx context.Context, msg *Message, opts ...PublishOpt) error
}

// DeadLetterCounts is what is parked on the dead-letter queue.
type DeadLetterCounts struct {
	// Tables maps table name → parked messages, for the tables asked about.
	Tables map[string]uint64
	// Total is every parked message, whatever the filter.
	Total uint64
}

// ErrNoDeadLetterQueue is returned by DeadLetterStats.DeadLetterCounts when
// the dead-letter queue cannot be resolved (it may not exist yet).
var ErrNoDeadLetterQueue = errors.New("dead-letter queue not found")

// DeadLetterStats reports on the dead-letter queue.
type DeadLetterStats interface {
	// DeadLetterCounts counts parked messages per table; a non-empty table
	// narrows Tables to that one.
	DeadLetterCounts(ctx context.Context, table string) (DeadLetterCounts, error)
}

// ErrConsumerNotFound is returned by Purger.PurgeAcked when the named
// consumer does not exist (yet).
var ErrConsumerNotFound = errors.New("consumer not found")

// Purger reclaims ingest-queue storage.
type Purger interface {
	// PurgeAcked removes the ingest events that are BOTH acknowledged by the
	// named durable consumer (everything before its first unacked event) AND
	// stored before olderThan. Either bound alone keeps the event: unacked
	// events are not yet written, and recent ones are still needed for replay.
	// Reports whether anything was removed. ErrConsumerNotFound when the
	// consumer has not been created.
	PurgeAcked(ctx context.Context, consumer string, olderThan time.Time) (purged bool, err error)
}

// Replayer re-delivers stored events for SSE gap-fill.
type Replayer interface {
	// ReplaySince sends the data of every event on topic stored at or after
	// since, in order, until send returns false or the queue is caught up.
	// Running out of events is the normal end; failing to start the replay, or
	// a delivery failure before it catches up, is an error. A done ctx stops
	// the replay and returns ctx's error.
	ReplaySince(ctx context.Context, topic Topic, since time.Time, send func(data []byte) bool) error
}

// Broker is everything the process wiring needs from the MQ: every interface
// above plus the lifecycle and the byte budget. EmbeddedNATS is the one
// implementation; internal/app depends on this, not on it.
type Broker interface {
	Publisher
	Subscriber
	ConsumerManager
	DeadLetterer
	DeadLetterStats
	Purger
	Replayer
	// SetMaxBytes applies a new byte budget (the hot-reloadable
	// mq.max_bytes_gb) to the queues as a whole — how it is split between
	// them is the implementation's. On an error the previous budget stays in
	// effect. MaxBytes reports the budget in effect.
	SetMaxBytes(ctx context.Context, maxBytes int64) error
	MaxBytes() int64
	// Stats reports the broker counters the system gauges observe.
	Stats() (observability.MQStats, error)
}
