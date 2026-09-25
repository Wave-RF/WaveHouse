// Package mq is the message-queue boundary: the only package that imports
// NATS/JetStream (enforced by the depguard rule in .golangci.yml, Key Design
// Decision #20), and the only one that knows how the broker works. The rest of
// the process says what it wants — publish an event for a table, consume the
// ingest queue, park a message on the dead-letter queue, replay since a time,
// drop what is both written and expired — in the types below. How that maps to
// subjects, streams, sequences, and consumers is the implementation's
// (EmbeddedNATS, or ExternalNATS over an operator-owned cluster), so a broker
// change lands here once. The behavior below is
// what mqtest checks: every implementation passes its suite.
package mq

import (
	"context"
	"errors"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Topic addresses the events of one tenant's table, optionally narrowed to a
// scope within it. It is the only address the rest of the process handles;
// the broker's own naming (subjects, prefixes, wildcards, token encoding) is
// derived from it inside the implementation. Table and Scope are raw names —
// never pre-encoded.
type Topic struct {
	// Tenant is whose table it is — the settings folder the events were
	// admitted under (#583). Required: Publish refuses a topic without one,
	// so no caller falls into tenant.Default by omission.
	Tenant tenant.ID
	Table  string
	Scope  string
}

// key is the injective string form of the topic that a subject's tail
// carries: the tenant first, verbatim — its grammar makes it one token — then
// the table and scope as encoded tokens. A topic without a tenant has no
// subject, and its key parses back to a topic of no tenant with the whole key
// as its table (parseTopicKey's fallback). Callers key their own maps by the
// Topic value itself.
func (t Topic) key() string {
	key := string(t.Tenant) + "." + encodeToken(t.Table)
	if t.Scope != "" {
		key += "." + encodeToken(t.Scope)
	}
	return key
}

// Message represents a message received from the queue.
type Message struct {
	Ctx       context.Context
	Data      []byte
	Timestamp time.Time
	// topicKey is the key of the topic the message was published on, kept in
	// the form the broker delivered it, so parking it (DeadLetter) is a prefix
	// swap that never decodes or re-encodes a name, and TopicKey is free (see
	// TopicKey / Topic).
	topicKey    string
	doubleAckFn func(ctx context.Context) error
	ackFn       func() error
	nakFn       func() error
	nakDelayFn  func(time.Duration) error
}

// MessageOpt configures a Message beyond its required callbacks.
type MessageOpt func(*Message)

// WithNakDelay gives a Message its delayed negative acknowledgement (see
// NakWithDelay).
func WithNakDelay(fn func(time.Duration) error) MessageOpt {
	return func(m *Message) { m.nakDelayFn = fn }
}

// NewMessage constructs a Message with ack/nak callbacks.
func NewMessage(ctx context.Context, topic Topic, data []byte, ts time.Time, doubleAck func(context.Context) error, ack func() error, nak func() error, opts ...MessageOpt) *Message {
	return newMessage(ctx, topic.key(), data, ts, doubleAck, ack, nak, opts...)
}

func newMessage(ctx context.Context, topicKey string, data []byte, ts time.Time, doubleAck func(context.Context) error, ack func() error, nak func() error, opts ...MessageOpt) *Message {
	m := &Message{Ctx: ctx, topicKey: topicKey, Data: data, Timestamp: ts, doubleAckFn: doubleAck, ackFn: ack, nakFn: nak}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// TopicKey is the delivered form of the topic the message was published on,
// undecoded: free, and opaque — what a log line names the message by.
func (m *Message) TopicKey() string { return m.topicKey }

// Topic is the topic the message was published on — its tenant included,
// which is how the consumers learn whose event it is. It decodes the key on
// every call: a split and two unescapes, which the per-message paths (the
// hub bridge, the worker) pay once each ahead of decoding the envelope.
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

// NakWithDelay negatively acknowledges the message, asking for redelivery no
// sooner than delay — a retry that backs off rather than coming straight
// back. Fire-and-forget like Nak, which it falls back to when the message
// has no delayed form.
func (m *Message) NakWithDelay(delay time.Duration) error {
	if m.nakDelayFn != nil {
		return m.nakDelayFn(delay)
	}
	return m.Nak()
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

// idempotencyHeader carries WithIdempotencyKey's key: JetStream's own
// message-id header, which the stream deduplicates on.
const idempotencyHeader = "Nats-Msg-Id"

// WithIdempotencyKey marks a publish with key: a second publish carrying the
// same key within the queue's duplicate window is dropped by the broker and
// reported as success, so republishing an event whose first publish had an
// unknown outcome stores it once.
func WithIdempotencyKey(key string) PublishOpt {
	return func(h Headers) {
		h.Set(idempotencyHeader, key)
	}
}

// ErrQueueFull is returned by Publisher.Publish when the queue that holds the
// topic's tenant refuses new events because it is at a byte limit — the
// backpressure signal the API turns into a 503 with Retry-After. Which limits
// there are, and which tenants share one, is the implementation's (see
// Broker.SetMaxBytes). An implementation that opens a queue per tenant also
// returns it for a tenant whose queue it cannot open yet.
var ErrQueueFull = errors.New("ingest queue is full")

// ErrUnavailable is returned when the broker cannot be reached or does not
// answer in time — a transient failure, not a refusal, that the API turns
// into a 503 with a short Retry-After. Only a backend whose broker is out of
// process returns it; the embedded one's publish failures are plain errors.
var ErrUnavailable = errors.New("message queue unavailable")

// Publisher appends events to the ingest queue.
type Publisher interface {
	// Publish stores data as one event on topic, in the ingest queue that
	// holds the topic's tenant. A topic without a valid tenant is refused
	// before anything is sent. ErrQueueFull when that queue refuses the event
	// at a byte limit (or, per tenant, cannot be opened yet), ErrUnavailable
	// when the broker cannot take it now.
	Publish(ctx context.Context, topic Topic, data []byte, opts ...PublishOpt) error
	Close() error
}

// Subscriber delivers every event on the ingest queue, across all tenants
// and topics: each tenant's in the order it was published.
type Subscriber interface {
	// Subscribe registers a handler for incoming events, across every
	// tenant — those whose queues open after Subscribe included. Every event
	// published after Subscribe returns is delivered; whether earlier ones
	// are is the implementation's, and so is whether consumerName names a
	// durable consumer. The handler runs one message at a time on each
	// delivery unit — a tenant's queue, or the partition that holds it — so
	// it must be safe to call concurrently for different units. The messages
	// fetched ahead of it are a fixed number split across the units, as
	// Consumer.Consume's prefetch is, so they do not grow with the number of
	// tenants.
	//
	// CONTRACT: If the handler intends to return an error to trigger automatic
	// redelivery, it MUST NOT manually call msg.Ack() or msg.Nak() beforehand.
	//
	// CONTRACT: Symmetrically, if you explicitly call msg.Nak(), do NOT also
	// return an error — the consume loop will call Nak() again on any non-nil
	// error return.
	//
	// CONTRACT: Calling msg.DoubleAck(ctx) and then returning a non-nil error is
	// undefined behavior — the consume loop will Nak() after a successful
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
	// MaxAckPending caps unacked messages broker-side, per delivery unit (a
	// tenant's queue, or the partition that holds it): delivery from a unit
	// pauses when its unacked messages hit it (backpressure), and no other
	// unit's does.
	MaxAckPending int
}

// Consumer is a live durable consumer created by ConsumerManager.
type Consumer interface {
	// Consume delivers each message to handler on the delivery goroutine of
	// its delivery unit — the tenant's queue, or the partition that holds
	// it: one per unit, so a tenant's messages arrive in order, one at a
	// time, while different units' arrive concurrently — handler must be
	// safe for that. A handler that blocks holds back its unit's delivery —
	// that is the backpressure the ingest worker relies on. About prefetch
	// messages are fetched ahead across the units together: the units when
	// delivery starts split it, and a queue joined later fetches ahead its
	// share of it at that point, at least one message each (0 = the client
	// default, per unit). The returned stop asks delivery to end and returns
	// without waiting: a handler invocation already in flight, or one for a
	// message already queued client-side, may still run after stop returns,
	// so a handler must not write to anything the caller tears down right
	// after stopping.
	//
	// Delivery can also end on its own after Consume has returned: the broker
	// or the client gives up on the consumer (it was deleted, the connection
	// closed), or a queue opened later could not be joined. That is
	// reported on failed — exactly one error, and nothing once stop has been
	// called — because no message will ever arrive to say so. A caller that
	// ignores failed waits forever on a dead consumer.
	Consume(handler func(msg *Message), prefetch int) (stop func(), failed <-chan error, err error)
}

// ErrDeliveryEnded is the error a Consumer reports on failed, wrapping the
// broker's reason when it gave one.
var ErrDeliveryEnded = errors.New("consumer delivery ended")

// ConsumerManager gives access to durable consumers on the ingest queue, held
// on every tenant's queue — those opened later included. Whether
// CreateConsumer creates the durable, or only finds one someone else made and
// checks it against the config, is the implementation's. A delivered
// Message.Ctx is the ctx given to CreateConsumer: unlike Subscriber, the
// consumer path does not extract the trace context carried in the message
// headers, because its one consumer (the ingest worker) batches across
// messages and never reads a per-message context.
type ConsumerManager interface {
	CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error)
}

// DeadLetterer parks messages on the dead-letter queue.
type DeadLetterer interface {
	// DeadLetter stores msg's data on the dead-letter queue of msg's tenant,
	// under msg's topic, with the headers the options set. It does not ack
	// msg: the caller acks once the parking is confirmed, so a failure here
	// leaves the original to be redelivered.
	DeadLetter(ctx context.Context, msg *Message, opts ...PublishOpt) error
}

// DeadLetterCounts is what is parked on one tenant's dead-letter queue.
type DeadLetterCounts struct {
	// Tables maps table name → parked messages, for the tables asked about.
	// Scope is not broken out yet (it is inert until #235): a message parked
	// under a scoped topic counts under "table.scope", not under its table.
	Tables map[string]uint64
	// Total is every parked message of the tenant, whatever the filter.
	Total uint64
}

// ErrNoDeadLetterQueue is returned by DeadLetterStats.DeadLetterCounts when
// the tenant has no dead-letter queue of its own (nothing can have been
// parked for it). An implementation whose tenants share one queue returns
// zero counts instead. Any other failure to read it is a plain error.
var ErrNoDeadLetterQueue = errors.New("dead-letter queue not found")

// DeadLetterStats reports on the dead-letter queues.
type DeadLetterStats interface {
	// DeadLetterCounts counts tenant id's parked messages per table — a
	// tenant served, rejected, or removed alike, for as long as its queue is
	// kept. A non-empty table narrows Tables to that one (its unscoped
	// messages). A tenant with nothing parked has zero counts, or
	// ErrNoDeadLetterQueue when it has no queue at all.
	DeadLetterCounts(ctx context.Context, id tenant.ID, table string) (DeadLetterCounts, error)
}

// ErrConsumerNotFound is returned by Purger.PurgeAcked when the named
// consumer does not exist (yet) on a tenant's queue.
var ErrConsumerNotFound = errors.New("consumer not found")

// Purger reclaims ingest-queue storage.
type Purger interface {
	// PurgeAcked removes, from each tenant's ingest queue, the events that
	// are BOTH acknowledged by the named durable consumer (everything before
	// its first unacked event) AND stored before that tenant's cutoff in
	// olderThan. Either bound alone keeps the event: unacked events are not
	// yet written, and recent ones are still needed for replay. A tenant
	// olderThan does not name keeps no history: everything it has
	// acknowledged goes. Reports whether anything was removed, and joins
	// each failed tenant's error — ErrConsumerNotFound for one whose queue the
	// consumer has not been created on; the other tenants' are purged all the
	// same. An implementation whose retention the broker's operator owns
	// removes nothing and reports false: either way, no unacked event is
	// removed.
	PurgeAcked(ctx context.Context, consumer string, olderThan map[tenant.ID]time.Time) (purged bool, err error)
}

// Replayer re-delivers stored events for SSE gap-fill.
type Replayer interface {
	// ReplaySince sends the data of every event on topic stored at or after
	// since, in order, until send returns false or the queue is caught up.
	// Running out of events is the normal end; failing to start the replay, or
	// a delivery failure before it catches up, is an error. A done ctx stops
	// the replay and returns ctx's error. A topic without a valid tenant is
	// refused as Publish refuses it.
	ReplaySince(ctx context.Context, topic Topic, since time.Time, send func(data []byte) bool) error
}

// Broker is everything the process wiring needs from the MQ: every interface
// above plus the lifecycle and the byte budgets. EmbeddedNATS and
// ExternalNATS implement it; internal/app depends on this, not on either.
type Broker interface {
	Publisher
	Subscriber
	ConsumerManager
	DeadLetterer
	DeadLetterStats
	Purger
	Replayer
	// SetMaxBytes applies tenant id's byte budget (its hot-reloadable
	// mq.max_bytes_gb) to that tenant's queues — how it is split between them
	// is the implementation's — opening them if the tenant has none yet. No
	// other tenant's queues are touched. An implementation whose tenants
	// share queues may only record the budget, and say so where it does. On
	// an error the implementation restores the previous budget where it can
	// (best effort: the error says when it could not, and a canceled ctx
	// abandons the restore too), and MaxBytes keeps reporting the previous
	// budget so the next call retries.
	// MaxBytes reports the budget last applied in full for id, 0 when none
	// has been.
	SetMaxBytes(ctx context.Context, id tenant.ID, maxBytes int64) error
	MaxBytes(id tenant.ID) int64
	// Stats reports the broker counters the system gauges observe.
	Stats() (observability.MQStats, error)
}
