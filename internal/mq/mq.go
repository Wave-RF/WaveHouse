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
}

// NewMessage constructs a Message with ack/nak callbacks.
func NewMessage(ctx context.Context, topic Topic, data []byte, ts time.Time, doubleAck func(context.Context) error, ack func() error, nak func() error) *Message {
	return newMessage(ctx, topic.key(), data, ts, doubleAck, ack, nak)
}

func newMessage(ctx context.Context, topicKey string, data []byte, ts time.Time, doubleAck func(context.Context) error, ack func() error, nak func() error) *Message {
	return &Message{Ctx: ctx, topicKey: topicKey, Data: data, Timestamp: ts, doubleAckFn: doubleAck, ackFn: ack, nakFn: nak}
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

// ErrQueueFull is returned by Publisher.Publish when the topic's tenant's
// ingest queue refuses new events — it is at its byte budget, or the tenant
// has no queue open yet — the backpressure signal the API turns into a 503
// with Retry-After.
var ErrQueueFull = errors.New("ingest queue is full")

// Publisher appends events to the ingest queue.
type Publisher interface {
	// Publish stores data as one event on topic, in the ingest queue of the
	// topic's tenant. ErrQueueFull when that queue is at its byte budget, or
	// the tenant has no queue open yet (see Broker.SetMaxBytes).
	Publish(ctx context.Context, topic Topic, data []byte, opts ...PublishOpt) error
	Close() error
}

// Subscriber delivers every event on the ingest queue, across all tenants
// and topics: each tenant's in the order it was published, and different
// tenants' concurrently.
type Subscriber interface {
	// Subscribe registers a handler for incoming events under a durable
	// consumer named consumerName, held on every tenant's queue — those
	// opened after Subscribe included. The handler runs on one delivery
	// goroutine per tenant, one message at a time, so it must be safe to
	// call concurrently for different tenants.
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
	// MaxAckPending caps unacked messages broker-side, per tenant: delivery
	// of a tenant's events pauses when that tenant's unacked ones hit it
	// (backpressure), and no other tenant's does.
	MaxAckPending int
}

// Consumer is a live durable consumer created by ConsumerManager.
type Consumer interface {
	// Consume delivers each message to handler on a delivery goroutine of its
	// tenant's: one per tenant, so a tenant's messages arrive in order, one at
	// a time, while different tenants' arrive concurrently — handler must be
	// safe for that. A handler that blocks holds back its tenant's delivery —
	// that is the backpressure the ingest worker relies on. About prefetch
	// messages are fetched ahead across the tenants together: the tenants'
	// queues when delivery starts split it, and a queue joined later fetches
	// ahead its share of it at that point, at least one message each (0 = the
	// client default, per tenant). The returned stop asks delivery to end and
	// returns without waiting: a handler invocation already in flight, or one
	// for a message already queued client-side, may still run after stop
	// returns, so a handler must not write to anything the caller tears down
	// right after stopping.
	//
	// Delivery can also end on its own after Consume has returned: the broker
	// or the client gives up on the consumer (it was deleted, the connection
	// closed), or a tenant's queue opened later could not be joined. That is
	// reported on failed — exactly one error, and nothing once stop has been
	// called — because no message will ever arrive to say so. A caller that
	// ignores failed waits forever on a dead consumer.
	Consume(handler func(msg *Message), prefetch int) (stop func(), failed <-chan error, err error)
}

// ErrDeliveryEnded is the error a Consumer reports on failed, wrapping the
// broker's reason when it gave one.
var ErrDeliveryEnded = errors.New("consumer delivery ended")

// ConsumerManager creates durable consumers on the ingest queue, held on
// every tenant's queue — those opened later included. A delivered
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
// the tenant has no dead-letter queue (nothing can have been parked for it).
// Any other failure to read it is a plain error.
var ErrNoDeadLetterQueue = errors.New("dead-letter queue not found")

// DeadLetterStats reports on the dead-letter queues.
type DeadLetterStats interface {
	// DeadLetterCounts counts tenant id's parked messages per table — a
	// tenant served, rejected, or removed alike, for as long as its queue is
	// kept. A non-empty table narrows Tables to that one (its unscoped
	// messages).
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
	// acknowledged goes. Reports whether anything was removed.
	// ErrConsumerNotFound when the consumer has not been created on some
	// tenant's queue; the other tenants' are purged all the same.
	PurgeAcked(ctx context.Context, consumer string, olderThan map[tenant.ID]time.Time) (purged bool, err error)
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
// above plus the lifecycle and the byte budgets. EmbeddedNATS is the one
// implementation; internal/app depends on this, not on it.
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
	// other tenant's queues are touched. On an error the implementation
	// restores the previous budget where it can (best effort: the error says
	// when it could not, and a canceled ctx abandons the restore too), and
	// MaxBytes keeps reporting the previous budget so the next call retries.
	// MaxBytes reports the budget last applied in full for id, 0 when none
	// has been.
	SetMaxBytes(ctx context.Context, id tenant.ID, maxBytes int64) error
	MaxBytes(id tenant.ID) int64
	// Stats reports the broker counters the system gauges observe.
	Stats() (observability.MQStats, error)
}
