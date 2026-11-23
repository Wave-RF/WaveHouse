// Package mq is the message-queue boundary: the only package that imports
// NATS/JetStream (enforced by the depguard rule in .golangci.yml, Key Design
// Decision #20), and the only one that knows how the broker works. The rest of
// the process says what it wants — publish an event for a table, follow the
// events as they are accepted, consume the ingest queue, park a message on the
// dead-letter queue, record what was written, replay since a time — in the
// types below. How that maps to
// subjects, streams, sequences, and consumers is the implementation's
// (EmbeddedNATS, or ExternalNATS over an operator-owned cluster), so a broker
// change lands here once. The behavior below is
// what mqtest checks: every implementation passes its suite.
package mq

import (
	"context"
	"errors"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
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
// the table and scope joined as escaped tokens (keyenc.AppendJoin). A topic
// without a tenant has no subject, and its key parses back to a topic of no
// tenant with the whole key as its table (parseTopicKey's fallback). Callers
// key their own maps by the Topic value itself.
func (t Topic) key() string {
	key := append([]byte(t.Tenant), '.')
	if t.Scope == "" {
		return string(keyenc.AppendJoin(key, '.', t.Table))
	}
	return string(keyenc.AppendJoin(key, '.', t.Table, t.Scope))
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
	topicKey string
	// accepted is when the ingest queue stored the message, zero when the
	// message did not come off it: what Recorder files it under, so a replay
	// can still tell when it was accepted once it is written.
	accepted    time.Time
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
// into a 503. Retry-After is the dedupe lease, rounded up to whole seconds,
// when the record held a claim (so an obedient client waits out the window
// instead of retrying straight into it), else a flat few seconds. Only a
// backend whose broker is out of process returns it; the embedded one's
// publish failures are plain errors.
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

// Subscriber is the live feed: every event the ingest queue accepts, across
// all tenants and topics, as it is accepted.
type Subscriber interface {
	// Subscribe delivers to handler every event the ingest queue stores from
	// now on, once each, until ctx is done: every tenant's, those whose queues
	// open after Subscribe included, and each topic's in the order the queue
	// stored it. An event the queue did not store — a repeat its duplicate
	// window dropped, one it refused as full — is never delivered. handler
	// runs one event at a time, with the trace context the publisher's
	// headers carried.
	//
	// The feed is read beside the ingest queue, not from it: nothing is acked,
	// nothing is redelivered, and an event stored before Subscribe is the
	// Replayer's to serve. So the feed can skip events — the connection to the
	// broker was lost, or the process fell far enough behind that some were
	// dropped — and when it may have, lost is called before any later event
	// reaches handler. Whoever relays the feed must then send its readers back
	// to the Replayer: one that kept reading would take a later event's
	// position as its own and never be sent the skipped ones. lost may also be
	// called while handler runs, as soon as a lost connection is back.
	Subscribe(ctx context.Context, handler func(msg *Message), lost func()) error
	Close() error
}

// ConsumerConfig describes a durable, explicit-ack consumer of every event on
// the ingest queue, or of some of its units (see Sharded). Zero values take
// the broker's defaults.
type ConsumerConfig struct {
	Durable string
	// Units limits the consumer to these units of a Sharded broker's queue,
	// by the names IngestUnits gives. Nil is every unit. A broker that does
	// not implement Sharded refuses a non-nil Units.
	Units []string
	// AckWait is the redelivery timeout: a message not acked within it is
	// delivered again.
	AckWait time.Duration
	// MaxAckPending caps unacked messages broker-side, per delivery unit (a
	// tenant's queue, or the partition that holds it): delivery from a unit
	// pauses when its unacked messages hit it (backpressure), and no other
	// unit's does.
	MaxAckPending int
	// MaxHeld, when set, caps the messages each unit of a Sharded broker's
	// queue has delivered to this process and not yet had settled: at the
	// cap the consumer fetches no more from that unit than it takes to keep
	// its hold on the unit, until one settles. It is read before every fetch, so
	// the cap may change while consuming. Nil is no cap but the broker's own.
	// A broker that does not implement Sharded ignores it.
	MaxHeld func() int
}

// Sharded is implemented by a broker whose ingest queue is split into units
// that one consumer at a time owns: every event of one tenant table is in one
// unit, and the broker lets exactly one puller receive from a unit at a time.
// A broker that does not implement it is one queue every consumer shares.
type Sharded interface {
	// IngestUnits names the units: the configured ones, in a fixed order the
	// same in every process, then any extra ones a lower unit count left
	// holding rows, as last read, which are drained like the others.
	IngestUnits() (configured, extra []string)
	// ResetOrphaned redelivers at once what a unit's previous owner received
	// and never settled, when no consumer holds the unit, none has received
	// from it or settled a message of it for a while, and rows are awaiting
	// a settlement; it reports whether it did, and ErrUnitHeld while a
	// consumer holds the unit or was recently active on it. A missing hold
	// alone is no proof the owner is gone. Call it only for a unit whose
	// owner the caller has other evidence is dead (the membership view), and
	// before consuming it, never while consuming it: a reset also redelivers
	// what the caller itself holds.
	ResetOrphaned(ctx context.Context, unit string) (bool, error)
	// UnitStates reads every unit's backlog from the broker, one state per
	// unit, in IngestUnits' order, leaving out a unit whose consumer is
	// gone (the topology check reports it): what the ingest processes
	// export.
	UnitStates(ctx context.Context) ([]UnitState, error)
}

// UnitState is one unit's backlog as the broker last read it.
type UnitState struct {
	Unit string
	// Pending is the rows stored and not yet delivered; AckPending the rows
	// delivered and awaiting a settlement, which the broker caps at
	// MaxAckPending, delivering nothing more from the unit while it is
	// reached; Redelivered the rows among them delivered more than once.
	Pending, AckPending, MaxAckPending, Redelivered int
	// Held reports a consumer holding the unit, or one that received from
	// it or settled a message of it recently enough to be judged live.
	Held bool
}

// Unowned reports a unit with rows waiting that no consumer holds and none
// has received from or settled a message of for a while: rows nobody is
// writing.
func (s UnitState) Unowned() bool {
	return (s.Pending > 0 || s.AckPending > 0) && !s.Held
}

// Releaser is implemented by a consumer of a Sharded broker's units. Release
// gives the units up so that their next owner is served at once, rather than
// once the broker judges this consumer gone: call it after Consume's stop,
// once every message delivered has been settled. A unit this consumer never
// held is left alone.
type Releaser interface {
	Release(ctx context.Context) error
}

// Halter is implemented by a Consumer that can stop fetching ahead of its
// stop, so that its caller can write what it holds before the consumer lets
// its units go. Halt returns once no handler invocation is running and none
// will: everything already fetched has reached the handler. The consumer
// keeps its hold on its units until stop, but not indefinitely: a Sharded
// broker's for at least its ack wait after the halt has handed on what it
// fetched.
type Halter interface {
	Halt()
}

// ErrUnitHeld is ResetOrphaned's answer while a consumer still holds the
// unit, or has received from it or settled a message of it too recently to
// be judged gone.
var ErrUnitHeld = errors.New("unit held by a consumer")

// ErrConsumerMismatch is CreateConsumer's answer when a consumer the broker's
// operator owns no longer fits the ConsumerConfig: its ack_wait is shorter,
// or its max_ack_pending unset.
var ErrConsumerMismatch = errors.New("consumer does not match its config")

// ErrUnitsUnsupported is CreateConsumer's answer to a ConsumerConfig naming
// units on a broker that does not implement Sharded, or naming one it does
// not have.
var ErrUnitsUnsupported = errors.New("consumer units not supported")

// Consumer is a live durable consumer created by ConsumerManager.
type Consumer interface {
	// Consume delivers each message to handler on the delivery goroutine of
	// its delivery unit — the tenant's queue, or the partition that holds
	// it: one per unit, so a tenant's messages arrive in order, one at a
	// time, while different units' arrive concurrently — handler must be
	// safe for that. A handler that blocks holds back its unit's delivery —
	// that is the backpressure the ingest worker relies on; a Sharded
	// broker's consumer keeps its hold on the unit meanwhile. About prefetch
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
	// closed). That is reported on failed — exactly one error, and nothing
	// once stop has been called — because no message will ever arrive to say
	// so. A caller that ignores failed waits forever on a dead consumer. A
	// queue opened later that the consumer cannot join is not that: it is
	// its tenant's failure, reported by SetMaxBytes and to the tenant's
	// publishes, and delivery from every other queue goes on.
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
	// Tables maps table name → parked messages, for the tables asked about;
	// empty, never nil, when none has any.
	// Every scope of a table counts under the table; scope is not broken out
	// yet (it is inert until #235).
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
	// kept. A non-empty table narrows Tables to that one (all of its
	// scopes). A tenant with nothing parked has zero counts, or
	// ErrNoDeadLetterQueue when it has no queue at all.
	DeadLetterCounts(ctx context.Context, id tenant.ID, table string) (DeadLetterCounts, error)
}

// Recorder keeps written events for the Replayer.
type Recorder interface {
	// Record stores each of msgs — messages a Consumer delivered, whose rows
	// are now written — in the replay history of its tenant, under its topic
	// and with the time the ingest queue accepted it, in the order given: what
	// ReplaySince serves of it once the message is acked and gone from the
	// queue. Call it after the write and before the acks, so an event is
	// always in the queue or in the history. It acks nothing.
	//
	// Best effort, and bounded: the error counts the messages it could not
	// store in the time it allows itself, and the caller acks them all the
	// same — a written row left unacked is written again when the queue
	// redelivers it. Such a row is missing from replays from then on. How
	// long the history keeps a row, counted from its Record, is the tenant's
	// replay window (Broker.SetReplayWindow), or the broker operator's.
	Record(ctx context.Context, msgs []*Message) error
}

// ErrConsumerNotFound reports a durable consumer the broker's operator owns
// that is not there: CreateConsumer's answer for a name that maps to none or
// whose durable or stream is gone, and the failure a delivery ends on when its
// durable is deleted under it.
var ErrConsumerNotFound = errors.New("consumer not found")

// Replayer re-delivers stored events for SSE gap-fill.
type Replayer interface {
	// ReplaySince sends the data of every event on topic that the ingest
	// queue accepted at or after since and the broker still holds: the ones
	// already written that the history keeps (Recorder), in the order they
	// were written, then the ones still waiting in the queue to be written,
	// whatever their age, in the order they were accepted. An event written
	// while the replay runs may be sent twice; none it holds is left out. It
	// ends when send returns false or everything stored when it began has
	// been sent: what is accepted meanwhile is the Subscriber's to deliver.
	// Running out of events is the normal end; failing to start the replay, or
	// a read that fails before it catches up, is an error. A done ctx stops
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
	Recorder
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
	// SetReplayWindow applies tenant id's replay window (its hot-reloadable
	// stream.gap_window_minutes): how long the history keeps one of the
	// tenant's events after it is recorded as written (Recorder), 0 for not at
	// all. A tenant whose window was never set keeps none; one no longer
	// served keeps the window it last had. An implementation whose history
	// the broker's operator sizes only records the window, and warns when the
	// history keeps less.
	SetReplayWindow(ctx context.Context, id tenant.ID, window time.Duration) error
	// Stats reports the broker counters the system gauges observe.
	Stats() (observability.MQStats, error)
}
