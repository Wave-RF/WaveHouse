package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// slogNATSLogger adapts the default slog logger to the natsserver.Logger
// interface.
type slogNATSLogger struct{}

func (slogNATSLogger) Noticef(format string, v ...any) {
	slog.Info(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Warnf(format string, v ...any) {
	slog.Warn(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Fatalf(format string, v ...any) {
	slog.Error(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Errorf(format string, v ...any) {
	slog.Error(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Debugf(format string, v ...any) {
	slog.Debug(fmt.Sprintf(format, v...), "component", "nats")
}

func (slogNATSLogger) Tracef(format string, v ...any) {
	slog.Debug(fmt.Sprintf(format, v...), "component", "nats")
}

// EmbeddedNATS runs an in-process NATS server with JetStream, and gives each
// tenant a queue of its own: an ingest stream and a dead-letter stream
// (subject.go names them), each with its own byte cap, and the durable
// consumers on the ingest one. Nothing outside this package sees that layout.
type EmbeddedNATS struct {
	server *natsserver.Server
	conn   *nats.Conn
	js     jetstream.JetStream

	// mu guards queues, consumers and writes to opened, and serializes
	// opening or resizing a tenant's queue with registering a consumer, so a
	// queue opened while a consumer registers is never missed by it. It is
	// held across the JetStream calls that open or resize a queue.
	mu     sync.Mutex
	queues map[tenant.ID]*tenantQueue
	// consumers are the durable consumers held on every tenant's queue, each
	// joined to a queue as it opens.
	consumers []*fanIn
	// opened holds the tenants whose queue has both streams, every registered
	// consumer joined to it or told it could not be (fanIn.fail) — what
	// Publish trusts, rather than a stream answering: an open that gave up can
	// leave behind a stream JetStream goes on to create, which no consumer
	// holds. Written under mu, read without it.
	opened sync.Map // tenant.ID → struct{}
}

// tenantQueue is what the broker knows of one tenant's queue.
type tenantQueue struct {
	// ingest and dlq report whether each of the tenant's streams exists.
	ingest, dlq bool
	// maxBytes is the budget last applied in full (MaxBytes); asked is the
	// budget last asked for, which a publish or park that finds a stream
	// missing opens it at. Boot reads asked back from the ingest stream, so a
	// tenant no longer served keeps the budget it last had, and maxBytes too
	// when the pair is whole at it (takeStock).
	maxBytes, asked int64
	// ingestCap is the cap the ingest stream has — what a failed resize
	// restores it to. Not maxBytes: a pair boot found split has a cap but no
	// budget applied in full, and a cap of 0 would be none at all.
	ingestCap int64
}

// EmbeddedNATS is the one implementation of every mq interface.
var _ Broker = (*EmbeddedNATS)(nil)

const (
	// dlqShare is a tenant's dead-letter stream's slice of its byte budget: a
	// tenth of the ingest stream's cap.
	dlqShare = 10
	// resizeTimeout bounds the JetStream calls SetMaxBytes makes to open a
	// tenant's queue or apply a new cap to it — both streams share it. A
	// settings reload holds the store's lock while its hooks run, so an
	// in-process JetStream call that never returns would otherwise block every
	// later reload.
	resizeTimeout = 10 * time.Second
	// rollbackTimeout is the undo's own budget when the dead-letter resize
	// fails: in-process JetStream fails by stalling rather than erroring, so
	// the likely cause is that resizeTimeout has just run out, and an undo on
	// that context would fail without touching the stream. SetMaxBytes runs
	// for at most the sum of the two when it resizes, and for two
	// resizeTimeouts when it opens a queue: the consumers join on a budget of
	// their own (apply).
	rollbackTimeout = 5 * time.Second
)

// errNoQueue is why a publish or park finds no queue it can open: no budget
// has been asked for the tenant yet (see SetMaxBytes). Publish reports it as
// ErrQueueFull.
var errNoQueue = errors.New("no queue is open for it yet")

// NewEmbedded starts an embedded NATS server with JetStream over storeDir and
// takes stock of the tenants' queues already there: a consumer created later
// is held on every one of them, those of tenants no longer served included,
// whose queued rows still have to reach the ingest worker. The pair of streams
// an earlier build kept for every tenant together is deleted, since its
// subjects overlap every tenant's; the events it held are not carried over. A
// tenant's queue is opened by SetMaxBytes, the first time its budget is
// applied, or by a publish or park that finds it missing, at the budget last
// asked for it. The server logs through slog's default logger.
func NewEmbedded(storeDir string) (*EmbeddedNATS, error) {
	opts := &natsserver.Options{
		DontListen: true,
		JetStream:  true,
		StoreDir:   storeDir,
		SyncAlways: true, // fsync every JetStream write — publish ACKs only after data is on disk
		// Without NoSigs, Start() installs a process-wide SIGINT handler that
		// races the app's graceful shutdown (double Shutdown → "close of nil
		// channel" panic) and os.Exit(0)s past its cleanup. WaveHouse owns
		// the lifecycle; Close() shuts the server down. See #287.
		NoSigs: true,
		// JetStream counts every stream's byte cap as reserved disk and
		// refuses a stream once the caps together pass this limit — by
		// default 75% of the free disk at boot. A tenant's mq.max_bytes_gb
		// caps that tenant's queue and nothing else; what the tenants' caps
		// add up to against the disk is #138's to decide, not a limit the
		// server enforces on the side, so its own is set out of reach. Half
		// the int64 range, not all of it: the server subtracts its count of
		// reserved bytes from this limit, and a stream whose store fails to
		// open releases a reservation it never made (nats-server 2.14.6), so
		// the count can fall below zero — at the top of the range that
		// subtraction overflows, and every stream after it is refused.
		JetStreamMaxStore: math.MaxInt64 / 2,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("new nats server: %w", err)
	}
	ns.SetLogger(slogNATSLogger{}, false, false)
	ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, fmt.Errorf("nats server not ready")
	}

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("connect to embedded nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("jetstream new: %w", err)
	}

	e := &EmbeddedNATS{server: ns, conn: nc, js: js, queues: map[tenant.ID]*tenantQueue{}}
	if err := e.takeStock(context.Background()); err != nil {
		_ = e.Close()
		return nil, err
	}
	return e, nil
}

// takeStock deletes the pair of streams an earlier build kept for every
// tenant together, then records every tenant stream on disk, with the budget
// its ingest stream last had.
func (e *EmbeddedNATS) takeStock(ctx context.Context) error {
	for _, name := range []string{legacyIngestStream, legacyDLQStream} {
		if err := e.deleteLegacy(ctx, name); err != nil {
			return err
		}
	}
	type dlqState struct {
		limit int64
		held  uint64
	}
	dlqs := map[tenant.ID]dlqState{}
	streams := e.js.ListStreams(ctx)
	for info := range streams.Info() {
		name := info.Config.Name
		if id, ok := streamTenant(ingestStreamPrefix, name); ok {
			q := e.queue(id)
			q.ingest = true
			q.asked, q.ingestCap = info.Config.MaxBytes, info.Config.MaxBytes
		} else if id, ok := streamTenant(dlqStreamPrefix, name); ok {
			e.queue(id).dlq = true
			dlqs[id] = dlqState{limit: info.Config.MaxBytes, held: info.State.Bytes}
		}
	}
	if err := streams.Err(); err != nil {
		return fmt.Errorf("list streams: %w", err)
	}
	// A pair is at its budget when its dead-letter stream is at a tenth of
	// the ingest cap, or above it holding more than that: the shrink guard's
	// doing. Anything else is a pair a stop or a failed update left split, or
	// one missing its dead-letter stream, so its budget stays unapplied and
	// the boot's SetMaxBytes applies it to both streams again.
	for id, q := range e.queues {
		d, ok := dlqs[id]
		tenth := q.asked / dlqShare
		guarded := d.limit > tenth && d.held <= math.MaxInt64 && int64(d.held) > tenth
		if q.ingest && ok && (d.limit == tenth || guarded) {
			q.maxBytes = q.asked
		}
		e.record(id, q)
	}
	return nil
}

// deleteLegacy deletes one stream of the pair an earlier build kept for every
// tenant together, logging what it held; one that is not there is nothing to
// do.
func (e *EmbeddedNATS) deleteLegacy(ctx context.Context, name string) error {
	s, err := e.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("look up stream %s: %w", name, err)
	}
	held := s.CachedInfo().State.Msgs
	if err := e.js.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("delete stream %s: %w", name, err)
	}
	slog.Warn("mq: deleted the stream an earlier build kept for every tenant together; its messages are not carried over",
		"component", "nats", "stream", name, "messages", held)
	return nil
}

// queue returns what the broker knows of tenant id's queue, recording the
// tenant first if it knows nothing. Under e.mu (or before e is shared).
func (e *EmbeddedNATS) queue(id tenant.ID) *tenantQueue {
	q := e.queues[id]
	if q == nil {
		q = &tenantQueue{}
		e.queues[id] = q
	}
	return q
}

// ingestTenants lists the tenants whose ingest stream exists, in id order.
// Under e.mu.
func (e *EmbeddedNATS) ingestTenants() []tenant.ID {
	var ids []tenant.ID
	for id, q := range e.queues {
		if q.ingest {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// record brings opened in line with what the broker knows of tenant id's
// queue. Both streams known means every consumer has been joined to the
// queue too, or told it could not be: apply joins the consumers to a queue it
// opens before this records it, and a consumer registered later joins every
// ingest stream there is. Under e.mu (or before e is shared).
func (e *EmbeddedNATS) record(id tenant.ID, q *tenantQueue) {
	if q.ingest && q.dlq {
		e.opened.Store(id, struct{}{})
	} else {
		e.opened.Delete(id)
	}
}

// ingestStreamConfig is tenant id's ingest stream. LimitsPolicy: standard
// append-only log; the Active Sweeper handles message purging. MaxBytes caps
// the tenant's share of the disk. DiscardNew rejects new messages when full,
// propagating backpressure to the upstream API — for this tenant alone.
func ingestStreamConfig(id tenant.ID, maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      ingestStreamName(id),
		Subjects:  []string{tenantSubjects(ingestPrefix, id)},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardNew,
	}
}

// dlqStreamConfig is tenant id's dead-letter stream. DiscardOld: a full one
// drops its oldest parked rows rather than refusing new ones — backpressure
// belongs to the ingest stream, not the dead-letter one.
func dlqStreamConfig(id tenant.ID, maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      dlqStreamName(id),
		Subjects:  []string{tenantSubjects(dlqPrefix, id)},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardOld,
	}
}

// MaxBytes reports the budget tenant id's queue was last given in full (by
// SetMaxBytes, or read back from disk at boot), 0 when it has none.
func (e *EmbeddedNATS) MaxBytes(id tenant.ID) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if q := e.queues[id]; q != nil {
		return q.maxBytes
	}
	return 0
}

// SetMaxBytes applies tenant id's byte budget (its hot-reloadable
// mq.max_bytes_gb) to its queue: the ingest stream takes maxBytes and the
// dead-letter stream a tenth of it. A tenant with no queue yet has one opened,
// its dead-letter stream first, so no row is queued that could not be parked,
// and every registered consumer joins it. No other tenant's queue is touched.
//
// JetStream applies a limit change to a live stream without touching its
// messages: growing takes effect immediately; shrinking the ingest stream
// below its current size makes DiscardNew refuse new publishes until the
// sweeper purges it back under the cap — nothing buffered is dropped. The
// dead-letter stream is DiscardOld, which would delete its oldest parked rows
// to fit a smaller cap, so it is never capped below the bytes it holds (#532):
// it keeps what it has, and that is logged.
//
// The pair moves together where it can. If the dead-letter update fails after
// the ingest one succeeded, the ingest resize is undone so the pair stays at
// the previous budget, and the next call retries both. Safe in that direction
// — the ingest stream is DiscardNew, so shrinking it back drops nothing
// stored. The undo is best effort: if it fails too, the ingest stream stays at
// the new limit and the dead-letter one at the previous, and the error says
// so. On any error MaxBytes keeps reporting the previous budget, so a later
// call with the new budget reapplies both.
//
// The JetStream calls are bounded by resizeTimeout, plus rollbackTimeout for
// the undo — or another resizeTimeout for the consumers joining a queue just
// opened — all rooted in ctx. That is deliberate: ctx is the process's stop
// context, so a reload caught mid-hook by a stop gives up — undo included —
// rather than holding the drain past server.shutdown_timeout. A cancellation
// between the two updates is therefore the one way to leave the pair split,
// and only for the rest of a process that is exiting: the next boot applies
// the adopted settings to it again.
func (e *EmbeddedNATS) SetMaxBytes(ctx context.Context, id tenant.ID, maxBytes int64) error {
	if _, err := tenant.Parse(string(id)); err != nil {
		return fmt.Errorf("tenant: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queue(id)
	q.asked = maxBytes
	if q.ingest && q.dlq && maxBytes == q.maxBytes {
		return nil
	}
	return e.apply(ctx, id, q, maxBytes)
}

// apply brings tenant id's queue to maxBytes: opening it when its ingest
// stream is missing, resizing it otherwise (see SetMaxBytes). Under e.mu.
func (e *EmbeddedNATS) apply(ctx context.Context, id tenant.ID, q *tenantQueue, maxBytes int64) error {
	defer e.record(id, q)
	resizeCtx, cancel := context.WithTimeout(ctx, resizeTimeout)
	defer cancel()
	if !q.ingest {
		if err := e.applyDLQ(resizeCtx, id, q, maxBytes); err != nil {
			return err
		}
		if _, err := e.js.CreateOrUpdateStream(resizeCtx, ingestStreamConfig(id, maxBytes)); err != nil {
			return fmt.Errorf("open ingest stream: %w", err)
		}
		q.ingest, q.maxBytes, q.ingestCap = true, maxBytes, maxBytes
		// The joins run on a budget of their own: a queue that opened but no
		// consumer holds fails every consumer (fail), so a slow open must not
		// leave them no time.
		joinCtx, cancelJoin := context.WithTimeout(ctx, resizeTimeout)
		defer cancelJoin()
		for _, f := range e.consumers {
			if err := f.join(joinCtx, id); err != nil {
				f.fail(fmt.Errorf("tenant %s: %w: join its queue: %w", id, ErrDeliveryEnded, err))
			}
		}
		return nil
	}
	prevCap := q.ingestCap
	if _, err := e.js.UpdateStream(resizeCtx, ingestStreamConfig(id, maxBytes)); err != nil {
		return fmt.Errorf("resize ingest stream: %w", err)
	}
	q.ingestCap = maxBytes
	if err := e.applyDLQ(resizeCtx, id, q, maxBytes); err != nil {
		// The undo runs on its own budget, not the one the dead-letter call
		// has likely just exhausted.
		rollbackCtx, cancelRollback := context.WithTimeout(ctx, rollbackTimeout)
		defer cancelRollback()
		if _, rollbackErr := e.js.UpdateStream(rollbackCtx, ingestStreamConfig(id, prevCap)); rollbackErr != nil {
			return fmt.Errorf("%w (ingest stream rollback failed, so it stays at the new limit and the dlq at the previous: %w)", err, rollbackErr)
		}
		q.ingestCap = prevCap
		return fmt.Errorf("%w (ingest stream restored to the previous limit)", err)
	}
	q.maxBytes = maxBytes
	return nil
}

// applyDLQ gives tenant id's dead-letter stream a tenth of maxBytes, creating
// it when it is missing, but never caps it below the bytes it holds: those
// stay, the cap is what they take, and the stream then drops its oldest row
// to make room for each new one, as any full dead-letter stream does. Under
// e.mu.
func (e *EmbeddedNATS) applyDLQ(ctx context.Context, id tenant.ID, q *tenantQueue, maxBytes int64) error {
	limit := maxBytes / dlqShare
	verb := "resize"
	s, err := e.js.Stream(ctx, dlqStreamName(id))
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		verb = "open"
	case err != nil:
		return fmt.Errorf("dlq stream info: %w", err)
	default:
		// A stream's size fits an int64 as its cap does; the bound is
		// checked rather than assumed.
		if held := s.CachedInfo().State.Bytes; held <= math.MaxInt64 && int64(held) > limit {
			slog.Warn("mq: dead-letter queue kept at what it holds rather than shrunk to its budget, so no parked row is deleted",
				"component", "nats", "tenant", id, "held_bytes", held, "budget_bytes", limit)
			limit = int64(held)
		}
	}
	if _, err := e.js.CreateOrUpdateStream(ctx, dlqStreamConfig(id, limit)); err != nil {
		return fmt.Errorf("%s dlq stream: %w", verb, err)
	}
	q.dlq = true
	return nil
}

// reopen opens tenant id's queue at the budget last asked for it, for a
// publish that finds the queue not recorded open, or a publish or park that
// found one of its streams missing. errNoQueue when no budget has been asked
// for the tenant yet: a reload can make a tenant resolvable an instant before
// its budget arrives.
//
// It runs detached from ctx's cancellation, bounded by its own timeouts:
// ctx is one caller's — an ingest request — while the queue is every
// consumer's, and a client that goes away between the open and the joins
// would leave a queue no consumer holds, which fails the ingest worker.
func (e *EmbeddedNATS) reopen(ctx context.Context, id tenant.ID) error {
	ctx = context.WithoutCancel(ctx)
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queues[id]
	if q == nil || q.asked == 0 {
		return fmt.Errorf("tenant %s: %w", id, errNoQueue)
	}
	defer e.record(id, q)
	// What is missing is asked of JetStream rather than read off the flags,
	// which may still say the stream the publish just missed exists — or it
	// may be back already, opened by a caller that held mu first.
	for _, name := range []string{ingestStreamName(id), dlqStreamName(id)} {
		_, err := e.js.Stream(ctx, name)
		switch {
		case errors.Is(err, jetstream.ErrStreamNotFound):
			if name == ingestStreamName(id) {
				q.ingest = false
			} else {
				q.dlq = false
			}
		case err != nil:
			return fmt.Errorf("stream info: %w", err)
		}
	}
	if q.ingest && q.dlq {
		return nil
	}
	return e.apply(ctx, id, q, q.asked)
}

// Publish stores data on topic's ingest subject, in its tenant's queue. A
// topic without a valid tenant is refused before anything is sent (see
// subject). A tenant with no queue has one opened at the budget last asked
// for it (see SetMaxBytes) — and so does one whose stream exists but whose
// queue the broker has not recorded open, since no consumer may hold that
// stream. A queue that cannot be opened — none asked for yet, or JetStream
// refused it — and a queue at its byte budget (DiscardNew) are reported as
// ErrQueueFull: either way the tenant's queue takes nothing now, and a retry
// is the caller's answer.
func (e *EmbeddedNATS) Publish(ctx context.Context, topic Topic, data []byte, opts ...PublishOpt) error {
	subj, err := subject(ingestPrefix, topic)
	if err != nil {
		return err
	}
	if _, ok := e.opened.Load(topic.Tenant); !ok {
		if openErr := e.reopen(ctx, topic.Tenant); openErr != nil {
			return fmt.Errorf("%w: %w", ErrQueueFull, openErr)
		}
	}
	err = e.publish(ctx, subj, data, opts)
	if errors.Is(err, jetstream.ErrNoStreamResponse) {
		if openErr := e.reopen(ctx, topic.Tenant); openErr != nil {
			return fmt.Errorf("%w: %w", ErrQueueFull, openErr)
		}
		err = e.publish(ctx, subj, data, opts)
	}
	if err != nil && strings.Contains(err.Error(), "maximum bytes exceeded") {
		// The server reports a full store as a generic store failure whose
		// text is the only thing that names the cause.
		return fmt.Errorf("%w: %w", ErrQueueFull, err)
	}
	return err
}

// DeadLetter stores msg's data on its topic's dead-letter subject, in its
// tenant's queue — the subject it arrived on with the ingest prefix swapped
// for the dead-letter one, nothing decoded or re-encoded. The dead-letter
// stream is DiscardOld, so a full one drops its oldest parked rows rather than
// refusing. A dead-letter stream found missing is opened again with its
// tenant's queue, as Publish does.
func (e *EmbeddedNATS) DeadLetter(ctx context.Context, msg *Message, opts ...PublishOpt) error {
	subj := dlqPrefix + msg.topicKey
	err := e.publish(ctx, subj, msg.Data, opts)
	if errors.Is(err, jetstream.ErrNoStreamResponse) {
		if id, ok := keyTenant(msg.topicKey); ok {
			if err = e.reopen(ctx, id); err == nil {
				err = e.publish(ctx, subj, msg.Data, opts)
			}
		}
	}
	return err
}

func (e *EmbeddedNATS) publish(ctx context.Context, subj string, data []byte, opts []PublishOpt) error {
	msg := nats.NewMsg(subj)
	msg.Data = data

	// Apply any optional configurations (like headers) to the message
	headers := Headers{}
	for _, opt := range opts {
		opt(headers)
	}

	observability.InjectHeaders(ctx, headers)
	msg.Header = nats.Header(headers)
	// No retry on "no responders": in-process, that only ever means no
	// stream holds the subject — a tenant with no queue, which the callers
	// open rather than wait out.
	_, err := e.js.PublishMsg(ctx, msg, jetstream.WithRetryAttempts(0))
	return err
}

// wrapMsg adapts a received JetStream message to the mq-owned Message. ctx
// becomes Message.Ctx as given; Subscribe extracts the trace context first.
func wrapMsg(ctx context.Context, m jetstream.Msg) *Message {
	return newMessage(ctx, topicKey(ingestPrefix, m.Subject()), m.Data(), time.Now(),
		func(ctx context.Context) error {
			return m.DoubleAck(ctx)
		},
		func() error {
			return m.Ack()
		},
		func() error {
			return m.Nak()
		},
	)
}

// Subscribe holds a durable explicit-ack consumer named consumerName on every
// tenant's queue, those opened later included, and delivers each message to
// handler with the trace context its headers carry, until ctx is done. It
// fetches the client's default number of messages ahead across the tenants
// together (see fanIn.share), so what sits client-side does not grow with
// the tenants. A tenant's queue that cannot be joined when it opens is
// logged: its events reach handler from the next boot.
func (e *EmbeddedNATS) Subscribe(ctx context.Context, consumerName string, handler func(msg *Message) error) error {
	f := e.newFanIn(ctx, jetstream.ConsumerConfig{Durable: consumerName, AckPolicy: jetstream.AckExplicitPolicy})
	f.fail = func(err error) {
		slog.Error("mq: a tenant's events do not reach this consumer until the next boot", "component", "nats", "consumer", consumerName, "error", err)
	}
	if err := e.register(ctx, f); err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	stop, err := f.start(func(m jetstream.Msg) {
		msg := wrapMsg(observability.ExtractHeaders(ctx, m.Headers()), m)
		if err := handler(msg); err != nil {
			_ = msg.Nak()
		}
	}, jetstream.DefaultMaxMessages, false)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	go func() {
		<-ctx.Done()
		stop()
	}()

	return nil
}

// CreateConsumer creates or updates a durable explicit-ack pull consumer on
// every tenant's queue, and joins each queue opened later. ctx becomes every
// delivered Message.Ctx (see ConsumerManager); it does not stop delivery —
// Consumer.Consume's stop does.
func (e *EmbeddedNATS) CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	c := &workerConsumer{
		fanIn: e.newFanIn(ctx, jetstream.ConsumerConfig{
			Durable:       cfg.Durable,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       cfg.AckWait,
			MaxAckPending: cfg.MaxAckPending,
		}),
		failed: make(chan error, 1),
	}
	c.fail = func(err error) {
		// Exactly one error, and nothing once stop has been called.
		if c.stopped.Load() {
			return
		}
		select {
		case c.failed <- err:
		default:
		}
	}
	if err := e.register(ctx, c.fanIn); err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	return c, nil
}

// newFanIn is a fanIn over cfg, not yet holding any durable; the caller sets
// its fail and registers it.
func (e *EmbeddedNATS) newFanIn(ctx context.Context, cfg jetstream.ConsumerConfig) *fanIn {
	return &fanIn{
		e:       e,
		ctx:     ctx,
		cfg:     cfg,
		handles: map[tenant.ID]jetstream.Consumer{},
		running: map[tenant.ID]jetstream.ConsumeContext{},
	}
}

// register holds f's durable on every tenant's queue there is and registers
// f, so every queue opened from here on is joined too.
func (e *EmbeddedNATS) register(ctx context.Context, f *fanIn) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, id := range e.ingestTenants() {
		if err := f.join(ctx, id); err != nil {
			return fmt.Errorf("tenant %s: %w", id, err)
		}
	}
	e.consumers = append(e.consumers, f)
	return nil
}

// unregister stops joining f to the queues that open from here on. Under
// e.mu.
func (e *EmbeddedNATS) unregister(f *fanIn) {
	e.consumers = slices.DeleteFunc(e.consumers, func(c *fanIn) bool { return c == f })
}

// fanIn is one durable consumer held on every tenant's ingest stream — the
// ingest worker's (CreateConsumer) or the hub bridge's (Subscribe) —
// delivering them all into one handler: each tenant's messages on a
// goroutine of their own, so a tenant's arrive in order and different
// tenants' concurrently, and a handler blocked on one tenant holds back that
// tenant alone. Its fields are guarded by e.mu, bar stopped.
type fanIn struct {
	e   *EmbeddedNATS
	ctx context.Context // each delivered Message.Ctx (CreateConsumer), or where Subscribe extracts trace context into
	cfg jetstream.ConsumerConfig

	// fail reports a tenant's delivery that ended on its own, or a queue that
	// could not be joined when it opened.
	fail func(error)

	// handles is the durable on each tenant's ingest stream; running, the
	// delivery started on each once deliver is set.
	handles map[tenant.ID]jetstream.Consumer
	running map[tenant.ID]jetstream.ConsumeContext
	deliver func(jetstream.Msg)
	// prefetch is the fetch-ahead asked for across the tenants together; 0
	// leaves each tenant the client default.
	prefetch int
	// watch reports a delivery that ends on its own through fail.
	watch   bool
	stopped atomic.Bool
}

// join holds f's durable on tenant id's ingest stream — looked up first, and
// created or updated only when missing or configured otherwise, so a boot
// over thousands of queues writes nothing it need not — and starts delivery
// on it when f is delivering. Under e.mu.
func (f *fanIn) join(ctx context.Context, id tenant.ID) error {
	stream := ingestStreamName(id)
	c, err := f.e.js.Consumer(ctx, stream, f.cfg.Durable)
	if err != nil || !sameConsumer(c.CachedInfo().Config, f.cfg) {
		if c, err = f.e.js.CreateOrUpdateConsumer(ctx, stream, f.cfg); err != nil {
			return err
		}
	}
	f.handles[id] = c
	if f.deliver == nil || f.stopped.Load() {
		return nil
	}
	return f.run(id)
}

// sameConsumer reports whether a durable holds the fields this package sets;
// a zero field in want is the server's default, whatever that resolved to.
func sameConsumer(have, want jetstream.ConsumerConfig) bool {
	return have.AckPolicy == want.AckPolicy &&
		have.FilterSubject == want.FilterSubject &&
		(want.AckWait == 0 || have.AckWait == want.AckWait) &&
		(want.MaxAckPending == 0 || have.MaxAckPending == want.MaxAckPending)
}

// share is one tenant's part of the fetch-ahead: the total spread over the
// tenants' queues joined so far, at least one each, fixed when that queue's
// delivery starts. 0 leaves the client default. Under e.mu.
func (f *fanIn) share() int {
	if f.prefetch <= 0 {
		return 0
	}
	return max(1, f.prefetch/max(1, len(f.handles)))
}

// run starts delivery from tenant id's durable, once. Under e.mu.
func (f *fanIn) run(id tenant.ID) error {
	if _, ok := f.running[id]; ok {
		return nil
	}
	// The client reports what goes wrong after Consume returns only through
	// this handler, never through Consume's own error. It calls it for
	// passing conditions too (a missed heartbeat, a leadership change) and
	// keeps delivering; on a terminal one (the consumer was deleted, a bad
	// request, a closed connection) it calls it and then stops the
	// subscription itself. So the handler only records and logs, and "the
	// subscription closed without our stop" below is what terminal means —
	// the client's own verdict, not a list of error names kept in step here.
	var lastErr atomic.Pointer[error]
	opts := []jetstream.PullConsumeOpt{
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			lastErr.Store(&err)
			slog.Warn("mq: consumer reported an error", "component", "nats", "tenant", id, "error", err)
		}),
	}
	if n := f.share(); n > 0 {
		opts = append(opts, jetstream.PullMaxMessages(n))
	}
	cctx, err := f.handles[id].Consume(f.deliver, opts...)
	if err != nil {
		return err
	}
	f.running[id] = cctx
	if !f.watch {
		return nil
	}
	go func() {
		<-cctx.Closed()
		if f.stopped.Load() {
			return
		}
		reason := ErrDeliveryEnded
		if r := lastErr.Load(); r != nil {
			reason = fmt.Errorf("%w: %w", ErrDeliveryEnded, *r)
		}
		f.fail(fmt.Errorf("tenant %s: %w", id, reason))
	}()
	return nil
}

// start begins delivery to deliver from every tenant's durable, and from
// each queue joined later, fetching about prefetch messages ahead across the
// tenants together (see share); watch reports a delivery that ends on its own
// through fail. The returned stop ends every delivery and stops joining new
// queues, without waiting.
func (f *fanIn) start(deliver func(jetstream.Msg), prefetch int, watch bool) (stop func(), err error) {
	f.e.mu.Lock()
	defer f.e.mu.Unlock()
	f.deliver, f.prefetch, f.watch = deliver, prefetch, watch
	stop = func() {
		f.e.mu.Lock()
		defer f.e.mu.Unlock()
		f.stopped.Store(true)
		for _, cctx := range f.running {
			cctx.Stop()
		}
		f.e.unregister(f)
	}
	for _, id := range slices.Sorted(maps.Keys(f.handles)) {
		if err := f.run(id); err != nil {
			f.stopped.Store(true)
			for _, cctx := range f.running {
				cctx.Stop()
			}
			f.e.unregister(f)
			return nil, fmt.Errorf("tenant %s: %w", id, err)
		}
	}
	return stop, nil
}

// workerConsumer is the Consumer CreateConsumer returns: a fanIn with the
// failed channel its contract promises.
type workerConsumer struct {
	*fanIn
	failed chan error
}

func (c *workerConsumer) Consume(handler func(msg *Message), prefetch int) (func(), <-chan error, error) {
	stop, err := c.start(func(m jetstream.Msg) {
		handler(wrapMsg(c.ctx, m))
	}, prefetch, true)
	if err != nil {
		return nil, nil, fmt.Errorf("consume: %w", err)
	}
	return stop, c.failed, nil
}

// stream resolves a stream handle by name.
func (e *EmbeddedNATS) stream(ctx context.Context, name string) (*jsStream, error) {
	s, err := e.js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return &jsStream{s: s}, nil
}

// jsStream is the sequencedStream over a resolved JetStream stream.
type jsStream struct {
	s jetstream.Stream
}

func (s *jsStream) state(ctx context.Context, subjectFilter string) (streamState, error) {
	var opts []jetstream.StreamInfoOpt
	if subjectFilter != "" {
		opts = append(opts, jetstream.WithSubjectFilter(subjectFilter))
	}
	info, err := s.s.Info(ctx, opts...)
	if err != nil {
		return streamState{}, err
	}
	return streamState{
		FirstSeq: info.State.FirstSeq,
		LastSeq:  info.State.LastSeq,
		Msgs:     info.State.Msgs,
		Subjects: info.State.Subjects,
	}, nil
}

func (s *jsStream) messageTime(ctx context.Context, seq uint64) (time.Time, error) {
	msg, err := s.s.GetMsg(ctx, seq)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return time.Time{}, fmt.Errorf("sequence %d: %w", seq, errSequenceNotFound)
		}
		return time.Time{}, err
	}
	return msg.Time, nil
}

func (s *jsStream) purgeBelow(ctx context.Context, seq uint64) error {
	return s.s.Purge(ctx, jetstream.WithPurgeSequence(seq))
}

func (s *jsStream) consumerAckFloor(ctx context.Context, consumer string) (uint64, error) {
	cons, err := s.s.Consumer(ctx, consumer)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return 0, fmt.Errorf("consumer %q: %w", consumer, ErrConsumerNotFound)
		}
		return 0, err
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("consumer info: %w", err)
	}
	return info.AckFloor.Stream, nil
}

// PurgeAcked purges each tenant's ingest stream below MIN(consumer's ack
// floor + 1, first sequence stored at or after the tenant's cutoff) — see
// purgeAcked. A tenant olderThan does not name is purged up to its ack floor.
// A failure on one tenant's stream is joined into the error and the sweep
// goes on to the next; a done ctx ends it.
func (e *EmbeddedNATS) PurgeAcked(ctx context.Context, consumer string, olderThan map[tenant.ID]time.Time) (bool, error) {
	e.mu.Lock()
	ids := e.ingestTenants()
	e.mu.Unlock()

	now := time.Now()
	var (
		errs    []error
		tenants int
	)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		cutoff, ok := olderThan[id]
		if !ok {
			cutoff = now
		}
		s, err := e.stream(ctx, ingestStreamName(id))
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %s: get stream: %w", id, err))
			continue
		}
		report, err := purgeAcked(ctx, s, consumer, cutoff)
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %s: %w", id, err))
			continue
		}
		// The sweep's own log lines: their detail is in sequences, which only
		// this package speaks. Per tenant at Debug, since a sweep reaches
		// every tenant each minute; the summary below is the Info line.
		switch {
		case report.purged:
			tenants++
			slog.DebugContext(ctx, "sweeper: purged",
				"tenant", id,
				"purged_below_seq", report.target,
				"ack_floor", report.ackFloor,
				"gap_seq", report.gapSeq,
			)
		case report.gapSeq == 0:
			slog.DebugContext(ctx, "sweeper: all messages within gap window, skipping purge", "tenant", id)
		}
	}
	if tenants > 0 {
		slog.InfoContext(ctx, "sweeper: purged", "tenants", tenants)
	}
	return tenants > 0, errors.Join(errs...)
}

// DeadLetterCounts reads tenant id's dead-letter stream's per-subject counts
// and keys them by table. The table filter matches that table's unscoped
// subject, so it is applied to the parsed topic rather than as a subject
// filter; a scoped topic counts under "table.scope".
func (e *EmbeddedNATS) DeadLetterCounts(ctx context.Context, id tenant.ID, table string) (DeadLetterCounts, error) {
	if _, err := tenant.Parse(string(id)); err != nil {
		return DeadLetterCounts{}, fmt.Errorf("tenant: %w", err)
	}
	s, err := e.stream(ctx, dlqStreamName(id))
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return DeadLetterCounts{}, fmt.Errorf("%w: %w", ErrNoDeadLetterQueue, err)
		}
		return DeadLetterCounts{}, fmt.Errorf("get dlq stream: %w", err)
	}

	state, err := s.state(ctx, tenantSubjects(dlqPrefix, id))
	if err != nil {
		return DeadLetterCounts{}, fmt.Errorf("dlq stream info: %w", err)
	}

	counts := DeadLetterCounts{Tables: make(map[string]uint64, len(state.Subjects)), Total: state.Msgs}
	for subj, n := range state.Subjects {
		t := parseTopicKey(topicKey(dlqPrefix, subj))
		if table != "" && (t.Table != table || t.Scope != "") {
			continue
		}
		name := t.Table
		if t.Scope != "" {
			// TODO(#235): break scopes out rather than fold them into the name.
			name += "." + t.Scope
		}
		counts.Tables[name] += n
	}
	return counts, nil
}

// ReplaySince creates an ephemeral consumer on topic's ingest subject, in its
// tenant's queue, starting at since (DeliverByStartTime) and drains it to send
// until caught up. The consumer is ack-less and expires on its own once idle.
// Caught up is the client's no-messages or request-timeout answer to a pull;
// any other pull failure (a closed connection, a deleted consumer) is returned
// so the caller knows the replay ended short rather than empty. A done ctx
// ends the drain between pulls and returns ctx's error. A topic without a
// valid tenant is refused like a publish (see subject).
func (e *EmbeddedNATS) ReplaySince(ctx context.Context, topic Topic, since time.Time, send func(data []byte) bool) error {
	subj, err := subject(ingestPrefix, topic)
	if err != nil {
		return err
	}
	cons, err := e.js.CreateOrUpdateConsumer(ctx, ingestStreamName(topic.Tenant), jetstream.ConsumerConfig{
		FilterSubject:     subj,
		DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
		OptStartTime:      &since,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("replay consumer: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			if errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, nats.ErrTimeout) {
				return nil // caught up
			}
			return fmt.Errorf("replay next: %w", err)
		}
		if !send(msg.Data()) {
			return nil
		}
	}
}

// Stats reports the embedded server's connection and inbound-message counters
// for observability.RegisterSystemMetrics.
func (e *EmbeddedNATS) Stats() (observability.MQStats, error) {
	varz, err := e.server.Varz(nil)
	if err != nil {
		return observability.MQStats{}, err
	}
	return observability.MQStats{Connections: int64(varz.Connections), InMsgs: varz.InMsgs}, nil
}

func (e *EmbeddedNATS) Close() error {
	e.conn.Close()
	e.server.Shutdown()
	// Owning the lifecycle (NoSigs, #287) means waiting it out: without this,
	// run()'s remaining defers unwind while JetStream is still tearing down
	// and the process can exit mid-shutdown (as-if-crashed stream state).
	// Milliseconds for an in-process server.
	e.server.WaitForShutdown()
	return nil
}
