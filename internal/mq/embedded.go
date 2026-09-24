package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
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

// EmbeddedNATS runs an in-process NATS server with JetStream.
type EmbeddedNATS struct {
	server *natsserver.Server
	conn   *nats.Conn
	js     jetstream.JetStream

	limitMu  sync.Mutex
	maxBytes int64 // the ingest stream cap both streams were last reconciled to
}

// EmbeddedNATS is the one implementation of every mq interface.
var _ Broker = (*EmbeddedNATS)(nil)

const (
	// dlqShare is the DLQ stream's slice of the byte budget: a tenth of the
	// ingest stream's cap.
	dlqShare = 10
	// resizeTimeout bounds the JetStream calls SetMaxBytes makes to apply a
	// new cap — both streams share it. A settings reload holds the store's
	// lock while its hooks run, so an in-process JetStream call that never
	// returns would otherwise block every later reload.
	resizeTimeout = 10 * time.Second
	// rollbackTimeout is the undo's own budget when the DLQ resize fails:
	// in-process JetStream fails by stalling rather than erroring, so the
	// likely cause is that resizeTimeout has just run out, and an undo on
	// that context would fail without touching the stream. SetMaxBytes runs
	// for at most the sum of the two.
	rollbackTimeout = 5 * time.Second
)

// NewEmbedded starts an embedded NATS server with JetStream enabled and
// both streams in place: the ingest stream capped at maxBytes and the DLQ
// stream at a tenth of it. The DLQ stream is always present — an empty
// limits-policy stream costs nothing, and whether a poison row lands on it is
// the ingest worker's decision at the moment of the failure.
// The server logs through slog's default logger. The stream names are fixed
// (see subject.go) — the embedded server is private to this process, so
// there's no namespacing to do.
func NewEmbedded(storeDir string, maxBytes int64) (*EmbeddedNATS, error) {
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

	if _, err := js.CreateOrUpdateStream(context.Background(), ingestStreamConfig(maxBytes)); err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("create stream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(context.Background(), dlqStreamConfig(maxBytes/dlqShare)); err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("create dlq stream: %w", err)
	}

	return &EmbeddedNATS{server: ns, conn: nc, js: js, maxBytes: maxBytes}, nil
}

// ingestStreamConfig is the WAVEHOUSE stream. LimitsPolicy: standard
// append-only log; the Active Sweeper handles message purging. MaxBytes caps
// disk usage to protect the shared ClickHouse/NATS disk. DiscardNew rejects
// new messages when full, propagating backpressure to the upstream API.
func ingestStreamConfig(maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      ingestStream,
		Subjects:  []string{ingestAll},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardNew,
	}
}

// dlqStreamConfig is the WAVEHOUSE_DLQ stream. DiscardOld: a full DLQ drops
// its oldest parked rows rather than refusing new ones — backpressure belongs
// to the ingest stream, not the dead-letter one.
func dlqStreamConfig(maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      dlqStream,
		Subjects:  []string{dlqAll},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardOld,
	}
}

// MaxBytes reports the ingest stream cap both streams were last reconciled to
// (by NewEmbedded, then by each successful SetMaxBytes).
func (e *EmbeddedNATS) MaxBytes() int64 {
	e.limitMu.Lock()
	defer e.limitMu.Unlock()
	return e.maxBytes
}

// SetMaxBytes applies a new byte budget to both streams in place (the
// hot-reloadable mq.max_bytes_gb): the ingest stream takes maxBytes and the
// DLQ stream a tenth of it. JetStream applies a limit change to a live stream
// without touching its messages: growing takes effect immediately; shrinking
// the ingest stream below its current size makes DiscardNew refuse new
// publishes until the worker drains it — nothing buffered is dropped.
//
// The pair moves together where it can. If the DLQ update fails after the
// ingest one succeeded, the ingest resize is undone so the 10:1 pair stays at
// the previous budget, and the next call retries both. Safe in that direction
// — the ingest stream is DiscardNew, so shrinking it back drops nothing
// stored. The undo is best effort: if it fails too, the ingest stream stays at
// the new limit and the DLQ at the previous, and the error says so. On any
// error MaxBytes keeps reporting the previous budget, so a later call with the
// new budget reapplies both.
//
// The JetStream calls are bounded by resizeTimeout, plus rollbackTimeout for
// the undo, both rooted in ctx. That is deliberate: ctx is the process's stop
// context, so a reload caught mid-hook by a stop gives up — undo included —
// rather than holding the drain past server.shutdown_timeout. A cancellation
// between the two updates is therefore the one way to leave the pair split,
// and only for the rest of a process that is exiting: the next boot
// reconciles both streams from the adopted settings.
func (e *EmbeddedNATS) SetMaxBytes(ctx context.Context, maxBytes int64) error {
	e.limitMu.Lock()
	defer e.limitMu.Unlock()
	if maxBytes == e.maxBytes {
		return nil
	}

	resizeCtx, cancel := context.WithTimeout(ctx, resizeTimeout)
	defer cancel()
	if _, err := e.js.UpdateStream(resizeCtx, ingestStreamConfig(maxBytes)); err != nil {
		return fmt.Errorf("resize ingest stream: %w", err)
	}
	if _, err := e.js.CreateOrUpdateStream(resizeCtx, dlqStreamConfig(maxBytes/dlqShare)); err != nil {
		// The undo runs on its own budget, not the one the DLQ call has
		// likely just exhausted.
		rollbackCtx, cancelRollback := context.WithTimeout(ctx, rollbackTimeout)
		defer cancelRollback()
		if _, rollbackErr := e.js.UpdateStream(rollbackCtx, ingestStreamConfig(e.maxBytes)); rollbackErr != nil {
			return fmt.Errorf("resize dlq stream: %w (ingest stream rollback failed, so it stays at the new limit and the dlq at the previous: %w)", err, rollbackErr)
		}
		return fmt.Errorf("resize dlq stream: %w (ingest stream restored to the previous limit)", err)
	}
	e.maxBytes = maxBytes
	return nil
}

// Publish stores data on topic's ingest subject. A topic without a valid
// tenant is refused before anything is sent (see subject). A stream at its
// byte budget (DiscardNew) refuses the publish; that is reported as
// ErrQueueFull.
func (e *EmbeddedNATS) Publish(ctx context.Context, topic Topic, data []byte, opts ...PublishOpt) error {
	subj, err := subject(ingestPrefix, topic)
	if err != nil {
		return err
	}
	err = e.publish(ctx, subj, data, opts)
	if err != nil && strings.Contains(err.Error(), "maximum bytes exceeded") {
		// The server reports a full store as a generic store failure whose
		// text is the only thing that names the cause.
		return fmt.Errorf("%w: %w", ErrQueueFull, err)
	}
	return err
}

// DeadLetter stores msg's data on its topic's DLQ subject — the subject it
// arrived on with the ingest prefix swapped for the DLQ one, nothing decoded
// or re-encoded. The DLQ stream is DiscardOld, so a full DLQ drops its oldest
// parked rows rather than refusing.
func (e *EmbeddedNATS) DeadLetter(ctx context.Context, msg *Message, opts ...PublishOpt) error {
	return e.publish(ctx, dlqPrefix+msg.topicKey, msg.Data, opts)
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
	_, err := e.js.PublishMsg(ctx, msg)
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

func (e *EmbeddedNATS) Subscribe(ctx context.Context, consumerName string, handler func(msg *Message) error) error {
	cons, err := e.js.CreateOrUpdateConsumer(ctx, ingestStream, jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: ingestAll,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	cctx, err := cons.Consume(func(m jetstream.Msg) {
		msg := wrapMsg(observability.ExtractHeaders(ctx, m.Headers()), m)
		if err := handler(msg); err != nil {
			_ = msg.Nak()
		}
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	go func() {
		<-ctx.Done()
		cctx.Stop()
	}()

	return nil
}

// CreateConsumer creates or updates a durable explicit-ack pull consumer on
// the ingest stream. ctx becomes every delivered Message.Ctx (see
// ConsumerManager); it does not stop delivery — Consumer.Consume's stop does.
func (e *EmbeddedNATS) CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	cons, err := e.js.CreateOrUpdateConsumer(ctx, ingestStream, jetstream.ConsumerConfig{
		Durable:       cfg.Durable,
		FilterSubject: ingestAll,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       cfg.AckWait,
		MaxAckPending: cfg.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	return &jsConsumer{cons: cons, ctx: ctx}, nil
}

// jsConsumer is the Consumer over a JetStream pull consumer.
type jsConsumer struct {
	cons jetstream.Consumer
	ctx  context.Context // each delivered Message.Ctx (see ConsumerManager)
}

func (c *jsConsumer) Consume(handler func(msg *Message), prefetch int) (func(), <-chan error, error) {
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
			slog.Warn("mq: consumer reported an error", "component", "nats", "error", err)
		}),
	}
	if prefetch > 0 {
		opts = append(opts, jetstream.PullMaxMessages(prefetch))
	}
	cctx, err := c.cons.Consume(func(m jetstream.Msg) {
		handler(wrapMsg(c.ctx, m))
	}, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("consume: %w", err)
	}

	var stopped atomic.Bool
	failed := make(chan error, 1)
	go func() {
		<-cctx.Closed()
		if stopped.Load() {
			return
		}
		if reason := lastErr.Load(); reason != nil {
			failed <- fmt.Errorf("%w: %w", ErrDeliveryEnded, *reason)
			return
		}
		failed <- ErrDeliveryEnded
	}()
	stop := func() {
		stopped.Store(true)
		cctx.Stop()
	}
	return stop, failed, nil
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

// PurgeAcked purges the ingest stream below MIN(consumer's ack floor + 1,
// first sequence stored at or after olderThan) — see purgeAcked.
func (e *EmbeddedNATS) PurgeAcked(ctx context.Context, consumer string, olderThan time.Time) (bool, error) {
	s, err := e.stream(ctx, ingestStream)
	if err != nil {
		return false, fmt.Errorf("get stream: %w", err)
	}
	report, err := purgeAcked(ctx, s, consumer, olderThan)
	if err != nil {
		return false, err
	}
	// The sweep's own log lines: their detail is in sequences, which only
	// this package speaks.
	switch {
	case report.purged:
		slog.InfoContext(ctx, "sweeper: purged",
			"purged_below_seq", report.target,
			"ack_floor", report.ackFloor,
			"gap_seq", report.gapSeq,
		)
	case report.gapSeq == 0:
		slog.DebugContext(ctx, "sweeper: all messages within gap window, skipping purge")
	}
	return report.purged, nil
}

// DeadLetterCounts reads the DLQ stream's per-subject counts and keys them by
// table across every tenant (see DeadLetterCounts.Tables). The table filter
// matches that table's unscoped subject under any tenant, so it is applied
// to the parsed topic rather than as a subject filter; a scoped topic counts
// under "table.scope". A subject written before the tenant led it counts
// under its table like any other (parseTopicKey).
func (e *EmbeddedNATS) DeadLetterCounts(ctx context.Context, table string) (DeadLetterCounts, error) {
	s, err := e.stream(ctx, dlqStream)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return DeadLetterCounts{}, fmt.Errorf("%w: %w", ErrNoDeadLetterQueue, err)
		}
		return DeadLetterCounts{}, fmt.Errorf("get dlq stream: %w", err)
	}

	state, err := s.state(ctx, dlqAll)
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

// ReplaySince creates an ephemeral consumer on topic's ingest subject starting at
// since (DeliverByStartTime) and drains it to send until caught up. The
// consumer is ack-less and expires on its own once idle. Caught up is the
// client's no-messages or request-timeout answer to a pull; any other pull
// failure (a closed connection, a deleted consumer) is returned so the caller
// knows the replay ended short rather than empty. A done ctx ends the drain
// between pulls and returns ctx's error. A topic without a valid tenant is
// refused like a publish (see subject): the subject it names is exact, so
// events published before the tenant led the subject are not replayed.
func (e *EmbeddedNATS) ReplaySince(ctx context.Context, topic Topic, since time.Time, send func(data []byte) bool) error {
	subj, err := subject(ingestPrefix, topic)
	if err != nil {
		return err
	}
	cons, err := e.js.CreateOrUpdateConsumer(ctx, ingestStream, jetstream.ConsumerConfig{
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
