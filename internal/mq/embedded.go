package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// slogNATSLogger adapts slog to the natsserver.Logger interface.
type slogNATSLogger struct{ l *slog.Logger }

func (s *slogNATSLogger) Noticef(format string, v ...any) {
	s.l.Info(fmt.Sprintf(format, v...), "component", "nats")
}

func (s *slogNATSLogger) Warnf(format string, v ...any) {
	s.l.Warn(fmt.Sprintf(format, v...), "component", "nats")
}

func (s *slogNATSLogger) Fatalf(format string, v ...any) {
	s.l.Error(fmt.Sprintf(format, v...), "component", "nats")
}

func (s *slogNATSLogger) Errorf(format string, v ...any) {
	s.l.Error(fmt.Sprintf(format, v...), "component", "nats")
}

func (s *slogNATSLogger) Debugf(format string, v ...any) {
	s.l.Debug(fmt.Sprintf(format, v...), "component", "nats")
}

func (s *slogNATSLogger) Tracef(format string, v ...any) {
	s.l.Debug(fmt.Sprintf(format, v...), "component", "nats")
}

// EmbeddedNATS runs an in-process NATS server with JetStream.
type EmbeddedNATS struct {
	server *natsserver.Server
	conn   *nats.Conn
	js     jetstream.JetStream
}

// EmbeddedNATS is the one implementation of every mq interface.
var (
	_ Publisher       = (*EmbeddedNATS)(nil)
	_ Subscriber      = (*EmbeddedNATS)(nil)
	_ ConsumerManager = (*EmbeddedNATS)(nil)
	_ StreamManager   = (*EmbeddedNATS)(nil)
	_ Replayer        = (*EmbeddedNATS)(nil)
)

// NewEmbedded starts an embedded NATS server with JetStream enabled.
// An optional *slog.Logger can be passed to control server log output;
// if omitted, slog.Default() is used. The stream name is fixed (see
// StreamName / DLQStreamName) — the embedded server is private to this
// process, so there's no namespacing to do.
func NewEmbedded(storeDir string, maxBytes int64, logger ...*slog.Logger) (*EmbeddedNATS, error) {
	l := slog.Default()
	if len(logger) > 0 && logger[0] != nil {
		l = logger[0]
	}

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
	ns.SetLogger(&slogNATSLogger{l: l}, false, false)
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

	return &EmbeddedNATS{server: ns, conn: nc, js: js}, nil
}

// ingestStreamConfig is the WAVEHOUSE stream. LimitsPolicy: standard
// append-only log; the Active Sweeper handles message purging. MaxBytes caps
// disk usage to protect the shared ClickHouse/NATS disk. DiscardNew rejects
// new messages when full, propagating backpressure to the upstream API.
func ingestStreamConfig(maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      StreamName(),
		Subjects:  []string{"ingest.>"},
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
		Name:      DLQStreamName(),
		Subjects:  []string{"dlq.>"},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardOld,
	}
}

// Resize updates the ingest stream's MaxBytes in place (the hot-reloadable
// mq.max_bytes_gb). JetStream applies a limit change to a live stream
// without touching its messages: growing takes effect immediately; shrinking
// below the current size makes DiscardNew refuse new publishes until the
// worker drains it — nothing buffered is dropped.
func (e *EmbeddedNATS) Resize(ctx context.Context, maxBytes int64) error {
	if _, err := e.js.UpdateStream(ctx, ingestStreamConfig(maxBytes)); err != nil {
		return fmt.Errorf("resize stream: %w", err)
	}
	return nil
}

// EnsureDLQStream creates the DLQ stream if it doesn't exist, or updates its
// MaxBytes in place if it does (the same reload path as Resize).
func (e *EmbeddedNATS) EnsureDLQStream(ctx context.Context, maxBytes int64) error {
	_, err := e.js.CreateOrUpdateStream(ctx, dlqStreamConfig(maxBytes))
	return err
}

func (e *EmbeddedNATS) Publish(ctx context.Context, subject string, data []byte, opts ...PublishOpt) error {
	msg := nats.NewMsg(subject)
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
	return NewMessage(ctx, m.Subject(), m.Data(), time.Now(),
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

func (e *EmbeddedNATS) Subscribe(ctx context.Context, subject, consumerName string, handler func(msg *Message) error) error {
	cons, err := e.js.CreateOrUpdateConsumer(ctx, StreamName(), jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subject,
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
	cons, err := e.js.CreateOrUpdateConsumer(ctx, StreamName(), jetstream.ConsumerConfig{
		Durable:       cfg.Durable,
		FilterSubject: cfg.FilterSubject,
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

func (c *jsConsumer) Consume(handler func(msg *Message), prefetch int) (func(), error) {
	var opts []jetstream.PullConsumeOpt
	if prefetch > 0 {
		opts = append(opts, jetstream.PullMaxMessages(prefetch))
	}
	cctx, err := c.cons.Consume(func(m jetstream.Msg) {
		handler(wrapMsg(c.ctx, m))
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}
	return cctx.Stop, nil
}

// Stream resolves a stream handle by name.
func (e *EmbeddedNATS) Stream(ctx context.Context, name string) (Stream, error) {
	s, err := e.js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return &jsStream{s: s}, nil
}

// jsStream is the Stream handle over a resolved JetStream stream.
type jsStream struct {
	s jetstream.Stream
}

func (s *jsStream) State(ctx context.Context, subjectFilter string) (StreamState, error) {
	var opts []jetstream.StreamInfoOpt
	if subjectFilter != "" {
		opts = append(opts, jetstream.WithSubjectFilter(subjectFilter))
	}
	info, err := s.s.Info(ctx, opts...)
	if err != nil {
		return StreamState{}, err
	}
	return StreamState{
		FirstSeq: info.State.FirstSeq,
		LastSeq:  info.State.LastSeq,
		Msgs:     info.State.Msgs,
		Subjects: info.State.Subjects,
		MaxBytes: info.Config.MaxBytes,
	}, nil
}

func (s *jsStream) MessageTime(ctx context.Context, seq uint64) (time.Time, error) {
	msg, err := s.s.GetMsg(ctx, seq)
	if err != nil {
		return time.Time{}, err
	}
	return msg.Time, nil
}

func (s *jsStream) PurgeBelow(ctx context.Context, seq uint64) error {
	return s.s.Purge(ctx, jetstream.WithPurgeSequence(seq))
}

func (s *jsStream) ConsumerAckFloor(ctx context.Context, consumer string) (uint64, error) {
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

// ReplaySince creates an ephemeral consumer on the ingest stream starting at
// since (DeliverByStartTime) and drains it to send until caught up. The
// consumer is ack-less and expires on its own once idle. Caught up is the
// client's no-messages or request-timeout answer to a pull; any other pull
// failure (a closed connection, a deleted consumer) is returned so the caller
// knows the replay ended short rather than empty. A done ctx ends the drain
// between pulls and returns ctx's error.
func (e *EmbeddedNATS) ReplaySince(ctx context.Context, subject string, since time.Time, send func(data []byte) bool) error {
	cons, err := e.js.CreateOrUpdateConsumer(ctx, StreamName(), jetstream.ConsumerConfig{
		FilterSubject:     subject,
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
