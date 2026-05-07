package mq

import (
	"context"
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
	server     *natsserver.Server
	conn       *nats.Conn
	js         jetstream.JetStream
	streamName string
}

// NewEmbedded starts an embedded NATS server with JetStream enabled.
// An optional *slog.Logger can be passed to control server log output;
// if omitted, slog.Default() is used.
func NewEmbedded(storeDir, streamName string, maxBytes int64, logger ...*slog.Logger) (*EmbeddedNATS, error) {
	l := slog.Default()
	if len(logger) > 0 && logger[0] != nil {
		l = logger[0]
	}

	opts := &natsserver.Options{
		DontListen: true,
		JetStream:  true,
		StoreDir:   storeDir,
		SyncAlways: true, // fsync every JetStream write — publish ACKs only after data is on disk
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

	// LimitsPolicy: standard append-only log. The Active Sweeper handles
	// message purging. MaxBytes caps disk usage to protect the shared
	// ClickHouse/NATS disk. DiscardNew rejects new messages when full,
	// propagating backpressure to the upstream API.
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"ingest.>"},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardNew,
	})
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("create stream: %w", err)
	}

	return &EmbeddedNATS{server: ns, conn: nc, js: js, streamName: streamName}, nil
}

func (e *EmbeddedNATS) Publish(ctx context.Context, subject string, data []byte) error {
	msg := nats.NewMsg(subject)
	msg.Data = data

	observability.InjectNATS(ctx, msg)
	_, err := e.js.PublishMsg(ctx, msg)
	return err
}

func (e *EmbeddedNATS) Subscribe(ctx context.Context, subject, consumerName string, handler func(msg *Message) error) error {
	cons, err := e.js.CreateOrUpdateConsumer(ctx, e.streamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	cctx, err := cons.Consume(func(m jetstream.Msg) {
		msgCtx := observability.ExtractNATS(ctx, m)

		msg := NewMessage(msgCtx, m.Subject(), m.Data(), time.Now(),
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

// JetStream returns the underlying JetStream handle for direct access (e.g. gap-fill).
func (e *EmbeddedNATS) JetStream() jetstream.JetStream {
	return e.js
}

func (e *EmbeddedNATS) NatsConn() *nats.Conn {
	return e.conn
}

func (e *EmbeddedNATS) Close() error {
	e.conn.Close()
	e.server.Shutdown()
	return nil
}

func (e *EmbeddedNATS) GetServer() *natsserver.Server {
	return e.server
}
