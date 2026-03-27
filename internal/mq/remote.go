package mq

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// RemoteNATS connects to an external NATS cluster.
type RemoteNATS struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

// NewRemote connects to a NATS cluster at the given URL.
func NewRemote(url string, maxBytes int64) (*RemoteNATS, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("connect to nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream new: %w", err)
	}

	// LimitsPolicy: standard append-only log. Active Sweeper handles purging.
	// MaxBytes caps disk; DiscardNew propagates backpressure to the API.
	// NOTE: the remote NATS server should be configured with sync_always: true
	// in its JetStream config to guarantee fsync before publish ACKs.
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"ingest.>"},
		Retention: jetstream.LimitsPolicy,
		MaxBytes:  maxBytes,
		Discard:   jetstream.DiscardNew,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create stream: %w", err)
	}

	return &RemoteNATS{conn: nc, js: js}, nil
}

func (r *RemoteNATS) Publish(_ context.Context, subject string, data []byte) error {
	_, err := r.js.Publish(context.Background(), subject, data)
	return err
}

func (r *RemoteNATS) Subscribe(ctx context.Context, subject, consumerName string, handler func(msg *Message) error) error {
	cons, err := r.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	cctx, err := cons.Consume(func(m jetstream.Msg) {
		msg := NewMessage(m.Subject(), m.Data(), time.Now(), func() { _ = m.Ack() }, func() { _ = m.Nak() })
		if err := handler(msg); err != nil {
			_ = m.Nak()
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
func (r *RemoteNATS) JetStream() jetstream.JetStream {
	return r.js
}

func (r *RemoteNATS) NatsConn() *nats.Conn {
	return r.conn
}

func (r *RemoteNATS) Close() error {
	r.conn.Close()
	return nil
}
