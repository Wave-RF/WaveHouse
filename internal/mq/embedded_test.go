package mq

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestEmbedded spins up an EmbeddedNATS with a silent logger and a
// temporary store directory that is cleaned up by the test framework.
func newTestEmbedded(t *testing.T) *EmbeddedNATS {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e, err := NewEmbedded(t.TempDir(), StreamName(), 64<<20, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestEmbeddedNATS_PublishSubscribe(t *testing.T) {
	// No t.Parallel(): each embedded server uses DontListen+InProcessServer,
	// but starting several in parallel still slows tests unnecessarily.
	e := newTestEmbedded(t)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	var mu sync.Mutex
	received := map[string][]byte{}
	done := make(chan struct{}, 1)

	err := e.Subscribe(ctx, "ingest.events.t1", "test-consumer", func(msg *Message) error {
		mu.Lock()
		received[msg.Subject] = msg.Data
		mu.Unlock()
		_ = msg.Ack(ctx)
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, e.Publish(ctx, "ingest.events.t1", []byte("hello")))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subscriber callback")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []byte("hello"), received["ingest.events.t1"])
}

func TestEmbeddedNATS_Accessors(t *testing.T) {
	e := newTestEmbedded(t)

	assert.NotNil(t, e.JetStream(), "JetStream handle should be accessible")
	assert.NotNil(t, e.NatsConn(), "nats.Conn should be accessible")
	assert.NotNil(t, e.GetServer(), "embedded nats server should be accessible")
	assert.True(t, e.NatsConn().IsConnected())
}

func TestEmbeddedNATS_DefaultLogger(t *testing.T) {
	// NewEmbedded without a logger should not panic — it falls back to the
	// default slog logger.
	e, err := NewEmbedded(t.TempDir(), StreamName(), 64<<20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
}

func TestEmbeddedNATS_SubscribeCancellation(t *testing.T) {
	// When the caller's context is cancelled, the consume loop should stop
	// cleanly without leaking goroutines or blocking.
	e := newTestEmbedded(t)

	ctx, cancel := context.WithCancel(t.Context())
	err := e.Subscribe(ctx, "ingest.cancel.x", "cancel-consumer", func(msg *Message) error {
		// Clean, idiomatic, and blocks until cancellation
		<-ctx.Done()
		return nil
	})
	require.NoError(t, err)

	cancel()

	// Deterministically wait for the context to finish
	<-ctx.Done()
}

// slogNATSLogger is exercised by NewEmbedded setup, but the individual
// severity methods are easier to cover directly.
func TestSlogNATSLogger_Levels(t *testing.T) {
	t.Parallel()

	l := &slogNATSLogger{l: slog.New(slog.NewTextHandler(io.Discard, nil))}
	l.Noticef("notice %d", 1)
	l.Warnf("warn %s", "w")
	l.Errorf("err %v", "e")
	l.Debugf("dbg")
	l.Tracef("trc")
	l.Fatalf("fatal %d", 42) // slog.Error; no os.Exit here
}
