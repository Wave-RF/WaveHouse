package mq

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestEmbedded spins up an EmbeddedNATS with a silent logger and a
// temporary store directory that is cleaned up by the test framework.
func newTestEmbedded(t *testing.T) *EmbeddedNATS {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e, err := NewEmbedded(t.TempDir(), 64<<20, logger)
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
		_ = msg.Ack()
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

func TestEmbeddedNATS_Stats(t *testing.T) {
	e := newTestEmbedded(t)

	stats, err := e.Stats()
	require.NoError(t, err)
	// The process's own in-process client is connected.
	assert.GreaterOrEqual(t, stats.Connections, int64(1))
	assert.GreaterOrEqual(t, stats.InMsgs, int64(0))
}

func TestEmbeddedNATS_PublishHeaders(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, "ingest.hdr", []byte("x"),
		WithHeader("X-One", "1"), WithHeader("X-One", "2"), WithHeader("X-Two", "b")))

	// Read the stored message back raw: the option headers are on the wire
	// exactly as set, exact-key, with Add appending rather than replacing.
	s, err := e.js.Stream(ctx, StreamName())
	require.NoError(t, err)
	raw, err := s.GetLastMsgForSubject(ctx, "ingest.hdr")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, raw.Header.Values("X-One"))
	assert.Equal(t, "b", raw.Header.Get("X-Two"))
	assert.Equal(t, []byte("x"), raw.Data)
}

func TestEmbeddedNATS_EnsureDLQStream_Idempotent(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Create, then re-run with a new cap: the second call is an update in
	// place, not an error.
	require.NoError(t, e.EnsureDLQStream(ctx, 1<<20))
	require.NoError(t, e.EnsureDLQStream(ctx, 2<<20))

	info, err := e.js.Stream(ctx, DLQStreamName())
	require.NoError(t, err)
	cfg := info.CachedInfo().Config
	assert.Equal(t, DLQStreamName(), cfg.Name)
	assert.Equal(t, []string{"dlq.>"}, cfg.Subjects)
	assert.Equal(t, int64(2<<20), cfg.MaxBytes)
	assert.Equal(t, jetstream.DiscardOld, cfg.Discard)
}

func TestEmbeddedNATS_StreamHandle(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := e.Stream(ctx, "NO_SUCH_STREAM")
	require.Error(t, err, "an unknown stream is an error, not a nil handle")

	s, err := e.Stream(ctx, StreamName())
	require.NoError(t, err)

	empty, err := s.State(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, StreamState{}, empty, "a fresh stream has no sequences, no messages, no subject breakdown")

	before := time.Now()
	for i := range 3 {
		require.NoError(t, e.Publish(ctx, "ingest.a", []byte{byte(i)}))
	}
	require.NoError(t, e.Publish(ctx, "ingest.b", []byte("b")))

	st, err := s.State(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), st.FirstSeq)
	assert.Equal(t, uint64(4), st.LastSeq)
	assert.Equal(t, uint64(4), st.Msgs)
	assert.Nil(t, st.Subjects, "no filter → no per-subject counts")

	filtered, err := s.State(ctx, "ingest.a")
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"ingest.a": 3}, filtered.Subjects)
	assert.Equal(t, uint64(4), filtered.Msgs, "Msgs is the whole stream, filter or not")

	all, err := s.State(ctx, ">")
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"ingest.a": 3, "ingest.b": 1}, all.Subjects)

	ts, err := s.MessageTime(ctx, 1)
	require.NoError(t, err)
	assert.False(t, ts.Before(before.Add(-time.Second)), "stored time is the publish time")
	_, err = s.MessageTime(ctx, 99)
	require.Error(t, err, "a sequence that was never stored is an error")

	// No consumer yet: the sentinel the sweeper keys its "not yet" warning on.
	_, err = s.ConsumerAckFloor(ctx, "nobody")
	require.ErrorIs(t, err, ErrConsumerNotFound)

	// Consume and ack the first two messages; the ack floor follows.
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "floor", FilterSubject: "ingest.>", MaxAckPending: 10})
	require.NoError(t, err)
	// Acks are asserted on the test goroutine: the handler runs on the
	// client's delivery goroutine, where a require would Goexit the wrong one.
	acked := make(chan error, 4)
	stop, err := cons.Consume(func(msg *Message) {
		if msg.Subject == "ingest.a" && msg.Data[0] < 2 {
			acked <- msg.DoubleAck(ctx)
		}
	}, 2)
	require.NoError(t, err)
	t.Cleanup(stop)
	for range 2 {
		select {
		case ackErr := <-acked:
			require.NoError(t, ackErr)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for acks")
		}
	}
	require.Eventually(t, func() bool {
		floor, err := s.ConsumerAckFloor(ctx, "floor")
		return err == nil && floor == 2
	}, 5*time.Second, 20*time.Millisecond, "ack floor must reach the last contiguous acked sequence")

	// Purge below 3 drops sequences 1 and 2 and leaves 3 and 4.
	require.NoError(t, s.PurgeBelow(ctx, 3))
	st, err = s.State(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(3), st.FirstSeq)
	assert.Equal(t, uint64(2), st.Msgs)
	_, err = s.MessageTime(ctx, 1)
	require.Error(t, err, "a purged sequence is gone")
}

// TestEmbeddedNATS_CreateConsumer_Config pins the ConsumerConfig → broker
// mapping: AckWait (redelivery timing) and MaxAckPending (ingest backpressure)
// are checkable nowhere else, and a dropped field would compile and pass
// every delivery test.
func TestEmbeddedNATS_CreateConsumer_Config(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := e.CreateConsumer(ctx, ConsumerConfig{
		Durable:       "cfg",
		FilterSubject: "ingest.cfg.>",
		AckWait:       42 * time.Second,
		MaxAckPending: 123,
	})
	require.NoError(t, err)

	s, err := e.js.Stream(ctx, StreamName())
	require.NoError(t, err)
	cons, err := s.Consumer(ctx, "cfg")
	require.NoError(t, err)
	info, err := cons.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cfg", info.Config.Durable)
	assert.Equal(t, "ingest.cfg.>", info.Config.FilterSubject)
	assert.Equal(t, jetstream.AckExplicitPolicy, info.Config.AckPolicy)
	assert.Equal(t, 42*time.Second, info.Config.AckWait)
	assert.Equal(t, 123, info.Config.MaxAckPending)
}

func TestEmbeddedNATS_ReplaySince(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, "ingest.r", []byte("old")))
	require.NoError(t, e.Publish(ctx, "ingest.other", []byte("other")))
	time.Sleep(20 * time.Millisecond)
	since := time.Now()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, e.Publish(ctx, "ingest.r", []byte("new1")))
	require.NoError(t, e.Publish(ctx, "ingest.r", []byte("new2")))

	var got []string
	require.NoError(t, e.ReplaySince(ctx, "ingest.r", since, func(data []byte) bool {
		got = append(got, string(data))
		return true
	}))
	assert.Equal(t, []string{"new1", "new2"}, got, "only the subject's messages stored at or after since, in order")

	// send returning false stops the replay early.
	got = nil
	require.NoError(t, e.ReplaySince(ctx, "ingest.r", time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		return false
	}))
	assert.Equal(t, []string{"old"}, got)

	// Nothing stored at or after since: the replay ends cleanly with no sends.
	require.NoError(t, e.ReplaySince(ctx, "ingest.r", time.Now().Add(time.Hour), func([]byte) bool {
		t.Fatal("nothing should be replayed")
		return false
	}))
}

func TestEmbeddedNATS_DefaultLogger(t *testing.T) {
	// NewEmbedded without a logger should not panic — it falls back to the
	// default slog logger.
	e, err := NewEmbedded(t.TempDir(), 64<<20)
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

func TestEmbeddedNATS_Resize(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Resize(ctx, 128<<20))
	info, err := e.js.Stream(ctx, StreamName())
	require.NoError(t, err)
	assert.Equal(t, int64(128<<20), info.CachedInfo().Config.MaxBytes)
	// Everything but the limit is preserved.
	assert.Equal(t, []string{"ingest.>"}, info.CachedInfo().Config.Subjects)
	assert.Equal(t, jetstream.DiscardNew, info.CachedInfo().Config.Discard)
}

func TestEmbeddedNATS_ReplaySince_PullFailureIsAnError(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, "ingest.r", []byte("one")))
	require.NoError(t, e.Publish(ctx, "ingest.r", []byte("two")))

	// Closing the client connection under a running replay makes the next pull
	// fail outright — that is not "caught up" and must reach the caller.
	var got []string
	err := e.ReplaySince(ctx, "ingest.r", time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		e.conn.Close()
		return true
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, nats.ErrConnectionClosed)
	assert.Equal(t, []string{"one"}, got, "the message delivered before the failure was still sent")
}
