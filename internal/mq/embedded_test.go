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
	received := map[Topic][]byte{}
	done := make(chan struct{}, 1)

	// A name that needs encoding on the wire comes back as it went in.
	topic := Topic{Table: "default.events", Scope: "t1"}
	err := e.Subscribe(ctx, "test-consumer", func(msg *Message) error {
		mu.Lock()
		received[msg.Topic()] = msg.Data
		mu.Unlock()
		_ = msg.Ack()
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, e.Publish(ctx, topic, []byte("hello")))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subscriber callback")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []byte("hello"), received[topic])
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

	require.NoError(t, e.Publish(ctx, Topic{Table: "hdr"}, []byte("x"),
		WithHeader("X-One", "1"), WithHeader("X-One", "2"), WithHeader("X-Two", "b")))

	// Read the stored message back raw: the option headers are on the wire
	// exactly as set, exact-key, with Add appending rather than replacing.
	s, err := e.js.Stream(ctx, ingestStream)
	require.NoError(t, err)
	raw, err := s.GetLastMsgForSubject(ctx, "ingest.hdr")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, raw.Header.Values("X-One"))
	assert.Equal(t, "b", raw.Header.Get("X-Two"))
	assert.Equal(t, []byte("x"), raw.Data)
}

func TestNewEmbedded_CreatesBothStreams(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	assert.Equal(t, int64(64<<20), e.MaxBytes())

	ingest, err := e.js.Stream(ctx, ingestStream)
	require.NoError(t, err)
	assert.Equal(t, int64(64<<20), ingest.CachedInfo().Config.MaxBytes)

	// The DLQ stream is always present, at a tenth of the budget.
	dlq, err := e.js.Stream(ctx, dlqStream)
	require.NoError(t, err)
	cfg := dlq.CachedInfo().Config
	assert.Equal(t, []string{"dlq.>"}, cfg.Subjects)
	assert.Equal(t, int64(64<<20)/10, cfg.MaxBytes)
	assert.Equal(t, jetstream.DiscardOld, cfg.Discard)
}

func TestEmbeddedNATS_StreamHandle(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := e.stream(ctx, "NO_SUCH_STREAM")
	require.Error(t, err, "an unknown stream is an error, not a nil handle")

	s, err := e.stream(ctx, ingestStream)
	require.NoError(t, err)

	empty, err := s.state(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, streamState{}, empty, "a fresh stream has no sequences, no messages, no subject breakdown")

	before := time.Now()
	for i := range 3 {
		require.NoError(t, e.Publish(ctx, Topic{Table: "a"}, []byte{byte(i)}))
	}
	require.NoError(t, e.Publish(ctx, Topic{Table: "b"}, []byte("b")))

	st, err := s.state(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), st.FirstSeq)
	assert.Equal(t, uint64(4), st.LastSeq)
	assert.Equal(t, uint64(4), st.Msgs)
	assert.Nil(t, st.Subjects, "no filter → no per-subject counts")

	filtered, err := s.state(ctx, "ingest.a")
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"ingest.a": 3}, filtered.Subjects)
	assert.Equal(t, uint64(4), filtered.Msgs, "Msgs is the whole stream, filter or not")

	all, err := s.state(ctx, ">")
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"ingest.a": 3, "ingest.b": 1}, all.Subjects)

	ts, err := s.messageTime(ctx, 1)
	require.NoError(t, err)
	assert.False(t, ts.Before(before.Add(-time.Second)), "stored time is the publish time")
	_, err = s.messageTime(ctx, 99)
	require.Error(t, err, "a sequence that was never stored is an error")

	// No consumer yet: the sentinel the sweeper keys its "not yet" warning on.
	_, err = s.consumerAckFloor(ctx, "nobody")
	require.ErrorIs(t, err, ErrConsumerNotFound)

	// Consume and ack the first two messages; the ack floor follows.
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "floor", MaxAckPending: 10})
	require.NoError(t, err)
	// Acks are asserted on the test goroutine: the handler runs on the
	// client's delivery goroutine, where a require would Goexit the wrong one.
	acked := make(chan error, 4)
	stop, err := cons.Consume(func(msg *Message) {
		if msg.TopicKey() == "a" && msg.Data[0] < 2 {
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
		floor, err := s.consumerAckFloor(ctx, "floor")
		return err == nil && floor == 2
	}, 5*time.Second, 20*time.Millisecond, "ack floor must reach the last contiguous acked sequence")

	// Purge below 3 drops sequences 1 and 2 and leaves 3 and 4.
	require.NoError(t, s.purgeBelow(ctx, 3))
	st, err = s.state(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(3), st.FirstSeq)
	assert.Equal(t, uint64(2), st.Msgs)
	_, err = s.messageTime(ctx, 1)
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
		AckWait:       42 * time.Second,
		MaxAckPending: 123,
	})
	require.NoError(t, err)

	s, err := e.js.Stream(ctx, ingestStream)
	require.NoError(t, err)
	cons, err := s.Consumer(ctx, "cfg")
	require.NoError(t, err)
	info, err := cons.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cfg", info.Config.Durable)
	assert.Equal(t, "ingest.>", info.Config.FilterSubject, "the consumer sees every topic")
	assert.Equal(t, jetstream.AckExplicitPolicy, info.Config.AckPolicy)
	assert.Equal(t, 42*time.Second, info.Config.AckWait)
	assert.Equal(t, 123, info.Config.MaxAckPending)
}

func TestEmbeddedNATS_ReplaySince(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, Topic{Table: "r"}, []byte("old")))
	require.NoError(t, e.Publish(ctx, Topic{Table: "other"}, []byte("other")))
	time.Sleep(20 * time.Millisecond)
	since := time.Now()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, e.Publish(ctx, Topic{Table: "r"}, []byte("new1")))
	require.NoError(t, e.Publish(ctx, Topic{Table: "r"}, []byte("new2")))

	var got []string
	require.NoError(t, e.ReplaySince(ctx, Topic{Table: "r"}, since, func(data []byte) bool {
		got = append(got, string(data))
		return true
	}))
	assert.Equal(t, []string{"new1", "new2"}, got, "only the subject's messages stored at or after since, in order")

	// send returning false stops the replay early.
	got = nil
	require.NoError(t, e.ReplaySince(ctx, Topic{Table: "r"}, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		return false
	}))
	assert.Equal(t, []string{"old"}, got)

	// Nothing stored at or after since: the replay ends cleanly with no sends.
	require.NoError(t, e.ReplaySince(ctx, Topic{Table: "r"}, time.Now().Add(time.Hour), func([]byte) bool {
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
	err := e.Subscribe(ctx, "cancel-consumer", func(msg *Message) error {
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

func TestEmbeddedNATS_SetMaxBytes(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.SetMaxBytes(ctx, 128<<20))
	assert.Equal(t, int64(128<<20), e.MaxBytes())

	ingest, err := e.js.Stream(ctx, ingestStream)
	require.NoError(t, err)
	assert.Equal(t, int64(128<<20), ingest.CachedInfo().Config.MaxBytes)
	// Everything but the limit is preserved.
	assert.Equal(t, []string{"ingest.>"}, ingest.CachedInfo().Config.Subjects)
	assert.Equal(t, jetstream.DiscardNew, ingest.CachedInfo().Config.Discard)

	// The DLQ stream follows at a tenth of the budget.
	dlq, err := e.js.Stream(ctx, dlqStream)
	require.NoError(t, err)
	assert.Equal(t, int64(128<<20)/10, dlq.CachedInfo().Config.MaxBytes)
	assert.Equal(t, jetstream.DiscardOld, dlq.CachedInfo().Config.Discard)

	// The budget already in effect is a no-op, not an error.
	require.NoError(t, e.SetMaxBytes(ctx, 128<<20))
	assert.Equal(t, int64(128<<20), e.MaxBytes())
}

func TestEmbeddedNATS_SetMaxBytes_DLQFailureRollsBackIngest(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Put the DLQ stream where the update can't follow: JetStream refuses to
	// change a live stream's retention policy, so recreating it as a work
	// queue makes the DLQ resize fail after the ingest resize has already
	// succeeded.
	require.NoError(t, e.js.DeleteStream(ctx, dlqStream))
	_, err := e.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: dlqStream, Subjects: []string{dlqAll}, Retention: jetstream.WorkQueuePolicy, MaxBytes: (64 << 20) / 10,
	})
	require.NoError(t, err)

	err = e.SetMaxBytes(ctx, 128<<20)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ingest stream restored to the previous limit")
	assert.Equal(t, int64(64<<20), e.MaxBytes(), "the budget in effect is unchanged, so the next call retries both")

	ingest, err := e.js.Stream(ctx, ingestStream)
	require.NoError(t, err)
	assert.Equal(t, int64(64<<20), ingest.CachedInfo().Config.MaxBytes, "the ingest resize is undone so the pair stays at the previous limit")
	dlq, err := e.js.Stream(ctx, dlqStream)
	require.NoError(t, err)
	assert.Equal(t, int64(64<<20)/10, dlq.CachedInfo().Config.MaxBytes)
}

func TestEmbeddedNATS_SetMaxBytes_IngestFailureChangesNothing(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // a stop caught mid-reload: the first JetStream call gives up

	err := e.SetMaxBytes(ctx, 128<<20)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int64(64<<20), e.MaxBytes())

	dlq, err := e.js.Stream(t.Context(), dlqStream)
	require.NoError(t, err)
	assert.Equal(t, int64(64<<20)/10, dlq.CachedInfo().Config.MaxBytes, "the dlq is not touched when the ingest resize fails")
}

func TestEmbeddedNATS_ReplaySince_PullFailureIsAnError(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, Topic{Table: "r"}, []byte("one")))
	require.NoError(t, e.Publish(ctx, Topic{Table: "r"}, []byte("two")))

	// Closing the client connection under a running replay makes the next pull
	// fail outright — that is not "caught up" and must reach the caller.
	var got []string
	err := e.ReplaySince(ctx, Topic{Table: "r"}, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		e.conn.Close()
		return true
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, nats.ErrConnectionClosed)
	assert.Equal(t, []string{"one"}, got, "the message delivered before the failure was still sent")
}

func TestEmbeddedNATS_ReplaySince_StopsWhenContextIsDone(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, e.Publish(ctx, Topic{Table: "r"}, []byte("one")))
	require.NoError(t, e.Publish(ctx, Topic{Table: "r"}, []byte("two")))

	// Cancelling mid-replay (the client went away, or the server is shutting
	// down) ends the drain before the next pull rather than running to caught up.
	var got []string
	err := e.ReplaySince(ctx, Topic{Table: "r"}, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		cancel()
		return true
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"one"}, got)
}

func TestEmbeddedNATS_DeadLetter(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	empty, err := e.DeadLetterCounts(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, DeadLetterCounts{Tables: map[string]uint64{}}, empty)

	park := func(topic Topic, data string) {
		t.Helper()
		msg := NewMessage(ctx, topic, []byte(data), time.Now(), nil, nil, nil)
		require.NoError(t, e.DeadLetter(ctx, msg, WithHeader("X-DLQ-Error", "boom")))
	}
	park(Topic{Table: "default.orders"}, "o1")
	park(Topic{Table: "default.orders"}, "o2")
	park(Topic{Table: "users"}, "u1")

	// Parked under the same topic on the DLQ stream, headers intact, and
	// nothing lands on the ingest stream.
	dlq, err := e.js.Stream(ctx, dlqStream)
	require.NoError(t, err)
	raw, err := dlq.GetLastMsgForSubject(ctx, "dlq.default%2Eorders")
	require.NoError(t, err)
	assert.Equal(t, []byte("o2"), raw.Data)
	assert.Equal(t, "boom", raw.Header.Get("X-DLQ-Error"))
	ingest, err := e.stream(ctx, ingestStream)
	require.NoError(t, err)
	st, err := ingest.state(ctx, "")
	require.NoError(t, err)
	assert.Zero(t, st.Msgs)

	all, err := e.DeadLetterCounts(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, DeadLetterCounts{Tables: map[string]uint64{"default.orders": 2, "users": 1}, Total: 3}, all, "table names come back decoded")

	one, err := e.DeadLetterCounts(ctx, "default.orders")
	require.NoError(t, err)
	assert.Equal(t, DeadLetterCounts{Tables: map[string]uint64{"default.orders": 2}, Total: 3}, one, "Total is every parked message, filter or not")

	none, err := e.DeadLetterCounts(ctx, "never_failed")
	require.NoError(t, err)
	assert.Empty(t, none.Tables)
}

func TestEmbeddedNATS_DeadLetterCounts_NoQueue(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.js.DeleteStream(ctx, dlqStream))
	_, err := e.DeadLetterCounts(ctx, "")
	require.ErrorIs(t, err, ErrNoDeadLetterQueue)
}

func TestEmbeddedNATS_DeadLetter_IsAPrefixSwap(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// A subject this package would never write still parks under the very
	// same tail: nothing on the dead-letter path decodes or re-encodes it.
	_, err := e.js.Publish(ctx, "ingest.a.b.c", []byte("foreign"))
	require.NoError(t, err)

	got := make(chan *Message, 1)
	require.NoError(t, e.Subscribe(ctx, "foreign", func(msg *Message) error {
		got <- msg
		return nil
	}))
	var msg *Message
	select {
	case msg = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
	assert.Equal(t, "a.b.c", msg.TopicKey())
	require.NoError(t, e.DeadLetter(ctx, msg))

	dlq, err := e.js.Stream(ctx, dlqStream)
	require.NoError(t, err)
	raw, err := dlq.GetLastMsgForSubject(ctx, "dlq.a.b.c")
	require.NoError(t, err)
	assert.Equal(t, []byte("foreign"), raw.Data)
}

func TestEmbeddedNATS_Publish_QueueFull(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e, err := NewEmbedded(t.TempDir(), 4<<10, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// DiscardNew refuses the publish that would pass the byte budget; that is
	// the backpressure signal, named so callers need not read broker errors.
	payload := make([]byte, 1<<10)
	for range 8 {
		if err = e.Publish(ctx, Topic{Table: "full"}, payload); err != nil {
			break
		}
	}
	require.ErrorIs(t, err, ErrQueueFull)
}

func TestEmbeddedNATS_PurgeAcked(t *testing.T) {
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// No consumer yet: the sentinel the sweeper keys its "not yet" warning on.
	_, err := e.PurgeAcked(ctx, "buffer", time.Now())
	require.ErrorIs(t, err, ErrConsumerNotFound)

	for i := range 4 {
		require.NoError(t, e.Publish(ctx, Topic{Table: "p"}, []byte{byte(i)}))
	}

	// Ack the first two; the last two stay unwritten.
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 10})
	require.NoError(t, err)
	acked := make(chan error, 4)
	stop, err := cons.Consume(func(msg *Message) {
		if msg.Data[0] < 2 {
			acked <- msg.DoubleAck(ctx)
		}
	}, 4)
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
	s, err := e.stream(ctx, ingestStream)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		floor, err := s.consumerAckFloor(ctx, "buffer")
		return err == nil && floor == 2
	}, 5*time.Second, 20*time.Millisecond)

	// Everything is acked-or-not but nothing is old enough: keep it all.
	purged, err := e.PurgeAcked(ctx, "buffer", time.Now().Add(-time.Hour))
	require.NoError(t, err)
	assert.False(t, purged)

	// Everything is old enough: only the acked two go.
	purged, err = e.PurgeAcked(ctx, "buffer", time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, purged)
	st, err := s.state(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(3), st.FirstSeq, "unacked events survive however old they are")
	assert.Equal(t, uint64(2), st.Msgs)
}
