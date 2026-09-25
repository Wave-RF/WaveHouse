package mq

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testBudget is the byte budget newTestEmbedded opens each queue at.
const testBudget = 64 << 20

// storeDir is a temporary directory for a broker's store whose removal
// retries briefly: a consumer's state file can land after Close has returned,
// which fails t.TempDir's one-shot RemoveAll (#442). The retrying cleanup runs
// first (cleanups are LIFO), leaving t.TempDir an empty directory to remove.
func storeDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	t.Cleanup(func() {
		var err error
		for range 50 {
			if err = os.RemoveAll(dir); err == nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Errorf("remove %s: %v", dir, err)
	})
	return dir
}

// openEmbedded starts an EmbeddedNATS over dir, closed by the test framework.
func openEmbedded(t *testing.T, dir string) *EmbeddedNATS {
	t.Helper()
	e, err := NewEmbedded(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// newTestEmbedded spins up an EmbeddedNATS over a temporary store directory
// with a queue open for each of tenants — tenant.Default when none is named —
// at testBudget.
func newTestEmbedded(t *testing.T, tenants ...tenant.ID) *EmbeddedNATS {
	t.Helper()
	e := openEmbedded(t, storeDir(t))
	if len(tenants) == 0 {
		tenants = []tenant.ID{tenant.Default}
	}
	for _, id := range tenants {
		require.NoError(t, e.SetMaxBytes(t.Context(), id, testBudget))
	}
	return e
}

// streamConfig is the stored config of the named stream.
func streamConfig(t *testing.T, e *EmbeddedNATS, name string) jetstream.StreamConfig {
	t.Helper()
	s, err := e.js.Stream(t.Context(), name)
	require.NoError(t, err)
	return s.CachedInfo().Config
}

// ackAll consumes every message delivered to consumer on the ingest queue,
// acknowledging each, until n have been acked.
func ackAll(t *testing.T, e *EmbeddedNATS, consumer string, n int) {
	t.Helper()
	ctx := t.Context()
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: consumer, MaxAckPending: 100})
	require.NoError(t, err)
	acked := make(chan error, n)
	stop, _, err := cons.Consume(func(msg *Message) { acked <- msg.DoubleAck(ctx) }, 10)
	require.NoError(t, err)
	t.Cleanup(stop)
	for range n {
		select {
		case err := <-acked:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for acks")
		}
	}
}

func TestEmbeddedNATS_PublishSubscribe(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	var mu sync.Mutex
	received := map[Topic][]byte{}
	done := make(chan struct{}, 1)

	// A name that needs encoding on the wire comes back as it went in.
	topic := Topic{Tenant: tenant.Default, Table: "default.events", Scope: "t1"}
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
	t.Parallel()
	e := newTestEmbedded(t)

	stats, err := e.Stats()
	require.NoError(t, err)
	// The process's own in-process client is connected.
	assert.GreaterOrEqual(t, stats.Connections, int64(1))
	assert.GreaterOrEqual(t, stats.InMsgs, int64(0))
}

func TestEmbeddedNATS_PublishHeaders(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "hdr"}, []byte("x"),
		WithHeader("X-One", "1"), WithHeader("X-One", "2"), WithHeader("X-Two", "b")))

	// Read the stored message back raw: the option headers are on the wire
	// exactly as set, exact-key, with Add appending rather than replacing.
	s, err := e.js.Stream(ctx, "INGEST_0")
	require.NoError(t, err)
	raw, err := s.GetLastMsgForSubject(ctx, "ingest.0.hdr")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, raw.Header.Values("X-One"))
	assert.Equal(t, "b", raw.Header.Get("X-Two"))
	assert.Equal(t, []byte("x"), raw.Data)
}

// A repeated idempotency key inside the duplicate window is dropped as a
// success, so an uncertain publish can be republished safely; a queue made
// with another window gets this one on its next budget apply.
func TestEmbeddedNATS_Publish_IdempotencyKeyDropsARepeat(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storeDir(t))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	// Explicit rather than the server's default, which happens to match today.
	require.Equal(t, EmbeddedDuplicateWindow, ingestStreamConfig(tenant.Default, testBudget).Duplicates)
	old := ingestStreamConfig(tenant.Default, testBudget)
	old.Duplicates = 10 * time.Second
	_, err := e.js.CreateStream(ctx, old)
	require.NoError(t, err)
	require.NoError(t, e.SetMaxBytes(ctx, tenant.Default, testBudget))
	require.Equal(t, EmbeddedDuplicateWindow, streamConfig(t, e, "INGEST_0").Duplicates)

	topic := Topic{Tenant: tenant.Default, Table: "t"}
	require.NoError(t, e.Publish(ctx, topic, []byte("a"), WithIdempotencyKey("k1")))
	require.NoError(t, e.Publish(ctx, topic, []byte("a again"), WithIdempotencyKey("k1")), "a repeat is a success")
	require.NoError(t, e.Publish(ctx, topic, []byte("b"), WithIdempotencyKey("k2")))
	require.NoError(t, e.Publish(ctx, topic, []byte("c")))

	var got []string
	require.NoError(t, e.ReplaySince(ctx, topic, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		return true
	}))
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

// A tenant's first budget opens its queue: an ingest stream holding its
// subjects alone at the budget, refusing when full, and a dead-letter stream
// at a tenth of it, dropping its oldest when full. No other tenant gets one.
func TestEmbeddedNATS_SetMaxBytes_OpensTheTenantsQueue(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storeDir(t))
	assert.Zero(t, e.MaxBytes("acme"), "no budget applied yet")

	require.NoError(t, e.SetMaxBytes(t.Context(), "acme", testBudget))
	assert.Equal(t, int64(testBudget), e.MaxBytes("acme"))

	ingest := streamConfig(t, e, "INGEST_acme")
	assert.Equal(t, []string{"ingest.acme.>"}, ingest.Subjects)
	assert.Equal(t, int64(testBudget), ingest.MaxBytes)
	assert.Equal(t, jetstream.DiscardNew, ingest.Discard)
	assert.Equal(t, EmbeddedDuplicateWindow, ingest.Duplicates)
	dlq := streamConfig(t, e, "DLQ_acme")
	assert.Equal(t, []string{"dlq.acme.>"}, dlq.Subjects)
	assert.Equal(t, int64(testBudget)/10, dlq.MaxBytes)
	assert.Equal(t, jetstream.DiscardOld, dlq.Discard)

	assert.Zero(t, e.MaxBytes("globex"))
	_, err := e.js.Stream(t.Context(), "INGEST_globex")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound, "another tenant's queue opens with its own budget")

	require.Error(t, e.SetMaxBytes(t.Context(), "a.b", testBudget), "a tenant outside the grammar has no queue")
}

func TestEmbeddedNATS_StreamHandle(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := e.stream(ctx, "NO_SUCH_STREAM")
	require.Error(t, err, "an unknown stream is an error, not a nil handle")

	s, err := e.stream(ctx, "INGEST_0")
	require.NoError(t, err)

	empty, err := s.state(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, streamState{}, empty, "a fresh stream has no sequences, no messages, no subject breakdown")

	before := time.Now()
	for i := range 3 {
		require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "a"}, []byte{byte(i)}))
	}
	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "b"}, []byte("b")))

	st, err := s.state(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), st.FirstSeq)
	assert.Equal(t, uint64(4), st.LastSeq)
	assert.Equal(t, uint64(4), st.Msgs)
	assert.Nil(t, st.Subjects, "no filter → no per-subject counts")

	filtered, err := s.state(ctx, "ingest.0.a")
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"ingest.0.a": 3}, filtered.Subjects)
	assert.Equal(t, uint64(4), filtered.Msgs, "Msgs is the whole stream, filter or not")

	all, err := s.state(ctx, ">")
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"ingest.0.a": 3, "ingest.0.b": 1}, all.Subjects)

	ts, err := s.messageTime(ctx, 1)
	require.NoError(t, err)
	assert.False(t, ts.Before(before.Add(-time.Second)), "stored time is the publish time")
	_, err = s.messageTime(ctx, 99)
	require.ErrorIs(t, err, errSequenceNotFound, "a sequence that was never stored holds no message")

	// No consumer yet: the sentinel the sweeper keys its "not yet" warning on.
	_, err = s.consumerAckFloor(ctx, "nobody")
	require.ErrorIs(t, err, ErrConsumerNotFound)

	// Consume and ack the first two messages; the ack floor follows.
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "floor", MaxAckPending: 10})
	require.NoError(t, err)
	// Acks are asserted on the test goroutine: the handler runs on the
	// client's delivery goroutine, where a require would Goexit the wrong one.
	acked := make(chan error, 4)
	stop, _, err := cons.Consume(func(msg *Message) {
		if msg.TopicKey() == "0.a" && msg.Data[0] < 2 {
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
	require.ErrorIs(t, err, errSequenceNotFound, "a purged sequence is gone")
}

// TestEmbeddedNATS_CreateConsumer_Config pins the ConsumerConfig → broker
// mapping on every tenant's queue: AckWait (redelivery timing) and
// MaxAckPending (ingest backpressure, per tenant) are checkable nowhere else,
// and a dropped field would compile and pass every delivery test.
func TestEmbeddedNATS_CreateConsumer_Config(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := e.CreateConsumer(ctx, ConsumerConfig{
		Durable:       "cfg",
		AckWait:       42 * time.Second,
		MaxAckPending: 123,
	})
	require.NoError(t, err)

	for _, stream := range []string{"INGEST_acme", "INGEST_globex"} {
		cons, err := e.js.Consumer(ctx, stream, "cfg")
		require.NoError(t, err, stream)
		cfg := cons.CachedInfo().Config
		assert.Equal(t, "cfg", cfg.Durable)
		assert.Empty(t, cfg.FilterSubject, "%s: the durable sees the whole of its tenant's stream", stream)
		assert.Equal(t, jetstream.AckExplicitPolicy, cfg.AckPolicy)
		assert.Equal(t, 42*time.Second, cfg.AckWait)
		assert.Equal(t, 123, cfg.MaxAckPending)
	}
}

func TestEmbeddedNATS_ReplaySince(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "r"}, []byte("old")))
	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "other"}, []byte("other")))
	time.Sleep(20 * time.Millisecond)
	since := time.Now()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "r"}, []byte("new1")))
	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "r"}, []byte("new2")))

	var got []string
	require.NoError(t, e.ReplaySince(ctx, Topic{Tenant: tenant.Default, Table: "r"}, since, func(data []byte) bool {
		got = append(got, string(data))
		return true
	}))
	assert.Equal(t, []string{"new1", "new2"}, got, "only the subject's messages stored at or after since, in order")

	// send returning false stops the replay early.
	got = nil
	require.NoError(t, e.ReplaySince(ctx, Topic{Tenant: tenant.Default, Table: "r"}, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		return false
	}))
	assert.Equal(t, []string{"old"}, got)

	// Nothing stored at or after since: the replay ends cleanly with no sends.
	require.NoError(t, e.ReplaySince(ctx, Topic{Tenant: tenant.Default, Table: "r"}, time.Now().Add(time.Hour), func([]byte) bool {
		t.Fatal("nothing should be replayed")
		return false
	}))
}

func TestEmbeddedNATS_DefaultLogger(t *testing.T) {
	t.Parallel()
	// NewEmbedded without a logger should not panic — it falls back to the
	// default slog logger.
	e, err := NewEmbedded(storeDir(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
}

func TestEmbeddedNATS_SubscribeCancellation(t *testing.T) {
	t.Parallel()
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

	l := slogNATSLogger{}
	l.Noticef("notice %d", 1)
	l.Warnf("warn %s", "w")
	l.Errorf("err %v", "e")
	l.Debugf("dbg")
	l.Tracef("trc")
	l.Fatalf("fatal %d", 42) // slog.Error; no os.Exit here
}

func TestEmbeddedNATS_SetMaxBytes(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.SetMaxBytes(ctx, "acme", 128<<20))
	assert.Equal(t, int64(128<<20), e.MaxBytes("acme"))

	ingest := streamConfig(t, e, "INGEST_acme")
	assert.Equal(t, int64(128<<20), ingest.MaxBytes)
	// Everything but the limit is preserved.
	assert.Equal(t, []string{"ingest.acme.>"}, ingest.Subjects)
	assert.Equal(t, jetstream.DiscardNew, ingest.Discard)

	// The dead-letter stream follows at a tenth of the budget.
	dlq := streamConfig(t, e, "DLQ_acme")
	assert.Equal(t, int64(128<<20)/10, dlq.MaxBytes)
	assert.Equal(t, jetstream.DiscardOld, dlq.Discard)

	// No other tenant's queue moves.
	assert.Equal(t, int64(testBudget), e.MaxBytes("globex"))
	assert.Equal(t, int64(testBudget), streamConfig(t, e, "INGEST_globex").MaxBytes)
	assert.Equal(t, int64(testBudget)/10, streamConfig(t, e, "DLQ_globex").MaxBytes)

	// The budget already in effect is a no-op, not an error.
	require.NoError(t, e.SetMaxBytes(ctx, "acme", 128<<20))
	assert.Equal(t, int64(128<<20), e.MaxBytes("acme"))
}

func TestEmbeddedNATS_SetMaxBytes_DLQFailureRollsBackIngest(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Put the dead-letter stream where the update can't follow: JetStream
	// refuses to change a live stream's retention policy, so recreating it as
	// a work queue makes the dead-letter resize fail after the ingest resize
	// has already succeeded.
	require.NoError(t, e.js.DeleteStream(ctx, "DLQ_0"))
	_, err := e.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "DLQ_0", Subjects: []string{"dlq.0.>"}, Retention: jetstream.WorkQueuePolicy, MaxBytes: testBudget / 10,
	})
	require.NoError(t, err)

	err = e.SetMaxBytes(ctx, tenant.Default, 128<<20)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ingest stream restored to the previous limit")
	assert.Equal(t, int64(testBudget), e.MaxBytes(tenant.Default), "the budget in effect is unchanged, so the next call retries both")

	assert.Equal(t, int64(testBudget), streamConfig(t, e, "INGEST_0").MaxBytes, "the ingest resize is undone so the pair stays at the previous limit")
	assert.Equal(t, int64(testBudget)/10, streamConfig(t, e, "DLQ_0").MaxBytes)
}

func TestEmbeddedNATS_SetMaxBytes_IngestFailureChangesNothing(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // a stop caught mid-reload: the first JetStream call gives up

	err := e.SetMaxBytes(ctx, tenant.Default, 128<<20)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int64(testBudget), e.MaxBytes(tenant.Default))

	assert.Equal(t, int64(testBudget)/10, streamConfig(t, e, "DLQ_0").MaxBytes, "the dead-letter stream is not touched when the ingest resize fails")
}

// A tenant whose queue JetStream will not open — here, a file where its
// dead-letter stream's store would go — is refused on its own: SetMaxBytes
// errors and applies no budget, and a publish is refused as a full queue,
// while every other tenant's queue opens after it (which a store limit at
// the very top of the int64 range would refuse: see NewEmbedded). Once the
// cause is gone, a reload opens the queue at the budget last asked for it,
// however recently a publish tried.
func TestEmbeddedNATS_SetMaxBytes_AQueueThatCannotOpen(t *testing.T) {
	t.Parallel()
	dir := storeDir(t)
	// The dead-letter stream is the first of the pair to open. A failed open
	// removes what was in the way, so the obstacle is put back before each
	// attempt meant to fail.
	block := filepath.Join(dir, "jetstream", "$G", "streams", dlqStreamName("acme"))
	obstruct := func() {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(block), 0o750))
		require.NoError(t, os.WriteFile(block, nil, 0o600))
	}
	obstruct()
	e := openEmbedded(t, dir)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.Error(t, e.SetMaxBytes(ctx, "acme", testBudget))
	assert.Zero(t, e.MaxBytes("acme"), "no budget applied")
	// The server removes the emptied streams and account directories on a
	// goroutine of its own after the failed open, and globex's open must not
	// race it (see the pacing test below). No queue may be open first to keep
	// them: the reservation count has to be at zero when the failed open
	// releases one it never made.
	account := filepath.Dir(filepath.Dir(block))
	require.Eventually(t, func() bool {
		_, err := os.Stat(account)
		return os.IsNotExist(err)
	}, 5*time.Second, 5*time.Millisecond, "the failed open's cleanup never removed %s", account)

	require.NoError(t, e.SetMaxBytes(ctx, "globex", testBudget), "one tenant's failed open costs the next nothing")
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "globex", Table: "t"}, []byte("x")))

	obstruct()
	err := e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x"))
	require.ErrorIs(t, err, ErrQueueFull, "the tenant's queue takes nothing; a retry is the answer")

	if err := os.Remove(block); err != nil {
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	require.NoError(t, e.SetMaxBytes(ctx, "acme", testBudget))
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x")))
	assert.Equal(t, int64(testBudget), e.MaxBytes("acme"))
}

// After a publish fails to open its tenant's queue, the tenant's publishes —
// and its parks, which find the dead-letter stream missing — are refused at
// once, without waiting on the broker's lock, until reopenRetry has passed:
// under clients retrying, or the worker parking row after row, one tenant's
// broken queue would otherwise hold the lock that every other tenant's open,
// resize and reload takes. Once the window has passed, a publish tries again.
func TestEmbeddedNATS_PacesTheRetriesOfAQueueThatCannotOpen(t *testing.T) {
	t.Parallel()
	dir := storeDir(t)
	block := filepath.Join(dir, "jetstream", "$G", "streams", dlqStreamName("acme"))
	obstruct := func() {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(block), 0o750))
		require.NoError(t, os.WriteFile(block, nil, 0o600))
	}
	obstruct()
	e := openEmbedded(t, dir)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	acme := Topic{Tenant: "acme", Table: "t"}
	// Another tenant's streams keep the streams directory occupied: after a
	// failed open the server, on a goroutine of its own, removes that
	// directory and the account's once they are empty, and the obstacle put
	// back below would race it — a file written into a directory being
	// removed.
	require.NoError(t, e.SetMaxBytes(ctx, "globex", testBudget))

	require.Error(t, e.SetMaxBytes(ctx, "acme", testBudget))
	obstruct()
	require.ErrorIs(t, e.Publish(ctx, acme, []byte("x")), ErrQueueFull, "the publish's own attempt fails")
	if err := os.Remove(block); err != nil {
		require.ErrorIs(t, err, os.ErrNotExist)
	}

	// The queue could open now, but within the window a publish or park
	// tries nothing: each is refused while the lock is held elsewhere.
	e.mu.Lock()
	var paced, parked error
	done := make(chan struct{})
	go func() {
		defer close(done)
		paced = e.Publish(ctx, acme, []byte("x"))
		parked = e.DeadLetter(ctx, NewMessage(ctx, acme, []byte("x"), time.Now(), nil, nil, nil))
	}()
	var returned bool
	select {
	case <-done:
		returned = true
	case <-time.After(2 * time.Second):
	}
	e.mu.Unlock()
	<-done
	require.True(t, returned, "a paced publish or park waited on the broker's lock")
	require.ErrorIs(t, paced, ErrQueueFull)
	require.Error(t, parked)
	assert.Zero(t, e.MaxBytes("acme"))

	v, ok := e.failedOpen.Load(tenant.ID("acme"))
	require.True(t, ok)
	failed := v.(openFailure)
	failed.until = time.Now()
	e.failedOpen.Store(tenant.ID("acme"), failed)
	require.NoError(t, e.Publish(ctx, acme, []byte("x")), "once the window has passed")
	assert.Equal(t, int64(testBudget), e.MaxBytes("acme"))
}

// An open that gives up on the ingest stream can leave one behind that
// JetStream goes on to create — in-process, a call fails by timing out — and
// no consumer holds it. A publish goes by the broker's record of the queue,
// not by the stream answering: it opens the queue properly first, consumers
// joined, so its row reaches them rather than a stream nobody reads.
func TestEmbeddedNATS_Publish_OpensAQueueItsOpenGaveUpOn(t *testing.T) {
	t.Parallel()
	dir := storeDir(t)
	block := filepath.Join(dir, "jetstream", "$G", "streams", ingestStreamName("acme"))
	require.NoError(t, os.MkdirAll(filepath.Dir(block), 0o750))
	require.NoError(t, os.WriteFile(block, nil, 0o600))
	e := openEmbedded(t, dir)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer"})
	require.NoError(t, err)
	got := make(chan string, 1)
	stop, _, err := cons.Consume(func(msg *Message) {
		got <- string(msg.Data)
		_ = msg.Ack()
	}, 10)
	require.NoError(t, err)
	defer stop()

	require.Error(t, e.SetMaxBytes(ctx, "acme", testBudget), "the ingest stream cannot open")
	if err := os.Remove(block); err != nil {
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	// JetStream creates it after all, behind the broker's back.
	_, err = e.js.CreateStream(ctx, ingestStreamConfig("acme", testBudget))
	require.NoError(t, err)

	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x")))
	select {
	case data := <-got:
		assert.Equal(t, "x", data)
	case <-ctx.Done():
		t.Fatal("the row reached no consumer")
	}
	assert.Equal(t, int64(testBudget), e.MaxBytes("acme"))
}

// A resize whose dead-letter update fails undoes the ingest one, back to the
// cap the ingest stream had. That is not the budget applied in full: a boot
// that found the pair split applied none, and a cap of 0 would leave the
// ingest stream with no cap at all.
func TestEmbeddedNATS_SetMaxBytes_UndoRestoresTheIngestStreamsCap(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dir := storeDir(t)
	first, err := NewEmbedded(dir)
	require.NoError(t, err)
	require.NoError(t, first.SetMaxBytes(ctx, "acme", 8<<20))
	require.NoError(t, first.js.DeleteStream(ctx, "DLQ_acme"))
	require.NoError(t, first.Close())
	// The dead-letter stream cannot open again: a file where its store goes.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "jetstream", "$G", "streams", dlqStreamName("acme")), nil, 0o600))

	e := openEmbedded(t, dir)
	require.Zero(t, e.MaxBytes("acme"), "a pair without its dead-letter stream is not at its budget")
	err = e.SetMaxBytes(ctx, "acme", 16<<20)
	require.ErrorContains(t, err, "ingest stream restored to the previous limit")
	assert.Equal(t, int64(8<<20), streamConfig(t, e, "INGEST_acme").MaxBytes, "back at the cap it had, not unlimited")
	assert.Zero(t, e.MaxBytes("acme"), "and the next call retries")
}

// A consumer that cannot join a tenant's queue opened after it started says so
// on failed — the one report that stops the ingest worker, which would
// otherwise let the tenant's ingest answer 200 for rows nobody reads. The
// queue itself is open, so SetMaxBytes succeeds.
func TestEmbeddedNATS_Consume_ReportsAQueueItCannotJoin(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storeDir(t))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	// A durable name the client refuses: with no queue yet, nothing checks it.
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "bad.name", MaxAckPending: 10})
	require.NoError(t, err)
	stop, failed, err := cons.Consume(func(*Message) {}, 4)
	require.NoError(t, err)
	t.Cleanup(stop)

	require.NoError(t, e.SetMaxBytes(ctx, "acme", testBudget))
	select {
	case err := <-failed:
		require.ErrorIs(t, err, ErrDeliveryEnded)
		assert.Contains(t, err.Error(), "acme")
	case <-time.After(5 * time.Second):
		t.Fatal("a queue the consumer could not join was not reported")
	}
}

// A budget that shrinks a tenant's dead-letter stream below what it holds
// would have DiscardOld delete the oldest parked rows to fit (#532), so the
// stream keeps what it holds, capped at that, and every row survives.
func TestEmbeddedNATS_SetMaxBytes_NeverShrinksTheDeadLetterQueueBelowWhatItHolds(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storeDir(t))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, e.SetMaxBytes(ctx, "acme", 10<<20))

	payload := make([]byte, 1<<10)
	for range 200 {
		msg := NewMessage(ctx, Topic{Tenant: "acme", Table: "t"}, payload, time.Now(), nil, nil, nil)
		require.NoError(t, e.DeadLetter(ctx, msg))
	}
	dlqState := func() jetstream.StreamState {
		t.Helper()
		s, err := e.js.Stream(ctx, "DLQ_acme")
		require.NoError(t, err)
		return s.CachedInfo().State
	}
	held := dlqState().Bytes
	require.Greater(t, held, uint64(100<<10), "the rows take more than a tenth of the budget below")

	// Shrunk to a 1 MB budget: a tenth of it is less than the stream holds.
	require.NoError(t, e.SetMaxBytes(ctx, "acme", 1<<20))
	assert.Equal(t, int64(1<<20), e.MaxBytes("acme"), "the budget applies")
	assert.Equal(t, int64(1<<20), streamConfig(t, e, "INGEST_acme").MaxBytes)
	assert.Equal(t, held, uint64(streamConfig(t, e, "DLQ_acme").MaxBytes), "capped at what it holds, not at a tenth") //nolint:gosec // G115: a stream cap is never negative
	assert.Equal(t, uint64(200), dlqState().Msgs, "no parked row is deleted")

	// A budget whose tenth covers what it holds applies as usual.
	require.NoError(t, e.SetMaxBytes(ctx, "acme", 4<<20))
	assert.Equal(t, int64(4<<20)/10, streamConfig(t, e, "DLQ_acme").MaxBytes)
	assert.Equal(t, uint64(200), dlqState().Msgs)
}

func TestEmbeddedNATS_ReplaySince_PullFailureIsAnError(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "r"}, []byte("one")))
	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "r"}, []byte("two")))

	// Closing the client connection under a running replay makes the next pull
	// fail outright — that is not "caught up" and must reach the caller.
	var got []string
	err := e.ReplaySince(ctx, Topic{Tenant: tenant.Default, Table: "r"}, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		e.conn.Close()
		return true
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, nats.ErrConnectionClosed)
	assert.Equal(t, []string{"one"}, got, "the message delivered before the failure was still sent")
}

func TestEmbeddedNATS_ReplaySince_StopsWhenContextIsDone(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "r"}, []byte("one")))
	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "r"}, []byte("two")))

	// Cancelling mid-replay (the client went away, or the server is shutting
	// down) ends the drain before the next pull rather than running to caught up.
	var got []string
	err := e.ReplaySince(ctx, Topic{Tenant: tenant.Default, Table: "r"}, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		cancel()
		return true
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"one"}, got)
}

func TestEmbeddedNATS_DeadLetter(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, tenant.Default, "acme")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	empty, err := e.DeadLetterCounts(ctx, tenant.Default, "")
	require.NoError(t, err)
	assert.Equal(t, DeadLetterCounts{Tables: map[string]uint64{}}, empty)

	park := func(topic Topic, data string) {
		t.Helper()
		msg := NewMessage(ctx, topic, []byte(data), time.Now(), nil, nil, nil)
		require.NoError(t, e.DeadLetter(ctx, msg, WithHeader("X-DLQ-Error", "boom")))
	}
	park(Topic{Tenant: tenant.Default, Table: "default.orders"}, "o1")
	park(Topic{Tenant: tenant.Default, Table: "default.orders"}, "o2")
	park(Topic{Tenant: tenant.Default, Table: "users"}, "u1")
	// Another tenant's table of the same name is its own queue and its own
	// count.
	park(Topic{Tenant: "acme", Table: "users"}, "acme-u1")

	// Parked under the same topic on the tenant's dead-letter stream, headers
	// intact, and nothing lands on the ingest stream.
	dlq, err := e.js.Stream(ctx, "DLQ_0")
	require.NoError(t, err)
	raw, err := dlq.GetLastMsgForSubject(ctx, "dlq.0.default%2Eorders")
	require.NoError(t, err)
	assert.Equal(t, []byte("o2"), raw.Data)
	assert.Equal(t, "boom", raw.Header.Get("X-DLQ-Error"))
	ingest, err := e.stream(ctx, "INGEST_0")
	require.NoError(t, err)
	st, err := ingest.state(ctx, "")
	require.NoError(t, err)
	assert.Zero(t, st.Msgs)

	all, err := e.DeadLetterCounts(ctx, tenant.Default, "")
	require.NoError(t, err)
	assert.Equal(t, DeadLetterCounts{Tables: map[string]uint64{"default.orders": 2, "users": 1}, Total: 3}, all, "table names come back decoded, the tenant's own alone")

	one, err := e.DeadLetterCounts(ctx, tenant.Default, "default.orders")
	require.NoError(t, err)
	assert.Equal(t, DeadLetterCounts{Tables: map[string]uint64{"default.orders": 2}, Total: 3}, one, "Total is every parked message of the tenant, filter or not")

	acme, err := e.DeadLetterCounts(ctx, "acme", "")
	require.NoError(t, err)
	assert.Equal(t, DeadLetterCounts{Tables: map[string]uint64{"users": 1}, Total: 1}, acme)

	none, err := e.DeadLetterCounts(ctx, tenant.Default, "never_failed")
	require.NoError(t, err)
	assert.Empty(t, none.Tables)
}

// A tenant with no queue — one never given a budget on this data directory —
// has nothing parked, which is not the same as a failed read.
func TestEmbeddedNATS_DeadLetterCounts_NoQueue(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := e.DeadLetterCounts(ctx, "globex", "")
	require.ErrorIs(t, err, ErrNoDeadLetterQueue)

	require.NoError(t, e.js.DeleteStream(ctx, "DLQ_0"))
	_, err = e.DeadLetterCounts(ctx, tenant.Default, "")
	require.ErrorIs(t, err, ErrNoDeadLetterQueue)

	_, err = e.DeadLetterCounts(ctx, "a.b", "")
	require.Error(t, err, "an id outside the grammar names no stream")
	assert.NotErrorIs(t, err, ErrNoDeadLetterQueue)
}

func TestEmbeddedNATS_DeadLetterCounts_BrokerFailureIsNotAnEmptyQueue(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// A lookup that fails for any reason other than "no such stream" must not
	// read as an empty queue.
	e.conn.Close()
	_, err := e.DeadLetterCounts(ctx, tenant.Default, "")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrNoDeadLetterQueue)
}

func TestEmbeddedNATS_DeadLetter_IsAPrefixSwap(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "a")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// A subject this package would never write (four tokens) still parks
	// under the very same tail, in the queue of the tenant its first token
	// names: nothing on the dead-letter path decodes or re-encodes it.
	_, err := e.js.Publish(ctx, "ingest.a.b.c.d", []byte("foreign"))
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
	assert.Equal(t, "a.b.c.d", msg.TopicKey())
	assert.Equal(t, Topic{Table: "a.b.c.d"}, msg.Topic(), "a foreign tail is the table of no tenant")
	require.NoError(t, e.DeadLetter(ctx, msg))

	dlq, err := e.js.Stream(ctx, "DLQ_a")
	require.NoError(t, err)
	raw, err := dlq.GetLastMsgForSubject(ctx, "dlq.a.b.c.d")
	require.NoError(t, err)
	assert.Equal(t, []byte("foreign"), raw.Data)
}

// A dead-letter stream that has gone missing is opened again with its
// tenant's queue, at a tenth of the budget last asked for it, rather than
// leaving the row to be redelivered.
func TestEmbeddedNATS_DeadLetter_ReopensAMissingQueue(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.js.DeleteStream(ctx, "DLQ_acme"))
	require.NoError(t, e.DeadLetter(ctx, NewMessage(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x"), time.Now(), nil, nil, nil)))
	assert.Equal(t, int64(testBudget)/10, streamConfig(t, e, "DLQ_acme").MaxBytes)
	counts, err := e.DeadLetterCounts(ctx, "acme", "")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), counts.Total)
}

func TestEmbeddedNATS_Publish_QueueFull(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storeDir(t))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, e.SetMaxBytes(ctx, "acme", 4<<10))
	require.NoError(t, e.SetMaxBytes(ctx, "globex", 4<<10))

	// DiscardNew refuses the publish that would pass the byte budget; that is
	// the backpressure signal, named so callers need not read broker errors.
	payload := make([]byte, 1<<10)
	var err error
	for range 8 {
		if err = e.Publish(ctx, Topic{Tenant: "acme", Table: "full"}, payload); err != nil {
			break
		}
	}
	require.ErrorIs(t, err, ErrQueueFull)

	// Only the tenant at its budget is refused: the next one has a budget of
	// its own.
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "globex", Table: "full"}, payload))
}

// A tenant's queue opens at the budget last asked for it when a publish finds
// it missing, and a tenant never given a budget has no queue to publish to:
// that is refused as a full queue, and nothing is opened for it.
func TestEmbeddedNATS_Publish_OpensTheQueueAtTheLastBudget(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for _, name := range []string{"INGEST_acme", "DLQ_acme"} {
		require.NoError(t, e.js.DeleteStream(ctx, name))
	}
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x")))
	assert.Equal(t, int64(testBudget), streamConfig(t, e, "INGEST_acme").MaxBytes)
	assert.Equal(t, int64(testBudget)/10, streamConfig(t, e, "DLQ_acme").MaxBytes)

	err := e.Publish(ctx, Topic{Tenant: "globex", Table: "t"}, []byte("x"))
	require.ErrorIs(t, err, ErrQueueFull)
	assert.Contains(t, err.Error(), "globex")
	_, err = e.js.Stream(ctx, "INGEST_globex")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound)
}

// The context a publish reopens a queue under is one client's request, but
// the queue is every consumer's: a client gone before the consumers join must
// not leave a queue that no consumer holds, which the ingest worker would
// report as its delivery ending. So the reopen — joins included — outlives
// the caller's cancellation.
func TestEmbeddedNATS_ReopenOutlivesTheCallersCancellation(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 10})
	require.NoError(t, err)
	for _, name := range []string{"INGEST_acme", "DLQ_acme"} {
		require.NoError(t, e.js.DeleteStream(ctx, name))
	}

	gone, stop := context.WithCancel(ctx)
	stop()
	require.NoError(t, e.reopen(gone, "acme"))

	_, err = e.js.Consumer(ctx, "INGEST_acme", "buffer")
	require.NoError(t, err, "the consumer joined the reopened queue")
	select {
	case err := <-cons.(*workerConsumer).failed:
		t.Fatalf("the reopen was reported as the consumer's failure: %v", err)
	default:
	}
	got := make(chan byte, 1)
	stopConsume, _, err := cons.Consume(func(msg *Message) {
		_ = msg.Ack()
		got <- msg.Data[0]
	}, 4)
	require.NoError(t, err)
	t.Cleanup(stopConsume)
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte{7}))
	select {
	case b := <-got:
		assert.Equal(t, byte(7), b)
	case <-time.After(5 * time.Second):
		t.Fatal("the reopened queue is not delivered")
	}
}

func TestEmbeddedNATS_PurgeAcked(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// No consumer yet: the sentinel the sweeper keys its "not yet" warning on.
	_, err := e.PurgeAcked(ctx, "buffer", map[tenant.ID]time.Time{tenant.Default: time.Now()})
	require.ErrorIs(t, err, ErrConsumerNotFound)

	for i := range 4 {
		require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "p"}, []byte{byte(i)}))
	}

	// Ack the first two; the last two stay unwritten.
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 10})
	require.NoError(t, err)
	acked := make(chan error, 4)
	stop, _, err := cons.Consume(func(msg *Message) {
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
	s, err := e.stream(ctx, "INGEST_0")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		floor, err := s.consumerAckFloor(ctx, "buffer")
		return err == nil && floor == 2
	}, 5*time.Second, 20*time.Millisecond)

	// Everything is acked-or-not but nothing is old enough: keep it all.
	purged, err := e.PurgeAcked(ctx, "buffer", map[tenant.ID]time.Time{tenant.Default: time.Now().Add(-time.Hour)})
	require.NoError(t, err)
	assert.False(t, purged)

	// Everything is old enough: only the acked two go.
	purged, err = e.PurgeAcked(ctx, "buffer", map[tenant.ID]time.Time{tenant.Default: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	assert.True(t, purged)
	st, err := s.state(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(3), st.FirstSeq, "unacked events survive however old they are")
	assert.Equal(t, uint64(2), st.Msgs)
}

// Each tenant's queue is purged at its own cutoff and below its own ack
// floor: a tenant keeping an hour of history keeps it while the next one's
// goes, and a tenant the cutoffs do not name — one no longer served — keeps
// no history at all.
func TestEmbeddedNATS_PurgeAcked_EachTenantAtItsOwnCutoff(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex", "initech")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for _, id := range []tenant.ID{"acme", "globex", "initech"} {
		for i := range 2 {
			require.NoError(t, e.Publish(ctx, Topic{Tenant: id, Table: "p"}, []byte{byte(i)}))
		}
	}
	ackAll(t, e, "buffer", 6)
	for _, id := range []tenant.ID{"acme", "globex", "initech"} {
		s, err := e.stream(ctx, ingestStreamName(id))
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			floor, err := s.consumerAckFloor(ctx, "buffer")
			return err == nil && floor == 2
		}, 5*time.Second, 20*time.Millisecond, id)
	}

	purged, err := e.PurgeAcked(ctx, "buffer", map[tenant.ID]time.Time{
		"acme":   time.Now().Add(-time.Hour), // an hour of history: all of it inside the window
		"globex": time.Now().Add(time.Hour),  // everything older than the cutoff
	})
	require.NoError(t, err)
	assert.True(t, purged)
	msgs := func(id tenant.ID) uint64 {
		s, err := e.stream(ctx, ingestStreamName(id))
		require.NoError(t, err)
		st, err := s.state(ctx, "")
		require.NoError(t, err)
		return st.Msgs
	}
	assert.Equal(t, uint64(2), msgs("acme"), "kept for its own window")
	assert.Zero(t, msgs("globex"), "past its own window")
	assert.Zero(t, msgs("initech"), "a tenant the cutoffs do not name keeps nothing it has acknowledged")
}

// One tenant's purge failing stops no other tenant's: the errors say which
// failed, and the sweep goes on to the next tenant at its own cutoff — here
// after one whose durable is gone and one whose stream is. A sweep whose
// context has already ended touches no tenant.
func TestEmbeddedNATS_PurgeAcked_OneTenantsFailureStopsNoOther(t *testing.T) {
	t.Parallel()
	ids := []tenant.ID{"acme", "globex", "initech", "umbrella"}
	e := newTestEmbedded(t, ids...)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for _, id := range ids {
		for i := range 2 {
			require.NoError(t, e.Publish(ctx, Topic{Tenant: id, Table: "p"}, []byte{byte(i)}))
		}
	}
	ackAll(t, e, "buffer", 8)
	for _, id := range ids {
		s, err := e.stream(ctx, ingestStreamName(id))
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			floor, err := s.consumerAckFloor(ctx, "buffer")
			return err == nil && floor == 2
		}, 5*time.Second, 20*time.Millisecond, id)
	}
	msgs := func(id tenant.ID) uint64 {
		s, err := e.stream(ctx, ingestStreamName(id))
		require.NoError(t, err)
		st, err := s.state(ctx, "")
		require.NoError(t, err)
		return st.Msgs
	}

	ended, end := context.WithCancel(ctx)
	end()
	purged, err := e.PurgeAcked(ended, "buffer", nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, purged)
	assert.Equal(t, uint64(2), msgs("umbrella"), "a sweep whose context has ended touches nothing")

	require.NoError(t, e.js.DeleteConsumer(ctx, ingestStreamName("acme"), "buffer"))
	require.NoError(t, e.js.DeleteStream(ctx, ingestStreamName("globex")))
	purged, err = e.PurgeAcked(ctx, "buffer", map[tenant.ID]time.Time{"initech": time.Now().Add(-time.Hour)})
	require.ErrorIs(t, err, ErrConsumerNotFound, "acme's durable is gone")
	require.ErrorContains(t, err, "tenant globex: get stream")
	assert.True(t, purged, "the tenants after them are purged all the same")
	assert.Equal(t, uint64(2), msgs("acme"))
	assert.Equal(t, uint64(2), msgs("initech"), "kept for its own window")
	assert.Zero(t, msgs("umbrella"))
}

// The isolation per-tenant queues buy: a tenant at MaxAckPending, or one
// whose handler is stuck, holds back its own delivery and no other tenant's —
// each tenant's messages arrive on a delivery of their own, in order.
func TestEmbeddedNATS_Consume_OneTenantsBacklogDoesNotHoldAnother(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex", "initech")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 2})
	require.NoError(t, err)
	release := make(chan struct{})
	var mu sync.Mutex
	delivered := map[tenant.ID][]byte{}
	stop, _, err := cons.Consume(func(msg *Message) {
		id := msg.Topic().Tenant
		mu.Lock()
		delivered[id] = append(delivered[id], msg.Data[0])
		mu.Unlock()
		if id == "initech" {
			<-release // never returns until the test ends
		}
		if id == "globex" {
			_ = msg.Ack() // acme never acks: its delivery stops at MaxAckPending
		}
	}, 12)
	require.NoError(t, err)
	t.Cleanup(func() {
		close(release)
		stop()
	})

	for i := range 5 {
		for _, id := range []tenant.ID{"acme", "globex", "initech"} {
			require.NoError(t, e.Publish(ctx, Topic{Tenant: id, Table: "t"}, []byte{byte(i)}))
		}
	}
	counts := func() (acme, globex, initech int) {
		mu.Lock()
		defer mu.Unlock()
		return len(delivered["acme"]), len(delivered["globex"]), len(delivered["initech"])
	}
	require.Eventually(t, func() bool {
		acme, globex, initech := counts()
		return acme == 2 && globex == 5 && initech == 1
	}, 5*time.Second, 20*time.Millisecond, "globex is delivered in full while acme waits on its acks and initech on its handler")
	time.Sleep(200 * time.Millisecond)
	acme, globex, initech := counts()
	assert.Equal(t, 2, acme, "no more than MaxAckPending unacked, for acme alone")
	assert.Equal(t, 5, globex)
	assert.Equal(t, 1, initech, "a stuck handler holds back its own tenant alone")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []byte{0, 1, 2, 3, 4}, delivered["globex"], "in the order published")
}

// A tenant's queue opened after the consumer started is joined to it: both
// consumer paths deliver its events as they do the queues that were there
// first, whether those were opened in this process or found on disk.
func TestEmbeddedNATS_ConsumersJoinQueuesOpenedLater(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	worker := make(chan Topic, 4)
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 10})
	require.NoError(t, err)
	stop, _, err := cons.Consume(func(msg *Message) {
		_ = msg.Ack()
		worker <- msg.Topic()
	}, 4)
	require.NoError(t, err)
	t.Cleanup(stop)
	hub := make(chan Topic, 4)
	require.NoError(t, e.Subscribe(ctx, "hub-bridge", func(msg *Message) error {
		_ = msg.Ack()
		hub <- msg.Topic()
		return nil
	}))

	require.NoError(t, e.SetMaxBytes(ctx, "globex", testBudget))
	for _, id := range []tenant.ID{"acme", "globex"} {
		require.NoError(t, e.Publish(ctx, Topic{Tenant: id, Table: "t"}, []byte("x")))
	}
	for name, got := range map[string]chan Topic{"worker": worker, "hub": hub} {
		var topics []Topic
		for range 2 {
			select {
			case topic := <-got:
				topics = append(topics, topic)
			case <-time.After(5 * time.Second):
				t.Fatalf("%s: timed out; delivered %v", name, topics)
			}
		}
		assert.ElementsMatch(t, []Topic{{Tenant: "acme", Table: "t"}, {Tenant: "globex", Table: "t"}}, topics, name)
	}
}

func TestEmbeddedNATS_Consume_ReportsDeliveryEndingOnItsOwn(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "doomed", MaxAckPending: 10})
	require.NoError(t, err)
	stop, failed, err := cons.Consume(func(*Message) {}, 4)
	require.NoError(t, err)
	t.Cleanup(stop)

	select {
	case err := <-failed:
		t.Fatalf("a healthy consumer reported a failure: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Deleting one tenant's durable underneath a running Consume is terminal
	// for that tenant: the client stops the subscription on its own, and no
	// message will ever say so. It must reach the caller.
	require.NoError(t, e.js.DeleteConsumer(ctx, "INGEST_globex", "doomed"))

	select {
	case err := <-failed:
		require.ErrorIs(t, err, ErrDeliveryEnded)
		require.ErrorIs(t, err, jetstream.ErrConsumerDeleted, "the broker's reason is kept")
		assert.Contains(t, err.Error(), "globex", "the tenant is named")
	case <-ctx.Done():
		t.Fatal("delivery ended underneath the consumer and nothing was reported")
	}
}

func TestEmbeddedNATS_Consume_StopIsNotAFailure(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "stopped", MaxAckPending: 10})
	require.NoError(t, err)
	stop, failed, err := cons.Consume(func(*Message) {}, 4)
	require.NoError(t, err)

	stop()
	select {
	case err := <-failed:
		t.Fatalf("our own stop was reported as a failure: %v", err)
	case <-time.After(time.Second):
	}
	// Nor is a queue opened after the stop joined to it.
	require.NoError(t, e.SetMaxBytes(ctx, "initech", testBudget))
	_, err = e.js.Consumer(ctx, "INGEST_initech", "stopped")
	require.ErrorIs(t, err, jetstream.ErrConsumerNotFound)
}

// The fetch-ahead asked for is shared by the tenants' queues, at least one
// each, so the rows held client-side stay about what the caller asked for
// however many tenants there are.
func TestFanIn_SharesThePrefetch(t *testing.T) {
	t.Parallel()
	handles := func(n int) map[tenant.ID]jetstream.Consumer {
		m := map[tenant.ID]jetstream.Consumer{}
		for i := range n {
			m[tenant.ID(fmt.Sprint(i))] = nil
		}
		return m
	}
	assert.Equal(t, 500, (&fanIn{prefetch: 500, handles: handles(1)}).share())
	assert.Equal(t, 250, (&fanIn{prefetch: 500, handles: handles(2)}).share())
	assert.Equal(t, 1, (&fanIn{prefetch: 500, handles: handles(1000)}).share(), "at least one per tenant")
	assert.Equal(t, 500, (&fanIn{prefetch: 500}).share(), "no tenant yet")
	assert.Zero(t, (&fanIn{handles: handles(3)}).share(), "0 leaves the client default")
}

// The hub bridge's fetch-ahead is the client default split across the
// tenants' queues, like the worker's prefetch, so what it holds client-side
// does not grow with the number of tenants.
func TestEmbeddedNATS_Subscribe_SharesTheClientDefault(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, e.Subscribe(ctx, "hub-bridge", func(*Message) error { return nil }))

	e.mu.Lock()
	defer e.mu.Unlock()
	require.Len(t, e.consumers, 1)
	assert.Equal(t, jetstream.DefaultMaxMessages, e.consumers[0].prefetch)
	assert.Equal(t, jetstream.DefaultMaxMessages/2, e.consumers[0].share())
}

// Nothing lands on the default tenant by omission (#583): the tenant is a
// required token, checked against its grammar before anything is sent.
func TestEmbeddedNATS_Publish_RefusesATopicWithoutATenant(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for _, topic := range []Topic{{Table: "events"}, {Tenant: "a.b", Table: "events"}} {
		require.Error(t, e.Publish(ctx, topic, []byte("x")), "%+v", topic)
		require.Error(t, e.ReplaySince(ctx, topic, time.Time{}, func([]byte) bool { return true }), "%+v", topic)
	}
	s, err := e.stream(ctx, "INGEST_0")
	require.NoError(t, err)
	st, err := s.state(ctx, "")
	require.NoError(t, err)
	assert.Zero(t, st.Msgs)
}

// Two tenants, one table name: a replay of one never carries the other's rows.
func TestEmbeddedNATS_ReplaySince_IsPerTenant(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "r"}, []byte("acme1")))
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "globex", Table: "r"}, []byte("globex1")))
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "r"}, []byte("acme2")))

	var got []string
	require.NoError(t, e.ReplaySince(ctx, Topic{Tenant: "acme", Table: "r"}, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		return true
	}))
	assert.Equal(t, []string{"acme1", "acme2"}, got)
}

// A store directory that cannot be created refuses the boot at once, rather
// than after the server's whole wait for a JetStream that will never start.
func TestNewEmbedded_AStoreItCannotCreateFailsAtOnce(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "nats")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	start := time.Now()
	_, err := NewEmbedded(file)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 3*time.Second)
}

// A boot over a directory an earlier build wrote deletes the pair of streams
// it kept for every tenant together: their subjects overlap every tenant's,
// so no tenant's queue could open beside them.
func TestNewEmbedded_DeletesTheStreamsAnEarlierBuildShared(t *testing.T) {
	t.Parallel()
	dir := storeDir(t)
	old, err := NewEmbedded(dir)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for name, subj := range map[string]string{legacyIngestStream: "ingest.>", legacyDLQStream: "dlq.>"} {
		_, err := old.js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subj}})
		require.NoError(t, err)
	}
	_, err = old.js.Publish(ctx, "ingest.events", []byte("pre-tenant"))
	require.NoError(t, err)
	require.NoError(t, old.Close())

	e := openEmbedded(t, dir)
	for _, name := range []string{legacyIngestStream, legacyDLQStream} {
		_, err := e.js.Stream(ctx, name)
		require.ErrorIs(t, err, jetstream.ErrStreamNotFound, name)
	}
	require.NoError(t, e.SetMaxBytes(ctx, tenant.Default, testBudget))
	require.NoError(t, e.Publish(ctx, Topic{Tenant: tenant.Default, Table: "events"}, []byte("x")))
}

// A pair a stop or a failed update left split — its dead-letter stream not
// at a tenth of the ingest cap — or one missing its dead-letter stream is not
// at its budget, so the boot's SetMaxBytes applies the budget to both streams
// again; a dead-letter stream kept above its tenth because it holds more (the
// shrink guard) is at its budget and left as it is.
func TestNewEmbedded_ASplitPairIsAppliedAgainAtBoot(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dir := storeDir(t)
	first, err := NewEmbedded(dir)
	require.NoError(t, err)
	for _, id := range []tenant.ID{"split", "gone", "guarded"} {
		require.NoError(t, first.SetMaxBytes(ctx, id, 10<<20))
	}
	_, err = first.js.UpdateStream(ctx, dlqStreamConfig("split", 2<<20))
	require.NoError(t, err)
	require.NoError(t, first.js.DeleteStream(ctx, "DLQ_gone"))
	payload := make([]byte, 1<<10)
	for range 200 {
		require.NoError(t, first.DeadLetter(ctx, NewMessage(ctx, Topic{Tenant: "guarded", Table: "t"}, payload, time.Now(), nil, nil, nil)))
	}
	require.NoError(t, first.SetMaxBytes(ctx, "guarded", 1<<20))
	guardedCap := streamConfig(t, first, "DLQ_guarded").MaxBytes
	require.Greater(t, guardedCap, int64(1<<20)/10, "the guard kept the parked rows")
	require.NoError(t, first.Close())

	e := openEmbedded(t, dir)
	assert.Zero(t, e.MaxBytes("split"), "a split pair is not at its budget")
	assert.Zero(t, e.MaxBytes("gone"), "nor one missing its dead-letter stream")
	assert.Equal(t, int64(1<<20), e.MaxBytes("guarded"), "a guarded dead-letter stream is")

	for _, id := range []tenant.ID{"split", "gone"} {
		require.NoError(t, e.SetMaxBytes(ctx, id, 10<<20))
		assert.Equal(t, int64(10<<20), e.MaxBytes(id))
		assert.Equal(t, int64(10<<20)/10, streamConfig(t, e, dlqStreamName(id)).MaxBytes, "%s: the pair is whole again", id)
	}
	require.NoError(t, e.SetMaxBytes(ctx, "guarded", 1<<20))
	assert.Equal(t, guardedCap, streamConfig(t, e, "DLQ_guarded").MaxBytes, "left as the guard kept it")
}

// A boot takes stock of the queues on disk: each keeps the budget it last
// had, and a consumer created afterwards is held on every one of them — a
// tenant no longer served, which is never given a budget again, included —
// so what such a tenant had queued still reaches the worker.
func TestNewEmbedded_TakesStockOfTheQueuesOnDisk(t *testing.T) {
	t.Parallel()
	dir := storeDir(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first, err := NewEmbedded(dir)
	require.NoError(t, err)
	require.NoError(t, first.SetMaxBytes(ctx, "acme", 8<<20))
	for i := range 2 {
		require.NoError(t, first.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte{byte(i)}))
	}
	require.NoError(t, first.Close())

	e := openEmbedded(t, dir)
	assert.Equal(t, int64(8<<20), e.MaxBytes("acme"), "the budget is read back")

	got := make(chan byte, 2)
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 10})
	require.NoError(t, err)
	stop, _, err := cons.Consume(func(msg *Message) {
		_ = msg.Ack()
		got <- msg.Data[0]
	}, 4)
	require.NoError(t, err)
	t.Cleanup(stop)
	for i := range 2 {
		select {
		case b := <-got:
			assert.Equal(t, byte(i), b)
		case <-time.After(5 * time.Second):
			t.Fatal("the queued rows of a tenant given no budget this boot were not delivered")
		}
	}
	// And a publish to it opens nothing new: the queue is there at its budget.
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x")))
	assert.Equal(t, int64(8<<20), streamConfig(t, e, "INGEST_acme").MaxBytes)
}

// A durable found on disk is kept as it stands when it holds the settings
// asked for — a boot over many queues writes nothing it need not — and is
// updated in place when they differ; either way delivery resumes past what it
// acknowledged before the restart.
func TestEmbeddedNATS_ADurableOnDiskIsReusedAcrossARestart(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		maxAckPending int
	}{
		{"same settings", 10},
		{"other settings", 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := storeDir(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			topic := Topic{Tenant: "acme", Table: "t"}

			first, err := NewEmbedded(dir)
			require.NoError(t, err)
			require.NoError(t, first.SetMaxBytes(ctx, "acme", 8<<20))
			require.NoError(t, first.Publish(ctx, topic, []byte{0}))
			cons, err := first.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 10})
			require.NoError(t, err)
			acked := make(chan error, 2)
			stop, _, err := cons.Consume(func(msg *Message) { acked <- msg.DoubleAck(ctx) }, 1)
			require.NoError(t, err)
			select {
			case err := <-acked:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("the first row was not delivered")
			}
			stop()
			require.NoError(t, first.Close())

			e := openEmbedded(t, dir)
			require.NoError(t, e.Publish(ctx, topic, []byte{1}))
			cons, err = e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: tt.maxAckPending})
			require.NoError(t, err)
			got := make(chan byte, 2)
			stop, _, err = cons.Consume(func(msg *Message) {
				_ = msg.Ack()
				got <- msg.Data[0]
			}, 4)
			require.NoError(t, err)
			t.Cleanup(stop)
			select {
			case b := <-got:
				assert.Equal(t, byte(1), b, "delivery resumes past what was acknowledged before the restart")
			case <-ctx.Done():
				t.Fatal("the row published after the restart was not delivered")
			}
			c, err := e.js.Consumer(ctx, ingestStreamName("acme"), "buffer")
			require.NoError(t, err)
			assert.Equal(t, tt.maxAckPending, c.CachedInfo().Config.MaxAckPending)
		})
	}
}
