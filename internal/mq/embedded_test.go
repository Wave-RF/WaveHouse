package mq

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
	"github.com/Wave-RF/WaveHouse/internal/testutil/storedir"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testBudget is the byte budget newTestEmbedded opens each queue at.
const testBudget = 64 << 20

// openEmbedded starts an EmbeddedNATS over dir, closed by the test framework.
func openEmbedded(t *testing.T, dir string) *EmbeddedNATS {
	t.Helper()
	e, err := NewEmbedded(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// testWindow is the replay window newTestEmbedded gives each tenant.
const testWindow = 15 * time.Minute

// newTestEmbedded spins up an EmbeddedNATS over a temporary store directory
// with a queue open for each of tenants — tenant.Default when none is named —
// at testBudget and testWindow, as the wiring would.
func newTestEmbedded(t *testing.T, tenants ...tenant.ID) *EmbeddedNATS {
	t.Helper()
	e := openEmbedded(t, storedir.New(t))
	if len(tenants) == 0 {
		tenants = []tenant.ID{tenant.Default}
	}
	for _, id := range tenants {
		require.NoError(t, e.SetReplayWindow(t.Context(), id, testWindow))
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

// held is how many messages the named stream holds.
func held(t *testing.T, e *EmbeddedNATS, name string) uint64 {
	t.Helper()
	s, err := e.js.Stream(t.Context(), name)
	require.NoError(t, err)
	info, err := s.Info(t.Context())
	require.NoError(t, err)
	return info.State.Msgs
}

// writeAll consumes the ingest queues as the ingest worker does until n
// messages have been delivered, recording each as written and then acking it,
// and stops.
func writeAll(t *testing.T, e *EmbeddedNATS, n int) {
	t.Helper()
	ctx := t.Context()
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 1000})
	require.NoError(t, err)
	done := make(chan error, n)
	stop, _, err := cons.Consume(func(msg *Message) {
		err := e.Record(ctx, []*Message{msg})
		if err == nil {
			err = msg.DoubleAck(ctx)
		}
		done <- err
	}, 10)
	require.NoError(t, err)
	defer stop()
	for range n {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out writing the queued rows")
		}
	}
}

// replayed is every event a replay of topic since sends.
func replayed(t *testing.T, e *EmbeddedNATS, topic Topic, since time.Time) []string {
	t.Helper()
	got := []string{}
	require.NoError(t, e.ReplaySince(t.Context(), topic, since, func(data []byte) bool {
		got = append(got, string(data))
		return true
	}))
	return got
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
	err := e.Subscribe(ctx, func(msg *Message) {
		mu.Lock()
		received[msg.Topic()] = msg.Data
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
	}, func() {})
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
	// The figures are the server's: the process's own in-process client is
	// connected, and its JetStream setup went through the server.
	assert.True(t, stats.Embedded)
	assert.GreaterOrEqual(t, stats.Connections, int64(1))
	assert.Positive(t, stats.InMsgs)
	assert.Positive(t, stats.OutMsgs)
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
// success, so an uncertain publish can be republished safely. This stream is
// created directly, never recorded by takeStock, so SetMaxBytes's next
// budget apply always runs and picks up the current window;
// TestNewEmbedded_TakeStockRefreshesAStaleDuplicateWindow covers the boot
// path, where takeStock itself must not mistake a stale window for one
// already at budget.
func TestEmbeddedNATS_Publish_IdempotencyKeyDropsARepeat(t *testing.T) {
	e := openEmbedded(t, storedir.New(t))
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
// subjects alone at the budget, a work queue that refuses when full,
// republishes every row to the tenant's live subject and allows direct gets;
// a history stream at a tenth of the budget, kept for the tenant's replay
// window and dropping its oldest when full; and a dead-letter stream at a
// tenth too, dropping its oldest when full. No other tenant gets one.
func TestEmbeddedNATS_SetMaxBytes_OpensTheTenantsQueue(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storedir.New(t))
	assert.Zero(t, e.MaxBytes("acme"), "no budget applied yet")

	require.NoError(t, e.SetReplayWindow(t.Context(), "acme", 7*time.Minute))
	require.NoError(t, e.SetMaxBytes(t.Context(), "acme", testBudget))
	assert.Equal(t, int64(testBudget), e.MaxBytes("acme"))

	ingest := streamConfig(t, e, "INGEST_acme")
	assert.Equal(t, []string{"ingest.acme.>"}, ingest.Subjects)
	assert.Equal(t, jetstream.WorkQueuePolicy, ingest.Retention, "a row leaves at its own ack")
	assert.Equal(t, int64(testBudget), ingest.MaxBytes)
	assert.Equal(t, jetstream.DiscardNew, ingest.Discard)
	assert.Equal(t, EmbeddedDuplicateWindow, ingest.Duplicates)
	assert.Equal(t, &jetstream.RePublish{Source: "ingest.acme.>", Destination: "live.acme.>"}, ingest.RePublish)
	assert.True(t, ingest.AllowDirect)
	history := streamConfig(t, e, "HISTORY_acme")
	assert.Equal(t, []string{"hist.acme.>"}, history.Subjects)
	assert.Equal(t, jetstream.LimitsPolicy, history.Retention)
	assert.Equal(t, int64(testBudget)/10, history.MaxBytes)
	assert.Equal(t, 7*time.Minute, history.MaxAge)
	assert.Equal(t, jetstream.DiscardOld, history.Discard)
	dlq := streamConfig(t, e, "DLQ_acme")
	assert.Equal(t, []string{"dlq.acme.>"}, dlq.Subjects)
	assert.Equal(t, int64(testBudget)/10, dlq.MaxBytes)
	assert.Equal(t, jetstream.DiscardOld, dlq.Discard)

	assert.Zero(t, e.MaxBytes("globex"))
	_, err := e.js.Stream(t.Context(), "INGEST_globex")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound, "another tenant's queue opens with its own budget")

	require.Error(t, e.SetMaxBytes(t.Context(), "a.b", testBudget), "a tenant outside the grammar has no queue")
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
	e, err := NewEmbedded(storedir.New(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
}

// A done context ends the live subscription: nothing accepted afterwards
// reaches the handler.
func TestEmbeddedNATS_SubscribeCancellation(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t)
	topic := Topic{Tenant: tenant.Default, Table: "t"}

	// The first publish has the client subscribe to its reply subjects, which
	// it keeps; the live subscription is the one counted from here.
	require.NoError(t, e.Publish(t.Context(), topic, []byte("first")))
	held := e.conn.NumSubscriptions()

	ctx, cancel := context.WithCancel(t.Context())
	got := make(chan string, 4)
	require.NoError(t, e.Subscribe(ctx, func(msg *Message) { got <- string(msg.Data) }, func() {}))
	require.Equal(t, held+1, e.conn.NumSubscriptions())
	require.NoError(t, e.Publish(t.Context(), topic, []byte("before")))
	select {
	case data := <-got:
		require.Equal(t, "before", data)
	case <-time.After(5 * time.Second):
		t.Fatal("the live feed delivered nothing")
	}

	cancel()
	// The subscription is dropped on a goroutine the context wakes.
	require.Eventually(t, func() bool { return e.conn.NumSubscriptions() == held }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, e.Publish(t.Context(), topic, []byte("after")))
	select {
	case data := <-got:
		t.Fatalf("%q was delivered after the context ended", data)
	case <-time.After(200 * time.Millisecond):
	}
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

	// The dead-letter and history streams follow at a tenth of the budget.
	dlq := streamConfig(t, e, "DLQ_acme")
	assert.Equal(t, int64(128<<20)/10, dlq.MaxBytes)
	assert.Equal(t, jetstream.DiscardOld, dlq.Discard)
	history := streamConfig(t, e, "HISTORY_acme")
	assert.Equal(t, int64(128<<20)/10, history.MaxBytes)
	assert.Equal(t, testWindow, history.MaxAge, "the window is not the budget's to change")

	// No other tenant's queue moves.
	assert.Equal(t, int64(testBudget), e.MaxBytes("globex"))
	assert.Equal(t, int64(testBudget), streamConfig(t, e, "INGEST_globex").MaxBytes)
	assert.Equal(t, int64(testBudget)/10, streamConfig(t, e, "DLQ_globex").MaxBytes)
	assert.Equal(t, int64(testBudget)/10, streamConfig(t, e, "HISTORY_globex").MaxBytes)

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
	dir := storedir.New(t)
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
	dir := storedir.New(t)
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
	dir := storedir.New(t)
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
	dir := storedir.New(t)
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

// A queue whose streams went missing under delivering consumers is opened
// again by the tenant's next publish, and delivery starts again on the new
// ingest stream, so the row reaches both consumers rather than a stream
// nobody reads. Losing the durable is still the worker's consumer's to
// report on failed, as any delivery that ends on its own is; that is not
// asserted here, since the reopen may get there first.
func TestEmbeddedNATS_Publish_RestartsDeliveryOnAReopenedQueue(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	x := newJoinFixture(ctx, t)
	globex := Topic{Tenant: "globex", Table: "t"}
	// Another tenant's streams keep the streams directory occupied: after a
	// stream is deleted the server, on a goroutine of its own, removes that
	// directory and the account's once they are empty, and with globex's
	// pair the only streams the reopen below would race it — a store made in
	// a directory being removed, which the server refuses as "error creating
	// store for stream" (nats-server 2.14.6 and 2.15.0, reported as
	// https://github.com/nats-io/nats-server/issues/8725).
	require.NoError(t, x.e.SetMaxBytes(ctx, "acme", testBudget))
	// A delivery proves the pulls are live before the streams go.
	require.NoError(t, x.e.Publish(ctx, globex, []byte("x")))
	x.delivered(t, globex)

	for _, name := range []string{"INGEST_globex", "DLQ_globex"} {
		require.NoError(t, x.e.js.DeleteStream(ctx, name))
	}
	require.NoError(t, x.e.Publish(ctx, globex, []byte("x")))
	x.delivered(t, globex)
}

// obstructJoin opens tenant id's ingest stream behind the broker's back, as
// an open that gave up can, with a file where the named durable's store goes:
// that consumer cannot join the queue until the returned clear removes it.
func obstructJoin(t *testing.T, e *EmbeddedNATS, dir string, id tenant.ID, durable string) (clearObstacle func()) {
	t.Helper()
	_, err := e.js.CreateStream(t.Context(), ingestStreamConfig(id, testBudget))
	require.NoError(t, err)
	consumers := filepath.Join(dir, "jetstream", "$G", "streams", ingestStreamName(id), "obs")
	require.NoError(t, os.MkdirAll(consumers, 0o750))
	block := filepath.Join(consumers, durable)
	require.NoError(t, os.WriteFile(block, nil, 0o600))
	return func() {
		t.Helper()
		require.NoError(t, os.Remove(block))
	}
}

// joinFixture is a broker with globex's queue open, the ingest worker's
// consumer delivering and the live feed subscribed to, and acme's queue not
// opened yet.
type joinFixture struct {
	e            *EmbeddedNATS
	dir          string
	worker, live chan Topic
	failed       <-chan error
}

func newJoinFixture(ctx context.Context, t *testing.T) joinFixture {
	t.Helper()
	dir := storedir.New(t)
	x := joinFixture{e: openEmbedded(t, dir), dir: dir, worker: make(chan Topic, 8), live: make(chan Topic, 8)}
	require.NoError(t, x.e.SetMaxBytes(ctx, "globex", testBudget))
	cons, err := x.e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 10})
	require.NoError(t, err)
	stop, failed, err := cons.Consume(func(msg *Message) {
		_ = msg.Ack()
		x.worker <- msg.Topic()
	}, 4)
	require.NoError(t, err)
	t.Cleanup(stop)
	x.failed = failed
	require.NoError(t, x.e.Subscribe(ctx, func(msg *Message) { x.live <- msg.Topic() }, func() {}))
	return x
}

// delivered waits for topic on the worker's consumer and on the live feed.
func (x joinFixture) delivered(t *testing.T, topic Topic) {
	t.Helper()
	for name, got := range map[string]chan Topic{"worker": x.worker, "live": x.live} {
		select {
		case have := <-got:
			assert.Equal(t, topic, have, name)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: %v was not delivered", name, topic)
		}
	}
}

// The worker's consumer failing to join a tenant's queue opened after it
// started costs that tenant alone: the tenant's publishes are refused, so no
// row is answered 200 that the worker would not read, while nothing is
// reported on failed — which would stop the ingest worker, and the process
// with it — and every other tenant publishes and consumes.
func TestEmbeddedNATS_AQueueAConsumerCannotJoin_CostsThatTenantAlone(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	x := newJoinFixture(ctx, t)
	obstructJoin(t, x.e, x.dir, "acme", "buffer")

	err := x.e.SetMaxBytes(ctx, "acme", testBudget)
	require.ErrorContains(t, err, "consumer buffer: join the queue")
	assert.Zero(t, x.e.MaxBytes("acme"), "no budget applied in full, so the next reload retries")
	err = x.e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x"))
	require.ErrorIs(t, err, ErrQueueFull, "the tenant's queue takes nothing; a retry is the answer")

	globex := Topic{Tenant: "globex", Table: "t"}
	require.NoError(t, x.e.Publish(ctx, globex, []byte("x")))
	x.delivered(t, globex)
	select {
	case err := <-x.failed:
		t.Fatalf("one tenant's failed join was reported as the consumer's failure: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}

// A queue the worker's consumer could not join is joined by the tenant's next
// publish once its pacing window has passed, or by the next reload, and the
// row published then reaches the worker and the live feed without a restart.
func TestEmbeddedNATS_AQueueAConsumerCannotJoin_IsJoinedLater(t *testing.T) {
	t.Parallel()
	acme := Topic{Tenant: "acme", Table: "t"}
	retries := map[string]func(context.Context, *testing.T, *EmbeddedNATS){
		"publish": func(ctx context.Context, t *testing.T, e *EmbeddedNATS) {
			// The refused publish below opened the window; it is closed here
			// rather than waited out.
			v, ok := e.failedOpen.Load(acme.Tenant)
			require.True(t, ok, "the refused publish's attempt is paced")
			failed := v.(openFailure)
			failed.until = time.Now()
			e.failedOpen.Store(acme.Tenant, failed)
		},
		"reload": func(ctx context.Context, t *testing.T, e *EmbeddedNATS) {
			require.NoError(t, e.SetMaxBytes(ctx, acme.Tenant, testBudget), "a reload retries regardless of the window")
		},
	}
	for name, retry := range retries {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			x := newJoinFixture(ctx, t)
			clearObstacle := obstructJoin(t, x.e, x.dir, acme.Tenant, "buffer")
			require.Error(t, x.e.SetMaxBytes(ctx, acme.Tenant, testBudget))
			require.ErrorIs(t, x.e.Publish(ctx, acme, []byte("x")), ErrQueueFull, "the publish's own attempt fails")

			clearObstacle()
			require.ErrorIs(t, x.e.Publish(ctx, acme, []byte("x")), ErrQueueFull, "within the window a publish tries nothing")
			retry(ctx, t, x.e)
			require.NoError(t, x.e.Publish(ctx, acme, []byte("x")))
			x.delivered(t, acme)
			assert.Equal(t, int64(testBudget), x.e.MaxBytes(acme.Tenant))
			select {
			case err := <-x.failed:
				t.Fatalf("a failure was reported: %v", err)
			default:
			}
		})
	}
}

// A budget that shrinks a tenant's dead-letter stream below what it holds
// would have DiscardOld delete the oldest parked rows to fit (#532), so the
// stream keeps what it holds, capped at that, and every row survives.
func TestEmbeddedNATS_SetMaxBytes_NeverShrinksTheDeadLetterQueueBelowWhatItHolds(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storedir.New(t))
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
	assert.Zero(t, held(t, e, "INGEST_0"))

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
	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 10})
	require.NoError(t, err)
	stop, _, err := cons.Consume(func(msg *Message) { got <- msg }, 4)
	require.NoError(t, err)
	t.Cleanup(stop)
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
	e := openEmbedded(t, storedir.New(t))
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
	// initech's streams keep the streams directory occupied while acme's are
	// gone, so the reopen does not race the server's removal of it (see
	// TestEmbeddedNATS_Publish_RestartsDeliveryOnAReopenedQueue).
	e := newTestEmbedded(t, "acme", "initech")
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
// not leave a queue that no consumer holds, which would refuse the tenant's
// publishes until a later one joined them. So the reopen — joins included —
// outlives the caller's cancellation.
func TestEmbeddedNATS_ReopenOutlivesTheCallersCancellation(t *testing.T) {
	t.Parallel()
	// globex's streams keep the streams directory occupied while acme's are
	// gone, so the reopen does not race the server's removal of it (see
	// TestEmbeddedNATS_Publish_RestartsDeliveryOnAReopenedQueue).
	e := newTestEmbedded(t, "acme", "globex")
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

// A tenant's queue opened after the consumer started is joined to it: the
// worker's consumer and the live feed deliver its events as they do those of
// the queues that were there first, whether those were opened in this process
// or found on disk.
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
	live := make(chan Topic, 4)
	require.NoError(t, e.Subscribe(ctx, func(msg *Message) { live <- msg.Topic() }, func() {}))

	require.NoError(t, e.SetMaxBytes(ctx, "globex", testBudget))
	for _, id := range []tenant.ID{"acme", "globex"} {
		require.NoError(t, e.Publish(ctx, Topic{Tenant: id, Table: "t"}, []byte("x")))
	}
	for name, got := range map[string]chan Topic{"worker": worker, "live": live} {
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
	assert.Zero(t, held(t, e, "INGEST_0"))
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
	dir := storedir.New(t)
	old, err := NewEmbedded(dir)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for name, subj := range map[string]string{legacyIngestStream: "ingest.>", legacyDLQStream: "dlq.>"} {
		_, err := old.js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subj}})
		require.NoError(t, err)
	}
	// A stream that is no tenant's keeps the streams directory occupied once
	// the boot below has deleted the pair, so the queue opened after it does
	// not race the server's removal of that directory (see
	// TestEmbeddedNATS_Publish_RestartsDeliveryOnAReopenedQueue).
	_, err = old.js.CreateStream(ctx, jetstream.StreamConfig{Name: "KEPT", Subjects: []string{"kept"}})
	require.NoError(t, err)
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

// A queue a stop or a failed update left split — its dead-letter or history
// stream not at a tenth of the ingest cap — or one missing either stream is
// not at its budget, so the boot's SetMaxBytes applies the budget to every
// stream again; a dead-letter stream kept above its tenth because it holds
// more (the shrink guard) is at its budget and left as it is.
func TestNewEmbedded_ASplitQueueIsAppliedAgainAtBoot(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dir := storedir.New(t)
	first, err := NewEmbedded(dir)
	require.NoError(t, err)
	for _, id := range []tenant.ID{"split", "gone", "unrecorded", "narrow", "guarded"} {
		require.NoError(t, first.SetReplayWindow(ctx, id, testWindow))
		require.NoError(t, first.SetMaxBytes(ctx, id, 10<<20))
	}
	_, err = first.js.UpdateStream(ctx, dlqStreamConfig("split", 2<<20))
	require.NoError(t, err)
	require.NoError(t, first.js.DeleteStream(ctx, "DLQ_gone"))
	require.NoError(t, first.js.DeleteStream(ctx, "HISTORY_unrecorded"))
	_, err = first.js.UpdateStream(ctx, historyStreamConfig("narrow", 2<<20, testWindow))
	require.NoError(t, err)
	payload := make([]byte, 1<<10)
	for range 200 {
		require.NoError(t, first.DeadLetter(ctx, NewMessage(ctx, Topic{Tenant: "guarded", Table: "t"}, payload, time.Now(), nil, nil, nil)))
	}
	require.NoError(t, first.SetMaxBytes(ctx, "guarded", 1<<20))
	guardedCap := streamConfig(t, first, "DLQ_guarded").MaxBytes
	require.Greater(t, guardedCap, int64(1<<20)/10, "the guard kept the parked rows")
	require.NoError(t, first.Close())

	e := openEmbedded(t, dir)
	assert.Zero(t, e.MaxBytes("split"), "a split queue is not at its budget")
	assert.Zero(t, e.MaxBytes("gone"), "nor one missing its dead-letter stream")
	assert.Zero(t, e.MaxBytes("unrecorded"), "nor one missing its history stream")
	assert.Zero(t, e.MaxBytes("narrow"), "nor one whose history stream is not at its tenth")
	assert.Equal(t, int64(1<<20), e.MaxBytes("guarded"), "a guarded dead-letter stream is")

	for _, id := range []tenant.ID{"split", "gone", "unrecorded", "narrow"} {
		// The wiring hands over the window first: a history stream that is
		// gone took the one it had with it.
		require.NoError(t, e.SetReplayWindow(ctx, id, testWindow))
		require.NoError(t, e.SetMaxBytes(ctx, id, 10<<20))
		assert.Equal(t, int64(10<<20), e.MaxBytes(id))
		assert.Equal(t, int64(10<<20)/10, streamConfig(t, e, dlqStreamName(id)).MaxBytes, "%s: the queue is whole again", id)
		history := streamConfig(t, e, historyStreamName(id))
		assert.Equal(t, int64(10<<20)/10, history.MaxBytes, "%s: the queue is whole again", id)
		assert.Equal(t, testWindow, history.MaxAge, "%s: at the tenant's window", id)
	}
	require.NoError(t, e.SetMaxBytes(ctx, "guarded", 1<<20))
	assert.Equal(t, guardedCap, streamConfig(t, e, "DLQ_guarded").MaxBytes, "left as the guard kept it")
}

// A boot takes stock of the queues on disk: each keeps the budget and the
// replay window it last had, and a consumer created afterwards is held on
// every one of them — a tenant no longer served, which is never given a
// budget or a window again, included — so what such a tenant had queued still
// reaches the worker, and is recorded for replay once written.
func TestNewEmbedded_TakesStockOfTheQueuesOnDisk(t *testing.T) {
	t.Parallel()
	dir := storedir.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first, err := NewEmbedded(dir)
	require.NoError(t, err)
	require.NoError(t, first.SetReplayWindow(ctx, "acme", 7*time.Minute))
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
		if string(msg.Data) == "x" {
			return // the row published last, which the test may end under
		}
		assert.NoError(t, e.Record(ctx, []*Message{msg}))
		assert.NoError(t, msg.DoubleAck(ctx))
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
	assert.Equal(t, []string{"\x00", "\x01"}, replayed(t, e, Topic{Tenant: "acme", Table: "t"}, time.Time{}),
		"recorded under the window read back from its history stream")
	assert.Equal(t, 7*time.Minute, streamConfig(t, e, "HISTORY_acme").MaxAge)
	// And a publish to it opens nothing new: the queue is there at its budget.
	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x")))
	assert.Equal(t, int64(8<<20), streamConfig(t, e, "INGEST_acme").MaxBytes)
}

// takeStock must not count a stream as at its budget when a field this
// package sets on it is stale — its Duplicates window from before
// EmbeddedDuplicateWindow existed, or the republish the live feed reads
// changed underneath it: otherwise SetMaxBytes's same-budget early return
// never lets a later apply bring the field forward, and the stream keeps
// whatever it had indefinitely.
func TestNewEmbedded_TakeStockRefreshesAStaleIngestStream(t *testing.T) {
	t.Parallel()
	for name, stale := range map[string]func(*jetstream.StreamConfig){
		"duplicate window": func(c *jetstream.StreamConfig) { c.Duplicates = 10 * time.Second },
		"republish":        func(c *jetstream.StreamConfig) { c.RePublish = nil },
		"direct get":       func(c *jetstream.StreamConfig) { c.AllowDirect = false },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := storedir.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			first, err := NewEmbedded(dir)
			require.NoError(t, err)
			require.NoError(t, first.SetMaxBytes(ctx, "acme", 8<<20))
			want := ingestStreamConfig("acme", 8<<20)
			old := want
			stale(&old)
			_, err = first.js.UpdateStream(ctx, old)
			require.NoError(t, err)
			require.NoError(t, first.Close())

			e := openEmbedded(t, dir)
			require.False(t, sameIngestStream(streamConfig(t, e, "INGEST_acme"), want), "the stale field is still on disk")

			require.NoError(t, e.SetMaxBytes(ctx, "acme", 8<<20), "same budget as before")
			assert.True(t, sameIngestStream(streamConfig(t, e, "INGEST_acme"), want),
				"takeStock must not have marked this queue already at budget, or this apply would have no-op'd")
		})
	}
}

// An ingest stream an earlier build created keeps every row for the sweeper
// to purge, and the server refuses to make it a work queue, so a boot deletes
// it, saying how many messages it held and how many of them the ingest worker
// had not acknowledged: the rows that never reached ClickHouse. The tenant's
// queue then opens at the budget it had, as a work queue, and its dead-letter
// stream keeps what it holds.
func TestNewEmbedded_DeletesAnIngestStreamOfLimitsRetention(t *testing.T) { //nolint:paralleltest // captures the default logger
	logs := logtest.Capture(t, slog.LevelWarn)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dir := storedir.New(t)
	old, err := NewEmbedded(dir)
	require.NoError(t, err)
	for _, id := range []tenant.ID{"acme", "globex"} {
		earlier := ingestStreamConfig(id, 8<<20)
		earlier.Retention, earlier.RePublish, earlier.AllowDirect = jetstream.LimitsPolicy, nil, false
		_, err = old.js.CreateStream(ctx, earlier)
		require.NoError(t, err)
		_, err = old.js.CreateStream(ctx, dlqStreamConfig(id, (8<<20)/10))
		require.NoError(t, err)
		for i := range 5 {
			_, err = old.js.Publish(ctx, "ingest."+string(id)+".t", []byte{byte(i)})
			require.NoError(t, err)
		}
	}
	_, err = old.js.Publish(ctx, "dlq.acme.t", []byte("parked"))
	require.NoError(t, err)
	// acme's worker wrote three of its five rows; globex's never ran.
	worker, err := old.js.CreateOrUpdateConsumer(ctx, "INGEST_acme", jetstream.ConsumerConfig{Durable: workerDurable, AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	batch, err := worker.Fetch(4)
	require.NoError(t, err)
	n := 0
	for m := range batch.Messages() {
		if n++; n <= 3 {
			require.NoError(t, m.DoubleAck(ctx))
		}
	}
	require.NoError(t, batch.Error())
	require.NoError(t, old.Close())

	e := openEmbedded(t, dir)
	for _, want := range []string{
		`"stream":"INGEST_acme","messages":5,"unwritten":2`,
		`"stream":"INGEST_globex","messages":5,"unwritten":5`,
	} {
		assert.Contains(t, logs.String(), want)
	}
	_, err = e.js.Stream(ctx, "INGEST_acme")
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound, "deleted, and opened again with its queue")
	assert.Zero(t, e.MaxBytes("acme"), "its budget is applied again")

	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("x")), "a publish opens the queue at the budget it had")
	ingest := streamConfig(t, e, "INGEST_acme")
	assert.Equal(t, jetstream.WorkQueuePolicy, ingest.Retention)
	assert.Equal(t, int64(8<<20), ingest.MaxBytes)
	assert.Equal(t, uint64(1), held(t, e, "INGEST_acme"), "nothing of the deleted stream is carried over")
	assert.Equal(t, uint64(1), held(t, e, "DLQ_acme"), "the dead-letter stream keeps what it holds")
}

// The failure #653 describes: one row left unacked — its table's inserts
// failing, or its DLQ off — pinned the purge of its tenant's whole stream,
// which filled to its budget and answered every table of the tenant 503.
// Here that row stays unacked while three budgets' worth of another table's
// rows are written behind it, and none is refused.
func TestEmbeddedNATS_AnUnackedRowDoesNotFillTheQueue(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storedir.New(t))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	const budget = 256 << 10
	require.NoError(t, e.SetReplayWindow(ctx, "acme", testWindow))
	require.NoError(t, e.SetMaxBytes(ctx, "acme", budget))

	cons, err := e.CreateConsumer(ctx, ConsumerConfig{Durable: "buffer", MaxAckPending: 1000})
	require.NoError(t, err)
	written := make(chan error, 8)
	stop, _, err := cons.Consume(func(msg *Message) {
		if msg.Topic().Table == "stuck" {
			return
		}
		err := e.Record(ctx, []*Message{msg})
		if err == nil {
			err = msg.DoubleAck(ctx)
		}
		written <- err
	}, 16)
	require.NoError(t, err)
	t.Cleanup(stop)

	require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "stuck"}, []byte("x")))
	payload := make([]byte, 4<<10)
	rows := 3 * budget / len(payload)
	for i := range rows {
		require.NoError(t, e.Publish(ctx, Topic{Tenant: "acme", Table: "fine"}, payload), "row %d of %d", i, rows)
		select {
		case err := <-written:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatalf("row %d of %d was not written", i, rows)
		}
	}
	assert.Equal(t, uint64(1), held(t, e, "INGEST_acme"), "the unacked row is all the queue holds")
}

// A tenant's replay window is its history stream's max_age: kept for the
// stream's opening when the queue is not open yet, applied to the stream when
// it is, without touching its cap. A window of 0 keeps no history: nothing
// written from then on is recorded.
func TestEmbeddedNATS_SetReplayWindow(t *testing.T) {
	t.Parallel()
	e := openEmbedded(t, storedir.New(t))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	topic := Topic{Tenant: "acme", Table: "t"}

	require.NoError(t, e.SetReplayWindow(ctx, "acme", 7*time.Minute), "before the queue opens")
	require.NoError(t, e.SetMaxBytes(ctx, "acme", testBudget))
	assert.Equal(t, 7*time.Minute, streamConfig(t, e, "HISTORY_acme").MaxAge)

	require.NoError(t, e.SetReplayWindow(ctx, "acme", time.Hour))
	history := streamConfig(t, e, "HISTORY_acme")
	assert.Equal(t, time.Hour, history.MaxAge)
	assert.Equal(t, int64(testBudget)/10, history.MaxBytes, "the cap is not the window's to change")

	require.NoError(t, e.Publish(ctx, topic, []byte("kept")))
	writeAll(t, e, 1)
	assert.Equal(t, []string{"kept"}, replayed(t, e, topic, time.Time{}))

	require.NoError(t, e.SetReplayWindow(ctx, "acme", 0))
	assert.Equal(t, noHistoryAge, streamConfig(t, e, "HISTORY_acme").MaxAge, "a max_age of 0 would keep everything")
	require.NoError(t, e.Publish(ctx, topic, []byte("unrecorded")))
	writeAll(t, e, 1)
	assert.Equal(t, uint64(1), held(t, e, "HISTORY_acme"), "nothing is recorded for a tenant that keeps no history")

	require.Error(t, e.SetReplayWindow(ctx, "acme", -time.Minute))
	require.Error(t, e.SetReplayWindow(ctx, "a.b", time.Minute), "a tenant outside the grammar has no window")
}

// A written row whose tenant has no history stream is not recorded, and the
// error says how many were not: the caller acks them all the same. The rows
// of the tenants that have one are recorded.
func TestEmbeddedNATS_Record_AMissingHistoryCostsItsTenantsRowsAlone(t *testing.T) {
	t.Parallel()
	e := newTestEmbedded(t, "acme", "globex")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, e.js.DeleteStream(ctx, "HISTORY_globex"))

	at := time.Now().Add(-time.Hour)
	msgs := []*Message{
		NewMessage(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("a1"), at, nil, nil, nil),
		NewMessage(ctx, Topic{Tenant: "globex", Table: "t"}, []byte("g1"), at, nil, nil, nil),
		NewMessage(ctx, Topic{Tenant: "acme", Table: "t"}, []byte("a2"), at, nil, nil, nil),
	}
	err := e.Record(ctx, msgs)
	require.ErrorContains(t, err, "1 of 3 rows not recorded")
	assert.Equal(t, []string{"a1", "a2"}, replayed(t, e, Topic{Tenant: "acme", Table: "t"}, time.Time{}))
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
			dir := storedir.New(t)
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
