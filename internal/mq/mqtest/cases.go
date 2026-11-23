package mqtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// delivery is one message as a handler saw it.
type delivery struct {
	topic mq.Topic
	data  string
	msg   *mq.Message
}

func publish(t *testing.T, b mq.Broker, topic mq.Topic, data string, opts ...mq.PublishOpt) {
	t.Helper()
	require.NoError(t, b.Publish(ctx(t), topic, []byte(data), opts...), "publish %q on %+v", data, topic)
}

// consume runs the suite's durable on b with handle called before each
// delivery is reported on the returned channel. Stopped at cleanup.
func consume(c context.Context, t *testing.T, b mq.Broker, cfg mq.ConsumerConfig, handle func(*mq.Message)) (<-chan delivery, func(), <-chan error) {
	t.Helper()
	if cfg.Durable == "" {
		cfg.Durable = Durable
	}
	cons, err := b.CreateConsumer(c, cfg)
	require.NoError(t, err)
	got := make(chan delivery, 256)
	stop, failed, err := cons.Consume(func(m *mq.Message) {
		if handle != nil {
			handle(m)
		}
		got <- delivery{topic: m.Topic(), data: string(m.Data), msg: m}
	}, 16)
	require.NoError(t, err)
	t.Cleanup(stop)
	return got, stop, failed
}

// ackEach DoubleAcks every message, reporting a failed ack on t.
func ackEach(t *testing.T) func(*mq.Message) {
	return func(m *mq.Message) {
		assert.NoError(t, m.DoubleAck(m.Ctx))
	}
}

// next waits for n deliveries.
func next(t *testing.T, got <-chan delivery, n int) []delivery {
	t.Helper()
	out := make([]delivery, 0, n)
	timeout := time.After(wait)
	for len(out) < n {
		select {
		case d := <-got:
			out = append(out, d)
		case <-timeout:
			t.Fatalf("timed out after %d of %d deliveries: %+v", len(out), n, out)
		}
	}
	return out
}

// none asserts nothing arrives on ch for a while.
func none[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s: %+v", what, v)
	case <-time.After(quiet):
	}
}

func replay(t *testing.T, b mq.Broker, topic mq.Topic, since time.Time) []string {
	t.Helper()
	got := []string{}
	require.NoError(t, b.ReplaySince(ctx(t), topic, since, func(data []byte) bool {
		got = append(got, string(data))
		return true
	}))
	return got
}

// write runs the suite's durable as the ingest worker would until n messages
// have been delivered: each one take accepts is recorded as written and then
// acked, in the order it arrived, and the others are left unacked, as rows
// ClickHouse cannot take yet are.
func write(t *testing.T, b mq.Broker, n int, take func(data string) bool) {
	t.Helper()
	got, stop, _ := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 1000}, func(m *mq.Message) {
		if take == nil || take(string(m.Data)) {
			assert.NoError(t, b.Record(m.Ctx, []*mq.Message{m}))
			assert.NoError(t, m.DoubleAck(m.Ctx))
		}
	})
	next(t, got, n)
	stop()
}

type ctxKey struct{}

// A topic whose names need encoding comes back as it went in, with its data,
// under its tenant; the consumer path delivers with CreateConsumer's ctx.
func roundTrip(t *testing.T, h Harness) {
	b := h.New(t)
	topics := []mq.Topic{
		{Tenant: Acme, Table: "events"},
		{Tenant: Acme, Table: "a.b*c> d%e", Scope: "s.1 *>%"},
		{Tenant: Globex, Table: "events", Scope: "x"},
	}
	for i, topic := range topics {
		publish(t, b, topic, fmt.Sprint(i))
	}
	c := context.WithValue(ctx(t), ctxKey{}, "worker")
	got, _, _ := consume(c, t, b, mq.ConsumerConfig{MaxAckPending: 100}, ackEach(t))

	byTopic := map[mq.Topic]string{}
	for _, d := range next(t, got, len(topics)) {
		byTopic[d.topic] = d.data
		assert.Equal(t, "worker", d.msg.Ctx.Value(ctxKey{}), "a delivered Message.Ctx is CreateConsumer's")
		assert.NotEmpty(t, d.msg.TopicKey())
	}
	for i, topic := range topics {
		assert.Equal(t, fmt.Sprint(i), byTopic[topic], "%+v", topic)
	}
}

// A publish repeated with the same idempotency key inside the duplicate
// window is a no-op reported as success: the ingest handler relies on this to
// make a retry of an uncertain publish (the outcome unknown after a failure
// other than a full queue) safe rather than a second copy. A different key
// is its own event.
func idempotencyKeyDropsARepeat(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "idem"}

	require.NoError(t, b.Publish(ctx(t), topic, []byte("first"), mq.WithIdempotencyKey("k1")))
	require.NoError(t, b.Publish(ctx(t), topic, []byte("repeat"), mq.WithIdempotencyKey("k1")),
		"a repeat under the same key is reported as success, not stored again")
	require.NoError(t, b.Publish(ctx(t), topic, []byte("second"), mq.WithIdempotencyKey("k2")))

	assert.Equal(t, []string{"first", "second"}, replay(t, b, topic, time.Time{}))
}

// Nothing lands on a tenant by omission (#583), and an invalid tenant is not
// backpressure a retry could clear.
func refusesATopicWithoutATenant(t *testing.T, h Harness) {
	b := h.New(t)
	for _, topic := range []mq.Topic{{Table: "events"}, {Tenant: "a.b", Table: "events"}, {Tenant: "*", Table: "events"}} {
		err := b.Publish(ctx(t), topic, []byte("x"))
		require.Error(t, err, "%+v", topic)
		assert.NotErrorIs(t, err, mq.ErrQueueFull, "%+v", topic)
		require.Error(t, b.ReplaySince(ctx(t), topic, time.Time{}, func([]byte) bool { return true }), "%+v", topic)
	}
}

// A live reader is handed each event once, when the queue accepts it: in the
// order it was accepted, whether or not anything consumes the queue, and never
// a repeat the queue's duplicate window dropped.
func liveOnceWhenAccepted(t *testing.T, h Harness) {
	b := h.New(t)
	got := make(chan delivery, 16)
	lost := make(chan struct{}, 1)
	require.NoError(t, b.Subscribe(t.Context(), func(m *mq.Message) {
		got <- delivery{topic: m.Topic(), data: string(m.Data)}
	}, func() { lost <- struct{}{} }))

	topic := mq.Topic{Tenant: Acme, Table: "live", Scope: "s.1"}
	publish(t, b, topic, "first", mq.WithIdempotencyKey("k1"))
	publish(t, b, topic, "repeat", mq.WithIdempotencyKey("k1"))
	publish(t, b, topic, "second")

	for _, want := range []string{"first", "second"} {
		d := next(t, got, 1)[0]
		assert.Equal(t, want, d.data)
		assert.Equal(t, topic, d.topic)
	}
	// Nothing has consumed the queue: the events are still waiting to be
	// written, and each has been handed over all the same.
	assert.Equal(t, []string{"first", "second"}, replay(t, b, topic, time.Time{}))

	// Writing them hands none over again.
	write(t, b, 2, nil)
	none(t, got, "an event was delivered twice")
	none(t, lost, "the feed reported a break")
}

// The trace context of the publishing request reaches the Subscribe handler
// through the message's headers, alongside any the options set.
func subscribeCarriesTheTraceContext(t *testing.T, h Harness) {
	b := h.New(t)
	got := make(chan context.Context, 4)
	require.NoError(t, b.Subscribe(t.Context(), func(m *mq.Message) { got <- m.Ctx }, func() {}))

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36},
		SpanID:     trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7},
		TraceFlags: trace.FlagsSampled,
	})
	pubCtx := trace.ContextWithSpanContext(ctx(t), sc)
	require.NoError(t, b.Publish(pubCtx, mq.Topic{Tenant: Acme, Table: "traced"}, []byte("x"), mq.WithHeader("X-Test", "1")))

	select {
	case c := <-got:
		have := trace.SpanContextFromContext(c)
		assert.Equal(t, sc.TraceID(), have.TraceID())
		assert.Equal(t, sc.SpanID(), have.SpanID())
		assert.True(t, have.IsRemote())
	case <-time.After(wait):
		t.Fatal("the subscriber was never called")
	}
}

// A message the worker's consumer delivers carries the trace context its
// publisher's headers held, on CreateConsumer's ctx, so the worker's flush
// can link back to the request that published each row.
func consumeCarriesTheTraceContext(t *testing.T, h Harness) {
	b := h.New(t)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x37},
		SpanID:     trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb8},
		TraceFlags: trace.FlagsSampled,
	})
	pubCtx := trace.ContextWithSpanContext(ctx(t), sc)
	require.NoError(t, b.Publish(pubCtx, mq.Topic{Tenant: Acme, Table: "traced"}, []byte("x")))

	c := context.WithValue(ctx(t), ctxKey{}, "worker")
	got, _, _ := consume(c, t, b, mq.ConsumerConfig{MaxAckPending: 100}, ackEach(t))
	d := next(t, got, 1)[0]
	have := trace.SpanContextFromContext(d.msg.Ctx)
	assert.Equal(t, sc.TraceID(), have.TraceID())
	assert.Equal(t, sc.SpanID(), have.SpanID())
	assert.True(t, have.IsRemote())
	assert.Equal(t, "worker", d.msg.Ctx.Value(ctxKey{}), "the trace context rides CreateConsumer's ctx, not another")
}

// Every tenant's events reach one Subscribe, whichever tenant published them.
func subscribeSeesEveryTenant(t *testing.T, h Harness) {
	b := h.New(t)
	got := make(chan mq.Topic, 8)
	require.NoError(t, b.Subscribe(t.Context(), func(m *mq.Message) { got <- m.Topic() }, func() {}))
	want := []mq.Topic{{Tenant: Acme, Table: "t"}, {Tenant: Globex, Table: "t"}}
	for _, topic := range want {
		publish(t, b, topic, "x")
	}
	var have []mq.Topic
	timeout := time.After(wait)
	for len(have) < len(want) {
		select {
		case topic := <-got:
			have = append(have, topic)
		case <-timeout:
			t.Fatalf("timed out; delivered %+v", have)
		}
	}
	assert.ElementsMatch(t, want, have)
}

// Each tenant's events arrive in the order they were published, however the
// tenants interleave.
func eachTenantInOrder(t *testing.T, h Harness) {
	b := h.New(t)
	const n = 5
	for i := range n {
		publish(t, b, mq.Topic{Tenant: Acme, Table: "a"}, fmt.Sprint(i))
		publish(t, b, mq.Topic{Tenant: Globex, Table: "b"}, fmt.Sprint(i))
	}
	got, _, _ := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, ackEach(t))
	order := map[tenant.ID][]string{}
	for _, d := range next(t, got, 2*n) {
		order[d.topic.Tenant] = append(order[d.topic.Tenant], d.data)
	}
	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprint(i)
	}
	assert.Equal(t, want, order[Acme])
	assert.Equal(t, want, order[Globex])
}

// A Nak'd message comes back; a DoubleAck is confirmed.
func nakRedelivers(t *testing.T, h Harness) {
	b := h.New(t)
	publish(t, b, mq.Topic{Tenant: Acme, Table: "n"}, "x")
	seen := 0
	got, _, _ := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, func(m *mq.Message) {
		seen++ // one tenant: one delivery goroutine
		if seen == 1 {
			assert.NoError(t, m.Nak())
			return
		}
		assert.NoError(t, m.DoubleAck(m.Ctx))
	})
	d := next(t, got, 2)
	assert.Equal(t, "x", d[0].data)
	assert.Equal(t, "x", d[1].data)
}

// A message not acked within the consumer's AckWait is delivered again; one
// that was acked is not.
func ackWaitRedelivers(t *testing.T, h Harness) {
	b := h.New(t)
	publish(t, b, mq.Topic{Tenant: Acme, Table: "w"}, "acked")
	publish(t, b, mq.Topic{Tenant: Acme, Table: "w"}, "left")
	// Long enough that a DoubleAck under load lands inside it.
	got, _, _ := consume(ctx(t), t, b, mq.ConsumerConfig{AckWait: 500 * time.Millisecond, MaxAckPending: 100}, func(m *mq.Message) {
		if string(m.Data) == "acked" {
			assert.NoError(t, m.DoubleAck(m.Ctx))
		}
	})
	seen := map[string]int{}
	for seen["left"] < 2 {
		seen[next(t, got, 1)[0].data]++
	}
	assert.Equal(t, 1, seen["acked"], "an acked message is not redelivered")
}

// DeadLetter parks a delivered message under its own topic and leaves the
// original unacked: a Nak after parking still brings it back.
func deadLetterKeepsTheTopicAndDoesNotAck(t *testing.T, h Harness) {
	b := h.New(t)
	publish(t, b, mq.Topic{Tenant: Acme, Table: "t", Scope: "s"}, "x")
	seen := 0
	got, _, _ := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, func(m *mq.Message) {
		seen++
		if seen == 1 {
			assert.NoError(t, b.DeadLetter(m.Ctx, m, mq.WithHeader("X-Error", "boom")))
			assert.NoError(t, m.Nak())
			return
		}
		assert.NoError(t, m.DoubleAck(m.Ctx))
	})
	next(t, got, 2)

	counts, err := b.DeadLetterCounts(ctx(t), Acme, "")
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"t": 1}, counts.Tables, "a scoped topic counts under its table")
	assert.Equal(t, uint64(1), counts.Total)
}

// Counts are per tenant and per table, every scope of a table under the table
// itself (so a dotted table name never shares a count with a table + scope
// pair), a table filter narrows Tables but not Total, and a tenant with
// nothing parked has zero counts.
func deadLetterCounts(t *testing.T, h Harness) {
	b := h.New(t)
	c := ctx(t)

	empty, err := b.DeadLetterCounts(c, Globex, "")
	require.NoError(t, err, "a tenant with a budget and nothing parked")
	assert.Equal(t, map[string]uint64{}, empty.Tables, "empty, not nil: the ops API encodes it as {}")
	assert.Zero(t, empty.Total)

	park := func(topic mq.Topic, n int) {
		for range n {
			require.NoError(t, b.DeadLetter(c, mq.NewMessage(c, topic, []byte("x"), time.Now(), nil, nil, nil)))
		}
	}
	park(mq.Topic{Tenant: Acme, Table: "t1"}, 2)
	park(mq.Topic{Tenant: Acme, Table: "t2"}, 1)
	park(mq.Topic{Tenant: Acme, Table: "t1", Scope: "s"}, 1)
	park(mq.Topic{Tenant: Acme, Table: "t1.s"}, 1)
	park(mq.Topic{Tenant: Globex, Table: "t1"}, 1)

	tests := []struct {
		name   string
		id     tenant.ID
		table  string
		tables map[string]uint64
		total  uint64
	}{
		{"every table", Acme, "", map[string]uint64{"t1": 3, "t2": 1, "t1.s": 1}, 5},
		{"one table, all of its scopes", Acme, "t1", map[string]uint64{"t1": 3}, 5},
		{"a dotted table", Acme, "t1.s", map[string]uint64{"t1.s": 1}, 5},
		{"a table with nothing parked", Acme, "none", map[string]uint64{}, 5},
		{"the other tenant", Globex, "", map[string]uint64{"t1": 1}, 1},
	}
	for _, tt := range tests {
		counts, err := b.DeadLetterCounts(c, tt.id, tt.table)
		require.NoError(t, err, tt.name)
		assert.Equal(t, tt.tables, counts.Tables, tt.name)
		assert.Equal(t, tt.total, counts.Total, tt.name)
	}

	unbudgeted, err := b.DeadLetterCounts(c, "initech", "")
	if h.Caps.UnbudgetedNotFound {
		require.ErrorIs(t, err, mq.ErrNoDeadLetterQueue)
		return
	}
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{}, unbudgeted.Tables)
	assert.Zero(t, unbudgeted.Total)
}

// A replay sends one topic's events in order from since on, and nothing of
// another table, scope or tenant.
func replaySince(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "r"}
	publish(t, b, topic, "one")
	time.Sleep(retryPause)
	since := time.Now()
	time.Sleep(retryPause)
	publish(t, b, topic, "two")
	publish(t, b, topic, "three")
	publish(t, b, mq.Topic{Tenant: Acme, Table: "r2"}, "other table")
	publish(t, b, mq.Topic{Tenant: Acme, Table: "r", Scope: "s"}, "scoped")
	publish(t, b, mq.Topic{Tenant: Globex, Table: "r"}, "other tenant")

	tests := []struct {
		name  string
		topic mq.Topic
		since time.Time
		want  []string
	}{
		{"since", topic, since, []string{"two", "three"}},
		{"everything", topic, time.Time{}, []string{"one", "two", "three"}},
		{"future", topic, time.Now().Add(time.Hour), []string{}},
		{"scoped", mq.Topic{Tenant: Acme, Table: "r", Scope: "s"}, time.Time{}, []string{"scoped"}},
		{"other tenant", mq.Topic{Tenant: Globex, Table: "r"}, time.Time{}, []string{"other tenant"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, replay(t, b, tt.topic, tt.since))
		})
	}
}

// A replay sends what the ingest queue accepted since a time and the broker
// still holds, wherever it is: the events already written, which the worker
// recorded before it acked them off the queue, then the ones still waiting to
// be written. An event is in one or the other, whether it was accepted before
// or after another topic's, a second ago or an hour.
func replayServesWrittenAndWaitingEvents(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "r"}
	other := mq.Topic{Tenant: Acme, Table: "other"}
	publish(t, b, topic, "a")
	time.Sleep(retryPause)
	since := time.Now()
	time.Sleep(retryPause)
	for _, data := range []string{"b", "c", "d"} {
		publish(t, b, topic, data)
	}
	publish(t, b, other, "x")

	// The worker writes a and b, and the other table's row; ClickHouse cannot
	// take c and d yet.
	write(t, b, 5, func(data string) bool { return data == "a" || data == "b" || data == "x" })

	assert.Equal(t, []string{"a", "b", "c", "d"}, replay(t, b, topic, time.Time{}), "written, then waiting")
	assert.Equal(t, []string{"b", "c", "d"}, replay(t, b, topic, since), "by when the queue accepted each, written or not")
	assert.Equal(t, []string{"x"}, replay(t, b, other, time.Time{}))
	assert.Equal(t, []string{}, replay(t, b, topic, time.Now().Add(time.Hour)))

	// send returning false ends it, wherever it is reading.
	for stopAt := 1; stopAt <= 4; stopAt++ {
		var got []string
		require.NoError(t, b.ReplaySince(ctx(t), topic, time.Time{}, func(data []byte) bool {
			got = append(got, string(data))
			return len(got) < stopAt
		}))
		assert.Equal(t, []string{"a", "b", "c", "d"}[:stopAt], got)
	}
}

// A replay sends the written events in the order they were written, which is
// the worker's and need not be the queue's, and picks each by when the queue
// accepted it, not by when it was written.
func replayOrdersWrittenEventsByTheirWrite(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "o"}
	publish(t, b, topic, "a")
	time.Sleep(retryPause)
	since := time.Now()
	time.Sleep(retryPause)
	publish(t, b, topic, "b")
	publish(t, b, topic, "c")

	got, stop, _ := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, nil)
	held := map[string]*mq.Message{}
	for _, d := range next(t, got, 3) {
		held[d.data] = d.msg
	}
	for _, data := range []string{"c", "a", "b"} {
		m := held[data]
		require.NoError(t, b.Record(m.Ctx, []*mq.Message{m}))
		require.NoError(t, m.DoubleAck(m.Ctx))
	}
	stop()

	assert.Equal(t, []string{"c", "a", "b"}, replay(t, b, topic, time.Time{}), "in the order written")
	assert.Equal(t, []string{"c", "b"}, replay(t, b, topic, since), "a was written after since, and accepted before it")
}

// The replay window bounds how long a written event is kept, counted from
// its write, and nothing else: an event still waiting to be written is
// replayed whatever its age (#1007). With a window of a second the written
// event is gone once it has passed, and the waiting one, accepted before it,
// is still sent.
func aWaitingEventOutlivesTheReplayWindow(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "w"}
	publish(t, b, topic, "waiting")
	publish(t, b, topic, "written")
	write(t, b, 2, func(data string) bool { return data == "written" })
	require.Equal(t, []string{"written", "waiting"}, replay(t, b, topic, time.Time{}))

	h.Window(t, b, Acme, time.Second)
	var got []string
	for deadline := time.Now().Add(wait); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if got = replay(t, b, topic, time.Time{}); len(got) == 1 {
			break
		}
	}
	assert.Equal(t, []string{"waiting"}, got)
}

// An event written while a replay runs is not missed: the worker records it
// before the queue lets it go, and the replay reads what was recorded
// meanwhile last. More events than one read of the queue returns, all of them
// written once the replay has sent its first: every one is sent, some twice.
func replayCoversEventsWrittenMeanwhile(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "moving"}
	const n = 300 // one read of the queue returns 256
	for i := range n {
		publish(t, b, topic, fmt.Sprint(i))
	}

	seen := map[string]int{}
	require.NoError(t, b.ReplaySince(ctx(t), topic, time.Time{}, func(data []byte) bool {
		if len(seen) == 0 {
			write(t, b, n, nil)
		}
		seen[string(data)]++
		return true
	}))
	for i := range n {
		assert.Positive(t, seen[fmt.Sprint(i)], "event %d was not replayed", i)
	}
	assert.Equal(t, n, len(seen))
}

// A done ctx ends a replay before the next event, with ctx's error.
func replaySinceStopsWhenContextIsDone(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "r"}
	publish(t, b, topic, "one")
	publish(t, b, topic, "two")

	c, cancel := context.WithCancel(ctx(t))
	defer cancel()
	var got []string
	err := b.ReplaySince(c, topic, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		cancel()
		return true
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"one"}, got)
}

// A replay that loses the broker before catching up says so, rather than
// passing for a caught-up one.
func replaySincePullFailureIsAnError(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "r"}
	publish(t, b, topic, "one")
	publish(t, b, topic, "two")

	var got []string
	err := b.ReplaySince(ctx(t), topic, time.Time{}, func(data []byte) bool {
		got = append(got, string(data))
		assert.NoError(t, b.Close())
		return true
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), "not a ctx error: %v", err)
	assert.Equal(t, []string{"one"}, got)
}

// Delivery ended underneath a running Consume is reported on failed exactly
// once.
func failedOnceWhenDeliveryEnds(t *testing.T, h Harness) {
	b := h.New(t)
	got, _, failed := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, nil)
	// A delivery on each tenant proves the pulls are live before delivery is
	// ended underneath them.
	publish(t, b, mq.Topic{Tenant: Acme, Table: "t"}, "x")
	publish(t, b, mq.Topic{Tenant: Globex, Table: "t"}, "x")
	next(t, got, 2)
	h.EndDelivery(t, b)
	select {
	case err := <-failed:
		require.ErrorIs(t, err, mq.ErrDeliveryEnded)
	case <-time.After(wait):
		t.Fatal("delivery ended underneath the consumer and nothing was reported")
	}
	none(t, failed, "a second failure was reported")
}

// A delivery the caller stopped is not a failure, even if delivery would
// have ended afterwards.
func failedNeverAfterStop(t *testing.T, h Harness) {
	b := h.New(t)
	_, stop, failed := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, nil)
	stop()
	h.EndDelivery(t, b)
	none(t, failed, "a stopped consumer reported a failure")
}

func maxBytesReportsTheBudget(t *testing.T, h Harness) {
	b := h.New(t)
	for _, n := range []int64{32 << 20, 48 << 20} {
		require.NoError(t, b.SetMaxBytes(ctx(t), Acme, n))
		assert.Equal(t, n, b.MaxBytes(Acme))
	}
}

func stats(t *testing.T, h Harness) {
	b := h.New(t)
	s, err := b.Stats()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, s.Connections, int64(1), "the broker's own connection")
}

// A full queue refuses the publish with mq.ErrQueueFull, which is the
// backpressure the ingest handler answers 503 for: for the whole queue, or,
// where each tenant has a budget of its own, for that tenant alone. A refused
// event was not accepted, so no live reader is handed it.
func queueFull(t *testing.T, h Harness) {
	b := h.New(t)
	h.Fill(t, b, Acme)
	live := make(chan delivery, 4)
	require.NoError(t, b.Subscribe(t.Context(), func(m *mq.Message) {
		live <- delivery{topic: m.Topic(), data: string(m.Data)}
	}, func() {}))

	err := b.Publish(ctx(t), mq.Topic{Tenant: Acme, Table: "full"}, []byte("x"))
	require.ErrorIs(t, err, mq.ErrQueueFull)
	if h.Caps.PerTenantBudget {
		other := mq.Topic{Tenant: Globex, Table: "full"}
		publish(t, b, other, "x")
		assert.Equal(t, other, next(t, live, 1)[0].topic)
	}
	none(t, live, "a refused event reached the live feed")
}

// An event left unacked holds back only itself (#653): the ones accepted
// after it are delivered all the same, and each leaves the queue at its own
// ack, so the unacked one is all the queue still holds.
func anUnackedEventHoldsBackOnlyItself(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "p"}
	for _, data := range []string{"stuck", "a", "b", "c"} {
		publish(t, b, topic, data)
	}
	got, _, _ := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, func(m *mq.Message) {
		if string(m.Data) != "stuck" {
			assert.NoError(t, m.DoubleAck(m.Ctx))
		}
	})
	next(t, got, 4)

	assert.Equal(t, []string{"stuck"}, replay(t, b, topic, time.Time{}), "an acked event nothing recorded is gone with its ack")
}
