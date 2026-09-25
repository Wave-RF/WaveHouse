package mqtest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
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

// replayEventually waits for a replay of topic since to be want: a backend
// may serve replays from a store that trails the ingest queue.
func replayEventually(t *testing.T, b mq.Broker, topic mq.Topic, since time.Time, want []string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		got := replay(t, b, topic, since)
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			assert.Equal(t, want, got, "replay of %+v since %v", topic, since)
			return
		}
		time.Sleep(retryPause)
	}
}

// replayReaches waits until a replay of topic from the start holds at least
// n events: that they are stored where the backend replays from. It stops the
// replay at n, so it never waits out a caught-up.
func replayReaches(t *testing.T, b mq.Broker, topic mq.Topic, n int) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		got := 0
		require.NoError(t, b.ReplaySince(ctx(t), topic, time.Time{}, func([]byte) bool {
			got++
			return got < n
		}))
		if got >= n {
			return
		}
		require.False(t, time.Now().After(deadline), "a replay of %+v never reached %d events", topic, n)
		time.Sleep(retryPause)
	}
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

// The trace context of the publishing request reaches the Subscribe handler
// through the message's headers, alongside any the options set.
func subscribeCarriesTheTraceContext(t *testing.T, h Harness) {
	b := h.New(t)
	got := make(chan context.Context, 4)
	require.NoError(t, b.Subscribe(t.Context(), "hub-bridge", func(m *mq.Message) error {
		got <- m.Ctx
		return nil
	}))

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

// Every tenant's events reach one Subscribe, whichever tenant published them.
func subscribeSeesEveryTenant(t *testing.T, h Harness) {
	b := h.New(t)
	got := make(chan mq.Topic, 8)
	require.NoError(t, b.Subscribe(t.Context(), "hub-bridge", func(m *mq.Message) error {
		got <- m.Topic()
		return nil
	}))
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
	got, _, _ := consume(ctx(t), t, b, mq.ConsumerConfig{AckWait: 200 * time.Millisecond, MaxAckPending: 100}, func(m *mq.Message) {
		if string(m.Data) == "acked" {
			assert.NoError(t, m.DoubleAck(m.Ctx))
		}
	})
	var data []string
	for _, d := range next(t, got, 3) {
		data = append(data, d.data)
	}
	assert.Equal(t, []string{"acked", "left", "left"}, data)
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
	assert.Equal(t, map[string]uint64{"t.s": 1}, counts.Tables, "a scoped topic counts under table.scope")
	assert.Equal(t, uint64(1), counts.Total)
}

// Counts are per tenant and per table, a table filter narrows Tables but not
// Total, and a tenant with nothing parked has zero counts.
func deadLetterCounts(t *testing.T, h Harness) {
	b := h.New(t)
	c := ctx(t)

	empty, err := b.DeadLetterCounts(c, Globex, "")
	require.NoError(t, err, "a tenant with a budget and nothing parked")
	assert.Empty(t, empty.Tables)
	assert.Zero(t, empty.Total)

	park := func(topic mq.Topic, n int) {
		for range n {
			require.NoError(t, b.DeadLetter(c, mq.NewMessage(c, topic, []byte("x"), time.Now(), nil, nil, nil)))
		}
	}
	park(mq.Topic{Tenant: Acme, Table: "t1"}, 2)
	park(mq.Topic{Tenant: Acme, Table: "t2"}, 1)
	park(mq.Topic{Tenant: Acme, Table: "t1", Scope: "s"}, 1)
	park(mq.Topic{Tenant: Acme, Table: "odd.name"}, 1)
	park(mq.Topic{Tenant: Globex, Table: "t1"}, 1)

	tests := []struct {
		name   string
		id     tenant.ID
		table  string
		tables map[string]uint64
		total  uint64
	}{
		{"every table", Acme, "", map[string]uint64{"t1": 2, "t2": 1, "t1.s": 1, "odd.name": 1}, 5},
		{"one table", Acme, "t1", map[string]uint64{"t1": 2}, 5},
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
	assert.Zero(t, unbudgeted.Total)
}

// A replay sends one topic's events in order from since on, and nothing of
// another table, scope or tenant.
func replaySince(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "r"}
	publish(t, b, topic, "one")
	// Waiting until "one" replays puts it before since in whatever store the
	// backend replays from.
	replayReaches(t, b, topic, 1)
	since := time.Now()
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
			replayEventually(t, b, tt.topic, tt.since, tt.want)
		})
	}
}

// A done ctx ends a replay before the next event, with ctx's error.
func replaySinceStopsWhenContextIsDone(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "r"}
	publish(t, b, topic, "one")
	publish(t, b, topic, "two")
	replayReaches(t, b, topic, 2)

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
	replayReaches(t, b, topic, 2)

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

// A full queue refuses with ErrQueueFull; with per-tenant budgets, only its
// own tenant.
func queueFull(t *testing.T, h Harness) {
	b := h.New(t)
	h.Fill(t, b, Acme)
	err := b.Publish(ctx(t), mq.Topic{Tenant: Acme, Table: "full"}, []byte("x"))
	require.ErrorIs(t, err, mq.ErrQueueFull)
	if h.Caps.PerTenantBudget {
		publish(t, b, mq.Topic{Tenant: Globex, Table: "full"}, "x")
	}
}

// PurgeAcked never removes an unacked event. A backend that purges removes
// the acked ones past the cutoff; one that leaves retention to the operator
// reports nothing purged.
func purgeAcked(t *testing.T, h Harness) {
	b := h.New(t)
	topic := mq.Topic{Tenant: Acme, Table: "p"}
	for _, data := range []string{"a", "b", "c", "left"} {
		publish(t, b, topic, data)
	}
	got, _, _ := consume(ctx(t), t, b, mq.ConsumerConfig{MaxAckPending: 100}, func(m *mq.Message) {
		if string(m.Data) != "left" {
			assert.NoError(t, m.DoubleAck(m.Ctx))
		}
	})
	next(t, got, 4)

	future := time.Now().Add(time.Hour)
	purged, err := b.PurgeAcked(ctx(t), Durable, map[tenant.ID]time.Time{Acme: future, Globex: future})
	require.NoError(t, err)
	if h.Caps.PurgesAcked {
		assert.True(t, purged)
		replayEventually(t, b, topic, time.Time{}, []string{"left"})
		return
	}
	assert.False(t, purged)
	replayEventually(t, b, topic, time.Time{}, []string{"a", "b", "c", "left"})
}

func purgeAckedUnknownConsumer(t *testing.T, h Harness) {
	b := h.New(t)
	_, err := b.PurgeAcked(ctx(t), "no-such-consumer", nil)
	require.ErrorIs(t, err, mq.ErrConsumerNotFound)
}
