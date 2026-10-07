//go:build integration

package natsspike

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin how a shard durable with the pinned_client
// priority policy behaves, which the shard ownership rests on: one puller at
// a time receives, a pin lapses on its own once its holder stops pulling,
// UNPIN hands it over at once, and RESET redelivers a dead holder's unacked
// rows without replaying acked ones — but also the ones the caller holds.

const group = "g"

// pinnedShard is one work-queue partition with one pinned-client durable,
// "s0", on its only shard.
func pinnedShard(t *testing.T, ttl, ackWait time.Duration) (*natsserver.Server, jetstream.JetStream) {
	t.Helper()
	s := server(t)
	js := connect(t, s)
	_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "IN", Subjects: []string{"wh.ingest.>"}, Retention: jetstream.WorkQueuePolicy})
	require.NoError(t, err)
	_, err = js.CreateConsumer(t.Context(), "IN", jetstream.ConsumerConfig{
		Durable: "s0", AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait, MaxDeliver: -1,
		FilterSubject:  "wh.ingest.0.0.>",
		PriorityPolicy: jetstream.PriorityPolicyPinned, PinnedTTL: ttl, PriorityGroups: []string{group},
	})
	require.NoError(t, err)
	return s, js
}

type delivery struct {
	worker string
	seq    uint64
	pin    string
	at     time.Time
}

type recorder struct {
	mu  sync.Mutex
	all []delivery
}

func (r *recorder) add(d delivery) { r.mu.Lock(); r.all = append(r.all, d); r.mu.Unlock() }

func (r *recorder) since(from time.Time) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := map[string]int{}
	for _, d := range r.all {
		if !d.at.Before(from) {
			m[d.worker]++
		}
	}
	return m
}

func (r *recorder) of(worker string) []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []delivery
	for _, d := range r.all {
		if d.worker == worker {
			out = append(out, d)
		}
	}
	return out
}

// puller is a process pulling s0 in the group on a connection of its own.
type puller struct {
	nc   *nats.Conn
	cc   jetstream.ConsumeContext
	hold atomic.Bool // leave rows unacked
}

func pull(t *testing.T, s *natsserver.Server, name string, rec *recorder, expiry time.Duration) *puller {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), nats.Name(name))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	c, err := js.Consumer(t.Context(), "IN", "s0")
	require.NoError(t, err)
	p := &puller{nc: nc}
	p.cc, err = c.Consume(func(m jetstream.Msg) {
		md, _ := m.Metadata()
		rec.add(delivery{worker: name, seq: md.Sequence.Stream, pin: m.Headers().Get("Nats-Pin-Id"), at: time.Now()})
		if !p.hold.Load() {
			_ = m.Ack()
		}
	}, jetstream.PullPriorityGroup(group), jetstream.PullExpiry(expiry),
		jetstream.ConsumeErrHandler(func(jetstream.ConsumeContext, error) {}))
	require.NoError(t, err)
	return p
}

func publishEvery(t *testing.T, js jetstream.JetStream, every time.Duration) (stop func()) {
	t.Helper()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tk := time.NewTicker(every)
		defer tk.Stop()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			case <-tk.C:
				_, _ = js.Publish(context.Background(), "wh.ingest.0.0.acme.t1", fmt.Appendf(nil, "%d", i))
			}
		}
	})
	return func() { close(done); wg.Wait() }
}

func pinned(t *testing.T, js jetstream.JetStream) string {
	t.Helper()
	c, err := js.Consumer(t.Context(), "IN", "s0")
	require.NoError(t, err)
	for _, g := range c.CachedInfo().PriorityGroups {
		if g.Group == group {
			return g.PinnedClientID
		}
	}
	return ""
}

// Two processes pull one shard; only the first one pinned receives. The pin
// id the server reports is the one its rows carry.
func TestS2_PinnedOneReceiverAtATime(t *testing.T) {
	t.Parallel()
	s, js := pinnedShard(t, 10*time.Second, time.Minute)
	rec := &recorder{}
	pull(t, s, "A", rec, time.Second)
	time.Sleep(100 * time.Millisecond) // A's pull arrives first
	pull(t, s, "B", rec, time.Second)
	stop := publishEvery(t, js, 10*time.Millisecond)
	time.Sleep(time.Second)
	stop()
	time.Sleep(100 * time.Millisecond)
	got := rec.since(time.Time{})
	assert.Positive(t, got["A"])
	assert.Zero(t, got["B"])
	a := rec.of("A")
	require.NotEmpty(t, a)
	assert.NotEmpty(t, a[0].pin, "a pinned delivery carries the pin id")
	assert.Equal(t, a[0].pin, pinned(t, js), "the consumer info names the same pin")
}

// A holder whose connection dies stops pulling, and its pin lapses within
// the TTL of its last pull; the other puller receives from then on. Rows the
// dead holder held unacked come back only after ack_wait.
func TestS2_PinLapsesWithinTTLOfADeadHolder(t *testing.T) {
	t.Parallel()
	const ttl, ackWait = 2 * time.Second, 5 * time.Second
	s, js := pinnedShard(t, ttl, ackWait)
	rec := &recorder{}
	a := pull(t, s, "A", rec, time.Second)
	time.Sleep(100 * time.Millisecond)
	pull(t, s, "B", rec, time.Second)
	stop := publishEvery(t, js, 20*time.Millisecond)
	defer stop()
	time.Sleep(500 * time.Millisecond)
	a.hold.Store(true)
	time.Sleep(300 * time.Millisecond)
	a.nc.Close()
	killed := time.Now()
	require.Eventually(t, func() bool { return rec.since(killed)["B"] > 0 }, ttl+2*time.Second, 20*time.Millisecond)
	first := rec.of("B")[0].at.Sub(killed)
	t.Logf("B's first row %s after A died (pinned ttl %s)", first.Round(10*time.Millisecond), ttl)
	assert.Less(t, first, ttl+time.Second)
}

// UNPIN by the holder, after it stopped pulling, hands the shard to the next
// puller at once, well before the TTL.
func TestS2_UnpinHandsOverAtOnce(t *testing.T) {
	t.Parallel()
	const ttl = 30 * time.Second
	s, js := pinnedShard(t, ttl, time.Minute)
	rec := &recorder{}
	a := pull(t, s, "A", rec, time.Second)
	time.Sleep(100 * time.Millisecond)
	pull(t, s, "B", rec, time.Second)
	stop := publishEvery(t, js, 20*time.Millisecond)
	defer stop()
	time.Sleep(500 * time.Millisecond)
	a.cc.Drain()
	<-a.cc.Closed()
	st, err := js.Stream(t.Context(), "IN")
	require.NoError(t, err)
	unpinned := time.Now()
	require.NoError(t, st.UnpinConsumer(t.Context(), "s0", group))
	require.Eventually(t, func() bool { return rec.since(unpinned)["B"] > 0 }, 5*time.Second, 10*time.Millisecond)
	took := rec.of("B")[0].at.Sub(unpinned)
	t.Logf("B's first row %s after the unpin (pinned ttl %s)", took.Round(time.Millisecond), ttl)
	assert.Less(t, took, 2*time.Second)
	assert.Zero(t, rec.since(time.Time{})["B"]-rec.since(unpinned)["B"], "nothing reached B before the unpin")
}

// A holder that stops pulling past the TTL is fenced: once another puller
// holds the pin, the old holder gets nothing though it pulls again.
func TestS2_StalledHolderIsFenced(t *testing.T) {
	t.Parallel()
	s, js := pinnedShard(t, 2*time.Second, time.Minute)
	rec := &recorder{}
	a := pull(t, s, "A", rec, time.Second)
	time.Sleep(100 * time.Millisecond)
	pull(t, s, "B", rec, time.Second)
	stop := publishEvery(t, js, 20*time.Millisecond)
	defer stop()
	time.Sleep(500 * time.Millisecond)
	a.cc.Stop()
	<-a.cc.Closed()
	time.Sleep(3 * time.Second) // past the TTL: B holds the pin
	resumed := time.Now()
	pull(t, s, "A", rec, time.Second) // the same process pulls again
	time.Sleep(2 * time.Second)
	got := rec.since(resumed)
	assert.Positive(t, got["B"])
	assert.Zero(t, got["A"], "the stalled holder is fenced")
}

// A live idle holder keeps its pin only while its pull expiry is under the
// TTL: the server renews a pin on a new pull, not on acks or heartbeats.
func TestS2_ExpiryOverTTLLosesAnIdlePin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		expiry time.Duration
		flaps  bool
	}{{time.Second, false}, {10 * time.Second, true}} {
		t.Run(tc.expiry.String(), func(t *testing.T) {
			t.Parallel()
			s, js := pinnedShard(t, 2*time.Second, time.Minute)
			rec := &recorder{}
			pull(t, s, "A", rec, tc.expiry)
			time.Sleep(100 * time.Millisecond)
			pull(t, s, "B", rec, tc.expiry)
			var owners []string
			for range 3 {
				from := time.Now()
				_, err := js.Publish(t.Context(), "wh.ingest.0.0.acme.t1", []byte("x"))
				require.NoError(t, err)
				require.Eventually(t, func() bool { return len(rec.since(from)) == 1 }, 5*time.Second, 10*time.Millisecond)
				for w := range rec.since(from) {
					owners = append(owners, w)
				}
				time.Sleep(3 * time.Second) // idle past the TTL
			}
			t.Logf("expiry %s: owner of each row after an idle TTL: %v", tc.expiry, owners)
			if tc.flaps {
				assert.NotEqual(t, []string{"A", "A", "A"}, owners, "the pin moves while its holder is idle")
			} else {
				assert.Equal(t, []string{"A", "A", "A"}, owners)
			}
		})
	}
}

// RESET to the ack floor redelivers a dead holder's unacked rows at once and
// replays none of its acked ones.
func TestS2_ResetRedeliversOnlyTheUnacked(t *testing.T) {
	t.Parallel()
	s, js := pinnedShard(t, 2*time.Second, time.Minute)
	rec := &recorder{}
	a := pull(t, s, "A", rec, time.Second)
	for range 10 {
		_, err := js.Publish(t.Context(), "wh.ingest.0.0.acme.t1", []byte("acked"))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return len(rec.of("A")) == 10 }, 5*time.Second, 10*time.Millisecond)
	a.hold.Store(true)
	for range 5 {
		_, err := js.Publish(t.Context(), "wh.ingest.0.0.acme.t1", []byte("held"))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return len(rec.of("A")) == 15 }, 5*time.Second, 10*time.Millisecond)
	a.nc.Close()
	require.Eventually(t, func() bool { return pinned(t, js) == "" }, 5*time.Second, 50*time.Millisecond, "the dead holder's pin lapses")

	// Reset first, then pull: the new holder receives the five at once.
	_, err := js.ResetConsumer(t.Context(), "IN", "s0")
	require.NoError(t, err)
	reset := time.Now()
	pull(t, s, "B", rec, time.Second)
	require.Eventually(t, func() bool { return len(rec.of("B")) == 5 }, 2*time.Second, 10*time.Millisecond, "well before ack_wait (1m)")
	time.Sleep(300 * time.Millisecond)
	held := map[uint64]bool{}
	for _, d := range rec.of("A")[10:] {
		held[d.seq] = true
	}
	for _, d := range rec.of("B") {
		assert.True(t, held[d.seq], "row %d was acked by A and replayed", d.seq)
	}
	t.Logf("5 held rows redelivered within %s of the reset", rec.of("B")[4].at.Sub(reset).Round(time.Millisecond))
}

// A reset also redelivers what the caller itself holds unacked, and leaves
// the pin in place: why WaveHouse resets a shard before it starts pulling,
// never while it holds rows of it.
func TestS2_ResetRedeliversTheCallersOwnRows(t *testing.T) {
	t.Parallel()
	s, js := pinnedShard(t, 10*time.Second, time.Minute)
	rec := &recorder{}
	b := pull(t, s, "B", rec, time.Second)
	b.hold.Store(true)
	for range 3 {
		_, err := js.Publish(t.Context(), "wh.ingest.0.0.acme.t1", []byte("x"))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return len(rec.of("B")) == 3 }, 5*time.Second, 10*time.Millisecond)
	pin := pinned(t, js)
	require.NotEmpty(t, pin)
	_, err := js.ResetConsumer(t.Context(), "IN", "s0")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rec.of("B")) == 6 }, 5*time.Second, 10*time.Millisecond, "B receives its own three again")
	assert.Equal(t, pin, pinned(t, js), "the reset leaves the pin")
}

// A NATS permission wildcard is a whole token: wh-ingest-* names no durable,
// so the per-shard durables are listed one by one.
func TestS2_PartialTokenPermissionIsLiteral(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	conf := filepath.Join(dir, "nats.conf")
	require.NoError(t, os.WriteFile(conf, fmt.Appendf(nil, `
jetstream { store_dir: %q }
accounts { A { jetstream: enabled, users: [
  {user: admin, password: a},
  {user: partial, password: p, permissions: {publish: {allow: ["$JS.API.INFO", "$JS.API.CONSUMER.INFO.>", "$JS.API.CONSUMER.MSG.NEXT.IN.wh-ingest-*"]}, subscribe: {allow: ["_INBOX.>"]}}},
  {user: exact, password: e, permissions: {publish: {allow: ["$JS.API.INFO", "$JS.API.CONSUMER.INFO.>", "$JS.API.CONSUMER.MSG.NEXT.*.wh-ingest-0"]}, subscribe: {allow: ["_INBOX.>"]}}}
]}}
`, filepath.Join(dir, "js")), 0o600))
	opts, err := natsserver.ProcessConfigFile(conf)
	require.NoError(t, err)
	opts.Host, opts.Port, opts.NoSigs, opts.NoLog = "127.0.0.1", -1, true, true
	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	s.Start()
	t.Cleanup(func() { shutdown(t, s, dir) })
	require.True(t, s.ReadyForConnections(10*time.Second))

	as := func(user, pw string) jetstream.JetStream {
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(user, pw), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
		require.NoError(t, err)
		t.Cleanup(nc.Close)
		js, err := jetstream.New(nc)
		require.NoError(t, err)
		return js
	}
	admin := as("admin", "a")
	_, err = admin.CreateStream(t.Context(), jetstream.StreamConfig{Name: "IN", Subjects: []string{"wh.ingest.>"}, Retention: jetstream.WorkQueuePolicy})
	require.NoError(t, err)
	_, err = admin.CreateConsumer(t.Context(), "IN", jetstream.ConsumerConfig{Durable: "wh-ingest-0", AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	_, err = admin.Publish(t.Context(), "wh.ingest.0.0.acme.t1", []byte("x"))
	require.NoError(t, err)

	fetch := func(js jetstream.JetStream) int {
		c, err := js.Consumer(t.Context(), "IN", "wh-ingest-0")
		require.NoError(t, err)
		b, err := c.Fetch(1, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			return 0
		}
		n := 0
		for m := range b.Messages() {
			n++
			_ = m.Nak()
		}
		return n
	}
	assert.Zero(t, fetch(as("partial", "p")), "wh-ingest-* does not match wh-ingest-0")
	assert.Equal(t, 1, fetch(as("exact", "e")))
}
