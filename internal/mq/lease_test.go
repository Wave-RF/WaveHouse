//go:build integration

package mq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/coord/coordtest"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// Shortened lease timings, in the production ratios' order: a holder steps
// down (renew deadline) before a candidate may take over (duration).
const (
	testLeaseDuration = 400 * time.Millisecond
	testRenewDeadline = 300 * time.Millisecond
	testRenewEvery    = 50 * time.Millisecond
)

func testTimings() LeaseOption {
	return WithLeaseTimings(testLeaseDuration, testRenewDeadline, testRenewEvery)
}

// leaseFixture is a server with only the shipped lease bucket on it.
func leaseFixture(t *testing.T) *natsFixture {
	t.Helper()
	f := newNATSFixture(t)
	tp := shippedTopology(t)
	tp.Streams, tp.Consumers = nil, nil
	require.NoError(t, f.create(t.Context(), tp))
	return f
}

// bucketAs opens the lease bucket as the restricted wavehouse user, on a
// connection of its own: one process's view.
func (f *natsFixture) bucketAs(t *testing.T) jetstream.KeyValue {
	t.Helper()
	kv, err := f.connect(t, "wavehouse").KeyValue(t.Context(), natstest.CoordBucket)
	require.NoError(t, err)
	return kv
}

// The shared suite, each pair two processes' coordinators connected as the
// restricted wavehouse user. Loss is the operator overwriting the key, as a
// takeover would.
func TestLeases_Conformance(t *testing.T) {
	t.Parallel()
	var cur *natsFixture
	coordtest.Conformance(t, func(t *testing.T) (a, b coord.Coordinator) {
		cur = leaseFixture(t)
		return newNATSLeases(cur.bucketAs(t), "a", testTimings()), newNATSLeases(cur.bucketAs(t), "b", testTimings())
	}, coordtest.WithLoss(func(t *testing.T, name string) {
		kv, err := cur.admin.KeyValue(t.Context(), natstest.CoordBucket)
		require.NoError(t, err)
		_, err = kv.Put(t.Context(), leaseKeyPrefix+name, []byte(`{"holder":"operator"}`))
		require.NoError(t, err)
	}), coordtest.WithWait(2*time.Second))
}

// The token is the KV revision the term was taken at, and grows across every
// kind of handover: a resign, a takeover of a quiet lease, and a resume.
func TestLeases_TokensAreMonotonicRevisions(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	a := newNATSLeases(f.bucketAs(t), "a", testTimings())
	b := newNATSLeases(f.bucketAs(t), "b", testTimings())
	t.Cleanup(func() { _ = a.Close(context.Background()); _ = b.Close(context.Background()) })
	admin, err := f.admin.KeyValue(t.Context(), natstest.CoordBucket)
	require.NoError(t, err)

	var last uint64
	check := func(term coord.Term) {
		t.Helper()
		assert.Greater(t, term.Token(), last)
		last = term.Token()
		// Renewals move the revision on; the token stays the acquisition's.
		hist, err := admin.History(t.Context(), leaseKeyPrefix+"sweeper")
		require.NoError(t, err)
		require.Contains(t, revisions(hist), term.Token())
	}
	for i := range 3 {
		c := []*natsLeases{a, b}[i%2]
		term, err := c.TryAcquire(t.Context(), "sweeper")
		require.NoError(t, err)
		check(term)
		time.Sleep(3 * testRenewEvery)
		require.NoError(t, term.Resign(t.Context()))
	}

	// A holder gone quiet: b takes over once the revision has sat still.
	_, err = admin.Put(t.Context(), leaseKeyPrefix+"sweeper", []byte(`{"holder":"gone","duration_ms":400}`))
	require.NoError(t, err)
	term := acquireEventually(t, b, "sweeper", 5*time.Second)
	check(term)
}

func revisions(entries []jetstream.KeyValueEntry) []uint64 {
	out := make([]uint64, len(entries))
	for i, e := range entries {
		out[i] = e.Revision()
	}
	return out
}

func acquireEventually(t *testing.T, c coord.Coordinator, name string, within time.Duration) coord.Term {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		term, err := c.TryAcquire(t.Context(), name)
		if err == nil {
			return term
		}
		require.ErrorIs(t, err, coord.ErrHeld)
		require.True(t, time.Now().Before(deadline), "%s never acquired", name)
		time.Sleep(testRenewEvery / 2)
	}
}

// stallableKV is a bucket whose writes can be held up, as a holder in a long
// GC pause or behind a partition would be.
type stallableKV struct {
	jetstream.KeyValue
	mu      sync.Mutex
	stalled bool
}

func (s *stallableKV) stall() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stalled = true
}

func (s *stallableKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	s.mu.Lock()
	stalled := s.stalled
	s.mu.Unlock()
	if stalled {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return s.KeyValue.Update(ctx, key, value, revision)
}

// A live holder is never replaced; a stalled one steps down at its renew
// deadline, and a candidate takes over only once it has seen the revision
// unchanged for the lease duration on its own clock.
func TestLeases_StalledHolderIsReplacedAfterTheWindow(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	akv := &stallableKV{KeyValue: f.bucketAs(t)}
	a := newNATSLeases(akv, "a", testTimings())
	b := newNATSLeases(f.bucketAs(t), "b", testTimings())
	t.Cleanup(func() { _ = a.Close(context.Background()); _ = b.Close(context.Background()) })

	held, err := a.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)

	// Renewing, a holder outlasts many lease durations of campaigning.
	until := time.Now().Add(3 * testLeaseDuration)
	for time.Now().Before(until) {
		_, err := b.TryAcquire(t.Context(), "sweeper")
		require.ErrorIs(t, err, coord.ErrHeld)
		time.Sleep(testRenewEvery / 2)
	}
	require.NoError(t, held.Err())

	akv.stall()
	stalledAt := time.Now()
	var term coord.Term
	for term == nil {
		got, err := b.TryAcquire(t.Context(), "sweeper")
		if err == nil {
			term = got
			break
		}
		require.ErrorIs(t, err, coord.ErrHeld)
		require.Less(t, time.Since(stalledAt), 5*time.Second, "never taken over")
		time.Sleep(testRenewEvery / 2)
	}
	tookOver := time.Now()

	select {
	case <-held.Done():
	default:
		t.Fatal("the stalled holder had not stepped down when the lease was taken over")
	}
	require.ErrorIs(t, held.Err(), coord.ErrLost)
	assert.Greater(t, term.Token(), held.Token())
	// The holder's last write landed at most one renewal before the stall,
	// and the window runs from b's first sighting of it.
	assert.GreaterOrEqual(t, tookOver.Sub(stalledAt), testLeaseDuration-testRenewEvery)
}

// Close resigns by deleting the key, so a candidate takes the lease at once
// rather than after the lease duration.
func TestLeases_CloseHandsOverAtOnce(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	a := newNATSLeases(f.bucketAs(t), "a", WithLeaseTimings(time.Hour, 50*time.Minute, testRenewEvery))
	b := newNATSLeases(f.bucketAs(t), "b", testTimings())
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	_, err := a.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)
	_, err = b.TryAcquire(t.Context(), "sweeper")
	require.ErrorIs(t, err, coord.ErrHeld)
	require.NoError(t, a.Close(t.Context()))
	_, err = b.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)
}

// A coordinator whose term ended without the key moving on (its renewals
// failed past the deadline) resumes its own write without the wait.
func TestLeases_ResumesItsOwnWrite(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	akv := &stallableKV{KeyValue: f.bucketAs(t)}
	a := newNATSLeases(akv, "a", WithLeaseTimings(time.Hour, testRenewDeadline, testRenewEvery))
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	first, err := a.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)
	akv.stall()
	select {
	case <-first.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the term outlived its renew deadline")
	}
	require.ErrorIs(t, first.Err(), coord.ErrLost)
	akv.mu.Lock()
	akv.stalled = false
	akv.mu.Unlock()
	second, err := a.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err, "its own write, not a stranger's: no wait")
	assert.Greater(t, second.Token(), first.Token())
}

// Leases refuses a bucket the operator has not created, and boot with the
// bucket in the topology waits for it and then refuses naming it.
func TestNewNATS_RefusesAMissingLeaseBucket(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	tp := shippedTopology(t)
	tp.KeyValues = nil
	f.apply(t, tp)

	e := f.broker(t, nil)
	_, err := e.Leases(t.Context(), natstest.CoordBucket, "a")
	require.ErrorIs(t, err, ErrTopology)
	assert.ErrorContains(t, err, "kv bucket wh_coord does not exist")

	_, err = NewNATS(t.Context(), NATSConfig{
		URLs: []string{f.server.ClientURL()}, User: "wavehouse", PasswordFile: writeSecret(t, fixturePassword("wavehouse")),
		Topology:     NATSTopology{Partitions: 4, CoordBucket: natstest.CoordBucket},
		TopologyWait: 300 * time.Millisecond,
	})
	var terr *TopologyError
	require.ErrorAs(t, err, &terr)
	assert.ErrorContains(t, err, "kv bucket wh_coord: bucket: does not exist")
}

// Through the broker, as the restricted user, a lease works end to end.
func TestExternalNATS_Leases(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, func(c *NATSConfig) { c.Topology.CoordBucket = natstest.CoordBucket })
	c, err := e.Leases(t.Context(), natstest.CoordBucket, "a", testTimings())
	require.NoError(t, err)
	term, err := c.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)
	time.Sleep(3 * testRenewEvery)
	require.NoError(t, term.Err(), "renewals as the wavehouse user")
	require.NoError(t, c.Close(t.Context()))
	require.NoError(t, term.Err())
}

// The shipped permissions let the wavehouse user do exactly what a lease
// needs: read and write lease keys. Not the bucket itself, nor other keys.
func TestNATSPermissions_RefuseBucketChanges(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	js := f.connect(t, "wavehouse", nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	ctx := t.Context()
	kv, err := js.KeyValue(ctx, natstest.CoordBucket)
	require.NoError(t, err)
	denied := func(what string, err error) {
		t.Helper()
		require.Error(t, err, what)
		assert.False(t, errors.Is(err, context.Canceled), what)
	}

	rev, err := kv.Create(ctx, leaseKeyPrefix+"x", []byte("v"))
	require.NoError(t, err, "create a lease key")
	rev, err = kv.Update(ctx, leaseKeyPrefix+"x", []byte("v"), rev)
	require.NoError(t, err, "renew a lease key")
	_, err = kv.Get(ctx, leaseKeyPrefix+"x")
	require.NoError(t, err, "read a lease key")
	require.NoError(t, kv.Delete(ctx, leaseKeyPrefix+"x", jetstream.LastRevision(rev)), "resign a lease key")

	denied("write a key outside lease.", call(ctx, func(ctx context.Context) error {
		_, err := kv.Put(ctx, "other", []byte("v"))
		return err
	}))
	denied("read a key outside lease.", call(ctx, func(ctx context.Context) error {
		_, err := kv.Get(ctx, "other")
		return err
	}))
	denied("create a bucket", call(ctx, func(ctx context.Context) error {
		_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "rogue"})
		return err
	}))
	denied("delete the bucket", call(ctx, func(ctx context.Context) error {
		return js.DeleteKeyValue(ctx, natstest.CoordBucket)
	}))
	denied("purge the bucket", call(ctx, func(ctx context.Context) error {
		s, err := js.Stream(ctx, "KV_"+natstest.CoordBucket)
		if err != nil {
			return err
		}
		return s.Purge(ctx)
	}))
	_, err = f.admin.KeyValue(ctx, natstest.CoordBucket)
	require.NoError(t, err, "the bucket is still there")
}
