//go:build integration

package mq

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/coord/coordtest"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
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
	// loseReplies stores each write, then withholds its answer.
	loseReplies bool
	// blocked counts the writes held up so far, so a test can wait for one
	// to be in flight.
	blocked atomic.Int32
}

func (s *stallableKV) stall() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stalled = true
}

func (s *stallableKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	s.mu.Lock()
	stalled, lose := s.stalled, s.loseReplies
	s.mu.Unlock()
	if lose {
		if _, err := s.KeyValue.Update(ctx, key, value, revision); err != nil {
			return 0, err
		}
	}
	if stalled || lose {
		s.blocked.Add(1)
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return s.KeyValue.Update(ctx, key, value, revision)
}

// awaitBlocked waits until at least n writes are held up.
func (s *stallableKV) awaitBlocked(t *testing.T, n int32) {
	t.Helper()
	require.Eventually(t, func() bool { return s.blocked.Load() >= n }, 5*time.Second, testRenewEvery/5, "a renewal waiting on the stall")
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

// failingKV is a bucket whose writes can be made to fail, as a server that
// refuses or does not answer them would.
type failingKV struct {
	jetstream.KeyValue
	failing atomic.Bool
	// failed and stored count the writes refused and the writes that went
	// through, so a test can wait for the renewal loop to reach a state
	// rather than assume it from the clock.
	failed, stored atomic.Int32
}

func (f *failingKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if f.failing.Load() {
		f.failed.Add(1)
		return 0, errors.New("kv unreachable")
	}
	rev, err := f.KeyValue.Update(ctx, key, value, revision)
	if err == nil {
		f.stored.Add(1)
	}
	return rev, err
}

// await waits until counter reaches at least n.
func (f *failingKV) await(t *testing.T, counter *atomic.Int32, n int32, what string) {
	t.Helper()
	require.Eventually(t, func() bool { return counter.Load() >= n }, 5*time.Second, testRenewEvery/5, what)
}

// Renewals that fail are counted per lease and logged, the first of a streak
// at WARN and the rest below it, and the recovery at INFO; the term carries
// on while the renew deadline allows, and a renewal that then succeeds keeps
// it.
func TestLeases_RenewalFailuresAreLoggedAndCounted(t *testing.T) { //nolint:paralleltest // captures the default logger and substitutes the lease meter
	logs := logtest.Capture(t, slog.LevelInfo)
	reader := sdkmetric.NewManualReader()
	prev := leaseMeter
	leaseMeter = func() metric.Meter { return sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test") }
	t.Cleanup(func() { leaseMeter = prev })

	f := leaseFixture(t)
	kv := &failingKV{KeyValue: f.bucketAs(t)}
	a := newNATSLeases(kv, "a", WithLeaseTimings(2*time.Hour, time.Hour, testRenewEvery))
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	term, err := a.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)

	kv.failing.Store(true)
	kv.await(t, &kv.failed, 2, "a streak of failed renewals")
	// The renew loop is sequential, so once two failures have returned no
	// earlier write is still in flight and the count of stored writes is
	// settled.
	stored := kv.stored.Load()
	kv.failing.Store(false)
	kv.await(t, &kv.stored, stored+1, "a renewal stored again")
	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), `"msg":"coord: lease renewed again"`)
	}, 5*time.Second, testRenewEvery/5, "the recovery is logged once the stored renewal returns")
	require.NoError(t, term.Err(), "the deadline was never reached")

	warns := strings.Count(logs.String(), `"level":"WARN","msg":"coord: lease renewal failed`)
	assert.Equal(t, 1, warns, "one WARN per streak:\n%s", logs.String())
	assert.Contains(t, logs.String(), `"lease":"sweeper"`)
	assert.Contains(t, logs.String(), `"msg":"coord: lease renewed again"`)
	assert.Equal(t, 1, strings.Count(logs.String(), `"msg":"coord: lease renewed again"`))
	assert.Contains(t, logs.String(), "kv unreachable")

	failures, held := leaseMetrics(t, reader)
	assert.GreaterOrEqual(t, failures["sweeper"], int64(2), "every failed renewal of the streak counted")
	assert.Equal(t, map[string]int64{"sweeper": 1}, held, "the lease this process holds")

	require.NoError(t, term.Resign(t.Context()))
	_, held = leaseMetrics(t, reader)
	assert.Empty(t, held, "resigned")
	assert.Equal(t, 1, strings.Count(logs.String(), `"msg":"coord: lease renewal failed`), "a resign is not a failure:\n%s", logs.String())
}

// A resign that cuts a renewal short is not a renewal failure: nothing is
// counted or logged for it.
func TestLeases_ResignMidRenewalIsNotAFailure(t *testing.T) { //nolint:paralleltest // captures the default logger and substitutes the lease meter
	logs := logtest.Capture(t, slog.LevelDebug)
	reader := sdkmetric.NewManualReader()
	prev := leaseMeter
	leaseMeter = func() metric.Meter { return sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test") }
	t.Cleanup(func() { leaseMeter = prev })

	f := leaseFixture(t)
	kv := &stallableKV{KeyValue: f.bucketAs(t)}
	a := newNATSLeases(kv, "a", WithLeaseTimings(2*time.Hour, time.Hour, testRenewEvery))
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	term, err := a.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)
	kv.stall()
	kv.awaitBlocked(t, 1) // a renewal is now waiting on the stall
	require.NoError(t, term.Resign(t.Context()))
	assertTermEnded(t, term)

	failures, held := leaseMetrics(t, reader)
	assert.Zero(t, failures["sweeper"], "the renewal the resign cut short is not a failure")
	assert.Empty(t, held, "resigned")
	assert.NotContains(t, logs.String(), "coord: lease renewal failed", "nothing logged for it:\n%s", logs.String())
}

// leaseMetrics reads the renewal-failure counter and the held gauge, each
// per lease.
func leaseMetrics(t *testing.T, reader *sdkmetric.ManualReader) (failures, held map[string]int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	failures, held = map[string]int64{}, map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					lease, _ := dp.Attributes.Value(attribute.Key("lease"))
					failures[lease.AsString()] += dp.Value
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					lease, _ := dp.Attributes.Value(attribute.Key("lease"))
					held[lease.AsString()] = dp.Value
				}
			}
		}
	}
	return failures, held
}

// A renewal stuck on an unanswering server never holds a resign up: Resign
// aborts it, however long the renew deadline.
func TestLeases_ResignAbortsAStuckRenewal(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	akv := &stallableKV{KeyValue: f.bucketAs(t)}
	a := newNATSLeases(akv, "a", WithLeaseTimings(2*time.Hour, time.Hour, testRenewEvery))
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	term, err := a.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)
	akv.stall()
	time.Sleep(3 * testRenewEvery) // a renewal is now waiting on the stall
	start := time.Now()
	require.NoError(t, term.Resign(t.Context()))
	assert.Less(t, time.Since(start), time.Second)
	assertTermEnded(t, term)
	require.NoError(t, term.Err())
}

// A renewal that was stored but whose answer Resign cut off still leaves
// the key this term's own; Resign deletes it, so the lease is free at once.
func TestLeases_ResignClearsARenewalItCutShort(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	akv := &stallableKV{KeyValue: f.bucketAs(t)}
	a := newNATSLeases(akv, "a", WithLeaseTimings(2*time.Hour, time.Hour, testRenewEvery))
	b := newNATSLeases(f.bucketAs(t), "b", WithLeaseTimings(2*time.Hour, time.Hour, testRenewEvery))
	t.Cleanup(func() { _ = a.Close(context.Background()); _ = b.Close(context.Background()) })
	term, err := a.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)
	admin, err := f.admin.KeyValue(t.Context(), natstest.CoordBucket)
	require.NoError(t, err)
	akv.mu.Lock()
	akv.loseReplies = true
	akv.mu.Unlock()
	require.Eventually(t, func() bool {
		e, err := admin.Get(t.Context(), leaseKeyPrefix+"sweeper")
		return err == nil && e.Revision() > term.Token()
	}, 2*time.Second, testRenewEvery/2, "a renewal is stored with its answer withheld")
	require.NoError(t, term.Resign(t.Context()))
	_, err = admin.Get(t.Context(), leaseKeyPrefix+"sweeper")
	require.ErrorIs(t, err, jetstream.ErrKeyNotFound, "the resign deleted the renewal it cut short")
	_, err = b.TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err, "free at once, with no lease duration to wait out")
}

// Each term writes its own id, so a term never mistakes its coordinator's
// later term for itself.
func TestLeases_TermsWriteTheirOwnIDs(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	a := newNATSLeases(f.bucketAs(t), "a", testTimings())
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	admin, err := f.admin.KeyValue(t.Context(), natstest.CoordBucket)
	require.NoError(t, err)
	var ids []string
	for range 2 {
		term, err := a.TryAcquire(t.Context(), "sweeper")
		require.NoError(t, err)
		e, err := admin.Get(t.Context(), leaseKeyPrefix+"sweeper")
		require.NoError(t, err)
		var v leaseValue
		require.NoError(t, json.Unmarshal(e.Value(), &v))
		assert.Equal(t, "a", v.Holder)
		assert.NotEmpty(t, v.Term)
		ids = append(ids, v.Term)
		require.NoError(t, term.Resign(t.Context()))
	}
	assert.NotEqual(t, ids[0], ids[1])
}

// gatedCreateKV holds each Create, once stored, until released.
type gatedCreateKV struct {
	jetstream.KeyValue
	stored, release chan struct{}
}

func (g *gatedCreateKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	rev, err := g.KeyValue.Create(ctx, key, value, opts...)
	close(g.stored)
	<-g.release
	return rev, err
}

// A Close that lands while a TryAcquire's write is in flight wins: the
// campaign hands the lease straight back and reports ErrClosed.
func TestLeases_CloseDuringACampaign(t *testing.T) {
	t.Parallel()
	f := leaseFixture(t)
	g := &gatedCreateKV{KeyValue: f.bucketAs(t), stored: make(chan struct{}), release: make(chan struct{})}
	a := newNATSLeases(g, "a", testTimings())
	got := make(chan error, 1)
	go func() {
		_, err := a.TryAcquire(context.Background(), "sweeper")
		got <- err
	}()
	<-g.stored
	require.NoError(t, a.Close(t.Context()))
	close(g.release)
	require.ErrorIs(t, <-got, coord.ErrClosed)
	admin, err := f.admin.KeyValue(t.Context(), natstest.CoordBucket)
	require.NoError(t, err)
	_, err = admin.Get(t.Context(), leaseKeyPrefix+"sweeper")
	require.ErrorIs(t, err, jetstream.ErrKeyNotFound, "the lease was handed back")
}

func assertTermEnded(t *testing.T, term coord.Term) {
	t.Helper()
	select {
	case <-term.Done():
	default:
		t.Fatal("the term is still live")
	}
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
		Topology:     NATSTopology{Partitions: 4, Shards: 8, CoordBucket: natstest.CoordBucket},
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

// Every rule the verifier holds the lease bucket to, one mutation each. It is
// checked only when the process holds leases there (CoordBucket set).
func TestLeases_VerifierChecksTheBucket(t *testing.T) {
	t.Parallel()
	const obj = "kv bucket wh_coord"
	coordSpec := NATSTopology{Partitions: 4, Shards: 8, CoordBucket: natstest.CoordBucket}
	bucket := func(mut func(*jetstream.KeyValueConfig)) func(*fixtureTopology) {
		return func(tp *fixtureTopology) { mut(&tp.KeyValues[0]) }
	}
	// raw stands the bucket's stream up by hand, for what CreateKeyValue
	// would not create.
	raw := func(mut func(*jetstream.StreamConfig)) func(*fixtureTopology) {
		return func(tp *fixtureTopology) {
			tp.KeyValues = nil
			cfg := jetstream.StreamConfig{
				Name: "KV_wh_coord", Subjects: []string{"$KV.wh_coord.>"}, MaxMsgsPerSubject: 1,
				AllowDirect: true, Storage: jetstream.FileStorage, Discard: jetstream.DiscardNew,
			}
			mut(&cfg)
			tp.Streams = append(tp.Streams, cfg)
		}
	}
	cases := []struct {
		name   string
		mutate func(*fixtureTopology)
		spec   NATSTopology
		sev    FindingSeverity
		object string
		field  string
	}{
		{"missing", func(tp *fixtureTopology) { tp.KeyValues = nil }, coordSpec, FindingRequired, obj, "bucket"},
		{"named elsewhere", nil, NATSTopology{Partitions: 4, Shards: 8, CoordBucket: "other"}, FindingRequired, "kv bucket other", "bucket"},
		{"ttl", bucket(func(kv *jetstream.KeyValueConfig) { kv.TTL = time.Hour }), coordSpec, FindingRequired, obj, "ttl"},
		{"no direct get", raw(func(s *jetstream.StreamConfig) { s.AllowDirect = false }), coordSpec, FindingRequired, obj, "allow_direct"},
		{"keeps no value", raw(func(s *jetstream.StreamConfig) { s.MaxMsgsPerSubject = 0 }), coordSpec, FindingRequired, obj, "history"},
		{"memory storage", bucket(func(kv *jetstream.KeyValueConfig) { kv.Storage = jetstream.MemoryStorage }), coordSpec, FindingRecommended, obj, "storage"},
	}
	f := newNATSFixture(t)
	js := f.connect(t, "wavehouse")
	for _, tc := range cases {
		f.reset(t)
		tp := shippedTopology(t)
		if tc.mutate != nil {
			tc.mutate(tp)
		}
		require.NoError(t, f.create(t.Context(), tp), tc.name)
		findings, err := verifyNATSTopology(t.Context(), js, tc.spec, nil)
		require.NoError(t, err, tc.name)
		found := false
		for _, got := range findings {
			if got.Severity == tc.sev && got.Object == tc.object && got.Field == tc.field {
				found = true
			}
		}
		assert.True(t, found, "%s: want %s %s/%s among %v", tc.name, tc.sev, tc.object, tc.field, findings)
	}
}
