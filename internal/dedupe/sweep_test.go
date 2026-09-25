package dedupe

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stepClock is a clock a test moves by hand.
type stepClock struct{ ns atomic.Int64 }

func newStepClock() *stepClock {
	c := &stepClock{}
	c.ns.Store(time.Now().UnixNano())
	return c
}

func (c *stepClock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *stepClock) advance(d time.Duration) { c.ns.Add(int64(d)) }
func present(t *testing.T, e *Embedded, key []byte) bool {
	t.Helper()
	_, closer, err := e.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false
	}
	require.NoError(t, err)
	_ = closer.Close()
	return true
}

// commitIDs reserves and commits ids in table "events" with retention.
func commitIDs(t *testing.T, m *Managed, retention time.Duration, ids ...string) {
	t.Helper()
	keys := make([]Key, len(ids))
	for i, id := range ids {
		keys[i] = Key{Table: "events", ID: id}
	}
	claims, err := m.Reserve(context.Background(), keys, DefaultLease)
	require.NoError(t, err)
	require.NoError(t, m.Commit(context.Background(), claims, retention))
}

// A sweep deletes the keys whose retention has ended and every version-0
// key, across chunk boundaries, and leaves every live key: one kept forever,
// one not yet expired, and one that expired and was committed again.
func TestEmbedded_SweepDeletesExpiredAndVersionZeroKeys(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	clock := newStepClock()
	SetClock(e, clock.now)
	acme, globex := switchedOn(t, e, "acme"), switchedOn(t, e, "globex")

	// More expired keys than one chunk holds, interleaved with live ones.
	var expired, live []string
	for i := range 2*sweepChunk + 10 {
		expired = append(expired, fmt.Sprintf("x%05d", i))
		live = append(live, fmt.Sprintf("x%05d-live", i))
	}
	commitIDs(t, acme, time.Hour, expired...)
	commitIDs(t, acme, 3*time.Hour, live...)
	commitIDs(t, globex, 0, "forever")
	commitIDs(t, globex, time.Hour, "recommitted")
	for _, k := range []string{"acme\x00e1", "acme\x00e2", "globex\x00e1"} {
		require.NoError(t, e.db.Set([]byte(k), make([]byte, 8), pebble.Sync))
	}

	clock.advance(2 * time.Hour)
	commitIDs(t, globex, time.Hour, "recommitted")
	res, err := e.sweep(context.Background(), e.db)
	require.NoError(t, err)
	assert.Equal(t, sweepResult{Expired: len(expired), Version0: 3}, res)

	for _, id := range expired {
		require.False(t, present(t, e, AppendKey(nil, KeyPrefix("acme"), Key{Table: "events", ID: id})), id)
	}
	for _, id := range live {
		require.True(t, present(t, e, AppendKey(nil, KeyPrefix("acme"), Key{Table: "events", ID: id})), id)
	}
	assert.True(t, present(t, e, AppendKey(nil, KeyPrefix("globex"), Key{Table: "events", ID: "forever"})))
	assert.True(t, present(t, e, AppendKey(nil, KeyPrefix("globex"), Key{Table: "events", ID: "recommitted"})))
	assert.False(t, present(t, e, []byte("acme\x00e1")))
	assert.False(t, present(t, e, []byte("globex\x00e1")))

	dup, err := mark(context.Background(), globex, "recommitted")
	require.NoError(t, err)
	assert.True(t, dup, "the new commit survived the sweep")
	res, err = e.sweep(context.Background(), e.db)
	require.NoError(t, err)
	assert.Equal(t, sweepResult{}, res, "a second pass finds nothing")
}

// A Commit that arrives while a sweep chunk has read an expired key but not
// yet deleted it waits for the chunk, so the new commit is never deleted with
// the old value. Without the lock the Commit lands in the gap and the sweep
// then deletes it; the wait below only ever lets that pass, never fail.
func TestEmbedded_SweepNeverDeletesACommitLandingMidChunk(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	clock := newStepClock()
	SetClock(e, clock.now)
	m := switchedOn(t, e, "acme")
	commitIDs(t, m, time.Hour, "e1")
	clock.advance(2 * time.Hour)

	claims, err := m.Reserve(context.Background(), []Key{{Table: "events", ID: "e1"}}, DefaultLease)
	require.NoError(t, err)
	require.Equal(t, Claimed, claims[0].Status, "expired: claimable again")
	done := make(chan error, 1)
	e.sweepHook = func() {
		go func() { done <- m.Commit(context.Background(), claims, time.Hour) }()
		select {
		case <-done:
			done <- nil
		case <-time.After(50 * time.Millisecond):
		}
	}
	res, err := e.sweep(context.Background(), e.db)
	require.NoError(t, err)
	assert.Equal(t, sweepResult{Expired: 1}, res)
	require.NoError(t, <-done)

	dup, err := mark(context.Background(), m, "e1")
	require.NoError(t, err)
	assert.True(t, dup, "the commit made mid-chunk survived the sweep")
}

// A retention is honoured on read before any sweep has run: the key is a
// duplicate until the retention ends and claimable from that instant.
func TestEmbedded_RetentionHonouredOnRead(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	clock := newStepClock()
	SetClock(e, clock.now)
	m := switchedOn(t, e, "acme")
	commitIDs(t, m, time.Hour, "e1")

	clock.advance(time.Hour - time.Nanosecond)
	claims, err := m.Reserve(context.Background(), []Key{{Table: "events", ID: "e1"}}, DefaultLease)
	require.NoError(t, err)
	assert.Equal(t, Duplicate, claims[0].Status)

	clock.advance(time.Nanosecond)
	claims, err = m.Reserve(context.Background(), []Key{{Table: "events", ID: "e1"}}, DefaultLease)
	require.NoError(t, err)
	assert.Equal(t, Claimed, claims[0].Status)
}

// The sweep runs on its own once the instance opens, and stops with it.
func TestEmbedded_SweepRunsWhileOpen(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	e.sweepFirst, e.sweepEvery = time.Millisecond, time.Millisecond
	m := e.Tenant("acme")
	require.NoError(t, m.Apply(true))
	require.NoError(t, e.db.Set([]byte("acme\x00e1"), make([]byte, 8), pebble.Sync))
	assert.Eventually(t, func() bool { return !present(t, e, []byte("acme\x00e1")) }, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, m.Apply(false), "closing waits for the sweep to stop")
	assert.False(t, e.Open())
}

// A sweep stops between chunks when its context ends.
func TestEmbedded_SweepStopsWhenCancelled(t *testing.T) {
	t.Parallel()
	e := NewEmbedded(t.TempDir())
	switchedOn(t, e, "acme")
	b := e.db.NewBatch()
	for i := range 3 * sweepChunk {
		require.NoError(t, b.Set(fmt.Appendf(nil, "acme\x00%05d", i), nil, nil))
	}
	require.NoError(t, b.Commit(pebble.Sync))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := e.sweep(ctx, e.db)
	require.NoError(t, err)
	assert.Equal(t, sweepResult{Version0: sweepChunk}, res, "one chunk, then the cancellation is seen")
}

func TestExpiry(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_000)
	assert.Zero(t, expiry(now, 0))
	assert.Zero(t, expiry(now, -time.Second))
	assert.Equal(t, 1_000+int64(time.Hour), expiry(now, time.Hour))
	assert.Equal(t, int64(math.MaxInt64), expiry(now, time.Duration(math.MaxInt64)), "saturates rather than wrapping into the past")
}
