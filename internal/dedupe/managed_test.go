package dedupe

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManaged_FollowsEnabled(t *testing.T) {
	t.Parallel()
	m := NewEmbedded(t.TempDir()).Tenant("acme")
	t.Cleanup(func() { _ = m.Close() })
	ctx := context.Background()

	assert.False(t, m.Open())
	_, err := mark(ctx, m, "e1")
	require.ErrorIs(t, err, ErrDisabled)

	require.NoError(t, m.Apply(true))
	require.NoError(t, m.Apply(true), "re-applying the same state is a no-op")
	assert.True(t, m.Open())
	dup, err := mark(ctx, m, "e1")
	require.NoError(t, err)
	assert.False(t, dup)
	dup, err = mark(ctx, m, "e1")
	require.NoError(t, err)
	assert.True(t, dup)

	require.NoError(t, m.Apply(false))
	require.NoError(t, m.Apply(false))
	assert.False(t, m.Open())
	_, err = mark(ctx, m, "e1")
	require.ErrorIs(t, err, ErrDisabled)

	// Re-enabling reopens the same instance: previously seen ids persist.
	require.NoError(t, m.Apply(true))
	dup, err = mark(ctx, m, "e1")
	require.NoError(t, err)
	assert.True(t, dup, "toggling off and on must not forget seen ids")
}

// memDedup is the smallest possible backend: what a shared remote store's
// per-tenant view would be, minus the network.
type memDedup struct {
	seen     map[Key]bool
	closed   bool
	reserved [][]Key // every Reserve's keys, as the backend saw them
	leases   []time.Duration
	released []Claim
	short    bool // answer one claim too few
}

func (m *memDedup) Reserve(_ context.Context, keys []Key, lease time.Duration) ([]Claim, error) {
	m.reserved = append(m.reserved, keys)
	m.leases = append(m.leases, lease)
	claims := make([]Claim, 0, len(keys))
	for _, k := range keys {
		st := Claimed
		if m.seen[k] {
			st = Duplicate
		}
		claims = append(claims, Claim{Key: k, Status: st, Token: "t"})
	}
	if m.short {
		claims = claims[1:]
	}
	return claims, nil
}

func (m *memDedup) Commit(_ context.Context, claims []Claim, _ time.Duration) error {
	for _, c := range claims {
		m.seen[c.Key] = true
	}
	return nil
}

func (m *memDedup) Release(_ context.Context, claims []Claim) error {
	m.released = append(m.released, claims...)
	return nil
}
func (m *memDedup) Close() error { m.closed = true; return nil }

// The switch semantics belong to Managed, not to Pebble: any Deduplicator
// an opener returns gets them, and a failing opener reads as unavailable.
func TestManaged_AnyBackend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := &memDedup{seen: map[Key]bool{}}
	m := NewManaged(func() (Deduplicator, error) { return backend, nil })
	_, err := mark(ctx, m, "e1")
	require.ErrorIs(t, err, ErrDisabled)

	require.NoError(t, m.Apply(true))
	dup, err := mark(ctx, m, "e1")
	require.NoError(t, err)
	assert.False(t, dup)
	dup, err = mark(ctx, m, "e1")
	require.NoError(t, err)
	assert.True(t, dup)
	require.NoError(t, m.Close())
	assert.True(t, backend.closed, "closing the switch closes the backend")

	failing := NewManaged(func() (Deduplicator, error) { return nil, errors.New("backend down") })
	require.ErrorContains(t, failing.Apply(true), "backend down")
	assert.False(t, failing.Open())
	_, err = mark(ctx, failing, "e1")
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestManaged_OpenFailureStaysClosed(t *testing.T) {
	t.Parallel()
	m := NewManaged(func() (Deduplicator, error) { return nil, errors.New("disk full") })
	require.Error(t, m.Apply(true))
	assert.False(t, m.Open())
	_, err := mark(context.Background(), m, "e1")
	require.ErrorIs(t, err, ErrUnavailable, "switched on but not open must fail closed, not read as disabled")
	require.NoError(t, m.Close())
	_, err = mark(context.Background(), m, "e1")
	require.ErrorIs(t, err, ErrDisabled)
}

// Managed collapses a key repeated in one call before the backend sees it,
// so every backend answers repeats alike, and hands the backend only the
// claims it made.
func TestManaged_CollapsesRepeats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := &memDedup{seen: map[Key]bool{}}
	m := NewManaged(func() (Deduplicator, error) { return backend, nil })
	require.NoError(t, m.Apply(true))
	a, b := Key{Table: "t", ID: "a"}, Key{Table: "t", ID: "b"}

	claims, err := m.Reserve(ctx, []Key{a, b, a}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, [][]Key{{a, b}}, backend.reserved, "the backend sees each key once")
	assert.Equal(t, []Claim{{Key: a, Status: Claimed, Token: "t"}, {Key: b, Status: Claimed, Token: "t"}, {Key: a, Status: Duplicate}}, claims)

	_, err = m.Reserve(ctx, []Key{{Table: "t", ID: "c"}}, 0)
	require.NoError(t, err)
	assert.Equal(t, []time.Duration{time.Second, DefaultLease}, backend.leases, "no lease is the default, never an already-lapsed claim")

	backend.short = true
	backend.seen[b] = true
	_, err = m.Reserve(ctx, []Key{{Table: "t", ID: "d"}, b, {Table: "t", ID: "e"}}, time.Second)
	require.ErrorContains(t, err, "answered 2 claims for 3 keys", "a backend answering the wrong count is refused, not indexed past")
	assert.Equal(t, []Claim{{Key: Key{Table: "t", ID: "e"}, Status: Claimed, Token: "t"}}, backend.released,
		"and gets back only the claims it made, never its Duplicate")
}

// Commit and Release follow the switch like Reserve, and a call with no
// Claimed claim never reaches the backend.
func TestManaged_CommitAndReleaseFollowTheSwitch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	claimed := []Claim{{Key: Key{Table: "t", ID: "a"}, Status: Claimed, Token: "t"}}
	m := NewManaged(func() (Deduplicator, error) { return nil, errors.New("down") })
	require.ErrorIs(t, m.Commit(ctx, claimed, 0), ErrDisabled)
	require.ErrorIs(t, m.Release(ctx, claimed), ErrDisabled)
	require.NoError(t, m.Commit(ctx, []Claim{{Status: Duplicate}}, 0), "nothing to commit")

	require.Error(t, m.Apply(true))
	require.ErrorIs(t, m.Commit(ctx, claimed, 0), ErrUnavailable)
	require.ErrorIs(t, m.Release(ctx, claimed), ErrUnavailable)
}

// blockingDedup's Commit blocks until unblock is closed, standing in for a
// network backend mid-outage: the caller holds Managed's read lock for as
// long as the call takes.
type blockingDedup struct {
	memDedup
	inCommit chan struct{} // closed once Commit is entered
	unblock  chan struct{}
}

func (b *blockingDedup) Commit(ctx context.Context, claims []Claim, retention time.Duration) error {
	close(b.inCommit)
	<-b.unblock
	return b.memDedup.Commit(ctx, claims, retention)
}

// A no-op Apply must not queue behind an in-flight Commit: it settles under
// the read lock alone, so a reload naming the same state for every tenant
// never waits out another tenant's slow backend call. A real transition is
// the opposite — it still needs the store quiescent, so it waits for Commit
// to finish before touching it.
func TestManaged_ApplyNoOpDoesNotWaitOnCommit(t *testing.T) {
	t.Parallel()
	backend := &blockingDedup{memDedup: memDedup{seen: map[Key]bool{}}, inCommit: make(chan struct{}), unblock: make(chan struct{})}
	m := NewManaged(func() (Deduplicator, error) { return backend, nil })
	require.NoError(t, m.Apply(true))

	claimed := []Claim{{Key: Key{Table: "t", ID: "a"}, Status: Claimed, Token: "t"}}
	commitDone := make(chan error, 1)
	go func() { commitDone <- m.Commit(context.Background(), claimed, 0) }()
	<-backend.inCommit // Commit is inside the backend call, holding the read lock

	noop := make(chan error, 1)
	go func() { noop <- m.Apply(true) }()
	select {
	case err := <-noop:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Apply(true) blocked behind an in-flight Commit for a state that already held")
	}

	// A real transition is the genuine case: it must wait for Commit, not
	// race it — assert it's still pending, then let Commit finish and
	// confirm Apply(false) then proceeds and closes the store.
	transition := make(chan error, 1)
	go func() { transition <- m.Apply(false) }()
	select {
	case err := <-transition:
		t.Fatalf("Apply(false) returned (%v) before the in-flight Commit finished", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(backend.unblock)
	require.NoError(t, <-commitDone)
	require.NoError(t, <-transition)
	assert.True(t, backend.closed)
}
