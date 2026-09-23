package dedupe

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManaged_FollowsEnabled(t *testing.T) {
	t.Parallel()
	m := NewManaged(Embedded(filepath.Join(t.TempDir(), "pebble")))
	t.Cleanup(func() { _ = m.Close() })
	ctx := context.Background()

	assert.False(t, m.Open())
	assert.Nil(t, m.Stats(), "closed store reports no stats so the scraper skips it")
	_, err := m.CheckAndMark(ctx, "e1")
	require.ErrorIs(t, err, ErrDisabled)

	require.NoError(t, m.Apply(true))
	require.NoError(t, m.Apply(true), "re-applying the same state is a no-op")
	assert.True(t, m.Open())
	assert.NotNil(t, m.Stats())
	dup, err := m.CheckAndMark(ctx, "e1")
	require.NoError(t, err)
	assert.False(t, dup)
	dup, err = m.CheckAndMark(ctx, "e1")
	require.NoError(t, err)
	assert.True(t, dup)

	require.NoError(t, m.Apply(false))
	require.NoError(t, m.Apply(false))
	assert.False(t, m.Open())
	_, err = m.CheckAndMark(ctx, "e1")
	require.ErrorIs(t, err, ErrDisabled)

	// Re-enabling reopens the same directory: previously seen ids persist.
	require.NoError(t, m.Apply(true))
	dup, err = m.CheckAndMark(ctx, "e1")
	require.NoError(t, err)
	assert.True(t, dup, "toggling off and on must not forget seen ids")
}

// memDedup is the smallest possible backend: what a shared remote store's
// per-tenant view would be, minus the network.
type memDedup struct {
	seen   map[string]bool
	closed bool
}

func (m *memDedup) CheckAndMark(_ context.Context, id string) (bool, error) {
	if m.seen[id] {
		return true, nil
	}
	m.seen[id] = true
	return false, nil
}
func (m *memDedup) Stats() map[string]int64 { return map[string]int64{"seen": int64(len(m.seen))} }
func (m *memDedup) Close() error            { m.closed = true; return nil }

// The switch semantics belong to Managed, not to Pebble: any Deduplicator
// an opener returns gets them, and a failing opener reads as unavailable.
func TestManaged_AnyBackend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := &memDedup{seen: map[string]bool{}}
	m := NewManaged(func() (Deduplicator, error) { return backend, nil })
	_, err := m.CheckAndMark(ctx, "e1")
	require.ErrorIs(t, err, ErrDisabled)

	require.NoError(t, m.Apply(true))
	dup, err := m.CheckAndMark(ctx, "e1")
	require.NoError(t, err)
	assert.False(t, dup)
	dup, err = m.CheckAndMark(ctx, "e1")
	require.NoError(t, err)
	assert.True(t, dup)
	assert.Equal(t, map[string]int64{"seen": 1}, m.Stats())
	require.NoError(t, m.Close())
	assert.True(t, backend.closed, "closing the switch closes the backend")

	failing := NewManaged(func() (Deduplicator, error) { return nil, errors.New("backend down") })
	require.ErrorContains(t, failing.Apply(true), "backend down")
	assert.False(t, failing.Open())
	_, err = failing.CheckAndMark(ctx, "e1")
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestManaged_OpenFailureStaysClosed(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "not-a-dir")
	f, err := os.Create(path) //nolint:gosec // G304: path is rooted in t.TempDir()
	require.NoError(t, err)
	require.NoError(t, f.Close())

	m := NewManaged(Embedded(path))
	require.Error(t, m.Apply(true))
	assert.False(t, m.Open())
	_, err = m.CheckAndMark(context.Background(), "e1")
	require.ErrorIs(t, err, ErrUnavailable, "switched on but not open must fail closed, not read as disabled")
	require.NoError(t, m.Close())
	_, err = m.CheckAndMark(context.Background(), "e1")
	require.ErrorIs(t, err, ErrDisabled)
}
