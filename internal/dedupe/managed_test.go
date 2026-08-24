package dedupe

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManaged_FollowsEnabled(t *testing.T) {
	t.Parallel()
	m := NewManaged(filepath.Join(t.TempDir(), "pebble"))
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

func TestManaged_OpenFailureStaysClosed(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "not-a-dir")
	f, err := os.Create(path) //nolint:gosec // G304: path is rooted in t.TempDir()
	require.NoError(t, err)
	require.NoError(t, f.Close())

	m := NewManaged(path)
	require.Error(t, m.Apply(true))
	assert.False(t, m.Open())
	require.NoError(t, m.Close())
}
