package dedupe

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEmbedded(t *testing.T) *EmbeddedDeduplicator {
	t.Helper()
	d, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestEmbeddedDeduplicator_FirstSeenThenDuplicate(t *testing.T) {
	t.Parallel()

	d := newEmbedded(t)
	ctx := context.Background()

	dup, err := d.CheckAndMark(ctx, "clicks", "event-1")
	require.NoError(t, err)
	assert.False(t, dup, "first occurrence must not be a duplicate")

	dup, err = d.CheckAndMark(ctx, "clicks", "event-1")
	require.NoError(t, err)
	assert.True(t, dup, "second occurrence of the same id must be a duplicate")

	dup, err = d.CheckAndMark(ctx, "clicks", "event-2")
	require.NoError(t, err)
	assert.False(t, dup, "distinct ids are independent")
}

func TestEmbeddedDeduplicator_TableNamespacing(t *testing.T) {
	t.Parallel()

	d := newEmbedded(t)
	ctx := context.Background()

	dup, err := d.CheckAndMark(ctx, "clicks", "shared-id")
	require.NoError(t, err)
	assert.False(t, dup, "first table's id is new")

	dup, err = d.CheckAndMark(ctx, "purchases", "shared-id")
	require.NoError(t, err)
	assert.False(t, dup, "same id value in a different table must not be a duplicate")

	dup, err = d.CheckAndMark(ctx, "clicks", "shared-id")
	require.NoError(t, err)
	assert.True(t, dup, "same id in the same table is still a duplicate")
}

func TestEmbeddedDeduplicator_Stats(t *testing.T) {
	t.Parallel()

	d := newEmbedded(t)

	stats := d.Stats()
	require.NotNil(t, stats)
	_, hasWAL := stats["pebble_wal_size"]
	_, hasTables := stats["pebble_table_count"]
	assert.True(t, hasWAL, "stats must include pebble_wal_size")
	assert.True(t, hasTables, "stats must include pebble_table_count")
}

func TestEmbeddedDeduplicator_OpenFailsOnInvalidPath(t *testing.T) {
	t.Parallel()

	// A path that already exists as a regular file is invalid for Pebble —
	// it needs a directory. This exercises the error branch of NewEmbedded.
	dir := t.TempDir()
	path := filepath.Join(dir, "not-a-dir")
	f, err := os.Create(path) //nolint:gosec // G304: path is rooted in t.TempDir()
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = NewEmbedded(path)
	require.Error(t, err)
}
