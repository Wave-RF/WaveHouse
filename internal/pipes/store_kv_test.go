package pipes

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_NewStore_EmptyKV(t *testing.T) {
	t.Parallel()
	js := testutil.NewJetStream(t)
	store, err := NewStore(t.Context(), js, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Empty(t, store.List())
}

func TestStore_PutGet_BackedByKV(t *testing.T) {
	t.Parallel()
	js := testutil.NewJetStream(t)
	ctx := t.Context()

	store, err := NewStore(ctx, js, "", testutil.NopLogger())
	require.NoError(t, err)

	q := &NamedQuery{Name: "top_pages", SQL: "SELECT page FROM clicks LIMIT 10"}
	require.NoError(t, store.Put(ctx, q))

	got := store.Get("top_pages")
	require.NotNil(t, got)
	assert.Equal(t, q.SQL, got.SQL)

	// A fresh store pointed at the same bucket should hydrate via refresh().
	store2, err := NewStore(ctx, js, "", testutil.NopLogger())
	require.NoError(t, err)
	require.NotNil(t, store2.Get("top_pages"))
}

func TestStore_Delete_BackedByKV(t *testing.T) {
	t.Parallel()
	js := testutil.NewJetStream(t)
	ctx := t.Context()

	store, err := NewStore(ctx, js, "", testutil.NopLogger())
	require.NoError(t, err)

	require.NoError(t, store.Put(ctx, &NamedQuery{Name: "q", SQL: "SELECT 1"}))
	require.NotNil(t, store.Get("q"))

	require.NoError(t, store.Delete(ctx, "q"))
	assert.Nil(t, store.Get("q"))

	// Deletion should have persisted to KV as well: a fresh store doesn't
	// re-materialize the entry.
	store2, err := NewStore(ctx, js, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Nil(t, store2.Get("q"))
}

func TestStore_LoadFromDirectory_BootstrapsSQLFiles(t *testing.T) {
	t.Parallel()
	js := testutil.NewJetStream(t)
	ctx := t.Context()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "alpha.sql"), []byte("SELECT 1"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "beta.sql"), []byte("SELECT 2"), 0o600))
	// Non-.sql and subdirectories should be ignored.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.md"), []byte("not a pipe"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0o750))

	store, err := NewStore(ctx, js, dir, testutil.NopLogger())
	require.NoError(t, err)

	require.NotNil(t, store.Get("alpha"))
	require.NotNil(t, store.Get("beta"))
	assert.Equal(t, "SELECT 1", store.Get("alpha").SQL)
	assert.Nil(t, store.Get("notes"))
}

func TestStore_LoadFromDirectory_DoesNotOverwrite(t *testing.T) {
	t.Parallel()
	js := testutil.NewJetStream(t)
	ctx := t.Context()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "q.sql"), []byte("SELECT from_file"), 0o600))

	// Seed KV with a different SQL body first.
	store, err := NewStore(ctx, js, "", testutil.NopLogger())
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, &NamedQuery{Name: "q", SQL: "SELECT from_kv"}))

	// Second boot with the same bucket + directory: KV value must win.
	store2, err := NewStore(ctx, js, dir, testutil.NopLogger())
	require.NoError(t, err)
	got := store2.Get("q")
	require.NotNil(t, got)
	assert.Equal(t, "SELECT from_kv", got.SQL)
}
