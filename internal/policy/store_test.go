package policy

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func simplePolicy() *Policy {
	return &Policy{
		DefaultRole: "viewer",
		Tables: map[string]TablePolicy{
			"clicks": {
				Select: map[string]RolePermissions{
					"viewer": {AllowColumns: []string{"page", "count"}},
				},
			},
		},
	}
}

func TestStore_NewStore_EmptyKV(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	ctx := t.Context()

	store, err := NewStore(ctx, js, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Nil(t, store.Get(), "policy should be nil when KV is empty and no bootstrap file")
}

func TestStore_NewStore_BootstrapFromFile(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
default_role: viewer
tables:
  clicks:
    select:
      viewer:
        allow_columns: [page, count]
`), 0o600))

	store, err := NewStore(t.Context(), js, path, testutil.NopLogger())
	require.NoError(t, err)
	p := store.Get()
	require.NotNil(t, p)
	assert.Equal(t, "viewer", p.DefaultRole)
	assert.Contains(t, p.Tables, "clicks")
}

func TestStore_NewStore_BootstrapMissingFileIsNotFatal(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	// Non-existent bootstrap path should not error out — NewStore falls
	// through with an empty cache.
	store, err := NewStore(t.Context(), js, "/nonexistent/policy.yaml", testutil.NopLogger())
	require.NoError(t, err)
	assert.Nil(t, store.Get())
}

func TestStore_NewStore_BootstrapJSON(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "policy.json")
	data, err := json.Marshal(simplePolicy())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	store, err := NewStore(t.Context(), js, path, testutil.NopLogger())
	require.NoError(t, err)
	require.NotNil(t, store.Get())
	assert.Equal(t, "viewer", store.Get().DefaultRole)
}

func TestStore_PutGet_Roundtrip(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	store, err := NewStore(t.Context(), js, "", testutil.NopLogger())
	require.NoError(t, err)

	require.NoError(t, store.Put(t.Context(), simplePolicy()))
	got := store.Get()
	require.NotNil(t, got)
	assert.Equal(t, "viewer", got.DefaultRole)

	// A fresh store reading the same bucket should see the policy in KV.
	store2, err := NewStore(t.Context(), js, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.NotNil(t, store2.Get())
}

func TestStore_Put_ValidationError(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	store, err := NewStore(t.Context(), js, "", testutil.NopLogger())
	require.NoError(t, err)

	// Invalid: negative max_rows.
	bad := &Policy{
		Tables: map[string]TablePolicy{
			"t": {Select: map[string]RolePermissions{
				"viewer": {MaxRows: -1},
			}},
		},
	}
	err = store.Put(t.Context(), bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid policy")
}

func TestStore_Watch_PropagatesUpdates(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	writer, err := NewStore(t.Context(), js, "", testutil.NopLogger())
	require.NoError(t, err)
	reader, err := NewStore(t.Context(), js, "", testutil.NopLogger())
	require.NoError(t, err)

	watchCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		reader.Watch(watchCtx)
		close(done)
	}()

	// Write a policy and wait for the watcher to propagate it to the reader's cache.
	require.NoError(t, writer.Put(t.Context(), simplePolicy()))

	require.Eventually(t, func() bool {
		return reader.Get() != nil
	}, 3*time.Second, 20*time.Millisecond, "watcher never updated reader cache")

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("watcher did not exit after context cancellation")
	}
}

func TestStore_MemoryStore_Basics(t *testing.T) {
	t.Parallel()

	p := simplePolicy()
	store := NewMemoryStore(p)
	assert.Equal(t, p, store.Get())
}

func TestStore_HandleDelete_DefaultNils(t *testing.T) {
	t.Parallel()
	s := NewMemoryStore(simplePolicy())
	s.handleDelete()
	assert.Nil(t, s.Get(), "without failClosed, a KV delete nils the cache")
}

func TestStore_HandleDelete_FailClosedRetains(t *testing.T) {
	t.Parallel()
	s := NewMemoryStore(simplePolicy())
	s.SetFailClosed(true)
	s.handleDelete()
	assert.NotNil(t, s.Get(), "failClosed must retain the last-known policy on a KV delete")
}
