package policy

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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

// TestStore_NewStore_EmptyKV: with no bootstrap file configured, an empty KV
// is accepted (operator opted out of file bootstrap and will seed via Put);
// the cache stays nil and every request will fail closed in the meantime.
// We also assert the loud operator-facing warning so a misconfigured silent
// lockout is caught by stdout log scraping.
func TestStore_NewStore_EmptyKV(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	store, err := NewStore(t.Context(), js, "", logger)
	require.NoError(t, err)
	assert.Nil(t, store.Get(), "policy should be nil when KV is empty and no bootstrap file")
	assert.Contains(t, buf.String(), "policy KV is empty and no bootstrap file is configured",
		"operator should see a loud warning so a silent fail-closed lockout is obvious in logs")
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

// TestStore_NewStore_BootstrapMissingFileIsFatal: when a bootstrap path is
// configured but the file is absent, startup MUST fail. The earlier
// "missing file is not fatal" behavior silently swallowed misconfiguration
// — operators saw the process come up healthy but every request was denied
// (Evaluate/IsAdmin fail closed on nil policy), and we only noticed when the
// e2e harness pointed at a path that didn't exist. Failing loud at startup
// makes the misconfiguration obvious instead.
func TestStore_NewStore_BootstrapMissingFileIsFatal(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	_, err := NewStore(t.Context(), js, "/nonexistent/policy.yaml", testutil.NopLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load policy bootstrap file")
	assert.Contains(t, err.Error(), "/nonexistent/policy.yaml")
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

// TestStore_NewStore_BootstrapInvalidYAMLIsFatal: a file that exists but is
// malformed YAML must abort startup. Anything else silently leaves the cache
// empty and every request fails closed — exactly the failure mode we want to
// avoid.
func TestStore_NewStore_BootstrapInvalidYAMLIsFatal(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte("tables:\n  clicks:\n    select: [this isn't a map"), 0o600))

	_, err := NewStore(t.Context(), js, path, testutil.NopLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse policy yaml")
}

// TestStore_NewStore_BootstrapInvalidJSONIsFatal: the JSON branch of the
// loader has the same contract as the YAML branch — a malformed file aborts
// startup rather than degrading to a silent lockout.
func TestStore_NewStore_BootstrapInvalidJSONIsFatal(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"tables": {`), 0o600))

	_, err := NewStore(t.Context(), js, path, testutil.NopLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse policy json")
}

// TestStore_NewStore_BootstrapInvalidPolicyIsFatal: the file parses but
// Validate rejects it (negative max_rows here). Bootstrap routes through
// Store.Put, which applies Validate, so an invalid policy aborts startup
// instead of being persisted to KV.
func TestStore_NewStore_BootstrapInvalidPolicyIsFatal(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
tables:
  clicks:
    select:
      viewer:
        max_rows: -1
`), 0o600))

	_, err := NewStore(t.Context(), js, path, testutil.NopLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bootstrap policy")
	assert.Contains(t, err.Error(), "invalid policy")
}

// TestStore_NewStore_KVTakesPrecedenceOverFile: when KV already has a policy
// (a restart after the first run), the bootstrap file is irrelevant — even
// pointing at a missing path. The file is a one-shot seed; KV is the source
// of truth once populated, so operators can move/delete the seed file after
// the first run without breaking restarts.
func TestStore_NewStore_KVTakesPrecedenceOverFile(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)

	// Seed KV by writing a policy through one store instance.
	seeder, err := NewStore(t.Context(), js, "", testutil.NopLogger())
	require.NoError(t, err)
	require.NoError(t, seeder.Put(t.Context(), simplePolicy()))

	// A fresh store pointed at a NON-EXISTENT bootstrap file must still come
	// up cleanly because KV already wins. This is the "subsequent restart"
	// path that the strict bootstrap behavior must not break.
	store, err := NewStore(t.Context(), js, "/nonexistent/policy.yaml", testutil.NopLogger())
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

// TestStore_Put_WarnsDefaultRoleAdmin: a policy whose default_role equals the
// admin role is accepted (not a validation error) but logged loudly, since it
// grants every roleless request full admin. A normal default_role is silent.
func TestStore_Put_WarnsDefaultRoleAdmin(t *testing.T) {
	t.Parallel()

	js := testutil.NewJetStream(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	store, err := NewStore(t.Context(), js, "", logger)
	require.NoError(t, err)

	// A normal default_role does not warn.
	require.NoError(t, store.Put(t.Context(), &Policy{DefaultRole: "viewer", Tables: map[string]TablePolicy{}}))
	assert.NotContains(t, buf.String(), "default_role equals admin_role")

	// default_role == admin_role is accepted but logged loudly.
	require.NoError(t, store.Put(t.Context(), &Policy{DefaultRole: "admin", Tables: map[string]TablePolicy{}}))
	assert.Contains(t, buf.String(), "default_role equals admin_role")
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

func TestStore_HandleDelete_Nils(t *testing.T) {
	t.Parallel()
	s := NewMemoryStore(simplePolicy())
	s.handleDelete()
	assert.Nil(t, s.Get(), "a KV delete clears the cache; Evaluate then fails closed for non-admins")
}
