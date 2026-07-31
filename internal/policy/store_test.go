package policy

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/controldb"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openControlDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := controldb.Open(filepath.Join(t.TempDir(), "control.db"), testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

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

func strPtr(s string) *string { return &s }

// TestStore_NewStore_EmptyDB: with no bootstrap file configured, an empty
// database is accepted (operator opted out of file bootstrap and will seed
// via Put); the cache stays nil and every request will fail closed in the
// meantime. We also assert the loud operator-facing warning so a
// misconfigured silent lockout is caught by stdout log scraping.
func TestStore_NewStore_EmptyDB(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	store, err := NewStore(t.Context(), db, "", logger)
	require.NoError(t, err)
	assert.Nil(t, store.Get(), "policy should be nil when the db is empty and no bootstrap file")
	assert.Contains(t, buf.String(), "control db holds no policy and no bootstrap file is configured",
		"operator should see a loud warning so a silent fail-closed lockout is obvious in logs")
}

func TestStore_NewStore_BootstrapFromFile(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)

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

	store, err := NewStore(t.Context(), db, path, testutil.NopLogger())
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

	db := openControlDB(t)
	_, err := NewStore(t.Context(), db, "/nonexistent/policy.yaml", testutil.NopLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load policy bootstrap file")
	assert.Contains(t, err.Error(), "/nonexistent/policy.yaml")
}

func TestStore_NewStore_BootstrapJSON(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "policy.json")
	data, err := json.Marshal(simplePolicy())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	store, err := NewStore(t.Context(), db, path, testutil.NopLogger())
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

	db := openControlDB(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte("tables:\n  clicks:\n    select: [this isn't a map"), 0o600))

	_, err := NewStore(t.Context(), db, path, testutil.NopLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse policy yaml")
}

// TestStore_NewStore_BootstrapInvalidJSONIsFatal: the JSON branch of the
// loader has the same contract as the YAML branch — a malformed file aborts
// startup rather than degrading to a silent lockout.
func TestStore_NewStore_BootstrapInvalidJSONIsFatal(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"tables": {`), 0o600))

	_, err := NewStore(t.Context(), db, path, testutil.NopLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse policy json")
}

// TestStore_NewStore_BootstrapInvalidPolicyIsFatal: the file parses but
// Validate rejects it (negative max_rows here). Bootstrap routes through
// Store.Put, which applies Validate, so an invalid policy aborts startup
// instead of being persisted.
func TestStore_NewStore_BootstrapInvalidPolicyIsFatal(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
tables:
  clicks:
    select:
      viewer:
        max_rows: -1
`), 0o600))

	_, err := NewStore(t.Context(), db, path, testutil.NopLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bootstrap policy")
	assert.Contains(t, err.Error(), "invalid policy")
}

// TestStore_NewStore_DBTakesPrecedenceOverFile: when the database already has
// a policy (a restart after the first run), the bootstrap file is irrelevant
// — even pointing at a missing path. The file is a one-shot seed; the
// database is the source of truth once populated, so operators can
// move/delete the seed file after the first run without breaking restarts.
func TestStore_NewStore_DBTakesPrecedenceOverFile(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)

	// Seed the database by writing a policy through one store instance.
	seeder, err := NewStore(t.Context(), db, "", testutil.NopLogger())
	require.NoError(t, err)
	require.NoError(t, seeder.Put(t.Context(), simplePolicy()))

	// A fresh store pointed at a NON-EXISTENT bootstrap file must still come
	// up cleanly because the database already wins. This is the "subsequent
	// restart" path that the strict bootstrap behavior must not break.
	store, err := NewStore(t.Context(), db, "/nonexistent/policy.yaml", testutil.NopLogger())
	require.NoError(t, err)
	require.NotNil(t, store.Get())
	assert.Equal(t, "viewer", store.Get().DefaultRole)
}

// richPolicy exercises every persisted field: both operations, filters with
// multi-operator ranges, insert checks, all four column/aggregation lists
// (including the nil vs empty distinction), and every limit scalar.
func richPolicy() *Policy {
	return &Policy{
		DefaultRole: "viewer",
		AdminRole:   "superuser",
		Tables: map[string]TablePolicy{
			"events": {
				Select: map[string]RolePermissions{
					"analyst": {
						AllowColumns: []string{"ts", "event_id", "payload"},
						Filter: map[string]Filter{
							"tenant_id": {Eq: strPtr("{{ jwt.claims.tenant }}")},
							// A range: two operators on one column = two
							// predicate rows.
							"ts": {Gt: strPtr("2026-01-01"), Lt: strPtr("2027-01-01")},
						},
						AllowedAggregations: []string{"count", "avg"},
						MaxRows:             10000,
						MaxExecutionTime:    Millis(5000),
						MaxRowsToRead:       1_000_000,
						MaxMemoryUsage:      ByteSize(1 << 30),
					},
					// Empty perms: all-zero row must round-trip to the zero
					// struct.
					"auditor": {},
				},
				Insert: map[string]RolePermissions{
					"ingestor": {
						DenyColumns: []string{"internal_notes"},
						Check: map[string]Filter{
							"tenant_id": {Eq: strPtr("{{ jwt.claims.tenant }}")},
							"region":    {In: strPtr(`["us","eu"]`)},
						},
						DeniedAggregations: []string{},
					},
				},
			},
			"metrics": {
				Select: map[string]RolePermissions{
					"viewer": {AllowColumns: []string{"name", "value"}},
				},
			},
		},
	}
}

// TestStore_PutGet_RoundtripFidelity: the decompose→recompose cycle must
// reproduce the exact document — a fresh store over the same database sees
// what was PUT, field for field.
func TestStore_PutGet_RoundtripFidelity(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)
	store, err := NewStore(t.Context(), db, "", testutil.NopLogger())
	require.NoError(t, err)

	want := richPolicy()
	require.NoError(t, store.Put(t.Context(), want))
	assert.Equal(t, want, store.Get(), "cache must hold the exact document")

	// The restart path: recompose from rows.
	store2, err := NewStore(t.Context(), db, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Equal(t, want, store2.Get(), "recomposed policy must equal the stored one")
}

// TestStore_Put_ReplacesWholePolicy: a second Put fully replaces the first —
// stale grants and predicates must not survive.
func TestStore_Put_ReplacesWholePolicy(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)
	store, err := NewStore(t.Context(), db, "", testutil.NopLogger())
	require.NoError(t, err)

	require.NoError(t, store.Put(t.Context(), richPolicy()))
	require.NoError(t, store.Put(t.Context(), simplePolicy()))

	store2, err := NewStore(t.Context(), db, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Equal(t, simplePolicy(), store2.Get())

	// Roles are upserted, never garbage-collected: prior roles remain rows
	// (harmless), but no stale grants reference them.
	var grants int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM table_policies`).Scan(&grants))
	assert.Equal(t, 1, grants)
	var preds int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM policy_predicates`).Scan(&preds))
	assert.Zero(t, preds, "old predicates must cascade away with their grants")
}

func TestStore_Put_ValidationError(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)
	store, err := NewStore(t.Context(), db, "", testutil.NopLogger())
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

	db := openControlDB(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	store, err := NewStore(t.Context(), db, "", logger)
	require.NoError(t, err)

	// A normal default_role does not warn.
	require.NoError(t, store.Put(t.Context(), &Policy{DefaultRole: "viewer", Tables: map[string]TablePolicy{}}))
	assert.NotContains(t, buf.String(), "default_role equals admin_role")

	// default_role == admin_role is accepted but logged loudly.
	require.NoError(t, store.Put(t.Context(), &Policy{DefaultRole: "admin", Tables: map[string]TablePolicy{}}))
	assert.Contains(t, buf.String(), "default_role equals admin_role")
}

func TestStore_MemoryStore_Basics(t *testing.T) {
	t.Parallel()

	p := simplePolicy()
	store := NewMemoryStore(p)
	assert.Equal(t, p, store.Get())

	// Memory-mode Put updates the cache without a database.
	require.NoError(t, store.Put(t.Context(), richPolicy()))
	assert.Equal(t, richPolicy(), store.Get())
}
