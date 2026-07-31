package pipes

import (
	"database/sql"
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

func TestStore_NewStore_EmptyDB(t *testing.T) {
	t.Parallel()
	store, err := NewStore(t.Context(), openControlDB(t), "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Empty(t, store.List())
}

// TestStore_PutGet_RoundtripFidelity: the decompose→recompose cycle must
// reproduce the exact document, including parameter order, typed defaults
// (42 vs "42" vs [..]), and the role allowlist.
func TestStore_PutGet_RoundtripFidelity(t *testing.T) {
	t.Parallel()
	db := openControlDB(t)
	ctx := t.Context()

	store, err := NewStore(ctx, db, "", testutil.NopLogger())
	require.NoError(t, err)

	q := &NamedQuery{
		Name:        "top_pages",
		SQL:         "SELECT page, count() FROM clicks WHERE ts > {{since}} LIMIT {{limit:10}}",
		Description: "Most-clicked pages",
		Parameters: []ParamDef{
			{Name: "since", Type: "string", Required: true},
			{Name: "limit", Type: "number", Default: float64(10)},
			{Name: "regions", Type: "array", Default: []any{"us", "eu"}},
		},
		AllowedRoles: []string{"analyst", "viewer"},
	}
	require.NoError(t, store.Put(ctx, q))
	assert.Equal(t, q, store.Get("top_pages"))

	// The restart path: a fresh store recomposes from rows.
	store2, err := NewStore(ctx, db, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Equal(t, q, store2.Get("top_pages"))

	// A minimal pipe round-trips its zero fields too (nil Parameters and
	// AllowedRoles, empty Description).
	minimal := &NamedQuery{Name: "one", SQL: "SELECT 1"}
	require.NoError(t, store.Put(ctx, minimal))
	store3, err := NewStore(ctx, db, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Equal(t, minimal, store3.Get("one"))
}

// TestStore_Put_UpdateReplacesChildRows: a second PUT of the same name is the
// full document — old parameters and roles must not survive, and the pipe's
// surrogate id must be stable across updates.
func TestStore_Put_UpdateReplacesChildRows(t *testing.T) {
	t.Parallel()
	db := openControlDB(t)
	ctx := t.Context()

	store, err := NewStore(ctx, db, "", testutil.NopLogger())
	require.NoError(t, err)

	require.NoError(t, store.Put(ctx, &NamedQuery{
		Name: "q", SQL: "SELECT 1",
		Parameters:   []ParamDef{{Name: "a", Type: "string"}},
		AllowedRoles: []string{"viewer"},
	}))
	var idBefore string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT id FROM pipes WHERE name = 'q'`).Scan(&idBefore))

	updated := &NamedQuery{Name: "q", SQL: "SELECT 2", AllowedRoles: []string{"analyst"}}
	require.NoError(t, store.Put(ctx, updated))

	var idAfter string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT id FROM pipes WHERE name = 'q'`).Scan(&idAfter))
	assert.Equal(t, idBefore, idAfter, "surrogate id must survive an update")

	store2, err := NewStore(ctx, db, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Equal(t, updated, store2.Get("q"))
}

func TestStore_Delete_BackedByDB(t *testing.T) {
	t.Parallel()
	db := openControlDB(t)
	ctx := t.Context()

	store, err := NewStore(ctx, db, "", testutil.NopLogger())
	require.NoError(t, err)

	require.NoError(t, store.Put(ctx, &NamedQuery{Name: "q", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}}))
	require.NotNil(t, store.Get("q"))

	require.NoError(t, store.Delete(ctx, "q"))
	assert.Nil(t, store.Get("q"))

	// Deletion persisted: a fresh store doesn't re-materialize the entry, and
	// the child rows cascaded away.
	store2, err := NewStore(ctx, db, "", testutil.NopLogger())
	require.NoError(t, err)
	assert.Nil(t, store2.Get("q"))
	var childRows int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM pipe_roles`).Scan(&childRows))
	assert.Zero(t, childRows)

	// Deleting a pipe that doesn't exist is an idempotent no-op (the same
	// contract as before this store moved to the control db).
	require.NoError(t, store.Delete(ctx, "q"))
}

func TestStore_LoadFromDirectory_BootstrapsSQLFiles(t *testing.T) {
	t.Parallel()
	db := openControlDB(t)
	ctx := t.Context()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "alpha.sql"), []byte("SELECT 1"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "beta.sql"), []byte("SELECT 2"), 0o600))
	// Non-.sql and subdirectories should be ignored.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.md"), []byte("not a pipe"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0o750))

	store, err := NewStore(ctx, db, dir, testutil.NopLogger())
	require.NoError(t, err)

	require.NotNil(t, store.Get("alpha"))
	require.NotNil(t, store.Get("beta"))
	assert.Equal(t, "SELECT 1", store.Get("alpha").SQL)
	assert.Nil(t, store.Get("notes"))
}

func TestStore_LoadFromDirectory_DoesNotOverwrite(t *testing.T) {
	t.Parallel()
	db := openControlDB(t)
	ctx := t.Context()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "q.sql"), []byte("SELECT from_file"), 0o600))

	// Seed the database with a different SQL body first.
	store, err := NewStore(ctx, db, "", testutil.NopLogger())
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, &NamedQuery{Name: "q", SQL: "SELECT from_db"}))

	// Second boot with the same database + directory: the stored value wins.
	store2, err := NewStore(ctx, db, dir, testutil.NopLogger())
	require.NoError(t, err)
	got := store2.Get("q")
	require.NotNil(t, got)
	assert.Equal(t, "SELECT from_db", got.SQL)
}
