package controldb

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "control.db"), testLogger())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpen_AppliesMigrationsIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	db, err := Open(path, testLogger())
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if count == 0 {
		t.Fatal("expected at least one applied migration")
	}
	_ = db.Close()

	// Reopening must not re-apply (would fail on CREATE TABLE) or duplicate.
	db2, err := Open(path, testLogger())
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer func() { _ = db2.Close() }()
	var count2 int
	if err := db2.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM schema_migrations`).Scan(&count2); err != nil {
		t.Fatalf("count migrations after reopen: %v", err)
	}
	if count2 != count {
		t.Fatalf("migration count changed on reopen: %d -> %d", count, count2)
	}
}

// TestOpen_CreatesParentDir proves a first boot works when data_dir itself
// doesn't exist yet (fresh clone, the e2e orchestrator's clean-slate
// RemoveAll): SQLite won't create missing parent directories, so Open must.
func TestOpen_CreatesParentDir(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "data", "control.db"), testLogger())
	if err != nil {
		t.Fatalf("Open with missing parent dir: %v", err)
	}
	_ = db.Close()
}

// TestForeignKeysEnforced proves the per-connection pragma is active: SQLite
// ships with foreign keys OFF, and without the pragma every delete rule in
// the schema would be decoration.
func TestForeignKeysEnforced(t *testing.T) {
	db := openTestDB(t)
	_, err := db.ExecContext(context.Background(), `INSERT INTO table_policies (table_name, role_id, operation) VALUES ('events', 'no-such-role', 'select')`)
	if err == nil {
		t.Fatal("expected FK violation inserting grant with unknown role_id")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatalf("expected FOREIGN KEY error, got: %v", err)
	}
}

// TestDedupeSettingsConstraints proves the cross-field rule lives in storage:
// require_id without an id_field is refused by the database itself, no matter
// which code path writes.
func TestDedupeSettingsConstraints(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.ExecContext(context.Background(), `INSERT INTO dedupe_settings (id, id_field, require_id) VALUES (1, '', 1)`); err == nil {
		t.Fatal("expected CHECK violation: require_id=1 with empty id_field")
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO dedupe_settings (id, id_field, require_id) VALUES (2, 'event_id', 0)`); err == nil {
		t.Fatal("expected CHECK violation: id must be 1 (singleton)")
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO dedupe_settings (id, id_field, require_id) VALUES (1, 'event_id', 1)`); err != nil {
		t.Fatalf("valid row refused: %v", err)
	}
}

// TestPredicateConstraints proves the operator whitelist and the
// check-operator restriction (#224) are storage-enforced.
func TestPredicateConstraints(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	roleID, err := UpsertRole(ctx, db, "analyst")
	if err != nil {
		t.Fatalf("UpsertRole: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO table_policies (table_name, role_id, operation) VALUES ('events', ?, 'select')`, roleID); err != nil {
		t.Fatalf("insert grant: %v", err)
	}

	insert := func(kind, op string) error {
		_, err := db.ExecContext(context.Background(), `INSERT INTO policy_predicates (table_name, role_id, operation, kind, column_name, operator, value)
			VALUES ('events', ?, 'select', ?, 'tenant_id', ?, 'acme')`, roleID, kind, op)
		return err
	}
	if err := insert("filter", "_eq"); err != nil {
		t.Fatalf("valid filter predicate refused: %v", err)
	}
	if err := insert("filter", "_like"); err == nil {
		t.Fatal("expected CHECK violation: unknown operator _like")
	}
	if err := insert("check", "_gt"); err == nil {
		t.Fatal("expected CHECK violation: check predicates are _eq/_in only")
	}

	// Deleting the grant cascades its predicates.
	if _, err := db.ExecContext(context.Background(), `DELETE FROM table_policies WHERE table_name = 'events'`); err != nil {
		t.Fatalf("delete grant: %v", err)
	}
	var left int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM policy_predicates`).Scan(&left); err != nil {
		t.Fatalf("count predicates: %v", err)
	}
	if left != 0 {
		t.Fatalf("expected predicates to cascade with their grant, %d left", left)
	}
}

// TestRoleDeleteRestricted proves the points-at edge: a role still referenced
// by a grant cannot be deleted.
func TestRoleDeleteRestricted(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	roleID, err := UpsertRole(ctx, db, "analyst")
	if err != nil {
		t.Fatalf("UpsertRole: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO table_policies (table_name, role_id, operation) VALUES ('events', ?, 'select')`, roleID); err != nil {
		t.Fatalf("insert grant: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `DELETE FROM roles WHERE id = ?`, roleID); err == nil {
		t.Fatal("expected RESTRICT violation deleting a role with grants")
	}
	if _, err := db.ExecContext(context.Background(), `DELETE FROM table_policies WHERE role_id = ?`, roleID); err != nil {
		t.Fatalf("delete grant: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `DELETE FROM roles WHERE id = ?`, roleID); err != nil {
		t.Fatalf("delete unreferenced role: %v", err)
	}
}

func TestUpsertRole_StableAcrossCalls(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	id1, err := UpsertRole(ctx, db, "viewer")
	if err != nil {
		t.Fatalf("first UpsertRole: %v", err)
	}
	id2, err := UpsertRole(ctx, db, "viewer")
	if err != nil {
		t.Fatalf("second UpsertRole: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("role id not stable: %q vs %q", id1, id2)
	}
	if _, err := UpsertRole(ctx, db, ""); err == nil {
		t.Fatal("expected error for empty role name")
	}
}
