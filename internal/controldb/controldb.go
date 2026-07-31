// Package controldb owns the control-plane SQLite database: the single file
// (<data_dir>/control.db) that stores policy, pipes, and runtime settings.
//
// The database is the durable source of truth; each store keeps an in-memory
// snapshot as the read path, so no request ever waits on SQLite. Integrity
// rules that SQL can express — operator whitelists, cross-field constraints,
// role references — live in the schema itself (see migrations/), so no code
// path can persist a row that violates them.
package controldb

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite" // pure-Go SQLite driver (no cgo)
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Open opens (or creates) the control-plane database at path — creating the
// parent directory if missing — and applies any pending migrations. The
// connection enforces foreign keys (SQLite ships with them OFF), uses WAL for
// crash safety, and is capped at one connection — control-plane traffic is
// boot loads and admin edits, so serializing writes costs nothing and removes
// SQLITE_BUSY handling entirely.
func Open(path string, logger *slog.Logger) (*sql.DB, error) {
	// SQLite creates the file on demand but never its parent directory —
	// unlike NATS and Pebble, which make their own state dirs. (For
	// ":memory:", Dir is "." and MkdirAll is a no-op.)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create control db dir %q: %w", dir, err)
	}
	dsn := "file:" + path + "?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open control db %q: %w", path, err)
	}
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping control db %q: %w", path, err)
	}
	if err := migrate(ctx, db, logger); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate control db %q: %w", path, err)
	}
	return db, nil
}

// migrate applies embedded migrations in filename order, tracking applied
// versions in schema_migrations. Each migration runs in its own transaction:
// it fully applies or the boot fails — never a half-applied schema.
func migrate(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INTEGER NOT NULL PRIMARY KEY,
		applied_at TEXT NOT NULL
	) STRICT`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		version, err := migrationVersion(name)
		if err != nil {
			return err
		}
		var applied int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, version).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %d: %w", version, err)
		}
		if applied > 0 {
			continue
		}
		ddl, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, string(ddl)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
			version, now()); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %d: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", version, err)
		}
		logger.Info("control db migration applied", "version", version, "file", name)
	}
	return nil
}

// migrationVersion extracts the numeric prefix of a migration filename
// ("001_control_plane.sql" → 1). A file without one is a packaging bug.
func migrationVersion(name string) (int64, error) {
	base, ok := strings.CutSuffix(name, ".sql")
	if !ok {
		return 0, fmt.Errorf("migration %q: not a .sql file", name)
	}
	prefix, _, ok := strings.Cut(base, "_")
	if !ok {
		return 0, fmt.Errorf("migration %q: expected <version>_<name>.sql", name)
	}
	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("migration %q: bad version prefix: %w", name, err)
	}
	return version, nil
}

// now returns the canonical control-db timestamp: RFC 3339 UTC.
func now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// MustOpenMemory opens a control-plane database that lives in RAM — the real
// SQLite engine (same schema, constraints, and transactions), just not backed
// by a file. It backs the NewMemoryStore test constructors, whose signatures
// cannot return an error; failure here can only be a programming error, so it
// panics. The single-connection cap in Open keeps the one in-memory database
// attached to its one connection.
func MustOpenMemory() *sql.DB {
	db, err := Open(":memory:", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		panic(fmt.Sprintf("controldb: open in-memory database: %v", err))
	}
	return db
}

// Querier is the slice of *sql.Tx / *sql.DB the helpers below need.
type Querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// UpsertRole returns the id of the role named name, inserting the row if it
// does not exist yet — one statement: the no-op DO UPDATE makes RETURNING
// yield the existing row's id on conflict, so there is no read-then-branch.
// Roles are created on demand by whatever references them (a policy grant, a
// pipe allowlist) and never garbage-collected — an unreferenced role row is
// harmless, and keeping it means the RESTRICT edges pointing at roles only
// ever fire on an explicit future role-delete surface.
func UpsertRole(ctx context.Context, q Querier, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("role name is required")
	}
	var id string
	if err := q.QueryRowContext(ctx, `INSERT INTO roles (id, name) VALUES (?, ?)
		ON CONFLICT (name) DO UPDATE SET name = excluded.name
		RETURNING id`, uuid.NewString(), name).Scan(&id); err != nil {
		return "", fmt.Errorf("upsert role %q: %w", name, err)
	}
	return id, nil
}
