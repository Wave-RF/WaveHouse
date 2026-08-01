package settings

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/controldb"
)

// sectionDedupe is the dedupe section's name in the admin API. Its storage is
// the dedupe_settings table (one table per section: row absent = compiled
// defaults, row present = stored override).
const sectionDedupe = "dedupe"

// sections enumerates every section this build owns — used by States().
var sections = []string{sectionDedupe}

// ErrRevisionConflict is returned by a conditional write whose expected
// revision no longer matches the stored section: another admin wrote in
// between. The caller re-reads (GET /v1/admin/settings), reapplies their
// change to the fresh values, and retries with the new revision — so both
// edits survive instead of the second silently overwriting the first.
var ErrRevisionConflict = errors.New("settings revision conflict: section changed since it was read")

// ErrInvalid marks a rejected candidate document: the caller's input broke a
// validation rule. Distinct from a storage failure so the API can answer 400
// (fix your request) instead of 500 (server trouble).
var ErrInvalid = errors.New("invalid settings")

// SectionState reports where one section's live values come from: a stored
// database override (with the revision that produced them) or the compiled
// defaults.
type SectionState struct {
	Section  string `json:"section"`
	Source   string `json:"source"` // "db" | "default"
	Revision uint64 `json:"revision,omitempty"`
}

// Store manages runtime settings in the control-plane database: an in-memory
// cache is the read path, Put*/Reset* write through.
//
// There is no fail-closed empty state: an absent row always means Defaults()
// — which are safe — and deleting the row reverts its section to them.
type Store struct {
	db     *sql.DB
	logger *slog.Logger

	// writeMu serializes writers across the database statement AND the cache
	// update that follows, so the cache always lands in database-commit order.
	// Without it two racing unconditional writes can commit as A→B but cache
	// as B→A, serving the losing document until the next boot. Readers never
	// take it — mu alone guards the cache.
	writeMu sync.Mutex

	mu        sync.RWMutex
	cached    Values
	revisions map[string]uint64 // section → revision; absent/0 = no stored override
}

// NewStore loads any stored override on top of Defaults(). A stored row that
// fails validation refuses boot — the database CHECKs make that near
// impossible, but a refusal beats silently reverting a tunable the operator
// set.
func NewStore(ctx context.Context, db *sql.DB, logger *slog.Logger) (*Store, error) {
	s := &Store{db: db, logger: logger, cached: Defaults(), revisions: map[string]uint64{}}

	var (
		idField   string
		requireID bool
		revision  uint64
	)
	err := db.QueryRowContext(ctx,
		`SELECT id_field, require_id, revision FROM dedupe_settings WHERE id = 1`,
	).Scan(&idField, &requireID, &revision)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return s, nil // no override stored — compiled defaults apply
	case err != nil:
		return nil, fmt.Errorf("read dedupe settings: %w", err)
	}

	candidate := s.cached
	candidate.Dedupe = Dedupe{IDField: idField, RequireID: requireID}
	if err := candidate.Validate(); err != nil {
		return nil, fmt.Errorf("stored dedupe settings: %w", err)
	}
	s.cached = candidate
	s.revisions[sectionDedupe] = revision
	return s, nil
}

// NewMemoryStore creates a settings store over an in-memory control database,
// pre-loaded with the given values. Intended for testing: same engine, same
// constraints, same code paths as production — only the storage lives in RAM.
func NewMemoryStore(v Values) *Store {
	return &Store{
		db:        controldb.MustOpenMemory(),
		cached:    v,
		revisions: map[string]uint64{},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// Get returns the current snapshot. Values is a small value type, so the copy
// IS the snapshot — a caller reading it mid-request never observes a
// concurrent update.
func (s *Store) Get() Values {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cached
}

// Snapshot returns the live values and their per-section states as one
// consistent read: a revision in the result always describes the values
// returned beside it. Two separate accessors would let a concurrent write
// land between them, and a caller replaying that torn revision in If-Match
// would overwrite values it never saw.
func (s *Store) Snapshot() (Values, []SectionState) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SectionState, 0, len(sections))
	for _, section := range sections {
		st := SectionState{Section: section, Source: "default"}
		if rev := s.revisions[section]; rev > 0 {
			st.Source, st.Revision = "db", rev
		}
		out = append(out, st)
	}
	return s.cached, out
}

// PutDedupe validates the candidate document and stores the dedupe section.
// The write path is the validation chokepoint in Go, and the dedupe_settings
// CHECK constraints repeat the rules in storage, so nothing invalid can be
// persisted by any path.
//
// expected is the optimistic-concurrency guard for concurrent admin edits:
// nil replaces unconditionally (last writer wins); a non-nil value must match
// the section's current state — its revision, or 0 for "no override stored
// yet" — or the write fails with ErrRevisionConflict and changes nothing.
// The guard is a single conditional statement each way: the WHERE clause (or
// insert conflict) is the compare, zero rows affected is the lost race.
func (s *Store) PutDedupe(ctx context.Context, d Dedupe, expected *uint64) error {
	s.mu.RLock()
	candidate := s.cached
	s.mu.RUnlock()
	candidate.Dedupe = d
	if err := candidate.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	// One writer at a time from here on: the statement and the cache update
	// below must apply in the same order (see writeMu).
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var newRev uint64
	var err error
	switch {
	case expected == nil:
		// Unconditional: insert-or-replace, bumping the revision either way.
		err = s.db.QueryRowContext(ctx, `INSERT INTO dedupe_settings (id, id_field, require_id, revision)
			VALUES (1, ?, ?, 1)
			ON CONFLICT (id) DO UPDATE SET id_field = excluded.id_field,
				require_id = excluded.require_id, revision = dedupe_settings.revision + 1
			RETURNING revision`, d.IDField, d.RequireID).Scan(&newRev)
		if err != nil {
			return s.writeFailed(ctx, err)
		}
	case *expected == 0:
		// "No override stored yet": insert-only; a conflict means another
		// admin raced to write the first override and this one lost.
		res, execErr := s.db.ExecContext(ctx, `INSERT INTO dedupe_settings (id, id_field, require_id, revision)
			VALUES (1, ?, ?, 1) ON CONFLICT (id) DO NOTHING`, d.IDField, d.RequireID)
		if execErr != nil {
			return s.writeFailed(ctx, execErr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("%w (expected no stored override, but one exists)", ErrRevisionConflict)
		}
		newRev = 1
	default:
		res, execErr := s.db.ExecContext(ctx, `UPDATE dedupe_settings
			SET id_field = ?, require_id = ?, revision = revision + 1
			WHERE id = 1 AND revision = ?`, d.IDField, d.RequireID, *expected)
		if execErr != nil {
			return s.writeFailed(ctx, execErr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("%w (expected revision %d)", ErrRevisionConflict, *expected)
		}
		newRev = *expected + 1
	}

	s.mu.Lock()
	s.cached.Dedupe = d
	s.revisions[sectionDedupe] = newRev
	s.mu.Unlock()
	s.logger.Info("runtime settings updated", "section", sectionDedupe, "revision", newRev)
	return nil
}

// writeFailed logs a storage failure at ERROR — the API returns a generic 500
// for these, so this log line is where the driver detail survives — and wraps
// it for the caller.
func (s *Store) writeFailed(ctx context.Context, err error) error {
	s.logger.ErrorContext(ctx, "runtime settings write failed", "section", sectionDedupe, "error", err)
	return fmt.Errorf("write dedupe settings: %w", err)
}

// ResetDedupe deletes the stored override; the section reverts to Defaults()
// — never a fail-closed lockout, unlike a deleted policy, because every
// default is safe by design.
func (s *Store) ResetDedupe(ctx context.Context) error {
	// Same writer ordering as PutDedupe: without writeMu a concurrent put's
	// cache update could land after this reset's, leaving the cache holding an
	// override while the database holds no row.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if _, err := s.db.ExecContext(ctx, `DELETE FROM dedupe_settings WHERE id = 1`); err != nil {
		return s.writeFailed(ctx, err)
	}

	s.mu.Lock()
	s.cached.Dedupe = Defaults().Dedupe
	delete(s.revisions, sectionDedupe)
	s.mu.Unlock()
	s.logger.Info("runtime settings reset to defaults", "section", sectionDedupe)
	return nil
}
