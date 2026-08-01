package settings

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/controldb"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func openControlDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := controldb.Open(filepath.Join(t.TempDir(), "control.db"), testLogger())
	if err != nil {
		t.Fatalf("controldb.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newTestStore(t *testing.T, db *sql.DB) *Store {
	t.Helper()
	s, err := NewStore(context.Background(), db, testLogger())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func uintPtr(v uint64) *uint64 { return &v }

// dedupeState returns the dedupe section's state from a consistent snapshot.
func dedupeState(s *Store) SectionState {
	_, states := s.Snapshot()
	return states[0]
}

func TestNewStore_EmptyDBServesDefaults(t *testing.T) {
	s := newTestStore(t, openControlDB(t))
	if got := s.Get(); got != Defaults() {
		t.Fatalf("expected defaults, got %+v", got)
	}
	_, states := s.Snapshot()
	if len(states) != 1 || states[0].Section != "dedupe" || states[0].Source != "default" || states[0].Revision != 0 {
		t.Fatalf("unexpected states: %+v", states)
	}
}

func TestPutDedupe_PersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	db := openControlDB(t)
	s := newTestStore(t, db)

	want := Dedupe{IDField: "request_id", RequireID: true}
	if err := s.PutDedupe(ctx, want, nil); err != nil {
		t.Fatalf("PutDedupe: %v", err)
	}
	if got := s.Get().Dedupe; got != want {
		t.Fatalf("cache not updated: %+v", got)
	}
	if st := dedupeState(s); st.Source != "db" || st.Revision != 1 {
		t.Fatalf("unexpected state after put: %+v", st)
	}

	// A fresh store over the same database sees the stored override — the
	// restart path.
	s2 := newTestStore(t, db)
	if got := s2.Get().Dedupe; got != want {
		t.Fatalf("reopened store lost override: %+v", got)
	}
	if st := dedupeState(s2); st.Source != "db" || st.Revision != 1 {
		t.Fatalf("reopened store state: %+v", st)
	}
}

func TestPutDedupe_InvalidRejected(t *testing.T) {
	s := newTestStore(t, openControlDB(t))
	err := s.PutDedupe(context.Background(), Dedupe{IDField: "", RequireID: true}, nil)
	if err == nil {
		t.Fatal("expected validation error for require_id without id_field")
	}
	if s.Get() != Defaults() {
		t.Fatal("failed put must not change values")
	}
}

func TestPutDedupe_RevisionGuard(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, openControlDB(t))

	// expected=0 means "no override stored yet" — succeeds on an empty
	// section and lands at revision 1.
	if err := s.PutDedupe(ctx, Dedupe{IDField: "a"}, uintPtr(0)); err != nil {
		t.Fatalf("first conditional put: %v", err)
	}
	// A second admin who also read "no override" loses the race.
	err := s.PutDedupe(ctx, Dedupe{IDField: "b"}, uintPtr(0))
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("expected ErrRevisionConflict, got %v", err)
	}
	if got := s.Get().Dedupe.IDField; got != "a" {
		t.Fatalf("losing write must not apply, id_field=%q", got)
	}

	// Matching revision succeeds and bumps.
	if err := s.PutDedupe(ctx, Dedupe{IDField: "c"}, uintPtr(1)); err != nil {
		t.Fatalf("conditional update: %v", err)
	}
	if st := dedupeState(s); st.Revision != 2 {
		t.Fatalf("expected revision 2, got %+v", st)
	}
	// Stale revision conflicts.
	if err := s.PutDedupe(ctx, Dedupe{IDField: "d"}, uintPtr(1)); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("expected ErrRevisionConflict on stale revision, got %v", err)
	}
	// nil = unconditional replace.
	if err := s.PutDedupe(ctx, Dedupe{IDField: "e"}, nil); err != nil {
		t.Fatalf("unconditional put: %v", err)
	}
	if st := dedupeState(s); st.Revision != 3 {
		t.Fatalf("expected revision 3, got %+v", st)
	}
}

func TestResetDedupe_RevertsToDefaultsAndPersists(t *testing.T) {
	ctx := context.Background()
	db := openControlDB(t)
	s := newTestStore(t, db)

	if err := s.PutDedupe(ctx, Dedupe{IDField: "request_id"}, nil); err != nil {
		t.Fatalf("PutDedupe: %v", err)
	}
	if err := s.ResetDedupe(ctx); err != nil {
		t.Fatalf("ResetDedupe: %v", err)
	}
	if s.Get() != Defaults() {
		t.Fatalf("expected defaults after reset, got %+v", s.Get())
	}
	if st := dedupeState(s); st.Source != "default" || st.Revision != 0 {
		t.Fatalf("unexpected state after reset: %+v", st)
	}
	// Reset is idempotent.
	if err := s.ResetDedupe(ctx); err != nil {
		t.Fatalf("second ResetDedupe: %v", err)
	}
	// And durable.
	if got := newTestStore(t, db).Get(); got != Defaults() {
		t.Fatalf("reopened store not at defaults: %+v", got)
	}
}

// TestStore_ConcurrentEditsBothSurviveWithRetry exercises the optimistic
// concurrency protocol end to end: two admins race, the loser gets
// ErrRevisionConflict, re-reads, and retries — both edits land.
func TestStore_ConcurrentEditsBothSurviveWithRetry(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, openControlDB(t))
	if err := s.PutDedupe(ctx, Dedupe{IDField: "event_id"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	write := func(field string) error {
		for range 10 {
			rev := dedupeState(s).Revision
			err := s.PutDedupe(ctx, Dedupe{IDField: field}, &rev)
			if err == nil {
				return nil
			}
			if !errors.Is(err, ErrRevisionConflict) {
				return err
			}
		}
		return errors.New("retries exhausted")
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, field := range []string{"trace_id", "span_id"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = write(field)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	// Both writes applied: seed(1) + two successful conditional writes.
	if st := dedupeState(s); st.Revision != 3 {
		t.Fatalf("expected revision 3 after two racing writes, got %+v", st)
	}
}

func TestMemoryStore_EmulatesRevisionGuard(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore(Defaults())

	if err := s.PutDedupe(ctx, Dedupe{IDField: "a"}, uintPtr(0)); err != nil {
		t.Fatalf("first put: %v", err)
	}
	if err := s.PutDedupe(ctx, Dedupe{IDField: "b"}, uintPtr(0)); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if err := s.PutDedupe(ctx, Dedupe{IDField: "c"}, uintPtr(1)); err != nil {
		t.Fatalf("conditional update: %v", err)
	}
	if err := s.ResetDedupe(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if s.Get() != Defaults() {
		t.Fatalf("expected defaults after reset, got %+v", s.Get())
	}
}
