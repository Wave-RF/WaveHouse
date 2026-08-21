package settings

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Store owns the settings snapshot a running instance has adopted. Open
// validates and adopts the directory up front, so a *Store never exists
// without a good document behind it. Reload is the single code path every
// later trigger — SIGHUP, the directory watcher, and POST
// /v1/ops/settings/reload — funnels through: re-validate the directory, and
// swap the snapshot only when no finding is an error, so a bad edit (or a
// deleted file, or a vanished directory) can never evict the last good
// document. Readers go through one lock-free atomic load per lookup; the
// typed accessors below each resolve from a single snapshot load, so a
// reload lands between lookups, never inside one.
//
// There are no compiled defaults here on purpose: every key is required by
// Validate, so the snapshot is exactly what the files said when they were
// adopted. Defaults live in the seed directory (Seed / WriteSeed).
type Store struct {
	dir    string
	logger *slog.Logger

	// mu serializes Reload: concurrent triggers queue rather than racing
	// parse-then-swap sequences (a stale parse must not overwrite a newer one).
	mu   sync.Mutex
	snap atomic.Pointer[Document]
}

// Open validates dir and returns a Store holding its document. A rejected
// directory returns a nil Store with the findings — the caller (boot)
// refuses to start; it must never run without adopted settings.
func Open(dir string, logger *slog.Logger) (*Store, []Finding) {
	s := &Store{dir: dir, logger: logger}
	findings, adopted := s.TriggerReload("boot")
	if !adopted {
		return nil, findings
	}
	return s, findings
}

// Dir returns the directory this store reads.
func (s *Store) Dir() string { return s.dir }

// Reload re-validates the directory and adopts the parsed document when no
// finding is an error (warnings don't block adoption, matching `wavehouse
// validate`). On a rejected reload the previous snapshot stays in place.
// The returned bool reports whether the document was adopted.
func (s *Store) Reload() ([]Finding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, findings := parse(s.dir)
	if doc == nil {
		return findings, false
	}
	s.snap.Store(doc)
	return findings, true
}

// TriggerReload is Reload plus outcome logging, tagged with the trigger's
// name ("boot", "sighup", "watch", "api") so operators can tell which path
// fired from the log line alone.
func (s *Store) TriggerReload(trigger string) ([]Finding, bool) {
	findings, adopted := s.Reload()
	if s.logger != nil {
		var errs, warns int
		for _, f := range findings {
			if f.Severity == SeverityError {
				errs++
				s.logger.Error("settings finding", "trigger", trigger, "finding", f.String())
			} else {
				warns++
				s.logger.Warn("settings finding", "trigger", trigger, "finding", f.String())
			}
		}
		switch {
		case adopted:
			s.logger.Info("settings adopted", "trigger", trigger, "dir", s.dir, "warnings", warns)
		case s.snap.Load() == nil:
			s.logger.Error("settings rejected", "trigger", trigger, "dir", s.dir, "errors", errs, "warnings", warns)
		default:
			s.logger.Error("settings rejected — keeping previous settings", "trigger", trigger, "dir", s.dir, "errors", errs, "warnings", warns)
		}
	}
	return findings, adopted
}

// doc returns the current snapshot. Never nil for a Store returned by Open:
// adoption happened before the Store was handed out, and a rejected reload
// leaves the previous document in place.
func (s *Store) doc() *Document {
	return s.snap.Load()
}

// DedupeFor resolves the effective dedupe settings for a table: the table
// override for each field it names, the global value otherwise. Both fields
// resolve from one snapshot load, so a reload can never hand a record the
// id_field of one document and the require_id of another.
func (s *Store) DedupeFor(table string) (idField string, requireID bool) {
	d := s.doc().Config.Dedupe
	idField, requireID = *d.IDField, *d.RequireID
	if td, ok := d.Tables[table]; ok {
		if td.IDField != nil {
			idField = *td.IDField
		}
		if td.RequireID != nil {
			requireID = *td.RequireID
		}
	}
	return idField, requireID
}

// DefaultMaxRows returns the fallback result LIMIT for structured queries.
func (s *Store) DefaultMaxRows() int {
	return *s.doc().Config.Query.DefaultMaxRows
}

// SchemaRefreshInterval returns the schema-discovery auto-refresh period.
func (s *Store) SchemaRefreshInterval() time.Duration {
	return time.Duration(*s.doc().Config.Schema.RefreshInterval) * time.Second
}

// CORSOrigins returns the allowed CORS origins; the middleware treats an
// empty list and ["*"] identically (allow-all).
func (s *Store) CORSOrigins() []string {
	return s.doc().Config.CORS.AllowedOrigins
}
