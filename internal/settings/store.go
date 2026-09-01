package settings

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
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
	// validate-then-swap sequences (a stale document must not overwrite a newer one).
	mu   sync.Mutex
	snap atomic.Pointer[Document]
	// afterAdopt runs under mu after every successful swap, in registration
	// order — for consumers that own a resource whose lifecycle follows a
	// setting (the Pebble store behind dedupe.enabled) rather than reading
	// the snapshot per call.
	afterAdopt []func()
}

// Open validates dir and returns a Store holding its document. A rejected
// directory returns a nil Store with the findings — the caller (boot)
// refuses to start; it must never run without adopted settings.
func Open(dir string, logger *slog.Logger) (*Store, []Finding) {
	s := &Store{dir: dir, logger: logger}
	findings, adopted := s.Reload("boot")
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
// The returned bool reports whether the document was adopted. trigger names
// the path that fired ("boot", "sighup", "watch", "api") and tags every log
// line so operators can tell them apart.
func (s *Store) Reload(trigger string) ([]Finding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, findings := Validate(s.dir)
	adopted := doc != nil
	if adopted {
		s.snap.Store(doc)
		for _, fn := range s.afterAdopt {
			fn()
		}
	}
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

// AfterAdopt registers fn to run after each subsequent successful reload,
// serialized with the reload itself. Open's boot adoption has already
// happened by the time a caller can register, so the caller applies the
// boot state itself.
func (s *Store) AfterAdopt(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.afterAdopt = append(s.afterAdopt, fn)
}

// doc returns the current snapshot. Never nil for a Store returned by Open:
// adoption happened before the Store was handed out, and a rejected reload
// leaves the previous document in place.
func (s *Store) doc() *Document {
	return s.snap.Load()
}

// Policy returns the adopted access-control policy (policies.json). nil is
// the deliberate lockout an empty document spells: every token-based
// request is denied until a policy is adopted. Satisfies policy.Source.
func (s *Store) Policy() *policy.Policy {
	return s.doc().Policy
}

// Pipe returns the adopted named query, or nil when pipes.json defines
// none by that name. Satisfies pipes.Source with Pipes.
func (s *Store) Pipe(name string) *pipes.NamedQuery {
	ps := s.doc().Pipes
	for i := range ps {
		if ps[i].Name == name {
			return &ps[i]
		}
	}
	return nil
}

// Pipes returns every adopted named query.
func (s *Store) Pipes() []*pipes.NamedQuery {
	ps := s.doc().Pipes
	out := make([]*pipes.NamedQuery, len(ps))
	for i := range ps {
		out[i] = &ps[i]
	}
	return out
}

// DedupeEnabled reports the adopted dedupe.enabled switch.
func (s *Store) DedupeEnabled() bool {
	return *s.doc().Config.Dedupe.Enabled
}

// DedupeFor resolves the effective dedupe settings for a table: the switch,
// then the table override for each field it names, the global value
// otherwise. All three resolve from one snapshot load, so a reload can never
// hand a record the id_field of one document and the require_id (or enabled)
// of another.
func (s *Store) DedupeFor(table string) (enabled bool, idField string, requireID bool) {
	d := s.doc().Config.Dedupe
	enabled, idField, requireID = *d.Enabled, *d.IDField, *d.RequireID
	if td, ok := d.Tables[table]; ok {
		if td.IDField != nil {
			idField = *td.IDField
		}
		if td.RequireID != nil {
			requireID = *td.RequireID
		}
	}
	return enabled, idField, requireID
}

// ClickHouse is the adopted connection wiring, resolved as one value from
// one snapshot so a reconnect never mixes the address of one document with
// the database of another. The password is not here — it is boot config.
type ClickHouse struct {
	Addr         string
	HTTPPort     int
	HTTPScheme   string
	Database     string
	Username     string
	QueryTimeout time.Duration
}

// ClickHouse returns the adopted ClickHouse wiring.
func (s *Store) ClickHouse() ClickHouse {
	c := s.doc().Config.ClickHouse
	return ClickHouse{
		Addr:         *c.Addr,
		HTTPPort:     *c.HTTPPort,
		HTTPScheme:   *c.HTTPScheme,
		Database:     *c.Database,
		Username:     *c.Username,
		QueryTimeout: time.Duration(*c.QueryTimeout) * time.Second,
	}
}

// Auth is the adopted verifier wiring (secrets excluded — boot config).
type Auth struct {
	JWKSURL   string
	RoleClaim string
}

// Auth returns the adopted verifier wiring.
func (s *Store) Auth() Auth {
	a := s.doc().Config.Auth
	return Auth{JWKSURL: *a.JWKSURL, RoleClaim: *a.RoleClaim}
}

// DLQFor reports whether a poison row for table is parked on the DLQ (true)
// or left unacked for redelivery (false): the table override when present,
// the global switch otherwise.
func (s *Store) DLQFor(table string) bool {
	d := s.doc().Config.DLQ
	if td, ok := d.Tables[table]; ok && td.Enabled != nil {
		return *td.Enabled
	}
	return *d.Enabled
}

// DefaultMaxRows returns the fallback result LIMIT for structured queries.
func (s *Store) DefaultMaxRows() int {
	return *s.doc().Config.Query.DefaultMaxRows
}

// TimestampBucketSeconds returns the time-range bucket structured queries
// truncate to (0 = no bucketing).
func (s *Store) TimestampBucketSeconds() int {
	return *s.doc().Config.Query.TimestampBucketSeconds
}

// Keepalive returns the SSE keepalive period and bucket count together,
// from one snapshot, so the wheel is never rebuilt from a mixed pair.
func (s *Store) Keepalive() (period time.Duration, buckets int) {
	st := s.doc().Config.Stream
	return time.Duration(*st.KeepaliveInterval) * time.Second, *st.KeepaliveBuckets
}

// GapWindow returns how much ACKed history the sweeper keeps for SSE
// gap-fill.
func (s *Store) GapWindow() time.Duration {
	return time.Duration(*s.doc().Config.Stream.GapWindowMinutes) * time.Minute
}

// MQMaxBytes returns the ingest stream's disk budget in bytes.
func (s *Store) MQMaxBytes() int64 {
	return int64(*s.doc().Config.MQ.MaxBytesGB) << 30
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
