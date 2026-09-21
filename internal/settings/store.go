package settings

import (
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// Store holds the settings snapshot one tenant has adopted, and nothing
// else: the Registry validates, swaps the document in, and owns every reload
// trigger. Readers go through one lock-free atomic load per lookup; the typed
// accessors below each resolve from a single snapshot load, so a reload lands
// between lookups, never inside one.
//
// There are no compiled defaults here on purpose: every key is required by
// Validate, so the snapshot is exactly what the files said when they were
// adopted. Defaults live in the seed directory (Seed / WriteSeed).
type Store struct {
	snap atomic.Pointer[Document]
}

// adopt swaps in a validated document. The Registry calls it under its
// reload lock.
func (s *Store) adopt(doc *Document) { s.snap.Store(doc) }

// doc returns the current snapshot. Never nil for a Store a Registry from
// Open hands out: adoption happened first, and a rejected reload leaves the
// previous document in place.
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

// CORSOrigins returns the allowed CORS origins; an empty list denies every
// browser origin and ["*"] is the only allow-all spelling.
func (s *Store) CORSOrigins() []string {
	return s.doc().Config.CORS.AllowedOrigins
}
