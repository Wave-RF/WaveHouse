package settings

import (
	"maps"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Store holds the settings snapshot one tenant has adopted, and nothing
// else: the Registry validates, swaps the document in, and owns every reload
// trigger. Readers go through one lock-free atomic load per lookup; the typed
// accessors below each resolve from a single snapshot load, so a reload lands
// between lookups, never inside one.
//
// There are no compiled defaults here on purpose, but one: every key but
// dedupe.retention (missing means "0", forever) is required by Validate, so
// the snapshot is exactly what the files said when they were adopted. Defaults live in the seed directory (Seed / WriteSeed).
type Store struct {
	// tenant is the id the Registry created the store for; the zero value
	// only for a Store built outside a Registry (tests).
	tenant tenant.ID
	snap   atomic.Pointer[Document]
}

// Tenant returns the id of the tenant this store holds the settings of: how
// a handler holding the request's store names its tenant to a per-tenant
// resource (#583) without a second read of the context.
func (s *Store) Tenant() tenant.ID { return s.tenant }

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

// Dedupe is a table's effective dedupe settings.
type Dedupe struct {
	Enabled   bool
	IDField   string
	RequireID bool
	// Retention is how long a committed id stays a duplicate; 0 is forever.
	Retention time.Duration
}

// DedupeFor resolves the effective dedupe settings for a table: the switch,
// then the table override for each field it names, the global value
// otherwise. Every field resolves from one snapshot load, so a reload can
// never hand a record the id_field of one document and the require_id,
// retention or switch of another.
func (s *Store) DedupeFor(table string) Dedupe {
	d := s.doc().Config.Dedupe
	out := Dedupe{Enabled: *d.Enabled, IDField: *d.IDField, RequireID: *d.RequireID}
	retention := "0"
	if d.Retention != nil {
		retention = *d.Retention
	}
	if td, ok := d.Tables[table]; ok {
		if td.IDField != nil {
			out.IDField = *td.IDField
		}
		if td.RequireID != nil {
			out.RequireID = *td.RequireID
		}
		if td.Retention != nil {
			retention = *td.Retention
		}
	}
	// Validate has parsed it already.
	out.Retention, _ = time.ParseDuration(retention)
	return out
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
	TLS          TLS
	// Headers is this reader's own copy of the HTTP-interface headers.
	Headers      map[string]string
	MaxOpenConns int
	MaxIdleConns int
}

// TLS is the adopted `clickhouse.tls` block (see ClickHouseTLS).
type TLS struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
	ServerName         string
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
		TLS: TLS{
			Enabled:            *c.TLS.Enabled,
			CAFile:             *c.TLS.CAFile,
			CertFile:           *c.TLS.CertFile,
			KeyFile:            *c.TLS.KeyFile,
			InsecureSkipVerify: *c.TLS.InsecureSkipVerify,
			ServerName:         *c.TLS.ServerName,
		},
		Headers:      maps.Clone(c.Headers),
		MaxOpenConns: *c.MaxOpenConns,
		MaxIdleConns: *c.MaxIdleConns,
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

// MQMaxBytes returns the disk budget of the tenant's ingest queue in bytes.
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
