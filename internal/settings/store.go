package settings

import (
	"maps"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/pkg/pipes"
	"github.com/Wave-RF/WaveHouse/pkg/policy"
	"github.com/Wave-RF/WaveHouse/pkg/settings"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// Store holds the settings snapshot one tenant has adopted, and nothing
// else: the Registry validates, swaps the document in, and owns every reload
// trigger. Readers go through one lock-free atomic load per lookup; the typed
// accessors below each resolve from a single snapshot load, so a reload lands
// between lookups, never inside one.
//
// There are no compiled defaults here on purpose, but one: every key but
// dedupe.retention (missing means "0", forever) is required by Validate, so
// the snapshot is exactly what the files said when they were adopted.
// Defaults live in the seed directory (Seed / WriteSeed).
type Store struct {
	// tenant is the id the Registry created the store for; the zero value
	// only for a Store built outside a Registry (tests).
	tenant tenant.ID
	snap   atomic.Pointer[settings.Document]
}

// Tenant returns the id of the tenant this store holds the settings of: how
// a handler holding the request's store names its tenant to a per-tenant
// resource (#583) without a second read of the context.
func (s *Store) Tenant() tenant.ID { return s.tenant }

// adopt swaps in a validated document. The Registry calls it under its
// reload lock.
func (s *Store) adopt(doc *settings.Document) { s.snap.Store(doc) }

// doc returns the current snapshot. Never nil for a Store a Registry from
// Open hands out: adoption happened first, and a rejected reload leaves the
// previous document in place.
func (s *Store) doc() *settings.Document {
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

// DedupeRetentions is the retention of the default (key "") and of every
// table override that names one, from one snapshot; 0 is forever.
func (s *Store) DedupeRetentions() map[string]time.Duration {
	d := s.doc().Config.Dedupe
	out := map[string]time.Duration{"": 0}
	// Validate has parsed them already.
	if d.Retention != nil {
		out[""], _ = time.ParseDuration(*d.Retention)
	}
	for table, td := range d.Tables {
		if td.Retention != nil {
			out[table], _ = time.ParseDuration(*td.Retention)
		}
	}
	return out
}

// ClickHouse is the adopted connection wiring, resolved as one value from
// one snapshot so a reconnect never mixes the address of one document with
// the database of another.
type ClickHouse struct {
	Addr         string
	HTTPPort     int
	HTTPScheme   string
	Database     string
	Username     string
	Password     string
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
		Password:     *c.Password,
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

// Auth is the adopted verifier wiring, HMAC secret included (the operator
// key is boot config).
type Auth struct {
	JWKSURL   string
	JWTSecret string
	RoleClaim string
}

// Auth returns the adopted verifier wiring.
func (s *Store) Auth() Auth {
	a := s.doc().Config.Auth
	return Auth{JWKSURL: *a.JWKSURL, JWTSecret: *a.JWTSecret, RoleClaim: *a.RoleClaim}
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

// Ingest is the adopted batching of the tenant's tables and the deadline of
// its inserts, resolved as one value from one snapshot so a batch is never
// sized by one document and timed by another, and so the two the queue's
// redelivery timeout must cover (internal/app checks them against boot
// config's mq.ack_wait) come from one document.
type Ingest struct {
	// MaxBatchBytes flushes a table's batch once its rows' JSON reaches it.
	MaxBatchBytes int
	// MaxBatchWait flushes a batch this long after its first row.
	MaxBatchWait time.Duration
	// InsertTimeout is the deadline of each INSERT (clickhouse.insert_timeout).
	InsertTimeout time.Duration
	// RetryBackoffMax caps the retry backoff of a ClickHouse that cannot take
	// the tenant's inserts.
	RetryBackoffMax time.Duration
}

// Ingest returns the adopted ingest batching, insert deadline and retry cap.
func (s *Store) Ingest() Ingest {
	c := s.doc().Config
	return Ingest{
		MaxBatchBytes:   *c.Ingest.MaxBatchBytes,
		MaxBatchWait:    time.Duration(*c.Ingest.MaxBatchWaitMS) * time.Millisecond,
		InsertTimeout:   time.Duration(*c.ClickHouse.InsertTimeout) * time.Second,
		RetryBackoffMax: time.Duration(*c.Ingest.RetryBackoffMax) * time.Second,
	}
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

// SubscriberQueue returns the outbound queue, in frames, of each SSE
// connection opened from now on.
func (s *Store) SubscriberQueue() int {
	return *s.doc().Config.Stream.SubscriberQueue
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
