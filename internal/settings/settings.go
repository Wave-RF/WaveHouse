// Package settings defines the on-disk settings directory — the JSON documents
// that hold WaveHouse's hot-reloadable configuration — and Validate, the single
// gate every consumer of that directory runs before adopting its contents.
//
// The directory holds exactly four files: roles.json (the role registry),
// policies.json (the access-control policy), pipes.json (named queries), and
// config.json (behavioral tunables migrated out of boot config). Validate is
// deliberately pure — no network, no ClickHouse, no side effects — so the same
// function can gate the `wavehouse validate` CLI, boot, and a live reload.
// Table/column existence is out of scope by design: WaveHouse is
// Bring-Your-Own-Schema, and schema discovery owns what exists in ClickHouse.
package settings

import (
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// The four settings files. Any other entry in the directory is rejected — a
// typoed filename must fail loudly, not be silently ignored. The one
// exception is dot-prefixed entries (vim swap files, the `..data` machinery
// Kubernetes ConfigMap mounts publish through), which are skipped.
const (
	FileRoles    = "roles.json"
	FilePolicies = "policies.json"
	FilePipes    = "pipes.json"
	FileConfig   = "config.json"
)

// Files returns every expected file. All four must exist (an empty document
// is written as {}), so an accidental deletion reads as an error, never as
// "defaults". A function rather than a package variable because Go has no
// const slices — this keeps the set genuinely immutable.
func Files() []string {
	return []string{FileRoles, FilePolicies, FilePipes, FileConfig}
}

// Document is a fully parsed and validated settings directory.
type Document struct {
	// Roles is the role registry every role reference is checked against.
	Roles []string
	// Policy is nil when policies.json is an empty document — no policy,
	// fail closed (matching the deleted-policy semantics of the store).
	Policy *policy.Policy
	Pipes  []pipes.NamedQuery
	Config TenantConfig
}

// RolesFile is the shape of roles.json.
type RolesFile struct {
	Roles []string `json:"roles"`
}

// PipesFile is the shape of pipes.json.
type PipesFile struct {
	Pipes []pipes.NamedQuery `json:"pipes"`
}

// TenantConfig is the shape of config.json: the behavioral tunables that
// migrate out of boot config. Boot config (config.yaml/env) keeps only what
// cannot change under a running process — resource sizing (`data_dir`,
// `mq.max_bytes_gb`, `cache.l1_max_cost`), listeners, the observability
// exporters — and the secrets (`clickhouse.password`, `auth.jwt_secret`,
// `auth.operator_key`), which never belong in a tracked JSON file. Every
// block and every top-level key inside it is REQUIRED: the binary carries no
// compiled defaults, so the adopted snapshot is exactly what the files say.
// Defaults live in the seed directory (see Seed) that `wavehouse
// bootstrap` writes. The fields are pointers only so Validate can tell
// "absent" from the zero value and report it by path.
type TenantConfig struct {
	ClickHouse *ClickHouseConfig `json:"clickhouse"`
	Auth       *AuthConfig       `json:"auth"`
	Dedupe     *DedupeConfig     `json:"dedupe"`
	DLQ        *DLQConfig        `json:"dlq"`
	Query      *QueryConfig      `json:"query"`
	Schema     *SchemaConfig     `json:"schema"`
	Stream     *StreamConfig     `json:"stream"`
	CORS       *CORSConfig       `json:"cors"`
}

// ClickHouseConfig is the ClickHouse wiring minus the password (boot config
// `clickhouse.password` / WH_CH_PASSWORD, a secret). A reload that changes
// any of it swaps the connection unconditionally; reachability is a runtime
// concern (schema discovery, /readyz), never a reload one.
type ClickHouseConfig struct {
	// Addr is the native-protocol host:port (schema discovery, structured
	// queries, pipes, /readyz).
	Addr *string `json:"addr"`
	// HTTPPort and HTTPScheme address the HTTP interface (ingest INSERTs and
	// the raw-SQL proxy) on the same host as Addr.
	HTTPPort   *int    `json:"http_port"`
	HTTPScheme *string `json:"http_scheme"`
	Database   *string `json:"database"`
	Username   *string `json:"username"`
	// QueryTimeout is the read deadline in seconds (>= 1).
	QueryTimeout *int `json:"query_timeout"`
}

// AuthConfig is the JWT verifier wiring minus the secrets (boot config
// `auth.jwt_secret` and `auth.operator_key`). A reload rebuilds the
// verifier unconditionally; a JWKS endpoint that can't be fetched fails
// closed until it can.
type AuthConfig struct {
	// JWKSURL, when non-empty, makes JWKS the sole verifier (the HMAC
	// secret is then ignored). Must be an absolute http(s) URL.
	JWKSURL *string `json:"jwks_url"`
	// RoleClaim is the dot-separated claim path the role is read from.
	RoleClaim *string `json:"role_claim"`
}

// DedupeConfig tunes dedupe behavior, including the switch itself: a reload
// that flips enabled opens or closes the embedded Pebble store on the fly
// (dedupe.Managed), so the whole block is tenant-owned.
//
// id_field and require_id are required here and optional per table: a table
// override inherits whichever field it doesn't name. An empty,
// whitespace-only, or whitespace-padded id_field is rejected at every level,
// so the effective id_field can never be empty or silently unmatchable.
type DedupeConfig struct {
	Enabled   *bool   `json:"enabled"`
	IDField   *string `json:"id_field"`
	RequireID *bool   `json:"require_id"`
	// Tables holds per-table overrides keyed by ClickHouse table name (#222).
	// Names are format-checked only — existence is schema discovery's runtime
	// concern, same as policies.json table keys.
	Tables map[string]TableDedupe `json:"tables,omitempty"`
}

// TableDedupe is one table's dedupe override; it overrides only the fields it
// names.
type TableDedupe struct {
	IDField   *string `json:"id_field,omitempty"`
	RequireID *bool   `json:"require_id,omitempty"`
}

// DLQConfig gates the Dead Letter Queue: whether a row that still fails
// after the row-by-row isolation retry is parked on the WAVEHOUSE_DLQ stream
// (and its original acked) or left unacked to be redelivered indefinitely.
// The stream itself always exists — it is an empty limits-policy stream
// until something lands on it — so the switch is purely behavioral and
// resolves per table through the same override cascade as dedupe.
type DLQConfig struct {
	Enabled *bool `json:"enabled"`
	// Tables holds per-table overrides keyed by ClickHouse table name.
	Tables map[string]TableDLQ `json:"tables,omitempty"`
}

// TableDLQ is one table's DLQ override.
type TableDLQ struct {
	Enabled *bool `json:"enabled,omitempty"`
}

// QueryConfig holds query-shaping defaults. Server-wide *resource* limits
// (memory, rows scanned, execution time) deliberately live in ClickHouse
// itself — its settings profiles and quotas — so they apply uniformly to
// every query; this block holds the result-LIMIT default and the time-range
// normalization that drives cache hits.
type QueryConfig struct {
	// DefaultMaxRows is the result LIMIT applied to a structured query when
	// the caller and policy specify none. Must be >= 1.
	DefaultMaxRows *int `json:"default_max_rows"`
	// TimestampBucketSeconds truncates a structured query's relative time
	// range to this bucket so near-identical queries share a cache entry.
	// Must be >= 0; 0 disables bucketing.
	TimestampBucketSeconds *int `json:"timestamp_bucket_seconds"`
}

// SchemaConfig tunes ClickHouse schema discovery.
type SchemaConfig struct {
	// RefreshInterval is the auto-refresh period in seconds. Must be >= 1.
	RefreshInterval *int `json:"refresh_interval"`
}

// StreamConfig tunes GET /v1/stream: the SSE keepalive wheel and how much
// NATS history the Active Sweeper keeps for gap-fill. Keepalives are
// per-deployment-proxy knobs and the gap window is a per-tenant replay
// budget, both of which change while the server runs.
type StreamConfig struct {
	// KeepaliveInterval is the effective per-connection keepalive period in
	// seconds — the longest a quiet stream goes unwritten before a ":"
	// comment. Must be >= 1. A reload rebuilds the wheel in place; live
	// connections keep streaming.
	KeepaliveInterval *int `json:"keepalive_interval"`
	// KeepaliveBuckets spreads the keepalive writes across the interval so
	// each tick nudges ~1/N of live streams. Must be >= 1.
	KeepaliveBuckets *int `json:"keepalive_buckets"`
	// GapWindowMinutes is how many minutes of ACKed messages the sweeper
	// keeps in NATS for SSE gap-fill (Last-Event-ID replay). Must be >= 0;
	// applies from the next sweep.
	GapWindowMinutes *int `json:"gap_window_minutes"`
}

// CORSConfig carries the per-request CORS allowlist. ["*"] allows any

// browser origin; see corsMiddleware in internal/api for why that is safe
// for a Bearer-token API.
type CORSConfig struct {
	AllowedOrigins []string `json:"allowed_origins"`
}
