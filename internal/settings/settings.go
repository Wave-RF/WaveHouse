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

// TenantConfig is the shape of config.json: the tenant-owned behavioral tunables
// that migrate out of boot config (config.yaml/env keeps what the platform
// operator owns: wiring, lifecycle, secrets — and platform-infra knobs like
// the SSE keepalives, which exist for the deployment's proxies, not the
// tenant). Every block and every top-level key inside it is REQUIRED: the
// binary carries no compiled defaults, so the adopted snapshot is exactly
// what the files say. Defaults live in the seed directory (see Seed) that
// `wavehouse init-settings` writes. The fields are pointers only so Validate
// can tell "absent" from the zero value and report it by path.
type TenantConfig struct {
	Dedupe *DedupeConfig `json:"dedupe"`
	Query  *QueryConfig  `json:"query"`
	Schema *SchemaConfig `json:"schema"`
	CORS   *CORSConfig   `json:"cors"`
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

// QueryConfig holds query-shaping defaults. Server-wide *resource* limits
// (memory, rows scanned, execution time) deliberately live in ClickHouse
// itself — its settings profiles and quotas — so they apply uniformly to
// every query; this block holds only the result-LIMIT default.
type QueryConfig struct {
	// DefaultMaxRows is the result LIMIT applied to a structured query when
	// the caller and policy specify none. Must be >= 1.
	DefaultMaxRows *int `json:"default_max_rows"`
}

// SchemaConfig tunes ClickHouse schema discovery.
type SchemaConfig struct {
	// RefreshInterval is the auto-refresh period in seconds. Must be >= 1.
	RefreshInterval *int `json:"refresh_interval"`
}

// CORSConfig carries the per-request CORS allowlist. ["*"] allows any
// browser origin; see corsMiddleware in internal/api for why that is safe
// for a Bearer-token API.
type CORSConfig struct {
	AllowedOrigins []string `json:"allowed_origins"`
}
