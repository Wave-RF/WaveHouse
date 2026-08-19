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

// The four settings files. Anything else ending in .json in the directory is
// rejected — a typoed filename must fail loudly, not be silently ignored.
const (
	FileRoles    = "roles.json"
	FilePolicies = "policies.json"
	FilePipes    = "pipes.json"
	FileConfig   = "config.json"
)

// Files lists every expected file. All four must exist (an empty document is
// written as {}), so an accidental deletion reads as an error, never as
// "defaults".
var Files = []string{FileRoles, FilePolicies, FilePipes, FileConfig}

// EnvDir is the environment variable naming the settings directory.
const EnvDir = "WH_SETTINGS_DIR"

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
// tenant). Every field is a pointer or slice — absent means "compiled
// default", so validation only judges values the author actually wrote.
type TenantConfig struct {
	Dedupe *DedupeConfig `json:"dedupe,omitempty"`
	Query  *QueryConfig  `json:"query,omitempty"`
	Schema *SchemaConfig `json:"schema,omitempty"`
	CORS   *CORSConfig   `json:"cors,omitempty"`
}

// DedupeConfig tunes dedupe behavior. dedupe.enabled stays boot config: it
// owns Pebble's lifecycle, which only a restart can change.
//
// Each field resolves independently through a three-level cascade: table
// override → the global value here → compiled default. Absent means inherit,
// and an explicit empty, whitespace-only, or whitespace-padded id_field is
// rejected at every level, so the effective id_field can never be empty or
// silently unmatchable — require_id without an id_field simply requires the
// inherited one.
type DedupeConfig struct {
	IDField   *string `json:"id_field,omitempty"`
	RequireID *bool   `json:"require_id,omitempty"`
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

// QueryConfig mirrors the boot config Query block.
type QueryConfig struct {
	DefaultMaxRows *int `json:"default_max_rows,omitempty"`
}

// SchemaConfig mirrors the boot config Schema block (seconds).
type SchemaConfig struct {
	RefreshInterval *int `json:"refresh_interval,omitempty"`
}

// CORSConfig carries the per-request CORS allowlist (boot config
// server.cors_allowed_origins today).
type CORSConfig struct {
	AllowedOrigins []string `json:"allowed_origins,omitempty"`
}
