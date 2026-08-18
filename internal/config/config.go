package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

// Config is the top-level application configuration.
type Config struct {
	// DataDir is the root for embedded state. NATS JetStream lives at
	// `<DataDir>/nats`; Pebble (when dedupe is enabled) at `<DataDir>/pebble`.
	// Subdirectory names are conventions, not config — one knob, one mount.
	// In a container this MUST resolve to a host-backed volume; the relative
	// `./data` default is fine for local binary use only.
	DataDir    string     `yaml:"data_dir" env:"WH_DATA_DIR" env-default:"./data"`
	Server     Server     `yaml:"server"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
	MQ         MQ         `yaml:"mq"`
	Dedupe     Dedupe     `yaml:"dedupe"`
	Cache      Cache      `yaml:"cache"`
	Auth       Auth       `yaml:"auth"`
	Schema     Schema     `yaml:"schema"`
	DLQ        DLQ        `yaml:"dlq"`
	Policy     Policy     `yaml:"policy"`
	Pipes      Pipes      `yaml:"pipes"`
	OTel       OTel       `yaml:"otel"`
	Prometheus Prometheus `yaml:"prometheus"`
	Query      Query      `yaml:"query"`
	Stream     Stream     `yaml:"stream"`
}

// Query holds query-shaping defaults. Server-wide *resource* limits (memory,
// rows scanned, execution time) deliberately live in ClickHouse itself — its
// settings profiles and quotas, see docs/configuration — so they apply
// uniformly to every query (including raw admin SQL) and compose with the
// per-role caps WaveHouse adds via per-query settings. This block holds only
// the result-shaping default that is genuinely WaveHouse's to own.
type Query struct {
	// DefaultMaxRows is the result LIMIT applied to a structured query when the
	// caller and policy specify none — the visible, tunable form of what used to
	// be the hard-coded query.DefaultMaxRows. 0 falls back to that constant.
	DefaultMaxRows int `yaml:"default_max_rows" env:"WH_QUERY_DEFAULT_MAX_ROWS" env-default:"10000"`
}

// OTel configures the OpenTelemetry pipeline. `enabled` is the master switch;
// when false, no signals are initialized regardless of the per-signal toggles.
// The OTLP destination — endpoint, TLS, custom CA, mutual TLS, and auth headers
// — is configured through the standard OTEL_EXPORTER_OTLP_* environment
// variables read by the OpenTelemetry SDK, not WaveHouse config. See
// docs/src/content/docs/configuration.mdx.
type OTel struct {
	Enabled bool        `yaml:"enabled" env:"WH_OTEL_ENABLED" env-default:"false"`
	Traces  OTelTraces  `yaml:"traces"`
	Metrics OTelMetrics `yaml:"metrics"`
	Logs    OTelLogs    `yaml:"logs"`
}

type OTelTraces struct {
	Enabled    bool    `yaml:"enabled" env:"WH_OTEL_TRACES_ENABLED" env-default:"true"`
	SampleRate float64 `yaml:"sample_rate" env:"WH_OTEL_TRACES_SAMPLE_RATE" env-default:"1.0"`
}

type OTelMetrics struct {
	Enabled bool `yaml:"enabled" env:"WH_OTEL_METRICS_ENABLED" env-default:"true"`
}

// Prometheus controls a Prometheus exposition endpoint served alongside (or
// independently of) the OTLP push exporter. It is its own top-level block so
// operators using Prometheus scraping (Grafana Alloy / Mimir / etc.) don't
// have to hunt through `[otel]` for an output they don't otherwise care about.
// Internally the same OTel MeterProvider drives both — the Prometheus exporter
// is an additional Reader — but the user-facing config keeps the two outputs
// distinct.
//
// Works in any of three combinations: OTel only, Prometheus only, or both. If
// only Prometheus is enabled, the MeterProvider is still created so runtime +
// custom metrics flow to `/metrics`; no OTLP push is attempted.
//
// Port `0` mounts the endpoint on the existing API server router. A non-zero
// port spins up a dedicated HTTP listener — useful for firewalling metrics
// off the public API surface in production.
type Prometheus struct {
	Enabled bool   `yaml:"enabled" env:"WH_PROMETHEUS_ENABLED" env-default:"false"`
	Path    string `yaml:"path" env:"WH_PROMETHEUS_PATH" env-default:"/metrics"`
	Port    int    `yaml:"port" env:"WH_PROMETHEUS_PORT" env-default:"0"`
}

// OTelLogs sample rate applies to OTLP export of DEBUG/INFO only.
// WARN and ERROR always export at 100% — dropping them silently during
// incidents is too dangerous to expose as a knob. Stdout receives 100% of
// records regardless of this rate (sampling for scraped-log pipelines like
// Loki/Promtail belongs at the scraper, not the application).
type OTelLogs struct {
	Enabled    bool    `yaml:"enabled" env:"WH_OTEL_LOGS_ENABLED" env-default:"true"`
	SampleRate float64 `yaml:"sample_rate" env:"WH_OTEL_LOGS_SAMPLE_RATE" env-default:"1.0"`
}

type Server struct {
	Port               int      `yaml:"port" env:"WH_SERVER_PORT" env-default:"8080"`
	ShutdownTimeout    int      `yaml:"shutdown_timeout" env:"WH_SERVER_SHUTDOWN_TIMEOUT" env-default:"10"`
	CORSAllowedOrigins []string `yaml:"cors_allowed_origins" env:"WH_SERVER_CORS_ALLOWED_ORIGINS" env-default:"*"`
}

type Stream struct {
	// KeepaliveInterval is the effective per-connection SSE keepalive period: the
	// longest a quiet GET /v1/stream connection goes without a write before the
	// server sends a ":" keepalive comment. Keep it under your proxy/tunnel idle
	// timeout (default 30s clears the common 55–60s nginx/ALB/Heroku window).
	KeepaliveInterval time.Duration `yaml:"keepalive_interval" env:"WH_STREAM_KEEPALIVE_INTERVAL" env-default:"30s"`
	// KeepaliveBuckets spreads keepalive writes across the interval so the server
	// nudges ~1/N of connections per tick instead of all at once. Advanced knob;
	// most deployments never change it.
	KeepaliveBuckets int `yaml:"keepalive_buckets" env:"WH_STREAM_KEEPALIVE_BUCKETS" env-default:"3"`
}

type ClickHouse struct {
	Addr         string        `yaml:"addr" env:"WH_CH_ADDR" env-default:"localhost:9000"`
	HTTPPort     string        `yaml:"http_port" env:"WH_CH_HTTP_PORT" env-default:"8123"`
	HTTPScheme   string        `yaml:"http_scheme" env:"WH_CH_HTTP_SCHEME" env-default:"http"`
	Database     string        `yaml:"database" env:"WH_CH_DATABASE" env-default:"default"`
	Username     string        `yaml:"username" env:"WH_CH_USERNAME" env-default:"default"`
	Password     string        `yaml:"password" env:"WH_CH_PASSWORD"`
	QueryTimeout time.Duration `yaml:"query_timeout" env:"WH_CH_QUERY_TIMEOUT" env-default:"30s"`
}

type MQ struct {
	GapWindowMinutes int `yaml:"gap_window_minutes" env:"WH_MQ_GAP_WINDOW_MINUTES" env-default:"15"`
	MaxBytesGB       int `yaml:"max_bytes_gb" env:"WH_MQ_MAX_BYTES_GB" env-default:"50"`
}

type Dedupe struct {
	Enabled   bool   `yaml:"enabled" env:"WH_DEDUPE_ENABLED" env-default:"false"`
	IDField   string `yaml:"id_field" env:"WH_DEDUPE_ID_FIELD" env-default:"event_id"`
	RequireID bool   `yaml:"require_id" env:"WH_DEDUPE_REQUIRE_ID" env-default:"false"`
}

type Cache struct {
	L1MaxCost              int64 `yaml:"l1_max_cost" env:"WH_CACHE_L1_MAX_COST" env-default:"67108864"`
	TimestampBucketSeconds int   `yaml:"timestamp_bucket_seconds" env:"WH_CACHE_TIMESTAMP_BUCKET_SECONDS" env-default:"60"`
}

// Auth configures JWT validation. There is no on/off switch: the middleware
// always runs. A request with no token, or an invalid/expired one, falls back
// to the policy default_role; elevated access requires a valid token whose role
// claim matches a granted role (or the policy admin_role). With neither
// JWTSecret nor JWKSURL set, no token can validate, so every request is the
// default role — a pure public deployment.
//
// OperatorKey is an optional non-JWT credential for the operator running the
// deployment: a request presenting it via an "Authorization: Operator <key>"
// header (or the X-Operator-Key alias) is authorized as a full-access platform
// operator, independent of the JWT verifier, and keeps working even if the
// policy is missing/deleted (break-glass recovery). Empty (the default) disables
// it. Treat it as an admin secret.
type Auth struct {
	JWTSecret   string `yaml:"jwt_secret" env:"WH_AUTH_JWT_SECRET"`
	JWKSURL     string `yaml:"jwks_url" env:"WH_AUTH_JWKS_URL"`
	RoleClaim   string `yaml:"role_claim" env:"WH_AUTH_ROLE_CLAIM" env-default:"role"`
	OperatorKey string `yaml:"operator_key" env:"WH_AUTH_OPERATOR_KEY"`
}

// Policy configures the access control policy engine.
//
// FilePath has no default on purpose. When set, the file MUST exist and parse
// cleanly — policy.NewStore returns a fatal error otherwise, so a typo or a
// missing mount surfaces as a refused boot instead of a silent fail-closed
// (every request 403s, including admin). When left empty, the store comes up
// with no cached policy and the operator seeds via PUT /v1/ops/policy.
//
// A baked-in default like "policy.yaml" would re-introduce the silent-lockout
// failure mode for any deployment that didn't ship that exact file at CWD,
// and it would imply a convention the operator never opted into — keep the
// path an explicit choice.
type Policy struct {
	FilePath string `yaml:"file_path" env:"WH_POLICY_FILE_PATH"`
}

// Pipes configures named query pipes.
//
// Dir is an OPTIONAL bootstrap source: on startup, any `.sql` files in it are
// loaded into the NATS KV pipe store. After bootstrap, the API/KV is the
// authoritative store — the directory is read-only at runtime and not
// rewritten. Empty default skips bootstrap entirely (most users will create
// pipes via the API). When set, mount the directory read-only in containers
// (e.g. `./my-pipes:/app/pipes:ro`) so it's clear it's a seed, not state.
type Pipes struct {
	Dir string `yaml:"dir" env:"WH_PIPES_DIR" env-default:""`
}

// Schema configures ClickHouse schema discovery.
type Schema struct {
	RefreshInterval int `yaml:"refresh_interval" env:"WH_SCHEMA_REFRESH_INTERVAL" env-default:"60"` // seconds
}

// DLQ configures the Dead Letter Queue for failed batch inserts.
type DLQ struct {
	Enabled bool `yaml:"enabled" env:"WH_DLQ_ENABLED" env-default:"true"`
}

// Validate checks the loaded configuration for logical consistency.
func (c *Config) Validate() error {
	if c.ClickHouse.HTTPScheme != "http" && c.ClickHouse.HTTPScheme != "https" {
		return fmt.Errorf("clickhouse.http_scheme must be 'http' or 'https'")
	}

	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port %d out of range 1-65535", c.Server.Port)
	}

	if c.Server.ShutdownTimeout < 0 {
		return fmt.Errorf("server.shutdown_timeout must be non-negative")
	}

	if c.Stream.KeepaliveInterval < 0 {
		return fmt.Errorf("stream.keepalive_interval must be non-negative, got %s", c.Stream.KeepaliveInterval)
	}
	if c.Stream.KeepaliveBuckets < 0 {
		return fmt.Errorf("stream.keepalive_buckets must be non-negative, got %d", c.Stream.KeepaliveBuckets)
	}

	if c.Schema.RefreshInterval < 1 {
		return fmt.Errorf("schema.refresh_interval must be >= 1 second")
	}

	if c.ClickHouse.QueryTimeout <= time.Duration(0) {
		return fmt.Errorf("clickhouse.query_timeout must be > 0, got %s", c.ClickHouse.QueryTimeout)
	}

	// query.default_max_rows is the fallback result LIMIT. 0 (or a directly-built
	// config that omits it) means "use the built-in query.DefaultMaxRows" — the
	// builder substitutes the constant for any non-positive value — so only a
	// negative value is an error.
	if c.Query.DefaultMaxRows < 0 {
		return fmt.Errorf("query.default_max_rows must be non-negative, got %d", c.Query.DefaultMaxRows)
	}

	if c.MQ.GapWindowMinutes < 0 {
		return fmt.Errorf("mq.gap_window_minutes must be non-negative")
	}

	if c.OTel.Enabled {
		// Only validate sample rates for signals that are actually enabled —
		// an unused field shouldn't block startup.
		if c.OTel.Traces.Enabled {
			if r := c.OTel.Traces.SampleRate; r < 0 || r > 1 {
				return fmt.Errorf("otel.traces.sample_rate %g out of range [0.0, 1.0]", r)
			}
		}
		if c.OTel.Logs.Enabled {
			if r := c.OTel.Logs.SampleRate; r < 0 || r > 1 {
				return fmt.Errorf("otel.logs.sample_rate %g out of range [0.0, 1.0]", r)
			}
		}
	}

	// Prometheus exposition is independent of OTel — operators can run it
	// standalone (Alloy scrape, no OTLP push) or alongside OTLP push.
	if c.Prometheus.Enabled {
		p := c.Prometheus
		if p.Port < 0 || p.Port > 65535 {
			return fmt.Errorf("prometheus.port %d out of range [0, 65535]", p.Port)
		}
		if p.Port != 0 && p.Port == c.Server.Port {
			return fmt.Errorf("prometheus.port collides with server.port (%d); use 0 to mount on the API server or pick a different port", p.Port)
		}
		if !strings.HasPrefix(p.Path, "/") {
			return fmt.Errorf("prometheus.path %q must start with '/'", p.Path)
		}
		// Chi registers /metrics before the health routes — a collision here
		// would silently shadow the probe (first-registered-wins) rather than
		// erroring at boot. Fail loud so the misconfig is debuggable. Covers
		// the canonical /livez, /readyz and the deprecated /healthz, /health,
		// /ready aliases.
		for _, reserved := range []string{"/livez", "/readyz", "/healthz", "/health", "/ready"} {
			if p.Path == reserved {
				return fmt.Errorf("prometheus.path %q conflicts with reserved endpoint", p.Path)
			}
		}
		// Same-port mode mounts the (unauthenticated) metrics handler on the
		// main router. A path inside the /v1 namespace would shadow the
		// authenticated API subtree with a public handler — worse than the
		// /livez shadow case because it leaks at an authenticated-looking URL.
		if p.Port == 0 && (p.Path == "/v1" || strings.HasPrefix(p.Path, "/v1/")) {
			return fmt.Errorf("prometheus.path %q conflicts with authenticated /v1 API namespace when prometheus.port is 0", p.Path)
		}
	}

	return nil
}

// Load reads config from a YAML file (if it exists) with env var overrides.
func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := os.Stat(path); err == nil {
		if err := cleanenv.ReadConfig(path, &cfg); err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
	} else {
		if err := cleanenv.ReadEnv(&cfg); err != nil {
			return nil, fmt.Errorf("read env: %w", err)
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}
