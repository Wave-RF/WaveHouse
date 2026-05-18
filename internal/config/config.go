package config

import (
	"fmt"
	"os"
	"strings"

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
}

// OTel configures the OpenTelemetry pipeline. See docs/configuration.md for
// scheme/headers/per-signal-override semantics.
type OTel struct {
	Enabled bool        `yaml:"enabled" env:"WH_OTEL_ENABLED" env-default:"false"`
	Addr    string      `yaml:"addr" env:"WH_OTEL_ADDR" env-default:"127.0.0.1:4317"`
	Headers string      `yaml:"headers" env:"WH_OTEL_HEADERS"`
	Traces  OTelTraces  `yaml:"traces"`
	Metrics OTelMetrics `yaml:"metrics"`
	Logs    OTelLogs    `yaml:"logs"`
}

type OTelTraces struct {
	Enabled    bool    `yaml:"enabled" env:"WH_OTEL_TRACES_ENABLED" env-default:"true"`
	SampleRate float64 `yaml:"sample_rate" env:"WH_OTEL_TRACES_SAMPLE_RATE" env-default:"1.0"`
	Addr       string  `yaml:"addr" env:"WH_OTEL_TRACES_ADDR"`
}

type OTelMetrics struct {
	Enabled bool   `yaml:"enabled" env:"WH_OTEL_METRICS_ENABLED" env-default:"true"`
	Addr    string `yaml:"addr" env:"WH_OTEL_METRICS_ADDR"`
}

// Prometheus configures the Prometheus exposition endpoint. Independent of
// OTLP push — works alone or alongside. Port 0 mounts on the API router; a
// non-zero port spins a dedicated listener. See docs/configuration.md.
type Prometheus struct {
	Enabled bool   `yaml:"enabled" env:"WH_PROMETHEUS_ENABLED" env-default:"false"`
	Path    string `yaml:"path" env:"WH_PROMETHEUS_PATH" env-default:"/metrics"`
	Port    int    `yaml:"port" env:"WH_PROMETHEUS_PORT" env-default:"0"`
}

// OTelLogs.SampleRate applies to DEBUG/INFO OTLP export only.
// WARN/ERROR always export at 100% (non-configurable); stdout is always 100%.
type OTelLogs struct {
	Enabled    bool    `yaml:"enabled" env:"WH_OTEL_LOGS_ENABLED" env-default:"true"`
	SampleRate float64 `yaml:"sample_rate" env:"WH_OTEL_LOGS_SAMPLE_RATE" env-default:"1.0"`
	Addr       string  `yaml:"addr" env:"WH_OTEL_LOGS_ADDR"`
}

type Server struct {
	Port               int      `yaml:"port" env:"WH_SERVER_PORT" env-default:"8080"`
	ShutdownTimeout    int      `yaml:"shutdown_timeout" env:"WH_SERVER_SHUTDOWN_TIMEOUT" env-default:"10"`
	CORSAllowedOrigins []string `yaml:"cors_allowed_origins" env:"WH_SERVER_CORS_ALLOWED_ORIGINS" env-default:"*"`
}

type ClickHouse struct {
	Addr       string `yaml:"addr" env:"WH_CH_ADDR" env-default:"localhost:9000"`
	HTTPPort   string `yaml:"http_port" env:"WH_CH_HTTP_PORT" env-default:"8123"`
	HTTPScheme string `yaml:"http_scheme" env:"WH_CH_HTTP_SCHEME" env-default:"http"`
	Database   string `yaml:"database" env:"WH_CH_DATABASE" env-default:"default"`
	Username   string `yaml:"username" env:"WH_CH_USERNAME" env-default:"default"`
	Password   string `yaml:"password" env:"WH_CH_PASSWORD"`
}

type MQ struct {
	GapWindowMinutes int `yaml:"gap_window_minutes" env:"WH_MQ_GAP_WINDOW_MINUTES" env-default:"15"`
	MaxBytesGB       int `yaml:"max_bytes_gb" env:"WH_MQ_MAX_BYTES_GB" env-default:"50"`
}

type Dedupe struct {
	Enabled bool   `yaml:"enabled" env:"WH_DEDUPE_ENABLED" env-default:"false"`
	IDField string `yaml:"id_field" env:"WH_DEDUPE_ID_FIELD" env-default:"event_id"`
}

type Cache struct {
	L1MaxCost              int64 `yaml:"l1_max_cost" env:"WH_CACHE_L1_MAX_COST" env-default:"67108864"`
	DefaultTTL             int   `yaml:"default_ttl" env:"WH_CACHE_DEFAULT_TTL" env-default:"300"`
	TimestampBucketSeconds int   `yaml:"timestamp_bucket_seconds" env:"WH_CACHE_TIMESTAMP_BUCKET_SECONDS" env-default:"60"`
}

type Auth struct {
	Enabled   bool   `yaml:"enabled" env:"WH_AUTH_ENABLED" env-default:"false"`
	JWTSecret string `yaml:"jwt_secret" env:"WH_AUTH_JWT_SECRET"`
	JWKSURL   string `yaml:"jwks_url" env:"WH_AUTH_JWKS_URL"`
	RoleClaim string `yaml:"role_claim" env:"WH_AUTH_ROLE_CLAIM" env-default:"role"`
	DevMode   bool   `yaml:"dev_mode" env:"WH_AUTH_DEV_MODE" env-default:"false"`
}

// Policy configures the access control policy engine.
type Policy struct {
	FilePath string `yaml:"file_path" env:"WH_POLICY_FILE_PATH" env-default:"policy.yaml"`
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

// validateOTelHeaders mirrors observability.ParseOTelHeaders for boot-time
// validation. Hand-mirrored to keep config out of the OTel SDK's import graph;
// the parity is pinned by internal/config/header_parity_test.go.
func validateOTelHeaders(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	seen := map[string]struct{}{}
	for _, seg := range strings.Split(s, ",") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		// Error messages quote only the key, never the segment — `seg`
		// can contain a real API token.
		i := strings.IndexByte(seg, '=')
		if i < 0 {
			return fmt.Errorf("header entry missing '=' separator (format is key=value)")
		}
		key := strings.TrimSpace(seg[:i])
		if key == "" {
			return fmt.Errorf("header entry has empty key (format is key=value)")
		}
		if bad, ok := firstNonHeaderTokenChar(key); !ok {
			return fmt.Errorf("header key %q has invalid character %q (RFC 7230 token: letters, digits, and %s)", key, bad, headerNamePunctuation)
		}
		if strings.TrimSpace(seg[i+1:]) == "" {
			return fmt.Errorf("header key %q has empty or whitespace-only value", key)
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("header key %q appears more than once", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// headerNamePunctuation lists the punctuation characters legal in an RFC
// 7230 `token`. Mirrors the const of the same name in observability/endpoint.go.
const headerNamePunctuation = "!#$%&'*+-.^_`|~"

// firstNonHeaderTokenChar mirrors observability.firstNonTokenChar.
func firstNonHeaderTokenChar(s string) (rune, bool) {
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case strings.ContainsRune(headerNamePunctuation, c):
		default:
			return c, false
		}
	}
	return 0, true
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

	if c.Auth.Enabled && !c.Auth.DevMode && c.Auth.JWTSecret == "" && c.Auth.JWKSURL == "" {
		return fmt.Errorf("auth.enabled requires at least one of jwt_secret or jwks_url (or enable dev_mode)")
	}

	if c.Schema.RefreshInterval < 1 {
		return fmt.Errorf("schema.refresh_interval must be >= 1 second")
	}

	if c.Cache.DefaultTTL < 0 {
		return fmt.Errorf("cache.default_ttl must be non-negative")
	}

	if c.MQ.GapWindowMinutes < 0 {
		return fmt.Errorf("mq.gap_window_minutes must be non-negative")
	}

	if c.OTel.Enabled {
		if c.OTel.Addr == "" {
			return fmt.Errorf("otel.addr must be non-empty when otel.enabled is true")
		}
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
		// Fail at boot rather than silently dropping auth in production.
		if err := validateOTelHeaders(c.OTel.Headers); err != nil {
			return fmt.Errorf("otel.headers: %w", err)
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
		// erroring at boot. Fail loud so the misconfig is debuggable.
		for _, reserved := range []string{"/health", "/ready"} {
			if p.Path == reserved {
				return fmt.Errorf("prometheus.path %q conflicts with reserved endpoint", p.Path)
			}
		}
		// Same-port mode would shadow the authenticated /v1 subtree with an
		// unauthenticated metrics handler — leaks at an authenticated-looking URL.
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
