package config

import (
	"fmt"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

// Config is the top-level application configuration.
type Config struct {
	// DataDir is the root for embedded state. NATS JetStream lives at
	// `<DataDir>/nats`; Pebble (when dedupe is enabled) at `<DataDir>/pebble`.
	// Subdirectory names are conventions, not config — one knob, one mount.
	// In a container this MUST resolve to a host-backed volume; the relative
	// `./data` default is fine for local binary use only.
	DataDir       string        `yaml:"data_dir" env:"WH_DATA_DIR" env-default:"./data"`
	Server        Server        `yaml:"server"`
	ClickHouse    ClickHouse    `yaml:"clickhouse"`
	MQ            MQ            `yaml:"mq"`
	Dedupe        Dedupe        `yaml:"dedupe"`
	Cache         Cache         `yaml:"cache"`
	Auth          Auth          `yaml:"auth"`
	Schema        Schema        `yaml:"schema"`
	DLQ           DLQ           `yaml:"dlq"`
	Policy        Policy        `yaml:"policy"`
	Pipes         Pipes         `yaml:"pipes"`
	Observability Observability `yaml:"observability"`
}

type Observability struct {
	Enabled  bool                 `yaml:"enabled" env:"WH_OBSERVABILITY_ENABLED" env-default:"false"`
	OTelAddr string               `yaml:"otel_addr" env:"WH_OTEL_ADDR" env-default:"127.0.0.1:4317"`
	Traces   ObservabilityTraces  `yaml:"traces"`
	Metrics  ObservabilityMetrics `yaml:"metrics"`
	Logs     ObservabilityLogs    `yaml:"logs"`
}

type ObservabilityTraces struct {
	Enabled    bool    `yaml:"enabled" env:"WH_OBS_TRACES_ENABLED" env-default:"true"`
	SampleRate float64 `yaml:"sample_rate" env:"WH_OBS_TRACES_SAMPLE_RATE" env-default:"0.10"`
}

type ObservabilityMetrics struct {
	Enabled bool `yaml:"enabled" env:"WH_OBS_METRICS_ENABLED" env-default:"true"`
}

// ObservabilityLogs sample rate applies to DEBUG/INFO only.
// WARN and ERROR always export at 100% — dropping them silently
// during incidents is too dangerous to expose as a knob.
// Stdout receives 100% regardless of this rate.
type ObservabilityLogs struct {
	Enabled    bool    `yaml:"enabled" env:"WH_OBS_LOGS_ENABLED" env-default:"true"`
	SampleRate float64 `yaml:"sample_rate" env:"WH_OBS_LOGS_SAMPLE_RATE" env-default:"0.10"`
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

	if c.Observability.Enabled {
		if r := c.Observability.Traces.SampleRate; r < 0 || r > 1 {
			return fmt.Errorf("observability.traces.sample_rate %g out of range [0.0, 1.0]", r)
		}
		if r := c.Observability.Logs.SampleRate; r < 0 || r > 1 {
			return fmt.Errorf("observability.logs.sample_rate %g out of range [0.0, 1.0]", r)
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
