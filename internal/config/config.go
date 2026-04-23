package config

import (
	"fmt"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

// Mode determines standalone (embedded) vs clustered (distributed) deployment.
type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModeClustered  Mode = "clustered"
)

// Config is the top-level application configuration.
type Config struct {
	Mode          Mode          `yaml:"mode" env:"WH_MODE" env-default:"standalone"`
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
	Enabled  bool   `yaml:"enabled" env:"WH_OBSERVABILITY_ENABLED" env-default:"false"`
	OTelAddr string `yaml:"otel_addr" env:"WH_OTEL_ADDR" env-default:"127.0.0.1:4317"`
}

type Server struct {
	Port               int      `yaml:"port" env:"WH_SERVER_PORT" env-default:"8080"`
	ShutdownTimeout    int      `yaml:"shutdown_timeout" env:"WH_SERVER_SHUTDOWN_TIMEOUT" env-default:"10"`
	CORSAllowedOrigins []string `yaml:"cors_allowed_origins" env:"WH_SERVER_CORS_ALLOWED_ORIGINS" env-default:"*"`
}

type ClickHouse struct {
	Addr     string `yaml:"addr" env:"WH_CH_ADDR" env-default:"localhost:9000"`
	HTTPPort string `yaml:"http_port" env:"WH_CH_HTTP_PORT" env-default:"8123"`
	Database string `yaml:"database" env:"WH_CH_DATABASE" env-default:"default"`
	Username string `yaml:"username" env:"WH_CH_USERNAME" env-default:"default"`
	Password string `yaml:"password" env:"WH_CH_PASSWORD"`
}

type MQ struct {
	StreamName       string `yaml:"stream_name" env:"WH_MQ_STREAM_NAME" env-default:"WAVEHOUSE"`
	EmbeddedDir      string `yaml:"embedded_dir" env:"WH_MQ_EMBEDDED_DIR" env-default:"./data/nats"`
	URL              string `yaml:"url" env:"WH_MQ_URL" env-default:"nats://localhost:4222"`
	GapWindowMinutes int    `yaml:"gap_window_minutes" env:"WH_MQ_GAP_WINDOW_MINUTES" env-default:"15"`
	MaxBytesGB       int    `yaml:"max_bytes_gb" env:"WH_MQ_MAX_BYTES_GB" env-default:"50"`
}

type Dedupe struct {
	Enabled        bool     `yaml:"enabled" env:"WH_DEDUPE_ENABLED" env-default:"false"`
	IDField        string   `yaml:"id_field" env:"WH_DEDUPE_ID_FIELD" env-default:"event_id"`
	EmbeddedDir    string   `yaml:"embedded_dir" env:"WH_DEDUPE_EMBEDDED_DIR" env-default:"./data/pebble"`
	ScyllaHosts    []string `yaml:"scylla_hosts" env:"WH_DEDUPE_SCYLLA_HOSTS" env-default:"localhost:9042"`
	ScyllaKeyspace string   `yaml:"scylla_keyspace" env:"WH_DEDUPE_SCYLLA_KEYSPACE" env-default:"wavehouse"`
}

type Cache struct {
	L1MaxCost              int64  `yaml:"l1_max_cost" env:"WH_CACHE_L1_MAX_COST" env-default:"67108864"`
	RedisURL               string `yaml:"redis_url" env:"WH_CACHE_REDIS_URL" env-default:"redis://localhost:6379"`
	DefaultTTL             int    `yaml:"default_ttl" env:"WH_CACHE_DEFAULT_TTL" env-default:"300"`
	TimestampBucketSeconds int    `yaml:"timestamp_bucket_seconds" env:"WH_CACHE_TIMESTAMP_BUCKET_SECONDS" env-default:"60"`
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
type Pipes struct {
	Directory string `yaml:"directory" env:"WH_PIPES_DIRECTORY" env-default:"./pipes"`
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
	if c.Mode != ModeStandalone && c.Mode != ModeClustered {
		return fmt.Errorf("invalid mode %q: must be %q or %q", c.Mode, ModeStandalone, ModeClustered)
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

	if c.MQ.StreamName == "" {
		c.MQ.StreamName = "WAVEHOUSE"
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
