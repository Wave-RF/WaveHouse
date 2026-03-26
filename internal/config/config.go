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
	Mode       Mode       `yaml:"mode" env:"WH_MODE" env-default:"standalone"`
	Server     Server     `yaml:"server"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
	MQ         MQ         `yaml:"mq"`
	Dedupe     Dedupe     `yaml:"dedupe"`
	Cache      Cache      `yaml:"cache"`
	Auth       Auth       `yaml:"auth"`
	Schema     Schema     `yaml:"schema"`
	DLQ        DLQ        `yaml:"dlq"`
}

type Server struct {
	Port            int `yaml:"port" env:"WH_SERVER_PORT" env-default:"8080"`
	ShutdownTimeout int `yaml:"shutdown_timeout" env:"WH_SERVER_SHUTDOWN_TIMEOUT" env-default:"10"`
}

type ClickHouse struct {
	Addr     string `yaml:"addr" env:"WH_CH_ADDR" env-default:"localhost:9000"`
	HTTPPort string `yaml:"http_port" env:"WH_CH_HTTP_PORT" env-default:"8123"`
	Database string `yaml:"database" env:"WH_CH_DATABASE" env-default:"default"`
	Username string `yaml:"username" env:"WH_CH_USERNAME" env-default:"default"`
	Password string `yaml:"password" env:"WH_CH_PASSWORD"`
}

type MQ struct {
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
	L1MaxCost  int64  `yaml:"l1_max_cost" env:"WH_CACHE_L1_MAX_COST" env-default:"67108864"`
	RedisURL   string `yaml:"redis_url" env:"WH_CACHE_REDIS_URL" env-default:"redis://localhost:6379"`
	DefaultTTL int    `yaml:"default_ttl" env:"WH_CACHE_DEFAULT_TTL" env-default:"300"`
}

type Auth struct {
	Enabled   bool   `yaml:"enabled" env:"WH_AUTH_ENABLED" env-default:"false"`
	JWTSecret string `yaml:"jwt_secret" env:"WH_AUTH_JWT_SECRET"`
}

// Schema configures ClickHouse schema discovery.
type Schema struct {
	RefreshInterval int `yaml:"refresh_interval" env:"WH_SCHEMA_REFRESH_INTERVAL" env-default:"60"` // seconds
}

// DLQ configures the Dead Letter Queue for failed batch inserts.
type DLQ struct {
	Enabled bool `yaml:"enabled" env:"WH_DLQ_ENABLED" env-default:"true"`
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
	return &cfg, nil
}
