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
	Mode       Mode       `yaml:"mode" env:"BH_MODE" env-default:"standalone"`
	Server     Server     `yaml:"server"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
	MQ         MQ         `yaml:"mq"`
	Dedupe     Dedupe     `yaml:"dedupe"`
	Cache      Cache      `yaml:"cache"`
	Auth       Auth       `yaml:"auth"`
}

type Server struct {
	Port            int `yaml:"port" env:"BH_SERVER_PORT" env-default:"8080"`
	ShutdownTimeout int `yaml:"shutdown_timeout" env:"BH_SERVER_SHUTDOWN_TIMEOUT" env-default:"10"`
}

type ClickHouse struct {
	Addr        string `yaml:"addr" env:"BH_CH_ADDR" env-default:"localhost:9000"`
	Database    string `yaml:"database" env:"BH_CH_DATABASE" env-default:"default"`
	Username    string `yaml:"username" env:"BH_CH_USERNAME" env-default:"default"`
	Password    string `yaml:"password" env:"BH_CH_PASSWORD"`
	AutoMigrate *bool  `yaml:"auto_migrate"` // env: BH_CH_AUTO_MIGRATE — resolved in applyModeDefaults
}

type MQ struct {
	EmbeddedDir      string `yaml:"embedded_dir" env:"BH_MQ_EMBEDDED_DIR" env-default:"./data/nats"`
	URL              string `yaml:"url" env:"BH_MQ_URL" env-default:"nats://localhost:4222"`
	GapWindowMinutes int    `yaml:"gap_window_minutes" env:"BH_MQ_GAP_WINDOW_MINUTES" env-default:"15"`
	MaxBytesGB       int    `yaml:"max_bytes_gb" env:"BH_MQ_MAX_BYTES_GB" env-default:"50"`
}

type Dedupe struct {
	EmbeddedDir    string   `yaml:"embedded_dir" env:"BH_DEDUPE_EMBEDDED_DIR" env-default:"./data/pebble"`
	ScyllaHosts    []string `yaml:"scylla_hosts" env:"BH_DEDUPE_SCYLLA_HOSTS" env-default:"localhost:9042"`
	ScyllaKeyspace string   `yaml:"scylla_keyspace" env:"BH_DEDUPE_SCYLLA_KEYSPACE" env-default:"beachhouse"`
	AutoMigrate    *bool    `yaml:"auto_migrate"` // env: BH_DEDUPE_AUTO_MIGRATE — resolved in applyModeDefaults
}

type Cache struct {
	L1MaxCost  int64  `yaml:"l1_max_cost" env:"BH_CACHE_L1_MAX_COST" env-default:"67108864"`
	RedisURL   string `yaml:"redis_url" env:"BH_CACHE_REDIS_URL" env-default:"redis://localhost:6379"`
	DefaultTTL int    `yaml:"default_ttl" env:"BH_CACHE_DEFAULT_TTL" env-default:"300"`
}

type Auth struct {
	JWTSecret string `yaml:"jwt_secret" env:"BH_AUTH_JWT_SECRET"`
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
	cfg.applyModeDefaults()
	return &cfg, nil
}

// applyModeDefaults sets AutoMigrate flags based on deployment mode when not
// explicitly configured via YAML or environment variables. Standalone mode
// defaults to true (zero-setup), clustered defaults to false (operator-managed).
// Env vars are checked here (not via cleanenv struct tags) because cleanenv
// does not reliably handle *bool pointer fields.
func (c *Config) applyModeDefaults() {
	isStandalone := c.Mode == ModeStandalone
	c.ClickHouse.AutoMigrate = resolveAutoMigrate(c.ClickHouse.AutoMigrate, "BH_CH_AUTO_MIGRATE", isStandalone)
	c.Dedupe.AutoMigrate = resolveAutoMigrate(c.Dedupe.AutoMigrate, "BH_DEDUPE_AUTO_MIGRATE", isStandalone)
}

// resolveAutoMigrate determines the final auto-migrate value.
// Priority: env var > YAML value > mode default.
func resolveAutoMigrate(yamlValue *bool, envKey string, modeDefault bool) *bool {
	if v, ok := os.LookupEnv(envKey); ok {
		b := v == "true" || v == "1" || v == "yes"
		return &b
	}
	if yamlValue != nil {
		return yamlValue
	}
	return &modeDefault
}
