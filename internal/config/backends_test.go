package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withDefaultBackends sets what defaults() would: a literal Config
// names no backend and no role, and Validate refuses that.
func withDefaultBackends(c Config) *Config {
	c.Roles = AllRoles()
	c.MQ.Backend, c.Cache.Backend = MQEmbedded, CacheLocal
	c.Dedupe.Backend, c.Coord.Backend = DedupePebble, CoordLocal
	return &c
}

func defaultBackends() Config {
	return *withDefaultBackends(Config{Server: Server{Port: 8080}, Settings: Settings{Dir: "./settings"}})
}

func TestLoad_BackendDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, MQEmbedded, cfg.MQ.Backend)
	assert.Equal(t, CacheLocal, cfg.Cache.Backend)
	assert.Equal(t, DedupePebble, cfg.Dedupe.Backend)
	assert.Equal(t, Dedupe{
		Backend: DedupePebble, Lease: 30 * time.Second, ReserveConcurrency: 64,
		DynamoDB: DedupeDynamoDBConfig{Timeout: 250 * time.Millisecond, MaxAttempts: 3, RetryMode: "standard"},
	}, cfg.Dedupe)
	assert.Equal(t, CoordLocal, cfg.Coord.Backend)
	assert.False(t, cfg.Distributed())
	assert.True(t, cfg.NeedsDataDir())
	assert.Empty(t, cfg.Warnings())
}

func TestLoad_BackendsFromEnv(t *testing.T) {
	t.Setenv("WH_MQ_BACKEND", "embedded")
	t.Setenv("WH_CACHE_BACKEND", "local")
	t.Setenv("WH_DEDUPE_BACKEND", "pebble")
	t.Setenv("WH_COORD_BACKEND", "local")
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, MQEmbedded, cfg.MQ.Backend)
	assert.Equal(t, CoordLocal, cfg.Coord.Backend)
}

func TestLoad_BackendFromEnvRefusesAnUnknownValue(t *testing.T) {
	t.Setenv("WH_MQ_BACKEND", "nats")
	_, err := Load("nonexistent.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `mq.backend (WH_MQ_BACKEND) "nats" is not a backend this build has; valid: embedded`)
}

func TestLoad_BackendsFromYAML(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mq:
  backend: embedded
cache:
  backend: local
  l1_max_cost: 1024
dedupe:
  backend: pebble
coord:
  backend: local
`), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, MQEmbedded, cfg.MQ.Backend)
	assert.Equal(t, CacheLocal, cfg.Cache.Backend)
	assert.Equal(t, int64(1024), cfg.Cache.L1MaxCost)
	assert.Equal(t, DedupePebble, cfg.Dedupe.Backend)
	assert.Equal(t, CoordLocal, cfg.Coord.Backend)
}

// A sub-block written before its backend exists, and a settings-directory
// key under a block both files share, are unknown keys — not read and ignored.
func TestLoad_BackendBlocksRefuseUnknownKeys(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mq:
  backend: embedded
  max_bytes_gb: 5
  nats:
    urls: nats://localhost:4222
dedupe:
  enabled: true
`), 0o600))
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dedupe.enabled, mq.max_bytes_gb, mq.nats")
	assert.Contains(t, err.Error(), EnvSettingsDir)
}

func TestUnboundEnv_KnowsTheBackendVariables(t *testing.T) {
	t.Parallel()
	assert.Empty(t, unboundEnv([]string{
		"WH_MQ_BACKEND=embedded", "WH_CACHE_BACKEND=local",
		"WH_DEDUPE_BACKEND=pebble", "WH_COORD_BACKEND=local",
	}))
}

func TestValidate_UnknownBackend(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		set  func(*Config)
		want string
	}{
		{"mq", func(c *Config) { c.MQ.Backend = "kafka" }, `mq.backend (WH_MQ_BACKEND) "kafka" is not a backend this build has; valid: embedded`},
		{"cache", func(c *Config) { c.Cache.Backend = "redis" }, `cache.backend (WH_CACHE_BACKEND) "redis" is not a backend this build has; valid: local`},
		{"dedupe", func(c *Config) { c.Dedupe.Backend = "redis" }, `dedupe.backend (WH_DEDUPE_BACKEND) "redis" is not a backend this build has; valid: pebble, dynamodb`},
		{"coord", func(c *Config) { c.Coord.Backend = "nats" }, `coord.backend (WH_COORD_BACKEND) "nats" is not a backend this build has; valid: local`},
		// The zero value, which a Config built without Load carries.
		{"empty", func(c *Config) { c.MQ.Backend = "" }, `mq.backend (WH_MQ_BACKEND) "" is not a backend`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultBackends()
			require.NoError(t, cfg.Validate())
			tc.set(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// Every warning keys on a shared queue, which no backend offers yet, so the
// value is set directly: Warnings reads the choice, it doesn't validate it.
func TestWarnings_SharedQueue(t *testing.T) {
	t.Parallel()
	cfg := defaultBackends()
	assert.Empty(t, cfg.Warnings())

	cfg.MQ.Backend = "shared"
	require.True(t, cfg.Distributed())
	got := cfg.Warnings()
	require.Len(t, got, 2)
	assert.Contains(t, got[0], "cache.backend=local")
	assert.Contains(t, got[1], "dedupe.backend=pebble")

	cfg.Cache.Backend, cfg.Dedupe.Backend = "shared", "shared"
	assert.Empty(t, cfg.Warnings())
}

func TestNeedsDataDir(t *testing.T) {
	t.Parallel()
	cfg := defaultBackends()
	assert.True(t, cfg.NeedsDataDir())
	cfg.MQ.Backend = "shared"
	assert.True(t, cfg.NeedsDataDir(), "pebble dedupe still keeps state under data_dir")
	cfg.Dedupe.Backend = "shared"
	assert.False(t, cfg.NeedsDataDir())
	cfg.MQ.Backend = MQEmbedded
	assert.True(t, cfg.NeedsDataDir(), "the embedded mq keeps state under data_dir")
}

func TestLoad_DedupeDynamoDBFromEnv(t *testing.T) {
	for k, v := range map[string]string{
		"WH_DEDUPE_BACKEND":               "dynamodb",
		"WH_DEDUPE_LEASE":                 "45s",
		"WH_DEDUPE_RESERVE_CONCURRENCY":   "16",
		"WH_DEDUPE_DYNAMODB_TABLE":        "wavehouse-dedupe-dev",
		"WH_DEDUPE_DYNAMODB_REGION":       "us-east-2",
		"WH_DEDUPE_DYNAMODB_ENDPOINT":     "http://localhost:8000",
		"WH_DEDUPE_DYNAMODB_TIMEOUT":      "1s",
		"WH_DEDUPE_DYNAMODB_MAX_ATTEMPTS": "5",
		"WH_DEDUPE_DYNAMODB_RETRY_MODE":   "adaptive",
		"WH_DEDUPE_DYNAMODB_CREATE_TABLE": "true",
	} {
		t.Setenv(k, v)
	}
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, Dedupe{
		Backend: DedupeDynamoDB, Lease: 45 * time.Second, ReserveConcurrency: 16,
		DynamoDB: DedupeDynamoDBConfig{
			Table: "wavehouse-dedupe-dev", Region: "us-east-2", Endpoint: "http://localhost:8000",
			Timeout: time.Second, MaxAttempts: 5, RetryMode: "adaptive", CreateTable: true,
		},
	}, cfg.Dedupe)
	assert.True(t, cfg.NeedsDataDir(), "the embedded mq still keeps state under data_dir")
}

func TestLoad_DedupeDynamoDBFromYAML(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
dedupe:
  backend: dynamodb
  lease: 20s
  dynamodb:
    table: wavehouse-dedupe-prod
    timeout: 400ms
`), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, DedupeDynamoDB, cfg.Dedupe.Backend)
	assert.Equal(t, 20*time.Second, cfg.Dedupe.Lease)
	assert.Equal(t, 64, cfg.Dedupe.ReserveConcurrency)
	assert.Equal(t, DedupeDynamoDBConfig{
		Table: "wavehouse-dedupe-prod", Timeout: 400 * time.Millisecond, MaxAttempts: 3, RetryMode: "standard",
	}, cfg.Dedupe.DynamoDB)
}

func TestLoad_DedupeDynamoDBRefusesUnknownKeys(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
dedupe:
  backend: dynamodb
  dynamodb:
    table: t
    access_key_id: AKIA
  redis:
    addr: localhost:6379
`), 0o600))
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dedupe.dynamodb.access_key_id, dedupe.redis")
}

func TestUnboundEnv_KnowsTheDedupeVariables(t *testing.T) {
	t.Parallel()
	assert.Empty(t, unboundEnv([]string{
		"WH_DEDUPE_LEASE=30s", "WH_DEDUPE_RESERVE_CONCURRENCY=64",
		"WH_DEDUPE_DYNAMODB_TABLE=t", "WH_DEDUPE_DYNAMODB_REGION=us-east-1",
		"WH_DEDUPE_DYNAMODB_ENDPOINT=http://localhost:8000", "WH_DEDUPE_DYNAMODB_TIMEOUT=250ms",
		"WH_DEDUPE_DYNAMODB_MAX_ATTEMPTS=3", "WH_DEDUPE_DYNAMODB_RETRY_MODE=standard",
		"WH_DEDUPE_DYNAMODB_CREATE_TABLE=false",
	}))
}

func TestValidate_Dedupe(t *testing.T) {
	t.Parallel()
	dynamo := func(c *Config) {
		c.Dedupe.Backend = DedupeDynamoDB
		c.Dedupe.DynamoDB = DedupeDynamoDBConfig{Table: "t", Timeout: time.Second, MaxAttempts: 3, RetryMode: "standard"}
	}
	cases := []struct {
		name string
		set  func(*Config)
		want string // "" = valid
	}{
		{"dynamodb", dynamo, ""},
		{"zero values read as the defaults", func(c *Config) {
			c.Dedupe.Backend = DedupeDynamoDB
			c.Dedupe.DynamoDB = DedupeDynamoDBConfig{Table: "t"}
		}, ""},
		{"create_table with an endpoint", func(c *Config) {
			dynamo(c)
			c.Dedupe.DynamoDB.Endpoint, c.Dedupe.DynamoDB.CreateTable = "http://localhost:8000", true
		}, ""},
		{"the block is not read under pebble", func(c *Config) { c.Dedupe.DynamoDB.CreateTable = true }, ""},
		{"lease at the duplicate window", func(c *Config) { c.Dedupe.Lease = 2 * time.Minute }, ""},
		{"create_table without an endpoint", func(c *Config) {
			dynamo(c)
			c.Dedupe.DynamoDB.CreateTable = true
		}, "dedupe.dynamodb.create_table (WH_DEDUPE_DYNAMODB_CREATE_TABLE) is for dynamodb-local only"},
		{"no table", func(c *Config) { dynamo(c); c.Dedupe.DynamoDB.Table = " " }, "dedupe.dynamodb.table (WH_DEDUPE_DYNAMODB_TABLE) is required"},
		{"retry mode", func(c *Config) { dynamo(c); c.Dedupe.DynamoDB.RetryMode = "legacy" }, `retry_mode (WH_DEDUPE_DYNAMODB_RETRY_MODE) "legacy"`},
		{"negative timeout", func(c *Config) { dynamo(c); c.Dedupe.DynamoDB.Timeout = -time.Second }, "dedupe.dynamodb.timeout"},
		{"negative attempts", func(c *Config) { dynamo(c); c.Dedupe.DynamoDB.MaxAttempts = -1 }, "dedupe.dynamodb.max_attempts"},
		{"negative lease", func(c *Config) { c.Dedupe.Lease = -time.Second }, "dedupe.lease (WH_DEDUPE_LEASE) must be >= 0"},
		{"negative concurrency", func(c *Config) { c.Dedupe.ReserveConcurrency = -1 }, "dedupe.reserve_concurrency"},
		{"lease past the duplicate window", func(c *Config) { c.Dedupe.Lease = 3 * time.Minute }, "exceeds the embedded mq's 2m0s duplicate window"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultBackends()
			tc.set(&cfg)
			err := cfg.Validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestNeedsDataDir_DynamoDBDedupe(t *testing.T) {
	t.Parallel()
	cfg := defaultBackends()
	cfg.Dedupe.Backend = DedupeDynamoDB
	assert.True(t, cfg.NeedsDataDir(), "the embedded mq keeps state under data_dir")
	cfg.MQ.Backend = "shared"
	assert.False(t, cfg.NeedsDataDir(), "neither a shared mq nor dynamodb dedupe keeps state under data_dir")
	assert.Len(t, cfg.Warnings(), 1, "only the local cache warning: dynamodb dedupe is shared")
}
