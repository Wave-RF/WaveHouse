package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withDefaultBackends sets what Load's env-defaults would: a literal Config
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
		{"dedupe", func(c *Config) { c.Dedupe.Backend = "dynamodb" }, `dedupe.backend (WH_DEDUPE_BACKEND) "dynamodb" is not a backend this build has; valid: pebble`},
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
