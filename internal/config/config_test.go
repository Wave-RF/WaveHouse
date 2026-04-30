package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)

	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, 10, cfg.Server.ShutdownTimeout)
	assert.Equal(t, "localhost:9000", cfg.ClickHouse.Addr)
	assert.Equal(t, "8123", cfg.ClickHouse.HTTPPort)
	assert.Equal(t, "default", cfg.ClickHouse.Database)
	assert.Equal(t, "default", cfg.ClickHouse.Username)
	assert.Equal(t, "", cfg.ClickHouse.Password)
	assert.False(t, cfg.Auth.Enabled)
	assert.Equal(t, "role", cfg.Auth.RoleClaim)
	assert.False(t, cfg.Dedupe.Enabled)
	assert.Equal(t, "event_id", cfg.Dedupe.IDField)
	assert.True(t, cfg.DLQ.Enabled)
	assert.Equal(t, "policy.yaml", cfg.Policy.FilePath)
	assert.Equal(t, "./pipes", cfg.Pipes.Directory)
	assert.Equal(t, 60, cfg.Schema.RefreshInterval)
	assert.Equal(t, 300, cfg.Cache.DefaultTTL)
}

func TestLoad_FromYAML(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	yamlContent := `
server:
  port: 9090
clickhouse:
  addr: "clickhouse:9000"
  database: "mydb"
auth:
  enabled: true
  jwt_secret: "test-secret"
`
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlContent), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)

	assert.Equal(t, 9090, cfg.Server.Port)
	assert.Equal(t, "clickhouse:9000", cfg.ClickHouse.Addr)
	assert.Equal(t, "mydb", cfg.ClickHouse.Database)
	assert.True(t, cfg.Auth.Enabled)
	assert.Equal(t, "test-secret", cfg.Auth.JWTSecret)
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	dir := t.TempDir()
	yamlContent := `
server:
  port: 9090
`
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlContent), 0o600))

	t.Setenv("WH_SERVER_PORT", "7777")

	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, 7777, cfg.Server.Port)
}

func TestLoad_InvalidYAML(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte(":::invalid"), 0o600))

	_, err := Load(path)
	assert.Error(t, err)
}

func TestValidate_PortOutOfRange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		port int
	}{
		{"zero", 0},
		{"negative", -1},
		{"too high", 70000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Server: Server{Port: tt.port},
				Schema: Schema{RefreshInterval: 60},
			}
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "server.port")
		})
	}
}

func TestValidate_NegativeShutdownTimeout(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080, ShutdownTimeout: -1},
		Schema: Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown_timeout")
}

func TestValidate_AuthEnabledNoSecret(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080},
		Auth:   Auth{Enabled: true},
		Schema: Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jwt_secret or jwks_url")
}

func TestValidate_AuthEnabledWithJWKS(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080},
		Auth:   Auth{Enabled: true, JWKSURL: "https://example.com/.well-known/jwks.json"},
		Schema: Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidate_AuthDevModeBypassesCheck(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080},
		Auth:   Auth{Enabled: true, DevMode: true},
		Schema: Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidate_SchemaRefreshIntervalZero(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080},
		Schema: Schema{RefreshInterval: 0},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema.refresh_interval")
}

func TestValidate_NegativeCacheTTL(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080},
		Cache:  Cache{DefaultTTL: -1},
		Schema: Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cache.default_ttl")
}

func TestValidate_NegativeGapWindow(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080},
		MQ:     MQ{GapWindowMinutes: -1},
		Schema: Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gap_window_minutes")
}

func TestValidate_DefaultsStreamNameWhenEmpty(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080},
		Schema: Schema{RefreshInterval: 60},
	}

	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, "WAVEHOUSE", cfg.MQ.StreamName)
}

func TestLoad_AuthEnabledNoSecret_FailsValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	yamlContent := `
server:
  port: 8080
auth:
  enabled: true
`
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlContent), 0o600))

	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate config")
}
