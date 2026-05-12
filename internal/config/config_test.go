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
	assert.Equal(t, "http", cfg.ClickHouse.HTTPScheme)
	assert.Equal(t, "default", cfg.ClickHouse.Database)
	assert.Equal(t, "default", cfg.ClickHouse.Username)
	assert.Equal(t, "", cfg.ClickHouse.Password)
	assert.False(t, cfg.Auth.Enabled)
	assert.Equal(t, "role", cfg.Auth.RoleClaim)
	assert.False(t, cfg.Dedupe.Enabled)
	assert.Equal(t, "event_id", cfg.Dedupe.IDField)
	assert.True(t, cfg.DLQ.Enabled)
	assert.Equal(t, "policy.yaml", cfg.Policy.FilePath)
	assert.Equal(t, "", cfg.Pipes.Dir)
	assert.Equal(t, "./data", cfg.DataDir)
	assert.Equal(t, 60, cfg.Schema.RefreshInterval)
	assert.Equal(t, 300, cfg.Cache.DefaultTTL)
	assert.False(t, cfg.OTel.Enabled)
	assert.Equal(t, "127.0.0.1:4317", cfg.OTel.Addr)
	assert.True(t, cfg.OTel.Traces.Enabled)
	assert.InEpsilon(t, 1.0, cfg.OTel.Traces.SampleRate, 0.0001)
	assert.True(t, cfg.OTel.Metrics.Enabled)
	assert.True(t, cfg.OTel.Logs.Enabled)
	assert.InEpsilon(t, 1.0, cfg.OTel.Logs.SampleRate, 0.0001)
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
  http_scheme: "https"
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
	assert.Equal(t, "https", cfg.ClickHouse.HTTPScheme)
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
				Server:     Server{Port: tt.port},
				ClickHouse: ClickHouse{HTTPScheme: "http"},
				Schema:     Schema{RefreshInterval: 60},
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
		Server:     Server{Port: 8080, ShutdownTimeout: -1},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown_timeout")
}

func TestValidate_AuthEnabledNoSecret(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Auth:       Auth{Enabled: true},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jwt_secret or jwks_url")
}

func TestValidate_AuthEnabledWithJWKS(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Auth:       Auth{Enabled: true, JWKSURL: "https://example.com/.well-known/jwks.json"},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidate_AuthDevModeBypassesCheck(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Auth:       Auth{Enabled: true, DevMode: true},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidate_SchemaRefreshIntervalZero(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Schema:     Schema{RefreshInterval: 0},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema.refresh_interval")
}

func TestValidate_NegativeCacheTTL(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Cache:      Cache{DefaultTTL: -1},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cache.default_ttl")
}

func TestValidate_NegativeGapWindow(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		MQ:         MQ{GapWindowMinutes: -1},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gap_window_minutes")
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

func TestValidate_TracesSampleRateOutOfRange(t *testing.T) {
	t.Parallel()
	for _, rate := range []float64{-0.01, 1.01, 2.0} {
		cfg := Config{
			Server:     Server{Port: 8080},
			ClickHouse: ClickHouse{HTTPScheme: "http"},
			Schema:     Schema{RefreshInterval: 60},
			OTel: OTel{
				Enabled: true,
				Traces:  OTelTraces{Enabled: true, SampleRate: rate},
				Logs:    OTelLogs{Enabled: true, SampleRate: 0.10},
			},
		}
		err := cfg.Validate()
		require.Error(t, err, "rate %v should be rejected", rate)
		assert.Contains(t, err.Error(), "traces.sample_rate")
	}
}

func TestValidate_LogsSampleRateOutOfRange(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Schema:     Schema{RefreshInterval: 60},
		OTel: OTel{
			Enabled: true,
			Traces:  OTelTraces{Enabled: true, SampleRate: 0.10},
			Logs:    OTelLogs{Enabled: true, SampleRate: 1.5},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "logs.sample_rate")
}

func TestValidate_SampleRatesIgnoredWhenObservabilityDisabled(t *testing.T) {
	t.Parallel()
	// Out-of-range rates should not fail validation when the master switch is off —
	// the rates are unused so policing them would surprise operators iterating on
	// config that they haven't enabled yet.
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Schema:     Schema{RefreshInterval: 60},
		OTel: OTel{
			Enabled: false,
			Traces:  OTelTraces{SampleRate: 99},
			Logs:    OTelLogs{SampleRate: -1},
		},
	}
	assert.NoError(t, cfg.Validate())
}

func TestLoad_Defaults_PrometheusDisabled(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)

	assert.False(t, cfg.OTel.Metrics.Prometheus.Enabled)
	assert.Equal(t, "/metrics", cfg.OTel.Metrics.Prometheus.Path)
	assert.Equal(t, 0, cfg.OTel.Metrics.Prometheus.Port)
}

func TestValidate_PrometheusPortCollidesWithServerPort(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Schema:     Schema{RefreshInterval: 60},
		OTel: OTel{
			Enabled: true,
			Traces:  OTelTraces{SampleRate: 0.10},
			Logs:    OTelLogs{SampleRate: 0.10},
			Metrics: OTelMetrics{
				Enabled: true,
				Prometheus: OTelMetricsPrometheus{
					Enabled: true,
					Path:    "/metrics",
					Port:    8080, // collides
				},
			},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "collides with server.port")
}

func TestValidate_PrometheusPortOutOfRange(t *testing.T) {
	t.Parallel()
	for _, port := range []int{-1, 65536, 99999} {
		cfg := Config{
			Server:     Server{Port: 8080},
			ClickHouse: ClickHouse{HTTPScheme: "http"},
			Schema:     Schema{RefreshInterval: 60},
			OTel: OTel{
				Enabled: true,
				Traces:  OTelTraces{SampleRate: 0.10},
				Logs:    OTelLogs{SampleRate: 0.10},
				Metrics: OTelMetrics{
					Enabled: true,
					Prometheus: OTelMetricsPrometheus{
						Enabled: true,
						Path:    "/metrics",
						Port:    port,
					},
				},
			},
		}
		err := cfg.Validate()
		require.Error(t, err, "port %d should be rejected", port)
		assert.Contains(t, err.Error(), "prometheus.port")
	}
}

func TestValidate_PrometheusPathMustStartWithSlash(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Schema:     Schema{RefreshInterval: 60},
		OTel: OTel{
			Enabled: true,
			Traces:  OTelTraces{SampleRate: 0.10},
			Logs:    OTelLogs{SampleRate: 0.10},
			Metrics: OTelMetrics{
				Enabled: true,
				Prometheus: OTelMetricsPrometheus{
					Enabled: true,
					Path:    "metrics", // missing leading slash
					Port:    0,
				},
			},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must start with '/'")
}

func TestValidate_PrometheusIgnoredWhenDisabled(t *testing.T) {
	t.Parallel()
	// All sorts of invalid prometheus settings should be ignored when
	// prometheus.enabled is false — operators iterating on yaml shouldn't
	// get yelled at about unused fields.
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http"},
		Schema:     Schema{RefreshInterval: 60},
		OTel: OTel{
			Enabled: true,
			Traces:  OTelTraces{SampleRate: 0.10},
			Logs:    OTelLogs{SampleRate: 0.10},
			Metrics: OTelMetrics{
				Enabled: true,
				Prometheus: OTelMetricsPrometheus{
					Enabled: false,
					Path:    "garbage",
					Port:    8080,
				},
			},
		},
	}
	assert.NoError(t, cfg.Validate())
}

func TestValidate_InvalidHTTPScheme(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "ftp"}, // Intentionally invalid
		Schema:     Schema{RefreshInterval: 60},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clickhouse.http_scheme must be 'http' or 'https'")
}
