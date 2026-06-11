package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
	assert.Equal(t, time.Duration(30)*time.Second, cfg.ClickHouse.QueryTimeout)
	assert.Equal(t, "role", cfg.Auth.RoleClaim)
	assert.False(t, cfg.Dedupe.Enabled)
	assert.Equal(t, "event_id", cfg.Dedupe.IDField)
	assert.True(t, cfg.DLQ.Enabled)
	assert.Empty(t, cfg.Policy.FilePath, "no default bootstrap file — operators opt in explicitly so a missing file never produces a silent fail-closed boot")
	assert.Equal(t, "", cfg.Pipes.Dir)
	assert.Equal(t, "./data", cfg.DataDir)
	assert.Equal(t, 60, cfg.Schema.RefreshInterval)
	assert.False(t, cfg.OTel.Enabled)
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
	assert.Equal(t, time.Duration(30)*time.Second, cfg.ClickHouse.QueryTimeout)
	assert.Equal(t, "test-secret", cfg.Auth.JWTSecret)
}

func TestLoad_QueryLimits_Defaults(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, 10000, cfg.QueryLimits.DefaultMaxRows)
	assert.Equal(t, int64(0), cfg.QueryLimits.DefaultMaxRowsToRead, "rows-to-read default is off")
	assert.Equal(t, int64(4<<30), cfg.QueryLimits.DefaultMaxMemoryUsage.Bytes(), "4GiB memory backstop by default")
}

func TestLoad_QueryLimits_FromYAML(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// default_max_memory_usage uses the human-readable form; default_max_rows*
	// are plain counts.
	yamlContent := `
query_limits:
  default_max_rows: 25000
  default_max_rows_to_read: 5000000
  default_max_memory_usage: 2GiB
`
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlContent), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, 25000, cfg.QueryLimits.DefaultMaxRows)
	assert.Equal(t, int64(5_000_000), cfg.QueryLimits.DefaultMaxRowsToRead)
	assert.Equal(t, int64(2<<30), cfg.QueryLimits.DefaultMaxMemoryUsage.Bytes())
}

func TestValidate_NegativeQueryLimits(t *testing.T) {
	t.Parallel()
	base := func() Config {
		return Config{
			Server:     Server{Port: 8080},
			ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: 30 * time.Second},
			Schema:     Schema{RefreshInterval: 60},
		}
	}
	tests := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"negative default_max_rows", func(c *Config) { c.QueryLimits.DefaultMaxRows = -1 }, "default_max_rows"},
		{"negative default_max_rows_to_read", func(c *Config) { c.QueryLimits.DefaultMaxRowsToRead = -1 }, "default_max_rows_to_read"},
		{"negative default_max_memory_usage", func(c *Config) { c.QueryLimits.DefaultMaxMemoryUsage = -1 }, "default_max_memory_usage"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := base()
			tt.mut(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
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
				ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
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
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown_timeout")
}

func TestValidate_SchemaRefreshIntervalZero(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 0},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema.refresh_interval")
}

func TestValidate_NegativeQueryTime(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: -1},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clickhouse.query_timeout")
}

func TestValidate_ZeroQueryTime(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: 0},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clickhouse.query_timeout")
}

func TestValidate_NegativeGapWindow(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		MQ:         MQ{GapWindowMinutes: -1},
		Schema:     Schema{RefreshInterval: 60},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gap_window_minutes")
}

func TestValidate_TracesSampleRateOutOfRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		rate float64
	}{
		{"negative", -0.01},
		{"just above one", 1.01},
		{"well above one", 2.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Server:     Server{Port: 8080},
				ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
				Schema:     Schema{RefreshInterval: 60},
				OTel: OTel{
					Enabled: true,
					Traces:  OTelTraces{Enabled: true, SampleRate: tc.rate},
					Logs:    OTelLogs{Enabled: true, SampleRate: 0.10},
				},
			}
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "traces.sample_rate")
		})
	}
}

func TestValidate_LogsSampleRateOutOfRange(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
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
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 60},
		OTel: OTel{
			Enabled: false,
			Traces:  OTelTraces{SampleRate: 99},
			Logs:    OTelLogs{SampleRate: -1},
		},
	}
	assert.NoError(t, cfg.Validate())
}

func TestValidate_SampleRatesIgnoredWhenSignalDisabled(t *testing.T) {
	t.Parallel()
	// Same idea one level down — when the master switch is on but the individual
	// signal is off, its sample_rate is unused and should not gate startup.
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 60},
		OTel: OTel{
			Enabled: true,
			Traces:  OTelTraces{Enabled: false, SampleRate: 99},
			Logs:    OTelLogs{Enabled: false, SampleRate: -1},
		},
	}
	assert.NoError(t, cfg.Validate())
}

func TestLoad_Defaults_PrometheusDisabled(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)

	assert.False(t, cfg.Prometheus.Enabled)
	assert.Equal(t, "/metrics", cfg.Prometheus.Path)
	assert.Equal(t, 0, cfg.Prometheus.Port)
}

func TestValidate_PrometheusPortCollidesWithServerPort(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 60},
		Prometheus: Prometheus{
			Enabled: true,
			Path:    "/metrics",
			Port:    8080, // collides
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "collides with server.port")
}

func TestValidate_PrometheusPortOutOfRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		port int
	}{
		{"negative", -1},
		{"just above uint16", 65536},
		{"well above uint16", 99999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Server:     Server{Port: 8080},
				ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
				Schema:     Schema{RefreshInterval: 60},
				Prometheus: Prometheus{
					Enabled: true,
					Path:    "/metrics",
					Port:    tc.port,
				},
			}
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "prometheus.port")
		})
	}
}

func TestValidate_PrometheusPathMustStartWithSlash(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 60},
		Prometheus: Prometheus{
			Enabled: true,
			Path:    "metrics", // missing leading slash
			Port:    0,
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must start with '/'")
}

func TestValidate_PrometheusPathReservedConflicts(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		path    string
		port    int
		wantSub string
	}{
		// Canonical K8s-convention probe names.
		{"livez probe", "/livez", 0, "reserved endpoint"},
		{"readyz probe", "/readyz", 0, "reserved endpoint"},
		// Deprecated aliases (kept for v0.1.x, removed in v0.2.0).
		{"healthz probe", "/healthz", 0, "reserved endpoint"},
		{"health probe", "/health", 0, "reserved endpoint"},
		{"ready probe", "/ready", 0, "reserved endpoint"},
		{"v1 root same-port", "/v1", 0, "authenticated /v1"},
		{"v1 subpath same-port", "/v1/admin/metrics", 0, "authenticated /v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Server:     Server{Port: 8080},
				ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
				Schema:     Schema{RefreshInterval: 60},
				Prometheus: Prometheus{Enabled: true, Path: tc.path, Port: tc.port},
			}
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantSub)
		})
	}
}

func TestValidate_PrometheusV1PathAllowedOnSidecarPort(t *testing.T) {
	t.Parallel()
	// /v1* shadowing is only a concern on the same-port mount. A sidecar
	// listener on its own port is a separate route table entirely, so the
	// path doesn't collide with the API. Validation should let this through.
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 60},
		Prometheus: Prometheus{Enabled: true, Path: "/v1/metrics", Port: 9091},
	}
	assert.NoError(t, cfg.Validate())
}

func TestValidate_PrometheusOnly_NoOTel(t *testing.T) {
	t.Parallel()
	// Operators using Prometheus scrape (Alloy / Mimir) without OTel push:
	// otel.enabled stays false, prometheus.enabled is true. Must validate.
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 60},
		Prometheus: Prometheus{Enabled: true, Path: "/metrics", Port: 0},
	}
	assert.NoError(t, cfg.Validate())
}

func TestValidate_PrometheusIgnoredWhenDisabled(t *testing.T) {
	t.Parallel()
	// All sorts of invalid prometheus settings should be ignored when
	// prometheus.enabled is false — operators iterating on yaml shouldn't
	// get yelled at about unused fields.
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "http", QueryTimeout: time.Duration(30) * time.Second},
		Schema:     Schema{RefreshInterval: 60},
		Prometheus: Prometheus{
			Enabled: false,
			Path:    "garbage",
			Port:    8080,
		},
	}
	assert.NoError(t, cfg.Validate())
}

func TestValidate_InvalidHTTPScheme(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server:     Server{Port: 8080},
		ClickHouse: ClickHouse{HTTPScheme: "ftp", QueryTimeout: time.Duration(30) * time.Second}, // Intentionally invalid ftp
		Schema:     Schema{RefreshInterval: 60},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clickhouse.http_scheme must be 'http' or 'https'")
}
