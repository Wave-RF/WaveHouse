package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain pins WH_SETTINGS_DIR for the whole package: settings.dir is a
// required boot key, and the Load tests run in parallel (so t.Setenv is out).
// Tests that exercise the requirement itself build a Config literal.
func TestMain(m *testing.M) {
	if err := os.Setenv("WH_SETTINGS_DIR", "./settings"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestValidate_SettingsDirRequired(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Server: Server{Port: 8080},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "settings.dir")
	assert.Contains(t, err.Error(), "bootstrap")
}

func TestLoad_Defaults(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)

	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, 10, cfg.Server.ShutdownTimeout)
	assert.Equal(t, "", cfg.ClickHouse.Password)
	assert.Empty(t, cfg.Auth.OperatorKey, "operator key is empty by default (feature off)")
	assert.Empty(t, cfg.Policy.FilePath, "no default bootstrap file — operators opt in explicitly so a missing file never produces a silent fail-closed boot")
	assert.Equal(t, "", cfg.Pipes.Dir)
	assert.Equal(t, "./data", cfg.DataDir)
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
  password: "ch-pass"
auth:
  jwt_secret: "test-secret"
  operator_key: "op-key"
`
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlContent), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)

	assert.Equal(t, 9090, cfg.Server.Port)
	assert.Equal(t, "ch-pass", cfg.ClickHouse.Password)
	assert.Equal(t, "test-secret", cfg.Auth.JWTSecret)
	assert.Equal(t, "op-key", cfg.Auth.OperatorKey)
}

func TestLoad_OperatorKey_FromEnv(t *testing.T) {
	t.Setenv("WH_AUTH_OPERATOR_KEY", "env-operator-key")
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, "env-operator-key", cfg.Auth.OperatorKey)
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
				Server:   Server{Port: tt.port},
				Settings: Settings{Dir: "./settings"},
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
		Server:   Server{Port: 8080, ShutdownTimeout: -1},
		Settings: Settings{Dir: "./settings"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown_timeout")
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
				Server:   Server{Port: 8080},
				Settings: Settings{Dir: "./settings"},
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
		Server:   Server{Port: 8080},
		Settings: Settings{Dir: "./settings"},
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
		Server:   Server{Port: 8080},
		Settings: Settings{Dir: "./settings"},
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
		Server:   Server{Port: 8080},
		Settings: Settings{Dir: "./settings"},
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
		Server:   Server{Port: 8080},
		Settings: Settings{Dir: "./settings"},
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
				Server:   Server{Port: 8080},
				Settings: Settings{Dir: "./settings"},
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
		Server:   Server{Port: 8080},
		Settings: Settings{Dir: "./settings"},
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
		{"v1 subpath same-port", "/v1/ops/metrics", 0, "authenticated /v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Server:     Server{Port: 8080},
				Settings:   Settings{Dir: "./settings"},
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
		Settings:   Settings{Dir: "./settings"},
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
		Settings:   Settings{Dir: "./settings"},
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
		Server:   Server{Port: 8080},
		Settings: Settings{Dir: "./settings"},
		Prometheus: Prometheus{
			Enabled: false,
			Path:    "garbage",
			Port:    8080,
		},
	}
	assert.NoError(t, cfg.Validate())
}

// TestEnvSettingsDir_MatchesStructTag pins the exported constant to the
// Settings.Dir env tag. A struct tag must be a string literal, so it can't
// reference EnvSettingsDir directly; this test is what makes the constant the
// single authority (subcommands read the constant, config binding reads the
// tag — drift between them would be a silent misconfiguration).
func TestEnvSettingsDir_MatchesStructTag(t *testing.T) {
	t.Parallel()
	field, ok := reflect.TypeFor[Settings]().FieldByName("Dir")
	require.True(t, ok)
	assert.Equal(t, EnvSettingsDir, field.Tag.Get("env"))
}

// TestLoad_RejectsUnknownKeys pins the strict-YAML contract: a key the
// struct doesn't declare — a tunable that moved to the settings directory,
// or a typo — fails Load and names every offender, nested or top-level.
func TestLoad_RejectsUnknownKeys(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	yamlContent := `
server:
  port: 9090
  cors_allowed_origins: ["*"]
dlq:
  enabled: false
clickhouse:
  addr: localhost:9000
  password: x
settings:
  dir: ./settings
`
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlContent), 0o600))
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown key(s)")
	assert.Contains(t, err.Error(), "clickhouse.addr")
	assert.Contains(t, err.Error(), "dlq")
	assert.Contains(t, err.Error(), "server.cors_allowed_origins")
	assert.NotContains(t, err.Error(), "clickhouse.password", "declared keys are never reported")
	assert.NotContains(t, err.Error(), "settings.dir")
}
