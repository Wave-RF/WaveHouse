package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// zeroCase is one key whose default is not its zero value (#631's table).
type zeroCase struct {
	key     string // dotted YAML path
	env     string
	zero    any    // the zero value, as written to YAML and as Load must return it
	def     any    // defaults() value, returned when the key is absent
	envVal  string // a non-default, non-zero value set through env
	fromEnv any    // envVal as Load must return it
	get     func(*Config) any
}

// server.port is not here: 0 fails Validate, pinned by TestLoad_YAMLZeroPortIsRefused.
var zeroCases = []zeroCase{
	{"otel.traces.enabled", "WH_OTEL_TRACES_ENABLED", false, true, "false", false, func(c *Config) any { return c.OTel.Traces.Enabled }},
	{"otel.metrics.enabled", "WH_OTEL_METRICS_ENABLED", false, true, "false", false, func(c *Config) any { return c.OTel.Metrics.Enabled }},
	{"otel.logs.enabled", "WH_OTEL_LOGS_ENABLED", false, true, "false", false, func(c *Config) any { return c.OTel.Logs.Enabled }},
	{"otel.traces.sample_rate", "WH_OTEL_TRACES_SAMPLE_RATE", 0.0, 1.0, "0.25", 0.25, func(c *Config) any { return c.OTel.Traces.SampleRate }},
	{"otel.logs.sample_rate", "WH_OTEL_LOGS_SAMPLE_RATE", 0.0, 1.0, "0.25", 0.25, func(c *Config) any { return c.OTel.Logs.SampleRate }},
	{"server.shutdown_timeout", "WH_SERVER_SHUTDOWN_TIMEOUT", 0, 10, "3", 3, func(c *Config) any { return c.Server.ShutdownTimeout }},
	{"cache.l1_max_cost", "WH_CACHE_L1_MAX_COST", int64(0), int64(64 << 20), "1024", int64(1024), func(c *Config) any { return c.Cache.L1MaxCost }},
	{"prometheus.path", "WH_PROMETHEUS_PATH", "", "/metrics", "/prom", "/prom", func(c *Config) any { return c.Prometheus.Path }},
	{"data_dir", "WH_DATA_DIR", "", "./data", "/var/lib/wh", "/var/lib/wh", func(c *Config) any { return c.DataDir }},
}

// yamlAt renders a file setting key to value, plus otel.enabled: true so
// the test can tell the file was read.
func yamlAt(t *testing.T, key string, value any) string {
	t.Helper()
	tree := map[string]any{"otel": map[string]any{"enabled": true}}
	node := tree
	parts := strings.Split(key, ".")
	for _, p := range parts[:len(parts)-1] {
		sub, ok := node[p].(map[string]any)
		if !ok {
			sub = map[string]any{}
			node[p] = sub
		}
		node = sub
	}
	node[parts[len(parts)-1]] = value
	out, err := yaml.Marshal(tree)
	require.NoError(t, err)
	return string(out)
}

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// TestLoad_YAMLZeroIsKept is the #631 regression: an explicit false/0/"" in
// the file must survive Load. It fails if defaults are re-applied after the
// decode — by an env-default tag or by any fill-the-zero-fields pass.
func TestLoad_YAMLZeroIsKept(t *testing.T) {
	t.Parallel()
	for _, tc := range zeroCases {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load(writeYAML(t, yamlAt(t, tc.key, tc.zero)))
			require.NoError(t, err)
			assert.Equal(t, tc.zero, tc.get(cfg))
			assert.True(t, cfg.OTel.Enabled, "the file was read")
		})
	}
}

// The issue's repro file, loaded whole: every zero it sets comes back as set.
func TestLoad_IssueReproFile(t *testing.T) {
	t.Parallel()
	cfg, err := Load(writeYAML(t, `
settings:
  dir: ./settings
otel:
  enabled: true
  traces:  { enabled: false, sample_rate: 0 }
  metrics: { enabled: false }
  logs:    { enabled: false, sample_rate: 0 }
cache:
  l1_max_cost: 0
server:
  shutdown_timeout: 0
prometheus:
  path: ""
data_dir: ""
`))
	require.NoError(t, err)
	assert.True(t, cfg.OTel.Enabled)
	assert.False(t, cfg.OTel.Traces.Enabled)
	assert.Zero(t, cfg.OTel.Traces.SampleRate)
	assert.False(t, cfg.OTel.Metrics.Enabled)
	assert.False(t, cfg.OTel.Logs.Enabled)
	assert.Zero(t, cfg.OTel.Logs.SampleRate)
	assert.Zero(t, cfg.Cache.L1MaxCost)
	assert.Zero(t, cfg.Server.ShutdownTimeout)
	assert.Empty(t, cfg.Prometheus.Path)
	assert.Empty(t, cfg.DataDir)
	assert.Equal(t, 8080, cfg.Server.Port, "a key the file leaves out still gets its default")
}

func TestLoad_YAMLZeroPortIsRefused(t *testing.T) {
	t.Parallel()
	_, err := Load(writeYAML(t, "server:\n  port: 0\n"))
	require.ErrorContains(t, err, "server.port 0 out of range", "0 reaches Validate instead of becoming 8080")
}

// A file that exists but leaves a key out gets the default, like no file.
func TestLoad_AbsentKeyGetsDefault(t *testing.T) {
	t.Parallel()
	for _, tc := range zeroCases {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			cfg, err := Load(writeYAML(t, "server:\n  port: 9090\n"))
			require.NoError(t, err)
			assert.Equal(t, tc.def, tc.get(cfg))
			assert.Equal(t, 9090, cfg.Server.Port)
		})
	}
}

// Precedence env > YAML > default, both ways round: env sets a value over a
// YAML zero, and a zero over the default with no file key. Not parallel:
// t.Setenv.
func TestLoad_EnvWinsOverYAMLZeroAndDefault(t *testing.T) {
	for _, tc := range zeroCases {
		t.Run(tc.key+"/over yaml zero", func(t *testing.T) {
			t.Setenv(tc.env, tc.envVal)
			cfg, err := Load(writeYAML(t, yamlAt(t, tc.key, tc.zero)))
			require.NoError(t, err)
			assert.Equal(t, tc.fromEnv, tc.get(cfg))
		})
		t.Run(tc.key+"/zero over yaml value", func(t *testing.T) {
			t.Setenv(tc.env, fmt.Sprint(tc.zero))
			cfg, err := Load(writeYAML(t, yamlAt(t, tc.key, tc.fromEnv)))
			require.NoError(t, err)
			assert.Equal(t, tc.zero, tc.get(cfg))
		})
		t.Run(tc.key+"/zero over default, no file", func(t *testing.T) {
			t.Setenv(tc.env, fmt.Sprint(tc.zero))
			cfg, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
			require.NoError(t, err)
			assert.Equal(t, tc.zero, tc.get(cfg))
		})
	}
}

// Every non-zero default must be in zeroCases, so a new one gets the
// regression coverage above rather than silently skipping it.
func TestZeroCases_CoverEveryNonZeroDefault(t *testing.T) {
	t.Parallel()
	covered := map[string]bool{"server.port": true}
	for _, tc := range zeroCases {
		covered[tc.key] = true
	}
	for _, f := range configFields(t) {
		if !f.def.IsZero() {
			assert.True(t, covered[f.key], "%s has a non-zero default but no zeroCases entry", f.key)
		}
	}
}

// cleanenv's env-default is applied after the YAML decode, to any field still
// zero, which is the #631 bug. Defaults belong in defaults().
func TestConfig_NoEnvDefaultTags(t *testing.T) {
	t.Parallel()
	for _, f := range configFields(t) {
		_, has := f.tag.Lookup("env-default")
		assert.False(t, has, "%s: move its env-default into defaults()", f.key)
	}
}

type configField struct {
	key string
	tag reflect.StructTag
	def reflect.Value
}

// configFields walks defaults() and returns every leaf with its dotted YAML
// path — the same tree rejectUnknownKeys walks.
func configFields(t *testing.T) []configField {
	t.Helper()
	var out []configField
	var walk func(prefix string, v reflect.Value)
	walk = func(prefix string, v reflect.Value) {
		for i := range v.NumField() {
			f := v.Type().Field(i)
			key := strings.Split(f.Tag.Get("yaml"), ",")[0]
			if prefix != "" {
				key = prefix + "." + key
			}
			if f.Type.Kind() == reflect.Struct {
				walk(key, v.Field(i))
				continue
			}
			out = append(out, configField{key: key, tag: f.Tag, def: v.Field(i)})
		}
	}
	walk("", reflect.ValueOf(defaults()))
	require.NotEmpty(t, out)
	return out
}

// TestDocs_DefaultsMatchCode ties configuration.mdx's reference tables to
// defaults() and the env tags: every field has exactly one row, the row names
// its env var, and the documented default parses to the value in code.
func TestDocs_DefaultsMatchCode(t *testing.T) {
	t.Parallel()
	doc, err := os.ReadFile("../../docs/src/content/docs/configuration.mdx")
	require.NoError(t, err)
	type row struct{ env, def string }
	rows := map[string][]row{}
	re := regexp.MustCompile("(?m)^\\| `([a-z0-9_.]+)` \\| `(WH_[A-Z0-9_]+)` \\| ([^|]+?) \\|")
	for _, m := range re.FindAllStringSubmatch(string(doc), -1) {
		rows[m[1]] = append(rows[m[1]], row{m[2], m[3]})
	}
	fields := configFields(t)
	keys := map[string]bool{}
	for _, f := range fields {
		keys[f.key] = true
		got := rows[f.key]
		if !assert.Len(t, got, 1, "%s: want exactly one row in configuration.mdx", f.key) {
			continue
		}
		assert.Equal(t, f.tag.Get("env"), got[0].env, "%s: env var", f.key)
		assert.Equal(t, f.def.Interface(), parseDocDefault(t, f.key, got[0].def, f.def.Interface()), "%s: documented default", f.key)
	}
	for k := range rows {
		assert.True(t, keys[k], "configuration.mdx documents %s, which the Config struct does not declare", k)
	}
}

// parseDocDefault reads a table cell as the type of like.
func parseDocDefault(t *testing.T, key, cell string, like any) any {
	t.Helper()
	cell = strings.TrimSpace(cell)
	if cell == "*(empty)*" || cell == "*(required)*" {
		cell = ""
	} else {
		cell = strings.Trim(cell, "`")
	}
	var (
		v   any
		err error
	)
	switch like.(type) {
	case string:
		v = cell
	case bool:
		v, err = strconv.ParseBool(cell)
	case int:
		v, err = strconv.Atoi(cell)
	case int64:
		v, err = strconv.ParseInt(cell, 10, 64)
	case float64:
		v, err = strconv.ParseFloat(cell, 64)
	default:
		t.Fatalf("%s: no doc parser for %T", key, like)
	}
	require.NoError(t, err, "%s: documented default %q", key, cell)
	return v
}
