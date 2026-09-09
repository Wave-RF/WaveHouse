package config

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnboundEnv(t *testing.T) {
	t.Parallel()
	environ := []string{
		"WH_SERVER_PORT=9090",          // struct tag
		"WH_OTEL_LOGS_SAMPLE_RATE=0.5", // nested struct tag
		"WH_SETTINGS_DIR=./settings",   // struct tag
		"WH_CONFIG=/etc/wh.yaml",       // process-level, read by main
		"WH_LOG_LEVEL=debug",           // process-level, read by main
		"WH_DEDUPE_ENABLED=true",       // moved to the settings directory
		"WH_SERVER_PROT=1",             // typo
		"WH_=x",                        // bare prefix
		"PATH=/usr/bin",                // not ours
		"WHATEVER=1",                   // prefix is WH_, not WH
		"NOT_WH_FOO=1",
	}
	assert.Equal(t, []string{"WH_", "WH_DEDUPE_ENABLED", "WH_SERVER_PROT"}, UnboundEnv(environ))
	assert.Empty(t, UnboundEnv(nil))
}

// Every variable cleanenv itself reads must count as bound — a name the
// loader honors but this walk misses would refuse every boot that sets it.
// The oracle is cleanenv's own metadata (GetDescription lists each bound
// name), not collectEnvTags, so a divergence in tag grammar (comma lists,
// env-prefix) shows up here rather than in production.
func TestUnboundEnv_MatchesCleanenv(t *testing.T) {
	t.Parallel()
	desc, err := cleanenv.GetDescription(&Config{}, nil)
	require.NoError(t, err)
	names := regexp.MustCompile(`(?m)^  (WH_[A-Z0-9_]+) `).FindAllStringSubmatch(desc, -1)
	require.NotEmpty(t, names)
	var environ []string
	seen := map[string]bool{}
	for _, m := range names {
		environ = append(environ, m[1]+"=1")
		seen[m[1]] = true
	}
	require.True(t, seen[EnvSettingsDir])
	require.True(t, seen["WH_OTEL_TRACES_SAMPLE_RATE"])
	assert.Empty(t, UnboundEnv(environ))
}

func TestCollectEnvTags_CommaList(t *testing.T) {
	t.Parallel()
	type multi struct {
		A string `env:"WH_ONE,WH_TWO"`
		B string `env:""`
	}
	got := map[string]bool{}
	collectEnvTags(reflect.TypeFor[multi](), got)
	assert.Equal(t, map[string]bool{"WH_ONE": true, "WH_TWO": true}, got)
}

func TestLoad_RejectsUnboundEnv(t *testing.T) {
	t.Setenv("WH_DEDUPE_ENABLED", "true")
	t.Setenv("WH_CH_ADDR", "localhost:9000")
	_, err := Load("nonexistent.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unbound environment variable(s): WH_CH_ADDR, WH_DEDUPE_ENABLED")
	assert.Contains(t, err.Error(), EnvSettingsDir)
}

func TestCheckDataDir(t *testing.T) {
	t.Parallel()
	t.Run("existing writable directory", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, CheckDataDir(dir))
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Empty(t, entries, "the probe file must be removed")
	})

	t.Run("missing directory under a writable parent", func(t *testing.T) {
		t.Parallel()
		dir := filepath.Join(t.TempDir(), "data", "nested")
		require.NoError(t, CheckDataDir(dir))
		_, err := os.Stat(dir)
		assert.True(t, os.IsNotExist(err), "the check must not create the directory")
	})

	t.Run("a file is not a directory", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "data")
		require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))
		err := CheckDataDir(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a directory")
	})

	t.Run("unwritable directory carries the UID hint", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("root writes anywhere")
		}
		dir := t.TempDir()
		require.NoError(t, os.Chmod(dir, 0o500))       //nolint:gosec // G302: an unwritable directory is the point of the test
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) }) //nolint:gosec // G302: restore so TempDir cleanup works
		err := CheckDataDir(dir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not writable")
		assert.Contains(t, err.Error(), "65532")
	})

	t.Run("missing directory under an unwritable parent", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("root writes anywhere")
		}
		parent := t.TempDir()
		require.NoError(t, os.Chmod(parent, 0o500))       //nolint:gosec // G302: an unwritable directory is the point of the test
		t.Cleanup(func() { _ = os.Chmod(parent, 0o700) }) //nolint:gosec // G302: restore so TempDir cleanup works
		err := CheckDataDir(filepath.Join(parent, "data"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be created")
		assert.Contains(t, err.Error(), parent)
	})
}

// A blank data_dir must not pass by probing the working directory — an empty
// WH_DATA_DIR reaches Load as "" and would otherwise scatter NATS and Pebble
// state under the cwd.
func TestCheckDataDir_BlankIsRefused(t *testing.T) {
	t.Parallel()
	for _, dir := range []string{"", "  "} {
		err := CheckDataDir(dir)
		require.Error(t, err, "%q", dir)
		assert.Contains(t, err.Error(), "data_dir (WH_DATA_DIR) is required")
	}
}
