package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSettingsDir materializes a minimal settings directory. Content-rule
// coverage lives in internal/settings; these tests pin the CLI contract only:
// argument/env resolution and exit codes.
func writeSettingsDir(t *testing.T, policies string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"roles.json":    `{"roles": ["public"]}`,
		"policies.json": policies,
		"pipes.json":    `{}`,
		"config.json":   `{"dedupe": {"enabled": false, "id_field": "event_id", "require_id": false}, "query": {"default_max_rows": 10000}, "schema": {"refresh_interval": 60}, "cors": {"allowed_origins": ["*"]}}`,
	}
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	return dir
}

func TestRunValidate(t *testing.T) {
	t.Run("valid directory exits 0", func(t *testing.T) {
		assert.Equal(t, 0, runValidate([]string{writeSettingsDir(t, `{}`)}))
	})

	t.Run("invalid directory exits 1", func(t *testing.T) {
		assert.Equal(t, 1, runValidate([]string{writeSettingsDir(t, `{"default_role": "ghost"}`)}))
	})

	t.Run("env fallback", func(t *testing.T) {
		t.Setenv("WH_SETTINGS_DIR", writeSettingsDir(t, `{}`))
		assert.Equal(t, 0, runValidate(nil))
	})

	t.Run("argument beats env", func(t *testing.T) {
		t.Setenv("WH_SETTINGS_DIR", writeSettingsDir(t, `{}`))
		assert.Equal(t, 1, runValidate([]string{writeSettingsDir(t, `{"default_role": "ghost"}`)}))
	})

	t.Run("no directory is a usage error", func(t *testing.T) {
		t.Setenv("WH_SETTINGS_DIR", "")
		assert.Equal(t, 2, runValidate(nil))
	})

	t.Run("too many arguments is a usage error", func(t *testing.T) {
		assert.Equal(t, 2, runValidate([]string{"a", "b"}))
	})

	t.Run("-h prints help and exits 0", func(t *testing.T) {
		assert.Equal(t, 0, runValidate([]string{"-h"}))
	})

	t.Run("unknown flag is a usage error", func(t *testing.T) {
		assert.Equal(t, 2, runValidate([]string{"--bogus"}))
	})
}
