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
		"config.json":   `{"clickhouse": {"addr": "localhost:9000", "http_port": 8123, "http_scheme": "http", "database": "default", "username": "default", "query_timeout": 30, "tls": {"enabled": false, "ca_file": "", "cert_file": "", "key_file": "", "insecure_skip_verify": false, "server_name": ""}, "headers": {}, "max_open_conns": 10, "max_idle_conns": 5}, "auth": {"jwks_url": "", "role_claim": "role"}, "dedupe": {"enabled": false, "id_field": "event_id", "require_id": false}, "dlq": {"enabled": true}, "query": {"default_max_rows": 10000, "timestamp_bucket_seconds": 60}, "schema": {"refresh_interval": 60}, "stream": {"keepalive_interval": 30, "keepalive_buckets": 3, "gap_window_minutes": 15}, "mq": {"max_bytes_gb": 1}, "cors": {"allowed_origins": ["*"]}}`,
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

	// A nested root — one folder per tenant — shares the exit codes: every
	// folder valid is 0, one invalid folder is 1.
	t.Run("nested directory", func(t *testing.T) {
		nested := func(policies map[string]string) string {
			root := t.TempDir()
			for folder, p := range policies {
				require.NoError(t, os.Rename(writeSettingsDir(t, p), filepath.Join(root, folder)))
			}
			return root
		}
		assert.Equal(t, 0, runValidate([]string{nested(map[string]string{"acme": `{}`, "globex": `{}`})}))
		assert.Equal(t, 1, runValidate([]string{nested(map[string]string{"acme": `{}`, "globex": `{"default_role": "ghost"}`})}))
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
