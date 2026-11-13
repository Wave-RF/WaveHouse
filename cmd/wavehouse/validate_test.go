package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSettingsDir materializes a minimal settings directory. Content-rule
// coverage lives in pkg/settings; these tests pin the CLI contract only:
// argument/env resolution and exit codes.
func writeSettingsDir(t *testing.T, policies string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"roles.json":    `{"roles": ["public"]}`,
		"policies.json": policies,
		"pipes.json":    `{}`,
		"config.json":   `{"clickhouse": {"addr": "localhost:9000", "http_port": 8123, "http_scheme": "http", "database": "default", "username": "default", "password": "", "query_timeout": 30, "insert_timeout": 30, "tls": {"enabled": false, "ca_file": "", "cert_file": "", "key_file": "", "insecure_skip_verify": false, "server_name": ""}, "headers": {}, "max_open_conns": 10, "max_idle_conns": 5}, "auth": {"jwks_url": "", "jwt_secret": "", "role_claim": "role"}, "dedupe": {"enabled": false, "id_field": "event_id", "require_id": false, "retention": "0"}, "dlq": {"enabled": true}, "ingest": {"max_batch_bytes": 262144, "max_batch_wait_ms": 5000, "retry_backoff_max": 30}, "query": {"default_max_rows": 10000, "timestamp_bucket_seconds": 60}, "schema": {"refresh_interval": 60}, "stream": {"keepalive_interval": 30, "keepalive_buckets": 3, "gap_window_minutes": 15, "subscriber_queue": 64}, "mq": {"max_bytes_gb": 1}, "cors": {"allowed_origins": ["*"]}}`,
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

	// The token claims a pipe binds are listed under the findings, one line
	// per claim-scoped pipe with its file named as a finding names it, so a
	// reviewer sees the pipe is scoped by the caller's token. A pipe binding
	// none is not listed, and the list is no finding: the directory is valid
	// (#938).
	t.Run("claims a pipe binds are listed", func(t *testing.T) {
		pipes := `{"pipes": [
			{"name": "plain", "sql": "SELECT 1 LIMIT {{limit:10}}"},
			{"name": "daily_totals", "sql": "SELECT day FROM orders WHERE tenant_id IN {{ jwt.app_metadata.tenant_ids }} AND owner = {{jwt.sub}} AND day >= {{start}}", "parameters": [{"name": "start", "type": "string", "required": true}], "allowed_roles": ["public"]}
		]}`
		flat := writeSettingsDir(t, `{}`)
		require.NoError(t, os.WriteFile(filepath.Join(flat, "pipes.json"), []byte(pipes), 0o600))
		var code int
		out := captureStdout(t, func() { code = runValidate([]string{flat}) })
		assert.Equal(t, 0, code)
		assert.Contains(t, out, "pipes.json: pipes[1] \"daily_totals\" binds token claims jwt.app_metadata.tenant_ids, jwt.sub\n")
		assert.NotContains(t, out, `"plain"`)

		root := t.TempDir()
		acme := writeSettingsDir(t, `{}`)
		require.NoError(t, os.WriteFile(filepath.Join(acme, "pipes.json"), []byte(pipes), 0o600))
		require.NoError(t, os.Rename(acme, filepath.Join(root, "acme")))
		out = captureStdout(t, func() { code = runValidate([]string{root}) })
		assert.Equal(t, 0, code)
		assert.Contains(t, out, "acme/pipes.json: pipes[1] \"daily_totals\" binds token claims jwt.app_metadata.tenant_ids, jwt.sub\n")
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

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what
// it wrote. runValidate prints through the fmt package's default writer, as a
// CLI does, and the tests in this package run serially.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	stdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = stdout }()
	read := make(chan []byte)
	go func() {
		out, _ := io.ReadAll(r)
		read <- out
	}()
	fn()
	require.NoError(t, w.Close())
	return string(<-read)
}
