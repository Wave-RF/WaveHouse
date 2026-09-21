package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fullConfig is a complete config.json (every key is required) with the
// given query.default_max_rows.
func fullConfig(maxRows int) string {
	return fmt.Sprintf(`{"clickhouse": {"addr": "localhost:9000", "http_port": 8123, "http_scheme": "http", "database": "default", "username": "default", "query_timeout": 30}, "auth": {"jwks_url": "", "role_claim": "role"}, "dedupe": {"enabled": false, "id_field": "event_id", "require_id": false}, "dlq": {"enabled": true}, "query": {"default_max_rows": %d, "timestamp_bucket_seconds": 60}, "schema": {"refresh_interval": 60}, "stream": {"keepalive_interval": 30, "keepalive_buckets": 3, "gap_window_minutes": 15}, "mq": {"max_bytes_gb": 1}, "cors": {"allowed_origins": ["*"]}}`, maxRows)
}

// writeSettingsFixture materializes a minimal valid settings directory whose
// config.json content the caller controls.
func writeSettingsFixture(t *testing.T, configJSON string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range map[string]string{
		settings.FileRoles:    `{"roles": ["public"]}`,
		settings.FilePolicies: `{"default_role": "public"}`,
		settings.FilePipes:    `{}`,
		settings.FileConfig:   configJSON,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	return dir
}

// Subtests are ordered on purpose (the 422 case asserts the value the 200
// case adopted survives), so neither the parent nor the subtests are parallel.
func TestSettingsReload(t *testing.T) {
	dir := writeSettingsFixture(t, fullConfig(100))
	tenants, _ := settings.Open(dir)
	require.NotNil(t, tenants)
	store, _ := tenants.For(tenant.Default)
	h := NewSettingsHandler(tenants)

	post := func() (*httptest.ResponseRecorder, reloadResponse) {
		rec := httptest.NewRecorder()
		h.Reload(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/settings/reload", nil))
		var body reloadResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		return rec, body
	}

	t.Run("valid directory adopts with 200", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FileConfig), []byte(fullConfig(200)), 0o600))
		rec, body := post()
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, body.Adopted)
		assert.NotNil(t, body.Findings, "findings must encode as an array, never null")
		assert.Equal(t, 200, store.DefaultMaxRows(), "adopted settings must be live")
	})

	t.Run("invalid directory keeps previous settings with 422", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FileConfig), []byte(`{"unknown_key": true}`), 0o600))
		rec, body := post()
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.False(t, body.Adopted)
		assert.NotEmpty(t, body.Findings)
		assert.Equal(t, 200, store.DefaultMaxRows(), "rejected reload must keep the previous snapshot")
	})
}

// ?tenant= narrows the reload to one tenant's folder: the folder beside it is
// not read, a rejected folder is a 422 whose findings carry the folder, and a
// query that does not parse is a 400 that reloads nothing — never the whole
// tree the absent parameter means. Subtests are ordered on purpose, so
// neither the parent nor the subtests are parallel.
func TestSettingsReload_TenantParam(t *testing.T) {
	tenants := nestedTenants(t, map[string]string{"acme": fullConfig(100), "globex": fullConfig(100)})
	acme, _ := tenants.For("acme")
	globex, _ := tenants.For("globex")
	h := NewSettingsHandler(tenants)
	rewrite := func(folder, config string) {
		require.NoError(t, os.WriteFile(filepath.Join(tenants.Dir(), folder, settings.FileConfig), []byte(config), 0o600))
	}
	post := func(query string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/settings/reload", nil)
		req.URL.RawQuery = query
		h.Reload(rec, req)
		return rec
	}
	body := func(rec *httptest.ResponseRecorder) reloadResponse {
		var b reloadResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &b))
		return b
	}

	t.Run("a named tenant reloads that folder alone", func(t *testing.T) {
		rewrite("acme", fullConfig(200))
		rewrite("globex", fullConfig(200))
		rec := post("tenant=acme")
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, body(rec).Adopted)
		assert.Equal(t, 200, acme.DefaultMaxRows())
		assert.Equal(t, 100, globex.DefaultMaxRows(), "the folder beside it was not read")
	})

	t.Run("a query that does not parse reloads nothing", func(t *testing.T) {
		for _, query := range []string{"tenant=globex;x=1", "tenant=%zz", "tenant=", "tenant=acme&tenant=globex", "tenant=a.b"} {
			rec := post(query)
			assert.Equal(t, http.StatusBadRequest, rec.Code, query)
			testutil.AssertJSONErrorResponse(t, rec)
		}
		assert.Equal(t, 100, globex.DefaultMaxRows(), "a refused reload must not fall back to the whole tree")
	})

	t.Run("an unknown tenant is a 404", func(t *testing.T) {
		rec := post("tenant=initech")
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Body.String(), "unknown tenant: initech")
	})

	t.Run("a rejected folder is a 422 naming the folder, and the tenant stops being served", func(t *testing.T) {
		rewrite("globex", `{"unknown_key": true}`)
		rec := post("tenant=globex")
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		b := body(rec)
		assert.False(t, b.Adopted)
		require.NotEmpty(t, b.Findings)
		assert.Equal(t, "globex/config.json", b.Findings[0].File)
		_, ok := tenants.For("globex")
		assert.False(t, ok)
		_, ok = tenants.For("acme")
		assert.True(t, ok)
	})

	t.Run("no parameter reloads the whole tree: adopted in part is a 422", func(t *testing.T) {
		rewrite("acme", fullConfig(300))
		rec := post("")
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		b := body(rec)
		assert.False(t, b.Adopted, "globex is still rejected, so not everything was adopted")
		assert.Equal(t, "globex/config.json", b.Findings[0].File)
		assert.Equal(t, 300, acme.DefaultMaxRows(), "the tenant beside the rejected one was adopted")

		rewrite("globex", fullConfig(400))
		rec = post("")
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, body(rec).Adopted)
		recovered, ok := tenants.For("globex")
		require.True(t, ok)
		assert.Equal(t, 400, recovered.DefaultMaxRows())
	})
}

// A flat directory has the default tenant and no other, so ?tenant=0 is the
// reload it always was and any other id is unknown.
func TestSettingsReload_TenantParam_FlatDirectory(t *testing.T) {
	dir := writeSettingsFixture(t, fullConfig(100))
	tenants, _ := settings.Open(dir)
	require.NotNil(t, tenants)
	store, _ := tenants.For(tenant.Default)
	h := NewSettingsHandler(tenants)
	post := func(query string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/settings/reload", nil)
		req.URL.RawQuery = query
		h.Reload(rec, req)
		return rec
	}

	require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FileConfig), []byte(fullConfig(200)), 0o600))
	assert.Equal(t, http.StatusOK, post("tenant=0").Code)
	assert.Equal(t, 200, store.DefaultMaxRows())
	assert.Equal(t, http.StatusNotFound, post("tenant=acme").Code)
}
