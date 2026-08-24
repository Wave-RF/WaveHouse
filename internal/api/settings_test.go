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
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fullConfig is a complete config.json (every key is required) with the
// given query.default_max_rows.
func fullConfig(maxRows int) string {
	return fmt.Sprintf(`{"dedupe": {"enabled": false, "id_field": "event_id", "require_id": false}, "query": {"default_max_rows": %d}, "schema": {"refresh_interval": 60}, "cors": {"allowed_origins": ["*"]}}`, maxRows)
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
	store, _ := settings.Open(dir, testutil.NopLogger())
	require.NotNil(t, store)
	h := NewSettingsHandler(store, testutil.NopLogger())

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
