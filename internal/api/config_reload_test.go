package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestConfigReload_ReportsAppliedAndRestartOnly(t *testing.T) {
	t.Parallel()
	h := &ConfigReloadHandler{reload: func() (config.ReloadResult, error) {
		return config.ReloadResult{
			Applied:         []string{"dedupe.id_field"},
			RestartRequired: []string{"server"},
		}, nil
	}}

	rec := httptest.NewRecorder()
	h.Handle(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/admin/config/reload", nil))

	testutil.AssertJSONResponse(t, rec, http.StatusOK, map[string]any{
		"applied":          []any{"dedupe.id_field"},
		"restart_required": []any{"server"},
	})
}

func TestConfigReload_NoChangesRendersEmptyArrays(t *testing.T) {
	t.Parallel()
	h := &ConfigReloadHandler{reload: func() (config.ReloadResult, error) {
		return config.ReloadResult{Applied: []string{}, RestartRequired: []string{}}, nil
	}}

	rec := httptest.NewRecorder()
	h.Handle(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/admin/config/reload", nil))

	// [] not null — SDK/jq consumers shouldn't need a null guard.
	testutil.AssertJSONResponse(t, rec, http.StatusOK, map[string]any{
		"applied":          []any{},
		"restart_required": []any{},
	})
}

func TestConfigReload_FailureSurfacesErrorAndKeepsOldConfig(t *testing.T) {
	t.Parallel()
	h := &ConfigReloadHandler{reload: func() (config.ReloadResult, error) {
		return config.ReloadResult{}, errors.New("validate config: clickhouse.http_scheme must be 'http' or 'https'")
	}}

	rec := httptest.NewRecorder()
	h.Handle(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/admin/config/reload", nil))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "previous config still active")
	assert.Contains(t, rec.Body.String(), "http_scheme")
	testutil.AssertJSONErrorResponse(t, rec)
}
