package api

import (
	"encoding/json"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/config"
)

// ConfigReloadHandler handles POST /v1/admin/config/reload, the HTTP twin of
// SIGHUP (see internal/config/reload.go for the classification).
type ConfigReloadHandler struct {
	// reload is the bound Reloader method; kept as a func field so tests can
	// inject an outcome without a real Reloader + config file on disk.
	reload func() (config.ReloadResult, error)
}

func NewConfigReloadHandler(r *config.Reloader) *ConfigReloadHandler {
	return &ConfigReloadHandler{reload: r.Reload}
}

func (h *ConfigReloadHandler) Handle(w http.ResponseWriter, _ *http.Request) {
	res, err := h.reload()
	if err != nil {
		// A failed reload changes nothing; surface the loader's reason to the operator.
		writeJSONError(w, http.StatusInternalServerError, "config reload failed (previous config still active): "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}
