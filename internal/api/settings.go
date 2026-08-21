package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// SettingsHandler serves the ops surface for the hot-reloadable settings
// directory. Constructed only when a settings directory is configured — with
// none there is nothing to reload, so the route is simply absent (the same
// pattern as the DLQ and policy handlers).
type SettingsHandler struct {
	Store  *settings.Store
	logger *slog.Logger
}

func NewSettingsHandler(store *settings.Store, logger *slog.Logger) *SettingsHandler {
	return &SettingsHandler{Store: store, logger: logger}
}

// reloadResponse is the POST /v1/ops/settings/reload body: whether the
// directory was adopted, and every finding from the validation pass.
// Findings is never null — an empty array keeps clients off the
// nil-vs-empty distinction.
type reloadResponse struct {
	Adopted  bool               `json:"adopted"`
	Findings []settings.Finding `json:"findings"`
}

// Reload handles POST /v1/ops/settings/reload — the API trigger for the same
// serialized reload path SIGHUP and the directory watcher run. 200 when the
// directory was adopted (warnings included in the body), 422 when validation
// rejected it and the previous settings remain in effect.
func (h *SettingsHandler) Reload(w http.ResponseWriter, _ *http.Request) {
	findings, adopted := h.Store.TriggerReload("api")
	if findings == nil {
		findings = []settings.Finding{}
	}
	status := http.StatusOK
	if !adopted {
		status = http.StatusUnprocessableEntity
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(reloadResponse{Adopted: adopted, Findings: findings}); err != nil {
		h.logger.Error("settings reload response encode", "error", err)
	}
}
