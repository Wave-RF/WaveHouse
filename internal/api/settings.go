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
	Tenants *settings.Registry
}

func NewSettingsHandler(tenants *settings.Registry) *SettingsHandler {
	return &SettingsHandler{Tenants: tenants}
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
//
// ?tenant= narrows the reload to that tenant's folder of a nested directory
// (400 malformed, 404 unknown): adopted then speaks for that folder alone,
// and a 422 means the tenant is no longer served. Without it the whole tree
// is reloaded, and over a nested directory a 422 can mean adopted in part —
// the findings name the folders that were not (settings.Registry.Reload).
func (h *SettingsHandler) Reload(w http.ResponseWriter, r *http.Request) {
	id, named, ok := opsTenant(w, r)
	if !ok {
		return
	}
	var findings []settings.Finding
	var adopted bool
	if named {
		var known bool
		if findings, adopted, known = h.Tenants.ReloadTenant(id, "api"); !known {
			writeJSONError(w, http.StatusNotFound, "unknown tenant: "+id.String())
			return
		}
	} else {
		findings, adopted = h.Tenants.Reload("api")
	}
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
		slog.ErrorContext(r.Context(), "settings reload response encode", "error", err)
	}
}
