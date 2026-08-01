package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// SettingsHandler serves the runtime-settings admin surface
// (/v1/admin/settings): the live-tunable tier of the config split, stored in
// the control-plane database and disjoint from the boot config file/env (see
// internal/settings).
type SettingsHandler struct {
	Store *settings.Store

	// maxRequestBytes optionally overrides the default inbound body cap
	// (maxControlBodyBytes) on the decoding path (PutDedupe). Test-only seam;
	// mirrors the other body-decoding handlers in this package.
	maxRequestBytes int64
}

func NewSettingsHandler(store *settings.Store) *SettingsHandler {
	return &SettingsHandler{Store: store}
}

// settingsResponse is the GET body: effective values plus, per section,
// whether they come from a stored database override or the compiled defaults.
type settingsResponse struct {
	Values  settings.Values         `json:"values"`
	Sources []settings.SectionState `json:"sources"`
}

// Get returns every runtime setting's effective value and source. Values and
// sources come from one store snapshot: read separately, a concurrent PUT
// could pair stale values with the new revision, and a caller replaying that
// revision in If-Match would overwrite a change it never saw.
func (h *SettingsHandler) Get(w http.ResponseWriter, _ *http.Request) {
	values, sources := h.Store.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(settingsResponse{Values: values, Sources: sources})
}

// PutDedupe replaces the dedupe section, applied live (no restart, no signal).
// A full-section PUT: an omitted field takes its zero value, not its previous
// one — callers send the whole section. An optional If-Match header makes the
// write conditional on the section revision the caller read, so concurrent
// admins get a 409 instead of silently losing a race (see parseIfMatch).
func (h *SettingsHandler) PutDedupe(w http.ResponseWriter, r *http.Request) {
	expected, err := parseIfMatch(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	reqCap := int64(maxControlBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)
	dec := json.NewDecoder(r.Body)
	// Settings are config: a typo'd field silently ignored would read as
	// applied. Reject unknown fields instead.
	dec.DisallowUnknownFields()
	var d settings.Dedupe
	if err := dec.Decode(&d); err != nil {
		if writeMaxBytesError(w, err, reqCap) {
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if dec.More() {
		// Same fail-loud contract as DisallowUnknownFields: trailing content
		// would be silently discarded, reading as applied.
		writeJSONError(w, http.StatusBadRequest, "invalid json: unexpected data after the settings object")
		return
	}

	if err := h.Store.PutDedupe(r.Context(), d, expected); err != nil {
		switch {
		case errors.Is(err, settings.ErrRevisionConflict):
			writeJSONError(w, http.StatusConflict, err.Error()+"; GET /v1/admin/settings and retry with the new revision")
		case errors.Is(err, settings.ErrInvalid):
			writeJSONError(w, http.StatusBadRequest, err.Error())
		default:
			// A storage failure, not a bad request. The body stays generic —
			// driver detail is server-side information, logged by the store.
			writeJSONError(w, http.StatusInternalServerError, "store dedupe settings failed")
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// parseIfMatch reads the optional If-Match header: the section revision the
// caller saw (sources[].revision on GET; ETag-style quotes accepted), with 0
// meaning "no override stored yet". Absent → nil, an unconditional write.
func parseIfMatch(r *http.Request) (*uint64, error) {
	raw := strings.TrimSpace(r.Header.Get("If-Match"))
	if raw == "" {
		return nil, nil
	}
	rev, err := strconv.ParseUint(strings.Trim(raw, `"`), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid If-Match %q: expected a section revision number", raw)
	}
	return &rev, nil
}

// ResetDedupe deletes the stored override; the section reverts to the
// compiled defaults (the state of a deployment that never touched the API).
func (h *SettingsHandler) ResetDedupe(w http.ResponseWriter, r *http.Request) {
	if err := h.Store.ResetDedupe(r.Context()); err != nil {
		// Generic for the same reason as the PUT's 500: the store logged the
		// driver detail.
		writeJSONError(w, http.StatusInternalServerError, "reset dedupe settings failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}
