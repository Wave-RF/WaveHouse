package api

import (
	"encoding/json"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// PolicyHandler serves the adopted access-control policy read-only (the
// settings directory's policies.json is the only write path) and the
// validate-without-adopting dry run.
type PolicyHandler struct {
	Source policy.Source

	// maxRequestBytes optionally overrides the default inbound body cap
	// (maxControlBodyBytes) on the decoding path (Validate). When 0, the
	// default applies. Test-only seam (pin the cap-overflow path without
	// allocating 1 MiB per run); not a production knob. Mirrors the other
	// body-decoding handlers in this package.
	maxRequestBytes int64
}

func NewPolicyHandler(source policy.Source) *PolicyHandler {
	return &PolicyHandler{Source: source}
}

// Get returns the current access control policy.
func (h *PolicyHandler) Get(w http.ResponseWriter, r *http.Request) {
	p := h.Source()
	if p == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tables":{}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
}

// Validate checks a policy without saving it.
func (h *PolicyHandler) Validate(w http.ResponseWriter, r *http.Request) {
	reqCap := int64(maxControlBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)
	var p policy.Policy
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		if writeMaxBytesError(w, err, reqCap) {
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if err := policy.Validate(&p); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"valid": true})
}
