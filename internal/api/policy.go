package api

import (
	"encoding/json"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// PolicyHandler serves the adopted access-control policy read-only. The
// settings directory's policies.json is the only write path, and a candidate
// document is checked with `wavehouse validate` before it reaches the
// directory — there is no HTTP write or dry-run surface.
type PolicyHandler struct {
	Source policy.Source
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
