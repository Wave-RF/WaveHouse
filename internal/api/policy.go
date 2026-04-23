package api

import (
	"encoding/json"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// PolicyHandler handles policy CRUD endpoints.
type PolicyHandler struct {
	Store *policy.Store
}

func NewPolicyHandler(store *policy.Store) *PolicyHandler {
	return &PolicyHandler{Store: store}
}

// Get returns the current access control policy.
func (h *PolicyHandler) Get(w http.ResponseWriter, r *http.Request) {
	p := h.Store.Get()
	if p == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tables":{}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
}

// Put replaces the current access control policy.
func (h *PolicyHandler) Put(w http.ResponseWriter, r *http.Request) {
	var p policy.Policy
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if err := h.Store.Put(r.Context(), &p); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// Validate checks a policy without saving it.
func (h *PolicyHandler) Validate(w http.ResponseWriter, r *http.Request) {
	var p policy.Policy
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if err := policy.Validate(&p); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"valid": true})
}
