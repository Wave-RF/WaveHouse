package api

import (
	"encoding/json"
	"net/http"

	"github.com/Wave-RF/BeachHouse/internal/discovery"
	"github.com/go-chi/chi/v5"
)

// SchemaHandler exposes the discovered ClickHouse table schemas.
type SchemaHandler struct {
	Registry *discovery.SchemaRegistry
}

func NewSchemaHandler(registry *discovery.SchemaRegistry) *SchemaHandler {
	return &SchemaHandler{Registry: registry}
}

// List returns all discovered table schemas.
func (h *SchemaHandler) List(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.Registry.List())
}

// Get returns the schema for a single table.
func (h *SchemaHandler) Get(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	schema := h.Registry.Get(table)
	if schema == nil {
		http.Error(w, `{"error":"table not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schema)
}

// Refresh forces an immediate schema refresh from ClickHouse.
func (h *SchemaHandler) Refresh(w http.ResponseWriter, r *http.Request) {
	if err := h.Registry.Refresh(r.Context()); err != nil {
		http.Error(w, `{"error":"refresh failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.Registry.List())
}
