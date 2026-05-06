package api

import (
	"encoding/json"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
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
	_ = json.NewEncoder(w).Encode(h.Registry.List())
}

// Get returns the schema for a single table.
func (h *SchemaHandler) Get(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	schema := h.Registry.Get(table)
	if schema == nil {
		writeJSONError(w, http.StatusNotFound, "table not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(schema)
}

// Refresh forces an immediate schema refresh from ClickHouse.
func (h *SchemaHandler) Refresh(w http.ResponseWriter, r *http.Request) {
	if err := h.Registry.Refresh(r.Context()); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "refresh failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h.Registry.List())
}
