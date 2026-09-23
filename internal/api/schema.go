package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// RegistrySource yields a tenant's schema registry, its own since #583
// story 6 (the per-tenant discovery map in internal/app). Nil for a tenant
// whose registry is not built yet — the beat between its adoption and the
// hook that builds it — which reads as a schema not loaded yet.
type RegistrySource func(*settings.Store) *discovery.SchemaRegistry

// lookupSchema resolves table's schema for store's tenant, answering the two
// misses itself: a 503 with Retry-After while the tenant's schema is not
// discovered yet (the table may well exist), and a 404 with notFound for a
// table the discovered schema lacks. The error is nil once a schema is
// returned, and names the miss otherwise, after the response was written.
func lookupSchema(w http.ResponseWriter, registry RegistrySource, store *settings.Store, table, notFound string) (*discovery.TableSchema, error) {
	reg := registryOf(registry, store)
	if reg == nil {
		writeUnavailable(w, schemaNotLoadedMessage, retryAfterSchema)
		return nil, discovery.ErrNotLoaded
	}
	schema, err := reg.Lookup(table)
	switch {
	case errors.Is(err, discovery.ErrNotLoaded):
		writeUnavailable(w, schemaNotLoadedMessage, retryAfterSchema)
	case err != nil:
		writeJSONError(w, http.StatusNotFound, notFound)
	}
	return schema, err
}

// registryOf is registry's answer for store, nil for an unwired source.
func registryOf(registry RegistrySource, store *settings.Store) *discovery.SchemaRegistry {
	if registry == nil {
		return nil
	}
	return registry(store)
}

// SchemaHandler exposes the discovered ClickHouse table schemas.
type SchemaHandler struct {
	Registry RegistrySource
	// Tenants resolves the tenant the reads and the refresh serve: /v1/ops
	// is tenant-exempt, so they carry no request tenant and read the one
	// ?tenant= names, the default one without it (opsStore).
	Tenants *settings.Registry
}

func NewSchemaHandler(registry RegistrySource) *SchemaHandler {
	return &SchemaHandler{Registry: registry}
}

// List returns all discovered table schemas of the ?tenant=.
func (h *SchemaHandler) List(w http.ResponseWriter, r *http.Request) {
	store, ok := opsStore(w, r, h.Tenants)
	if !ok {
		return
	}
	h.list(w, store)
}

// list writes store's tenant's schemas, or the 503 of a schema not
// discovered yet: an empty list would read as "no tables".
func (h *SchemaHandler) list(w http.ResponseWriter, store *settings.Store) {
	reg := registryOf(h.Registry, store)
	if reg == nil || !reg.Loaded() {
		writeUnavailable(w, schemaNotLoadedMessage, retryAfterSchema)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reg.List())
}

// Get returns the schema for a single table of the ?tenant=, every table's
// without ?table=.
func (h *SchemaHandler) Get(w http.ResponseWriter, r *http.Request) {
	store, ok := opsStore(w, r, h.Tenants)
	if !ok {
		return
	}
	table := r.URL.Query().Get("table")
	if table == "" {
		h.list(w, store)
		return
	}
	schema, err := lookupSchema(w, h.Registry, store, table, "table not found")
	if err != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(schema)
}

// Refresh forces an immediate schema refresh of the ?tenant= from its
// ClickHouse, then returns its schemas. A tenant with no open pool — the
// connection ceiling refused it — is a 503 with Retry-After, like the reads
// it would answer.
func (h *SchemaHandler) Refresh(w http.ResponseWriter, r *http.Request) {
	store, ok := opsStore(w, r, h.Tenants)
	if !ok {
		return
	}
	reg := registryOf(h.Registry, store)
	if reg == nil {
		writeUnavailable(w, schemaNotLoadedMessage, retryAfterSchema)
		return
	}
	if err := reg.Refresh(r.Context()); err != nil {
		if errors.Is(err, discovery.ErrNoConnection) {
			writeUnavailable(w, noConnectionMessage, retryAfterPool)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "refresh failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reg.List())
}
