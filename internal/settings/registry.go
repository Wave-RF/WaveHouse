package settings

import "github.com/Wave-RF/WaveHouse/internal/tenant"

// Registry maps a tenant id to the Store holding that tenant's adopted
// settings. It holds exactly one store, under tenant.Default: the settings
// directory is one tenant's four files, and Open, Reload, and Watch stay on
// the Store behind it.
type Registry struct {
	stores map[tenant.ID]*Store
}

// NewRegistry returns a Registry serving store as tenant.Default.
func NewRegistry(store *Store) *Registry {
	return &Registry{stores: map[tenant.ID]*Store{tenant.Default: store}}
}

// For returns the store of tenant id, or false when no such tenant exists.
func (r *Registry) For(id tenant.ID) (*Store, bool) {
	s, ok := r.stores[id]
	return s, ok
}
