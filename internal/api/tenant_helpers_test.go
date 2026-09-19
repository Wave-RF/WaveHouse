package api

import (
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// testStore stands in for the store TenantMW resolves. It holds no document:
// handler tests inject fixed getters that ignore it, so a handler that read
// it directly would panic rather than pass.
var testStore = &settings.Store{}

// withTenant attaches testStore to r the way TenantMW would, for tests that
// call a tenant-route handler without the router.
func withTenant(r *http.Request) *http.Request {
	return r.WithContext(WithStore(r.Context(), testStore))
}

// testTenants is a registry whose default tenant is testStore.
func testTenants() *settings.Registry { return settings.NewRegistry(testStore) }

// staticPolicy is a PolicySource fixed to p, whatever the tenant.
func staticPolicy(p *policy.Policy) PolicySource {
	return func(*settings.Store) *policy.Policy { return p }
}

// staticPipes is a PipesHandler source fixed to queries, whatever the tenant.
func staticPipes(queries ...*pipes.NamedQuery) func(*settings.Store) pipes.Source {
	src := pipes.Static(queries...)
	return func(*settings.Store) pipes.Source { return src }
}
