package api

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// testStore stands in for the store TenantMW resolves. It holds no document:
// handler tests inject fixed getters that ignore it, so a handler that read
// it directly would panic rather than pass. It does carry its tenant, which
// the publishers address the message queue with.
var testStore = settings.NewStore(tenant.Default)

// withTenant attaches testStore to r the way TenantMW would, for tests that
// call a tenant-route handler without the router.
func withTenant(r *http.Request) *http.Request {
	return r.WithContext(WithStore(r.Context(), testStore))
}

// testTenants is a registry whose default tenant is testStore.
func testTenants() *settings.Registry { return settings.NewRegistry(testStore) }

// nestedTenants opens a nested settings directory, one folder per entry:
// tenant folder → its config.json (fullConfig for a tenant that is served,
// anything Validate rejects for one that is not).
func nestedTenants(t *testing.T, configs map[string]string) *settings.Registry {
	t.Helper()
	root := t.TempDir()
	for folder, config := range configs {
		require.NoError(t, os.Rename(writeSettingsFixture(t, config), filepath.Join(root, folder)))
	}
	tenants, _ := settings.Open(root)
	require.NotNil(t, tenants)
	return tenants
}

// staticPolicy is a PolicySource fixed to p, whatever the tenant.
func staticPolicy(p *policy.Policy) PolicySource {
	return func(*settings.Store) *policy.Policy { return p }
}

// staticDedup is an IngestHandler.Dedup fixed to d, whatever the tenant.
func staticDedup(d dedupe.Deduplicator) func(*settings.Store) dedupe.Deduplicator {
	return func(*settings.Store) dedupe.Deduplicator { return d }
}

// staticPipes is a PipesHandler source fixed to queries, whatever the tenant.
func staticPipes(queries ...*pipes.NamedQuery) func(*settings.Store) pipes.Source {
	src := pipes.Static(queries...)
	return func(*settings.Store) pipes.Source { return src }
}
