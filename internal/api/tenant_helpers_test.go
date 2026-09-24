package api

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// testStore stands in for the store TenantMW resolves. It holds no document:
// handler tests inject fixed getters that ignore it, so a handler that read
// it directly would panic rather than pass. It does carry its tenant — the
// registry below stamps it — which the ingest and stream handlers address the
// message queue with.
var testStore = &settings.Store{}

// withTenant attaches testStore to r the way TenantMW would, for tests that
// call a tenant-route handler without the router.
func withTenant(r *http.Request) *http.Request {
	return r.WithContext(WithStore(r.Context(), testStore))
}

// testTenantRegistry is the registry whose default tenant is testStore, built
// once: NewRegistry stamps the store with its tenant, and parallel tests must
// not each restamp the one they share.
var testTenantRegistry = settings.NewRegistry(testStore)

// testTenants is a registry whose default tenant is testStore.
func testTenants() *settings.Registry { return testTenantRegistry }

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

// fixedRegistry is a RegistrySource fixed to reg, whatever the tenant.
func fixedRegistry(reg *discovery.SchemaRegistry) RegistrySource {
	return func(*settings.Store) *discovery.SchemaRegistry { return reg }
}

// schemaHandlerOver is a SchemaHandler serving reg for every tenant of
// tenants, which the ops reads resolve ?tenant= against.
func schemaHandlerOver(reg *discovery.SchemaRegistry, tenants *settings.Registry) *SchemaHandler {
	h := NewSchemaHandler(fixedRegistry(reg))
	h.Tenants = tenants
	return h
}

// fixedConn is a connection source fixed to conn, whatever the tenant.
func fixedConn(conn driver.Conn) func(*settings.Store) driver.Conn {
	return func(*settings.Store) driver.Conn { return conn }
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

// staticOrigins is a CORS getter fixed to origins, whatever the tenant.
func staticOrigins(origins ...string) func(*settings.Store) []string {
	return func(*settings.Store) []string { return origins }
}

// configWithOrigins is fullConfig with cors.allowed_origins set to origins —
// an empty list with none.
func configWithOrigins(t *testing.T, origins ...string) string {
	t.Helper()
	if origins == nil {
		origins = []string{}
	}
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(fullConfig(100)), &doc))
	cors, err := json.Marshal(map[string][]string{"allowed_origins": origins})
	require.NoError(t, err)
	doc["cors"] = cors
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	return string(out)
}
