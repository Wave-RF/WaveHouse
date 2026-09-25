package settings

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// newLoadedStore materializes a valid directory (with overrides applied) and
// returns the store of the registry that has adopted it.
func newLoadedStore(t *testing.T, overrides map[string]string) *Store {
	t.Helper()
	s, ok := newLoadedRegistry(t, overrides).For(tenant.Default)
	require.True(t, ok)
	return s
}

// A store names its tenant, whichever way it was built: the one Open creates
// carries its folder's name, the flat directory's carries tenant.Default,
// and so does the one NewRegistry is handed.
func TestStore_Tenant(t *testing.T) {
	t.Parallel()
	assert.Equal(t, tenant.Default, newLoadedStore(t, nil).Tenant())

	handed := &Store{}
	assert.Empty(t, handed.Tenant(), "a store outside a registry names no tenant")
	NewRegistry(handed)
	assert.Equal(t, tenant.Default, handed.Tenant())

	reg, findings := Open(writeTree(t, map[string]map[string]string{"acme": validFiles(), "globex": validFiles()}))
	require.NotNil(t, reg, "findings: %s", findingStrings(findings))
	for id, store := range reg.All() {
		assert.Equal(t, id, store.Tenant())
	}
	acme, ok := reg.For("acme")
	require.True(t, ok)
	assert.Equal(t, tenant.ID("acme"), acme.Tenant())
}

func TestStore_DedupeFor_Cascade(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"dedupe": {"require_id": true, "retention": "720h", "tables": {"clicks": {"id_field": "click_id"}, "views": {"require_id": false, "retention": "24h"}, "audit": {"retention": "0"}}}}`),
	})

	tests := []struct {
		name, table string
		want        Dedupe
	}{
		{name: "table overrides id_field, inherits the rest", table: "clicks", want: Dedupe{IDField: "click_id", RequireID: true, Retention: 720 * time.Hour}},
		{name: "table overrides require_id and retention, inherits id_field", table: "views", want: Dedupe{IDField: "event_id", Retention: 24 * time.Hour}},
		{name: "table keeps ids forever under a finite tenant retention", table: "audit", want: Dedupe{IDField: "event_id", RequireID: true}},
		{name: "unlisted table gets globals", table: "other", want: Dedupe{IDField: "event_id", RequireID: true, Retention: 720 * time.Hour}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s.DedupeFor(tt.table))
		})
	}
}

// TestStore_SeedIsValid pins that the shipped starter directory passes its
// own gate: `wavehouse bootstrap` must never write something
// `wavehouse validate` rejects, and the defaults are readable back.
func TestStore_SeedIsValid(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "settings")
	require.NoError(t, WriteSeed(dir))
	reg, findings := Open(dir)
	require.NotNil(t, reg, "findings: %s", findingStrings(findings))
	assert.False(t, HasErrors(findings))
	s, _ := reg.For(tenant.Default)
	// The one expected finding: an empty policies.json is fail-closed and
	// says so. The seed ships no policy on purpose — a policy is a tenant's
	// decision (deployments/compose/settings ships the opt-in trial one).
	assert.Len(t, findings, 1, "findings: %s", findingStrings(findings))
	assert.Contains(t, findingStrings(findings), "no policy")
	assert.Equal(t, Dedupe{IDField: "event_id"}, s.DedupeFor("anything"), "retention 0: ids kept forever, as before retention existed")
	assert.Equal(t, ClickHouse{Addr: "localhost:9000", HTTPPort: 8123, HTTPScheme: "http", Database: "default", Username: "default", QueryTimeout: 30 * time.Second, Headers: map[string]string{}, MaxOpenConns: 10, MaxIdleConns: 5}, s.ClickHouse())
	assert.Equal(t, Auth{JWKSURL: "", RoleClaim: "role"}, s.Auth())
	assert.True(t, s.DLQFor("anything"))
	assert.Equal(t, 10000, s.DefaultMaxRows())
	assert.Equal(t, 60, s.TimestampBucketSeconds())
	assert.Equal(t, 60*time.Second, s.SchemaRefreshInterval())
	period, buckets := s.Keepalive()
	assert.Equal(t, 30*time.Second, period)
	assert.Equal(t, 3, buckets)
	assert.Equal(t, 15*time.Minute, s.GapWindow())
	assert.Equal(t, int64(50)<<30, s.MQMaxBytes())
	assert.Equal(t, []string{"*"}, s.CORSOrigins())

	// Non-empty directory: refused, contents untouched.
	require.NoError(t, os.WriteFile(filepath.Join(dir, FileConfig), []byte(`{}`), 0o600))
	err := WriteSeed(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not empty")
	data, _ := os.ReadFile(filepath.Join(dir, FileConfig)) //nolint:gosec // G304: path is rooted in t.TempDir()
	assert.Equal(t, `{}`, string(data))
}

func TestStore_TypedAccessors(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 250, "timestamp_bucket_seconds": 0}, "schema": {"refresh_interval": 5}, "stream": {"keepalive_interval": 10, "keepalive_buckets": 2, "gap_window_minutes": 0}, "mq": {"max_bytes_gb": 2}, "cors": {"allowed_origins": ["https://app.example.com"]}}`),
	})
	assert.Equal(t, 250, s.DefaultMaxRows())
	assert.Equal(t, 0, s.TimestampBucketSeconds())
	assert.Equal(t, 5*time.Second, s.SchemaRefreshInterval())
	period, buckets := s.Keepalive()
	assert.Equal(t, 10*time.Second, period)
	assert.Equal(t, 2, buckets)
	assert.Equal(t, time.Duration(0), s.GapWindow())
	assert.Equal(t, int64(2)<<30, s.MQMaxBytes())
	assert.Equal(t, []string{"https://app.example.com"}, s.CORSOrigins())
}

func TestStore_ClickHouseAndAuthAccessors(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"clickhouse": {"addr": "ch.internal:9440", "http_port": 8443, "http_scheme": "https", "database": "analytics", "username": "wh", "query_timeout": 5}, "auth": {"jwks_url": "https://idp.example/.well-known/jwks.json", "role_claim": "app_metadata.role"}}`),
	})
	assert.Equal(t, ClickHouse{Addr: "ch.internal:9440", HTTPPort: 8443, HTTPScheme: "https", Database: "analytics", Username: "wh", QueryTimeout: 5 * time.Second, Headers: map[string]string{}, MaxOpenConns: 10, MaxIdleConns: 5}, s.ClickHouse())
	assert.Equal(t, Auth{JWKSURL: "https://idp.example/.well-known/jwks.json", RoleClaim: "app_metadata.role"}, s.Auth())
}

func TestStore_DLQFor_Cascade(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"dlq": {"enabled": true, "tables": {"clicks": {"enabled": false}, "views": {}}}}`),
	})
	assert.False(t, s.DLQFor("clicks"), "table override wins")
	assert.True(t, s.DLQFor("views"), "an override that sets nothing inherits the global switch")
	assert.True(t, s.DLQFor("other"), "unlisted table gets the global switch")
}

func TestStore_PolicyAndPipesAccessors(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, nil) // validFiles: default_role public, one pipe "top_clicks" for analyst

	p := s.Policy()
	require.NotNil(t, p)
	assert.Equal(t, "public", p.DefaultRole)

	q := s.Pipe("top_clicks")
	require.NotNil(t, q)
	assert.Equal(t, []string{"analyst"}, q.AllowedRoles)
	assert.Nil(t, s.Pipe("missing"))
	require.Len(t, s.Pipes(), 1)
	assert.Same(t, q, s.Pipes()[0])

	// Empty documents: nil policy (lockout) and no pipes — never a panic.
	empty := newLoadedStore(t, map[string]string{FilePolicies: `{}`, FilePipes: `{}`})
	assert.Nil(t, empty.Policy())
	assert.Nil(t, empty.Pipe("top_clicks"))
	assert.Empty(t, empty.Pipes())
}
