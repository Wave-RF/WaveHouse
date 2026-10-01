package app

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// recordingBinder records what typeBindings asks the type layer to do, one
// line per call naming the tables a bind carried. during, when set, runs
// inside the next Bind.
type recordingBinder struct {
	mu     sync.Mutex
	calls  []string
	during func()
}

func (r *recordingBinder) Bind(id tenant.ID, _, _ string, tables []*discovery.TableSchema) {
	r.mu.Lock()
	call := "bind " + id.String()
	for _, ts := range tables {
		call += " " + ts.Name
	}
	r.calls = append(r.calls, call)
	during := r.during
	r.during = nil
	r.mu.Unlock()
	if during != nil {
		during()
	}
}

func (r *recordingBinder) Forget(id tenant.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, "forget "+id.String())
}

func (r *recordingBinder) log() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// registryOf is a registry of tenant id that discovers the one table named.
func registryOf(id tenant.ID, table string) *discovery.SchemaRegistry {
	conn := testutil.NewSchemaConn([]*discovery.TableSchema{{Name: table, Columns: []discovery.Column{{Name: "id", Type: "String"}}}})
	return discovery.NewSchemaRegistry(func() (driver.Conn, string) { return conn, "default" }, id,
		func(tenant.ID) time.Duration { return time.Hour })
}

// A registry binds its tenant from every successful refresh until it is
// retired; its retirement forgets the tenant, and a refresh that outlives it
// binds nothing.
func TestTypeBindings_TheOwnerBindsUntilRetired(t *testing.T) {
	t.Parallel()
	rec := &recordingBinder{}
	b := newTypeBindings(rec)
	reg := registryOf("acme", "a")
	b.attach("acme", reg)

	require.NoError(t, reg.Refresh(t.Context()))
	require.NoError(t, reg.Refresh(t.Context()))
	b.detach("acme", reg)
	require.NoError(t, reg.Refresh(t.Context()))
	b.detach("acme", reg)

	assert.Equal(t, []string{"bind acme a", "bind acme a", "forget acme"}, rec.log())
}

// A registry retired while it binds — the reload that moved its tenant
// landing mid-refresh — forgets the tenant again once its bind returns, and
// its successor's first bind waits for that, so the previous database's
// tables never stay bound over the new one's. A late detach of the retired
// registry leaves the successor's binding alone.
func TestTypeBindings_RetiredMidBindNeverOutlivesTheSuccessor(t *testing.T) {
	t.Parallel()
	rec := &recordingBinder{}
	b := newTypeBindings(rec)
	old, successor := registryOf("acme", "old_db"), registryOf("acme", "new_db")
	b.attach("acme", old)

	entered, release := make(chan struct{}), make(chan struct{})
	rec.during = func() {
		close(entered)
		<-release
	}
	oldDone := make(chan error, 1)
	go func() { oldDone <- old.Refresh(context.Background()) }()
	<-entered

	// The reload: retire the old registry, attach the new one, whose loop
	// refreshes at once.
	b.detach("acme", old)
	b.attach("acme", successor)
	successorDone := make(chan error, 1)
	go func() { successorDone <- successor.Refresh(context.Background()) }()
	select {
	case <-successorDone:
		t.Fatal("the successor bound while the retired registry's bind was in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-oldDone)
	require.NoError(t, <-successorDone)
	b.detach("acme", old)
	assert.Equal(t, []string{"bind acme old_db", "forget acme", "forget acme", "bind acme new_db"}, rec.log())
}

// Tenants bind independently: one tenant's retirement forgets only its own.
func TestTypeBindings_TenantsAreIndependent(t *testing.T) {
	t.Parallel()
	rec := &recordingBinder{}
	b := newTypeBindings(rec)
	acme, globex := registryOf("acme", "a"), registryOf("globex", "g")
	b.attach("acme", acme)
	b.attach("globex", globex)
	require.NoError(t, acme.Refresh(t.Context()))
	require.NoError(t, globex.Refresh(t.Context()))
	b.detach("acme", acme)
	require.NoError(t, globex.Refresh(t.Context()))
	assert.Equal(t, []string{"bind acme a", "bind globex g", "forget acme", "bind globex g"}, rec.log())
}

// Every registry a reload retires is reported with its tenant: the one a
// drop retires (a tenant moved to another database) and the one a reconcile
// retires (a tenant no longer served).
func TestDiscoveries_ReportEveryRetiredRegistry(t *testing.T) {
	t.Parallel()
	type retired struct {
		id  tenant.ID
		reg *discovery.SchemaRegistry
	}
	var got []retired
	// Registries with no connection: their loops retry without ever loading.
	build := func(id tenant.ID, _ *settings.Store) *discovery.SchemaRegistry {
		return discovery.NewSchemaRegistry(func() (driver.Conn, string) { return nil, "" }, id,
			func(tenant.ID) time.Duration { return time.Hour })
	}
	d := newDiscoveries(t.Context(), build, func(tenant.ID, error) {}, func(tenant.ID) {},
		func(id tenant.ID, reg *discovery.SchemaRegistry) { got = append(got, retired{id, reg}) })
	t.Cleanup(func() { assert.NoError(t, d.close(context.Background())) })

	root := writeNestedSettings(t, map[string]map[string]any{"acme": nil, "globex": nil})
	tenants, _ := settings.Open(root)
	require.NotNil(t, tenants)
	d.reconcile(tenants)
	acme, globex := d.For("acme"), d.For("globex")
	require.NotNil(t, acme)
	require.NotNil(t, globex)
	require.Empty(t, got)

	d.drop([]tenant.ID{"acme", "initech"})
	require.Equal(t, []retired{{"acme", acme}}, got, "only a tenant with a registry has one to retire")

	require.NoError(t, os.RemoveAll(filepath.Join(root, "globex")))
	tenants.Reload("test")
	d.reconcile(tenants)
	assert.Equal(t, []retired{{"acme", acme}, {"globex", globex}}, got)
	assert.NotNil(t, d.For("acme"), "the dropped tenant, still served, has a fresh registry")
	assert.NotSame(t, acme, d.For("acme"))
}
