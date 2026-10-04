package typelayer

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

const eventsRecord = `{"id":1,"name":"a","ts":"2026-01-15 10:30:00.000","tags":[],"score":null}` + "\n"

// answers asserts that tenant id's events table compiles and judges a record.
func answers(t *testing.T, eng *Engine, id tenant.ID) {
	t.Helper()
	tbl, err := eng.Table(id, "events")
	require.NoError(t, err, "tenant %s", id)
	defer tbl.Release()
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(eventsRecord))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	assert.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
}

// unavailable asserts that tenant id's events table answers *Unavailable for
// the tenant as a whole, and returns it.
func unavailable(t *testing.T, eng *Engine, id tenant.ID) *Unavailable {
	t.Helper()
	tbl, err := eng.Table(id, "events")
	if err == nil {
		tbl.Release() // so the failure below is reported instead of Close deadlocking on it
	}
	require.Error(t, err, "tenant %s", id)
	var u *Unavailable
	require.ErrorAs(t, err, &u)
	assert.Equal(t, id, u.Tenant)
	return u
}

// TestBind_TenantsOnOneLineWithDifferentZones: tenants on one line in
// different zones share its library. The one in another zone than the image's
// is refused only the table whose expressions read a zone-less DateTime (the
// events fixture's `created DEFAULT now()`), with a cause naming both zones;
// its other tables, and every table of the tenants in the image zone, answer.
func TestBind_TenantsOnOneLineWithDifferentZones(t *testing.T) {
	tables := []*discovery.TableSchema{eventsTable(), zonedTable()}
	eng := testEngine(t, tables...) // tenant.Default, UTC

	eng.Bind("berlin", testServerVersion, "Europe/Berlin", tables)
	eng.Bind("utc", testServerVersion, "UTC", tables)
	eng.Bind("unnamed", testServerVersion, "", tables)

	assert.Empty(t, eng.TenantCause("berlin"), "the zone is not tenant-wide")
	u := unavailable(t, eng, "berlin")
	assert.Equal(t, "events", u.Table)
	assert.Contains(t, u.Cause, `"Europe/Berlin"`)
	assert.Contains(t, u.Cause, `"UTC"`)
	assert.Contains(t, u.Error(), "berlin")
	zonedTableOf(t, eng, "berlin")

	answers(t, eng, tenant.Default)
	answers(t, eng, "utc")
	answers(t, eng, "unnamed") // "" is chtypes' own default, UTC

	// Role projections inherit the refusal: they are compiled off the base.
	_, err := eng.RoleTable("berlin", "events", RoleShape{Columns: []string{"id"}})
	require.True(t, IsUnavailable(err))
}

// TestBind_ZoneRefusalClearsWhenTheZoneMatchesAgain: the refusal is about the
// zone the server reports now, not a sticky mark on the tenant.
func TestBind_ZoneRefusalClearsWhenTheZoneMatchesAgain(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind("moved", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, eng, "moved")
	assert.Contains(t, u.Cause, `"Asia/Tokyo"`)

	eng.Bind("moved", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "moved")
}

// TestBind_MissingLineIsThatTenantOnly: a tenant whose server is on a line no
// artifact covers is refused with the SDK's own message; every other tenant
// answers, and the tenant answers once its line resolves.
func TestBind_MissingLineIsThatTenantOnly(t *testing.T) {
	eng := testEngine(t, eventsTable())

	eng.Bind("old", "1.2.3.4", "UTC", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, eng, "old")
	assert.Contains(t, u.Cause, string(chtypes.CodeArtifactMissing), "the SDK's own message and code")

	answers(t, eng, tenant.Default)

	eng.Bind("old", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "old")
}

// TestTable_UnboundTenantIsUnavailable: a tenant discovery has not bound yet
// is a 503, never a 404 — the table may well exist.
func TestTable_UnboundTenantIsUnavailable(t *testing.T) {
	eng := testEngine(t, eventsTable())
	u := unavailable(t, eng, "nobody")
	assert.Equal(t, causeUnbound, u.Cause)
}

// TestBind_TenantsCompileSeparateHandles: two tenants with the same table do
// not share a handle (sharing is a separate decision), and one tenant's
// rebind leaves the other's generation alone.
func TestBind_TenantsCompileSeparateHandles(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind("other", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})

	a, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	b, err := eng.Table("other", "events")
	require.NoError(t, err)
	assert.NotSame(t, a, b)
	assert.NotSame(t, a.c, b.c)
	a.Release()
	b.Release()

	changed := eventsTable()
	changed.Columns = append(changed.Columns, discovery.Column{Name: "extra", Type: "String", Position: 8})
	eng.Bind("other", testServerVersion, "UTC", []*discovery.TableSchema{changed})

	a, err = eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	defer a.Release()
	b, err = eng.Table("other", "events")
	require.NoError(t, err)
	defer b.Release()
	assert.Equal(t, uint64(1), a.Generation)
	assert.Equal(t, uint64(2), b.Generation)
	assert.NotContains(t, a.WireColumns, "extra")
	assert.Contains(t, b.WireColumns, "extra")
}

// TestForget_ClosesOnlyThatTenantAndDoesNotBlock: Forget is called under the
// reload lock, which must never wait on a request. With one of the tenant's
// tables held, Forget still returns at once; the tenant is Unavailable from
// that moment; its handles close once the holder lets go; and another
// tenant's handles are untouched throughout.
func TestForget_ClosesOnlyThatTenantAndDoesNotBlock(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind("gone", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})

	held, err := eng.Table("gone", "events")
	require.NoError(t, err)

	returned := make(chan struct{})
	go func() {
		eng.Forget("gone")
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Forget blocked on a held table")
	}

	u := unavailable(t, eng, "gone")
	assert.Equal(t, causeUnbound, u.Cause)
	answers(t, eng, tenant.Default)

	// The held handle still works: the teardown waits for it.
	batch, err := held.Ingest(FormatJSONEachRow, []byte(eventsRecord))
	require.NoError(t, err)
	assert.True(t, batch.Rows[0].Accepted)

	retired := make(chan struct{})
	go func() {
		eng.retiring.Wait()
		close(retired)
	}()
	select {
	case <-retired:
		t.Fatal("teardown finished while a request still held the table")
	case <-time.After(50 * time.Millisecond):
	}
	held.Release()
	select {
	case <-retired:
	case <-time.After(5 * time.Second):
		t.Fatal("teardown did not finish after the holder released")
	}
	held.mu.RLock()
	assert.Nil(t, held.c, "the forgotten tenant's handles are closed")
	assert.Equal(t, causeRetired, held.cause)
	held.mu.RUnlock()

	answers(t, eng, tenant.Default)

	// A later Bind brings the tenant back on fresh handles.
	eng.Bind("gone", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "gone")
}

// TestClose_WaitsForForgetAndRefusesAfterwards: shutdown leaves no teardown
// running, and nothing answers or binds after it.
func TestClose_WaitsForForgetAndRefusesAfterwards(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind("a", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	eng.Forget("a")
	eng.Close()

	u := unavailable(t, eng, tenant.Default)
	assert.Equal(t, causeClosed, u.Cause)
	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	u = unavailable(t, eng, tenant.Default)
	assert.Equal(t, causeClosed, u.Cause)
	eng.Forget(tenant.Default) // no-op, no panic
}

// TestBind_HeldRoleTableDoesNotBlockBaseLookups: a rebind detaches the old
// role projections and closes them after releasing the base table, so a
// request that holds a projection and then looks the base table up is not
// deadlocked against the rebind waiting for that projection.
func TestBind_HeldRoleTableDoesNotBlockBaseLookups(t *testing.T) {
	eng := testEngine(t, ordersTable())
	rt, err := eng.RoleTable(tenant.Default, "orders", RoleShape{Columns: []string{"id", "tenant"}})
	require.NoError(t, err)

	changed := ordersTable()
	changed.Columns[3].Type = "UInt32"
	bound := make(chan struct{})
	go func() {
		eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})
		close(bound)
	}()

	require.Eventually(t, func() bool {
		base, err := eng.Table(tenant.Default, "orders")
		if err != nil {
			return false
		}
		defer base.Release()
		return base.Generation == 2
	}, 5*time.Second, 5*time.Millisecond, "the base table must be reachable while the old projection is held")

	select {
	case <-bound:
		t.Fatal("Bind returned before the held projection was released")
	default:
	}
	rt.Release()
	select {
	case <-bound:
	case <-time.After(5 * time.Second):
		t.Fatal("Bind did not finish after the projection was released")
	}
}

// TestEngine_ConcurrentBindTableForget runs every tenancy entry point at once
// across a few tenants. Under -race it is the pin that the engine and tenant
// maps, the per-tenant bind serialization and the background teardown are
// safe; every answer is either a verdict or an Unavailable, never a panic or
// a hang.
func TestEngine_ConcurrentBindTableForget(t *testing.T) {
	eng := testEngine(t, eventsTable())
	ids := []tenant.ID{"t0", "t1", "t2", "t3"}
	schemas := func(i int) []*discovery.TableSchema {
		ts := eventsTable()
		if i%2 == 1 {
			ts.Columns = append(ts.Columns, discovery.Column{Name: "extra", Type: "String", Position: 8})
		}
		return []*discovery.TableSchema{ts}
	}

	var wg sync.WaitGroup
	for g := range 12 {
		wg.Go(func() {
			for i := range 15 {
				id := ids[(g+i)%len(ids)]
				switch (g + i) % 4 {
				case 0, 1:
					eng.Bind(id, testServerVersion, "UTC", schemas(i))
				case 2:
					eng.Forget(id)
				default:
					tbl, err := eng.Table(id, "events")
					if err != nil {
						assert.True(t, IsUnavailable(err), "%v", err)
						continue
					}
					batch, err := tbl.Ingest(FormatJSONEachRow, []byte(eventsRecord))
					tbl.Release()
					if assert.NoError(t, err) && assert.Len(t, batch.Rows, 1) {
						assert.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
					}
				}
			}
		})
	}
	wg.Wait()
	eng.retiring.Wait()

	for _, id := range ids {
		eng.Bind(id, testServerVersion, "UTC", schemas(0))
		answers(t, eng, id)
	}
	answers(t, eng, tenant.Default)
	assert.Len(t, eng.tenants, len(ids)+1)
}
