package typelayer

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	_, err := eng.Table(id, "events")
	require.Error(t, err, "tenant %s", id)
	var u *Unavailable
	require.ErrorAs(t, err, &u)
	assert.Equal(t, id, u.Tenant)
	return u
}

// TestBind_TenantsOnOneLineWithDifferentZones: the process opened the test
// line in UTC, so a second tenant on that line whose server reports another
// zone cannot be served by this process. It alone is refused, with a cause
// naming both zones; the first tenant, and a third in the line's own zone,
// keep answering.
func TestBind_TenantsOnOneLineWithDifferentZones(t *testing.T) {
	eng := testEngine(t, eventsTable()) // tenant.Default, UTC

	eng.Bind("berlin", testServerVersion, "Europe/Berlin", []*discovery.TableSchema{eventsTable()})
	eng.Bind("utc", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	eng.Bind("unnamed", testServerVersion, "", []*discovery.TableSchema{eventsTable()})

	u := unavailable(t, eng, "berlin")
	assert.Empty(t, u.Table, "the cause covers every table of the tenant")
	assert.Contains(t, u.Cause, `"Europe/Berlin"`)
	assert.Contains(t, u.Cause, `"UTC"`)
	assert.Contains(t, u.Cause, "one timezone per ClickHouse version line")
	assert.Contains(t, u.Error(), "berlin")

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
	assert.Contains(t, u.Cause, "Looked in:", "the SDK's message names every directory searched")

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
	assert.NotSame(t, a.pool.first(), b.pool.first())
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
	assert.Nil(t, held.pool, "the forgotten tenant's handles are closed")
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

// TestPool_GrowsLazilyToItsLimit: one handle after Bind; a call that finds
// every handle busy compiles one more, up to the limit; past it, calls share
// a busy handle; and an idle handle is reused rather than grown past.
func TestPool_GrowsLazilyToItsLimit(t *testing.T) {
	eng := testEngine(t, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
	require.NoError(t, err)
	defer tbl.Release()

	p, cause := newPool(tbl.lib, tbl.pool.ddl, 3)
	require.Empty(t, cause)
	t.Cleanup(p.close)
	require.Len(t, p.list(), 1)

	a := p.acquire()
	assert.Len(t, p.list(), 1, "an idle handle is taken, not grown past")
	b := p.acquire()
	assert.Len(t, p.list(), 2, "every handle busy: grow")
	c := p.acquire()
	assert.Len(t, p.list(), 3)
	d := p.acquire()
	assert.Len(t, p.list(), 3, "at the limit, a call shares a busy handle")
	assert.NotSame(t, a, b)
	assert.NotSame(t, b, c)
	assert.Contains(t, []*schemaSlot{a, b, c}, d)
	assert.Equal(t, int32(2), d.busy.Load())

	for _, s := range []*schemaSlot{a, b, c, d} {
		p.release(s)
	}
	for _, s := range p.list() {
		assert.Zero(t, s.busy.Load())
	}
	e := p.acquire()
	assert.Len(t, p.list(), 3)
	p.release(e)
	for _, s := range p.list() {
		assert.Equal(t, filterCacheSize, s.filters.cap, "every grown slot gets the whole filter budget")
	}
}

// TestPool_PrefersTheLowestIdleSlot: a filter answers only on the slot it
// was compiled on, so a serial run of calls must keep landing on one slot
// (and compile each filter once) rather than rotate through the pool and
// compile it on every slot. A busy slot is skipped, not waited for.
func TestPool_PrefersTheLowestIdleSlot(t *testing.T) {
	eng := testEngine(t, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
	require.NoError(t, err)
	defer tbl.Release()

	p, cause := newPool(tbl.lib, tbl.pool.ddl, 3)
	require.Empty(t, cause)
	t.Cleanup(p.close)
	grown := []*schemaSlot{p.acquire(), p.acquire(), p.acquire()}
	for _, s := range grown {
		p.release(s)
	}
	slots := p.list()
	require.Len(t, slots, 3)

	for range 10 {
		s := p.acquire()
		assert.Same(t, slots[0], s, "a serial caller stays on the first slot")
		p.release(s)
	}

	busy := p.acquire()
	require.Same(t, slots[0], busy)
	for range 5 {
		s := p.acquire()
		assert.Same(t, slots[1], s, "the lowest idle slot, past the busy one")
		p.release(s)
	}
	p.release(busy)
}

// TestTable_PoolGrowsUnderConcurrentHolders drives growth through the public
// path: a parsed Row keeps its handle busy until Close. A burst of concurrent
// parses grows the pool (a caller that loses the race to grow shares a busy
// handle rather than wait for a compile), never past its limit; holding Rows
// while more arrive grows it to exactly the limit.
func TestTable_PoolGrowsUnderConcurrentHolders(t *testing.T) {
	eng := testEngine(t, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
	require.NoError(t, err)
	defer tbl.Release()
	require.Len(t, tbl.pool.list(), 1, "one handle after Bind")
	limit := int(tbl.pool.limit.Load())

	parse := func() *Row {
		row, err := tbl.ParseRow(tbl.WireColumns, []byte(sampleRow))
		require.NoError(t, err)
		return row
	}

	burst := make([]*Row, limit+2)
	var wg sync.WaitGroup
	for i := range burst {
		wg.Go(func() {
			row, err := tbl.ParseRow(tbl.WireColumns, []byte(sampleRow))
			if assert.NoError(t, err) {
				burst[i] = row
			}
		})
	}
	wg.Wait()
	assert.LessOrEqual(t, len(tbl.pool.list()), limit, "never past the limit")

	held := append([]*Row(nil), burst...)
	for range limit {
		held = append(held, parse())
	}
	assert.Len(t, tbl.pool.list(), limit, "sustained holders grow it to exactly the limit")

	// Every held Row still answers on its own handle.
	for _, r := range held {
		require.NotNil(t, r)
		assert.True(t, r.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}}))
		r.Close()
		r.Close() // a second Close is a no-op, not a double release
	}
	for _, s := range tbl.pool.list() {
		assert.Zero(t, s.busy.Load(), "every handle given back")
	}
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
