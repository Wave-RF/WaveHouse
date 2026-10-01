package typelayer

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// ordersTable is the per-role fixture: an ordinary column a role may be denied
// (secret), one a check clause injects into (tenant), and a numeric column
// whose reader refuses a text literal (amount).
func ordersTable() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "orders",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "tenant", Type: "String", Position: 2},
			{Name: "secret", Type: "String", Position: 3},
			{Name: "amount", Type: "UInt64", Position: 4},
		},
	}
}

func roleTableFor(t *testing.T, eng *Engine, shape RoleShape) *Table {
	t.Helper()
	tbl, err := eng.RoleTable(tenant.Default, "orders", shape)
	require.NoError(t, err)
	t.Cleanup(tbl.Release)
	return tbl
}

// TestRoleTable_IdentityShapeIsTheBaseTable: a role that may write everything
// and injects nothing costs no second compile and no cache entry.
func TestRoleTable_IdentityShapeIsTheBaseTable(t *testing.T) {
	eng := testEngine(t, ordersTable())

	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	base.Release()

	role, err := eng.RoleTable(tenant.Default, "orders", RoleShape{})
	require.NoError(t, err)
	defer role.Release()

	assert.Same(t, base, role)
	assert.Equal(t, 0, base.roles.len())
}

// TestRoleTable_DeniedColumnIsUnnamableAndOffTheWire: a column the role may
// not write is declared MATERIALIZED, so a record naming it is ClickHouse's
// own per-row code 117 rather than a Go key walk's 403 — and the exported row
// carries the ROLE's column list.
func TestRoleTable_DeniedColumnIsUnnamableAndOffTheWire(t *testing.T) {
	eng := testEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Columns: []string{"id", "tenant", "amount"}})

	assert.Equal(t, []string{"id", "tenant", "amount"}, tbl.WireColumns)
	assert.Equal(t, []string{"id", "tenant", "secret", "amount"}, compiledColumnNames(tbl.pool.first().schema),
		"still declared, so an expression or check over it compiles")

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(
		`{"id":1,"tenant":"acme","amount":5}`+"\n"+
			`{"id":2,"tenant":"acme","secret":"x","amount":5}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 2)

	assert.True(t, batch.Rows[0].Accepted)
	assert.Equal(t, `[1, "acme", 5]`, string(batch.Rows[0].Line))

	assert.False(t, batch.Rows[1].Accepted)
	assert.False(t, batch.Rows[1].Declined, "a denied column is a verdict about the data, not a decline")
	assert.Equal(t, 117, batch.Rows[1].Code)
	assert.Contains(t, batch.Rows[1].Message, "secret")
}

// TestRoleTable_DefaultInjectsWhenAbsentAndLosesToASuppliedValue, measured on
// the 26.6 and 26.8 artifacts: DEFAULT '<claim>' fills a column the record
// omits, and a value the record DOES supply still wins.
func TestRoleTable_DefaultInjectsWhenAbsentAndLosesToASuppliedValue(t *testing.T) {
	eng := testEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"tenant": "acme"}})

	assert.Equal(t, []string{"id", "tenant", "secret", "amount"}, tbl.WireColumns)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(
		`{"id":1,"amount":5}`+"\n"+
			`{"id":2,"tenant":"other","amount":5}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 2)

	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, `[1, "acme", "", 5]`, string(batch.Rows[0].Line))
	require.True(t, batch.Rows[1].Accepted, batch.Rows[1].Message)
	assert.Equal(t, `[2, "other", "", 5]`, string(batch.Rows[1].Line))
}

// TestRoleTable_LiteralEscaping: the injected value is the one thing in this
// DDL that is SQL TEXT, spelled by the library's QuoteLiteral, so every
// spelling that could close the literal early has to survive as data. The
// breakout attempt is the case that matters — it must not add, remove or
// retype a single column.
func TestRoleTable_LiteralEscaping(t *testing.T) {
	eng := testEngine(t, ordersTable())

	for name, value := range map[string]string{
		"apostrophe":       "O'Brien",
		"backslash":        `back\slash`,
		"backslash quote":  `x\'y`,
		"trailing escape":  `ends with \`,
		"ddl breakout":     `', x UInt8 DEFAULT '`,
		"comment breakout": `' --`,
		"empty":            "",
		"nul":              "a\x00b",
		"newline and tab":  "a\nb\tc",
		"unicode":          "Ünï 日本",
	} {
		t.Run(name, func(t *testing.T) {
			tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"tenant": value}})

			// An escape that closed the literal early would show up here, as an
			// extra or missing column, not as a mangled value.
			assert.Equal(t, []string{"id", "tenant", "secret", "amount"}, tbl.WireColumns)
			assert.Equal(t, []string{"id", "tenant", "secret", "amount"},
				compiledColumnNames(tbl.pool.first().schema))

			batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"amount":5}`+"\n"))
			require.NoError(t, err)
			require.Len(t, batch.Rows, 1)
			require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)

			// The exported line is ClickHouse's own writer on the stored value,
			// so it is the ground truth for what the DEFAULT actually holds.
			want, err := json.Marshal(value)
			require.NoError(t, err)
			assert.Equal(t, `[1, `+string(want)+`, "", 5]`, string(batch.Rows[0].Line))
		})
	}
}

// TestRoleTable_UnparseableLiteralFailsClosed: a literal the column's reader
// cannot read is a compile refusal (ClickHouse code 6). It must be a
// RoleRefused — never a handle that silently drops the injection, and never an
// Unavailable, which a caller answers with a retry hint.
func TestRoleTable_UnparseableLiteralFailsClosed(t *testing.T) {
	eng := testEngine(t, ordersTable())

	_, err := eng.RoleTable(tenant.Default, "orders", RoleShape{Defaults: map[string]string{"amount": "abc"}})
	require.Error(t, err)
	refused, ok := errors.AsType[*RoleRefused](err)
	require.True(t, ok, "%T: %v", err, err)
	assert.False(t, IsUnavailable(err))
	assert.Equal(t, "orders", refused.Table)
	assert.Contains(t, refused.Cause, "code 6")
}

// TestRoleTable_DeniedColumnAnExpressionReadsStillCompiles: dropping a denied
// column would leave every expression over it uncompilable and refuse the
// whole role. Re-declared MATERIALIZED with what the server stores when the
// worker omits it, the role compiles, and a DEFAULT column the role DOES write
// is computed from the same value the server would use — its own DEFAULT, or
// its type's default.
func TestRoleTable_DeniedColumnAnExpressionReadsStillCompiles(t *testing.T) {
	ts := &discovery.TableSchema{
		Name: "visits",
		Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "ip", Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'0.0.0.0'", Position: 2},
			{Name: "ip_hash", Type: "UInt64", HasDefault: true, DefaultKind: "MATERIALIZED", DefaultExpression: "cityHash64(ip)", Position: 3},
			{Name: "ip_len", Type: "UInt64", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "length(ip)", Position: 4},
			{Name: "n", Type: "Nullable(UInt8)", IsNullable: true, Position: 5},
			{Name: "n_set", Type: "UInt8", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "isNotNull(n)", Position: 6},
		},
	}
	eng := testEngine(t, ts)
	tbl, err := eng.RoleTable(tenant.Default, "visits", RoleShape{Columns: []string{"page", "ip_len", "n_set"}})
	require.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, []string{"page", "ip_len", "n_set"}, tbl.WireColumns)
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(
		`{"page":"/home"}`+"\n"+`{"page":"/a","ip":"1.2.3.4"}`+"\n"+`{"page":"/b","n":1}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 3)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, `["\/home", 7, 0]`, string(batch.Rows[0].Line), "length('0.0.0.0'), and n at its type default NULL")
	assert.Equal(t, 117, batch.Rows[1].Code)
	assert.Contains(t, batch.Rows[1].Message, "ip")
	assert.Equal(t, 117, batch.Rows[2].Code)
	assert.Contains(t, batch.Rows[2].Message, "n")

	// A check over a denied column tests what the server will store there.
	check, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"page":"/home"}`+"\n"+`{"page":"/b"}`+"\n"),
		Predicate{Column: "ip", Op: "=", Values: []string{"0.0.0.0"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"", ""}, checkReasons(t, check))
	check, err = tbl.Ingest(FormatJSONEachRow, []byte(`{"page":"/home"}`+"\n"),
		Predicate{Column: "ip", Op: "=", Values: []string{"1.2.3.4"}})
	require.NoError(t, err)
	assert.Equal(t, []string{ReasonFilter}, checkReasons(t, check))
}

// TestRoleTable_ShapeWithNothingToWriteIsRefused: a role that may write no
// plain or DEFAULT column would publish rows with no columns at all.
func TestRoleTable_ShapeWithNothingToWriteIsRefused(t *testing.T) {
	ts := ordersTable()
	ts.Columns = append(ts.Columns, discovery.Column{
		Name: "double", Type: "UInt64", HasDefault: true,
		DefaultKind: "MATERIALIZED", DefaultExpression: "amount * 2", Position: 5,
	})
	eng := testEngine(t, ts)

	for _, cols := range [][]string{{}, {"double"}, {"nosuch"}} {
		_, err := eng.RoleTable(tenant.Default, "orders", RoleShape{Columns: cols})
		refused, ok := errors.AsType[*RoleRefused](err)
		require.True(t, ok, "%v: %T %v", cols, err, err)
		assert.Contains(t, refused.Cause, "no plain or DEFAULT column")
	}
}

// TestRoleTable_EphemeralFollowsTheRoleColumns: a record may supply an
// EPHEMERAL column only when the role may write it. Denied, it is still
// declared — the DEFAULT over it compiles and takes its own default — and a
// record naming it is refused.
func TestRoleTable_EphemeralFollowsTheRoleColumns(t *testing.T) {
	ts := &discovery.TableSchema{
		Name: "eph",
		Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "secret", Type: "String", Position: 2},
			{Name: "ip", Type: "String", DefaultKind: "EPHEMERAL", HasDefault: true, Position: 3},
			{Name: "ip_len", Type: "UInt64", DefaultKind: "DEFAULT", DefaultExpression: "length(ip)", HasDefault: true, Position: 4},
		},
	}
	eng := testEngine(t, ts)
	body := []byte(`{"page":"/a","ip":"1.2.3.4"}` + "\n")

	allowed, err := eng.RoleTable(tenant.Default, "eph", RoleShape{Columns: []string{"page", "ip", "ip_len"}})
	require.NoError(t, err)
	batch, err := allowed.Ingest(FormatJSONEachRow, body)
	allowed.Release()
	require.NoError(t, err)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, `["\/a", 7]`, string(batch.Rows[0].Line))

	denied, err := eng.RoleTable(tenant.Default, "eph", RoleShape{Columns: []string{"page", "ip_len"}})
	require.NoError(t, err)
	batch, err = denied.Ingest(FormatJSONEachRow, append(body, `{"page":"/b"}`+"\n"...))
	denied.Release()
	require.NoError(t, err)
	assert.Equal(t, 117, batch.Rows[0].Code)
	require.True(t, batch.Rows[1].Accepted, batch.Rows[1].Message)
	assert.Equal(t, `["\/b", 0]`, string(batch.Rows[1].Line))
}

// TestRoleTable_ContradictoryShapeIsAnError: a default for a column the shape
// does not carry cannot be expressed. Dropping it silently would turn "force
// this value" into "whatever the caller sent".
func TestRoleTable_ContradictoryShapeIsAnError(t *testing.T) {
	eng := testEngine(t, ordersTable())

	_, err := eng.RoleTable(tenant.Default, "orders", RoleShape{
		Columns:  []string{"id", "amount"},
		Defaults: map[string]string{"tenant": "acme"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant")

	_, err = eng.RoleTable(tenant.Default, "orders", RoleShape{Defaults: map[string]string{"nosuch": "acme"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nosuch")
}

// TestRoleTable_DefaultIntoAComputedColumnIsRefused: MATERIALIZED/ALIAS/
// EPHEMERAL are never supplied by a record, and rewriting one into a plain
// DEFAULT would change what the server stores.
func TestRoleTable_DefaultIntoAComputedColumnIsRefused(t *testing.T) {
	ts := ordersTable()
	ts.Columns = append(ts.Columns, discovery.Column{
		Name: "double", Type: "UInt64", HasDefault: true,
		DefaultKind: "MATERIALIZED", DefaultExpression: "amount * 2", Position: 5,
	})
	eng := testEngine(t, ts)

	_, err := eng.RoleTable(tenant.Default, "orders", RoleShape{Defaults: map[string]string{"double": "1"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MATERIALIZED")

	// A computed column is kept whatever the allow-list says: dropping it would
	// change what the server computes. So is a denied one, MATERIALIZED.
	tbl := roleTableFor(t, eng, RoleShape{Columns: []string{"id", "amount"}})
	assert.Equal(t, []string{"id", "amount"}, tbl.WireColumns)
	assert.Equal(t, []string{"id", "tenant", "secret", "amount", "double"}, compiledColumnNames(tbl.pool.first().schema))
}

// TestRoleTable_CachedPerShapeAndGeneration: the same shape must reuse the
// handle (a compile per request is the thing this cache exists to stop), a
// different shape must not, and a rebind must invalidate both.
func TestRoleTable_CachedPerShapeAndGeneration(t *testing.T) {
	eng := testEngine(t, ordersTable())

	shape := RoleShape{Columns: []string{"tenant", "id", "amount"}, Defaults: map[string]string{"tenant": "acme"}}
	role := func(s RoleShape) *Table {
		tbl, err := eng.RoleTable(tenant.Default, "orders", s)
		require.NoError(t, err)
		// Released immediately: a rebind waits for every projection's readers,
		// so a held handle would block Bind rather than be invalidated by it.
		tbl.Release()
		return tbl
	}

	first := role(shape)
	// Column ORDER is not part of the shape — the DDL always follows the
	// table's declaration order.
	assert.Same(t, first, role(RoleShape{
		Columns:  []string{"amount", "tenant", "id"},
		Defaults: map[string]string{"tenant": "acme"},
	}))
	assert.NotSame(t, first, role(RoleShape{
		Columns:  []string{"tenant", "id", "amount"},
		Defaults: map[string]string{"tenant": "beta"},
	}), "the injected value is baked into the handle")

	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	assert.Equal(t, 2, base.roles.len())
	base.Release()

	// A rebind closes every projection; the next lookup compiles a fresh one.
	changed := ordersTable()
	changed.Columns[3].Type = "UInt32"
	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})

	after := role(shape)
	assert.NotSame(t, first, after)
	assert.Equal(t, uint64(2), after.Generation)
}

// TestRoleTable_NegativeEntryStopsRecompiling: a shape that will not compile
// costs one compile and one log line per generation, like filterCache.
func TestRoleTable_NegativeEntryStopsRecompiling(t *testing.T) {
	eng := testEngine(t, ordersTable())

	for range 3 {
		_, err := eng.RoleTable(tenant.Default, "orders", RoleShape{Defaults: map[string]string{"amount": "abc"}})
		require.Error(t, err)
	}
	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	defer base.Release()
	assert.Equal(t, 1, base.roles.len())
}

// TestRoleTable_BoundedUnderTenantValueChurn: the injected values come from
// tenant claims and are baked into compiled handles, so the cache must be
// bounded exactly like filterCache.
func TestRoleTable_BoundedUnderTenantValueChurn(t *testing.T) {
	eng := testEngine(t, ordersTable())

	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	base.Release()
	base.mu.Lock()
	base.roles = newRoleCache(4)
	base.mu.Unlock()

	for i := range 20 {
		tbl, err := eng.RoleTable(tenant.Default, "orders", RoleShape{
			Defaults: map[string]string{"tenant": string(rune('a' + i))},
		})
		require.NoError(t, err)
		tbl.Release()
	}

	base, err = eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	defer base.Release()
	assert.Equal(t, 4, base.roles.len())

	// The survivors still answer after their neighbours were closed.
	tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"tenant": "acme"}})
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"amount":5}`+"\n"))
	require.NoError(t, err)
	require.True(t, batch.Rows[0].Accepted)
	assert.Equal(t, `[1, "acme", "", 5]`, string(batch.Rows[0].Line))
}

// TestRoleTable_PoolGrowsUnderContentionToALowerCap: a role shape has its own
// pool, not the base table's. A quiet shape holds one handle; concurrent
// inserts through it grow the pool like a base table's — one handle would
// serialize the role's whole ingest — but only to min(GOMAXPROCS, 4), since a
// table holds up to roleCacheSize shapes.
func TestRoleTable_PoolGrowsUnderContentionToALowerCap(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(8))
	eng := testEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"tenant": "acme"}})
	assert.Nil(t, tbl.roles, "a projection is never itself projected")

	p := tbl.pool
	require.Len(t, p.list(), 1, "a quiet shape holds one handle")
	assert.Equal(t, int64(4), p.limit.Load(), "min(GOMAXPROCS=8, 4): capped below the base table's 8")

	held := make([]*schemaSlot, 0, 5)
	for range 5 {
		held = append(held, p.acquire())
	}
	assert.Len(t, p.list(), 4, "busy handles grow the pool to its cap and no further")
	for _, s := range held {
		p.release(s)
	}

	// The grown handles answer like the first: the injected default included.
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"amount":5}`+"\n"))
	require.NoError(t, err)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, `[1, "acme", "", 5]`, string(batch.Rows[0].Line))
}

func (c *roleCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// heldCompile wraps the orders table's role compile so the compile of one
// shape (Defaults tenant=claim) blocks until Release; entered is closed once
// that compile has started. compiles counts every compile.
type heldCompile struct {
	claim    string
	entered  chan struct{}
	release  chan struct{}
	released sync.Once
	compiles atomic.Int32
	// panics makes the held compile panic, once, when released.
	panics atomic.Bool
}

// Release lets the held compile finish. Also run at cleanup, before the
// engine closes: a failed assertion must not leave the engine's Close
// waiting on a compile nobody releases.
func (h *heldCompile) Release() { h.released.Do(func() { close(h.release) }) }

func holdCompile(t *testing.T, eng *Engine, claim string) (*heldCompile, *roleCache) {
	t.Helper()
	h := &heldCompile{claim: claim, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(h.Release)
	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	defer base.Release()
	c := base.roles
	c.mu.Lock()
	defer c.mu.Unlock()
	var once sync.Once
	c.compile = func(tbl *Table, shape RoleShape) (*Table, string) {
		h.compiles.Add(1)
		if shape.Defaults["tenant"] == h.claim {
			once.Do(func() { close(h.entered) })
			<-h.release
			if h.panics.CompareAndSwap(true, false) {
				panic("compile panicked")
			}
		}
		return tbl.compileRole(shape)
	}
	return h, c
}

// within fails the test unless fn returns within the deadline. The deadline
// only bounds a failure; a pass never waits for it.
func within(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return while another compile was held open", what)
	}
}

func claimShape(claim string) RoleShape {
	return RoleShape{Defaults: map[string]string{"tenant": claim}}
}

// injects asserts that a projection built from claimShape(claim) ingests a
// record omitting tenant with claim filled in.
func injects(t *testing.T, tbl *Table, claim string) {
	t.Helper()
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"amount":5}`+"\n"))
	if assert.NoError(t, err) && assert.Len(t, batch.Rows, 1) {
		assert.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
		assert.Equal(t, `[1, "`+claim+`", "", 5]`, string(batch.Rows[0].Line))
	}
}

// TestRoleTable_MissesOnDifferentShapesDoNotSerialize: a miss compiles
// outside the cache lock, so while one shape's compile is held open, a hit
// and a miss on another shape of the same table both answer. With the
// compile under the lock, both would wait for the held one.
func TestRoleTable_MissesOnDifferentShapesDoNotSerialize(t *testing.T) {
	eng := testEngine(t, ordersTable())
	warm, err := eng.RoleTable(tenant.Default, "orders", claimShape("warm"))
	require.NoError(t, err)
	warm.Release()

	h, _ := holdCompile(t, eng, "slow")
	slow := make(chan struct{})
	go func() {
		defer close(slow)
		tbl, err := eng.RoleTable(tenant.Default, "orders", claimShape("slow"))
		if assert.NoError(t, err) {
			injects(t, tbl, "slow")
			tbl.Release()
		}
	}()
	<-h.entered

	for _, claim := range []string{"warm", "other"} { // a hit, then a miss on another shape
		within(t, "a lookup of "+claim, func() {
			tbl, err := eng.RoleTable(tenant.Default, "orders", claimShape(claim))
			if assert.NoError(t, err) {
				injects(t, tbl, claim)
				tbl.Release()
			}
		})
	}

	h.Release()
	<-slow
}

// TestRoleTable_ConcurrentMissesOnOneShapeCompileOnce: lookups that miss on a
// shape already compiling wait for that compile instead of starting their
// own, and all of them get the one projection it built.
func TestRoleTable_ConcurrentMissesOnOneShapeCompileOnce(t *testing.T) {
	eng := testEngine(t, ordersTable())
	h, c := holdCompile(t, eng, "shared")

	const lookups = 8
	got := make([]*Table, lookups)
	var wg sync.WaitGroup
	for i := range got {
		wg.Go(func() {
			tbl, err := eng.RoleTable(tenant.Default, "orders", claimShape("shared"))
			if assert.NoError(t, err) {
				injects(t, tbl, "shared")
				tbl.Release()
				got[i] = tbl
			}
		})
	}
	<-h.entered
	key := claimShape("shared").key(1)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		f := c.inflight[key]
		return f != nil && f.waiters == lookups-1
	}, 10*time.Second, time.Millisecond, "every other lookup parks on the compile in flight")

	h.Release()
	wg.Wait()
	require.NotNil(t, got[0])
	for _, tbl := range got[1:] {
		assert.Same(t, got[0], tbl)
	}
	assert.Equal(t, int32(1), h.compiles.Load())
	c.mu.Lock()
	assert.Empty(t, c.inflight)
	c.mu.Unlock()
}

// TestRoleTable_PanickingCompileReleasesItsWaiters: a compile that panics
// withdraws its flight and releases the base table on the way out, so a
// lookup parked on it compiles for itself instead of waiting forever, and a
// rebind still completes.
func TestRoleTable_PanickingCompileReleasesItsWaiters(t *testing.T) {
	eng := testEngine(t, ordersTable())
	h, c := holdCompile(t, eng, "boom")
	h.panics.Store(true)

	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_, _ = eng.RoleTable(tenant.Default, "orders", claimShape("boom"))
	}()
	<-h.entered
	waited := make(chan struct{})
	go func() {
		defer close(waited)
		tbl, err := eng.RoleTable(tenant.Default, "orders", claimShape("boom"))
		if assert.NoError(t, err, "the waiter compiles the shape itself") {
			injects(t, tbl, "boom")
			tbl.Release()
		}
	}()
	key := claimShape("boom").key(1)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		f := c.inflight[key]
		return f != nil && f.waiters == 1
	}, 10*time.Second, time.Millisecond)

	h.Release()
	assert.Equal(t, "compile panicked", <-panicked)
	<-waited
	assert.Equal(t, int32(2), h.compiles.Load())

	changed := ordersTable()
	changed.Columns[3].Type = "UInt32"
	within(t, "a rebind after the panic", func() {
		eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})
	})
}

// TestRoleTable_ConcurrentLookupsEvictionAndRebind races lookups on shared
// and distinct shapes, through a cache small enough to evict on most
// misses, against rebinds. Under -race it pins the flight bookkeeping, the
// evicted handle closing outside the cache lock, and a rebind's closeAll
// waiting out compiles in flight; every lookup answers a projection that
// injects its own claim.
func TestRoleTable_ConcurrentLookupsEvictionAndRebind(t *testing.T) {
	eng := testEngine(t, ordersTable())
	base, err := eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	base.Release()
	base.mu.Lock()
	base.roles = newRoleCache(4) // a rebind keeps the capacity
	base.mu.Unlock()

	var wg sync.WaitGroup
	for g := range 12 {
		wg.Go(func() {
			for i := range 25 {
				claim := "c" + strconv.Itoa((g*7+i)%10)
				tbl, err := eng.RoleTable(tenant.Default, "orders", claimShape(claim))
				if !assert.NoError(t, err) {
					return
				}
				injects(t, tbl, claim)
				tbl.Release()
			}
		})
	}
	wg.Go(func() {
		for i := range 6 {
			changed := ordersTable()
			if i%2 == 0 {
				changed.Columns[3].Type = "UInt32"
			}
			eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})
		}
	})
	wg.Wait()

	base, err = eng.Table(tenant.Default, "orders")
	require.NoError(t, err)
	defer base.Release()
	assert.Equal(t, uint64(7), base.Generation)
	assert.LessOrEqual(t, base.roles.len(), 4)
	base.roles.mu.Lock()
	assert.Empty(t, base.roles.inflight)
	base.roles.mu.Unlock()
}

// BenchmarkRoleTable_ClaimChurn is a per-role lookup whose shape carries an
// end user's claim (an insert check such as user_id = {{jwt.sub}}, injected
// as a column default), so each distinct claim is a distinct shape.
// claims=64 sits inside roleCacheSize and measures the hit; claims=300
// cycles past it, so every lookup compiles. It runs in parallel because a
// miss compiles outside the cache lock: misses on different shapes should
// scale with cores rather than queue behind one another.
func BenchmarkRoleTable_ClaimChurn(b *testing.B) {
	eng := testEngine(b, rowsTable())
	for _, n := range []int{64, 300} {
		shapes := make([]RoleShape, n)
		for i := range shapes {
			shapes[i] = RoleShape{Defaults: map[string]string{"tenant": "user-" + strconv.Itoa(i)}}
		}
		b.Run(fmt.Sprintf("claims=%d", n), func(b *testing.B) {
			var next atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					tbl, err := eng.RoleTable(tenant.Default, "rows", shapes[int(next.Add(1))%n])
					if err != nil {
						b.Fatal(err)
					}
					tbl.Release()
				}
			})
		})
	}
}
