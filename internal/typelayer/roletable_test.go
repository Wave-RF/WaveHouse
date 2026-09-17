package typelayer

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
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
	tbl, err := eng.RoleTable("orders", shape)
	require.NoError(t, err)
	t.Cleanup(tbl.Release)
	return tbl
}

// TestRoleTable_IdentityShapeIsTheBaseTable: a role that may write everything
// and injects nothing costs no second compile and no cache entry.
func TestRoleTable_IdentityShapeIsTheBaseTable(t *testing.T) {
	eng := TestEngine(t, ordersTable())

	base, err := eng.Table("orders")
	require.NoError(t, err)
	base.Release()

	role, err := eng.RoleTable("orders", RoleShape{})
	require.NoError(t, err)
	defer role.Release()

	assert.Same(t, base, role)
	assert.Equal(t, 0, base.roles.len())
}

// TestRoleTable_DeniedColumnIsAbsentFromTheSchema is the §A.1 contract: a
// column the role may not write is simply not in the compiled DDL, so a record
// naming it is ClickHouse's own per-row code 117 rather than a Go key walk's
// 403 — and the exported row carries the ROLE's column list.
func TestRoleTable_DeniedColumnIsAbsentFromTheSchema(t *testing.T) {
	eng := TestEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Columns: []string{"id", "tenant", "amount"}})

	assert.Equal(t, []string{"id", "tenant", "amount"}, tbl.WireColumns)

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

// TestRoleTable_DefaultInjectsWhenAbsentAndLosesToASuppliedValue is the §A.3
// contract, measured: DEFAULT '<claim>' fills a column the record omits, and a
// value the record DOES supply still wins.
func TestRoleTable_DefaultInjectsWhenAbsentAndLosesToASuppliedValue(t *testing.T) {
	eng := TestEngine(t, ordersTable())
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
// DDL that is SQL TEXT, so every spelling that could close the literal early
// has to survive as data. The breakout attempt is the case that matters — it
// must not add, remove or retype a single column.
func TestRoleTable_LiteralEscaping(t *testing.T) {
	eng := TestEngine(t, ordersTable())

	for name, value := range map[string]string{
		"apostrophe":       "O'Brien",
		"backslash":        `back\slash`,
		"backslash quote":  `x\'y`,
		"trailing escape":  `ends with \`,
		"ddl breakout":     `', x UInt8 DEFAULT '`,
		"comment breakout": `' --`,
		"empty":            "",
	} {
		t.Run(name, func(t *testing.T) {
			tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"tenant": value}})

			// The column set is the defensive check: an escape that closed the
			// literal early would show up here, not as a mangled value.
			assert.Equal(t, []string{"id", "tenant", "secret", "amount"}, tbl.WireColumns)
			assert.Equal(t, []string{"id", "tenant", "secret", "amount"},
				compiledColumnNames(tbl.slots[0].schema))

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

func TestQuoteLiteral(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":                     `''`,
		"acme":                 `'acme'`,
		"O'Brien":              `'O\'Brien'`,
		`back\slash`:           `'back\\slash'`,
		`x\'y`:                 `'x\\\'y'`,
		`', x UInt8 DEFAULT '`: `'\', x UInt8 DEFAULT \''`,
	}
	for in, want := range cases {
		assert.Equal(t, want, quoteLiteral(in), in)
	}
}

// TestRoleTable_UnparseableLiteralFailsClosed: a literal the column's reader
// cannot read is a compile refusal (ClickHouse code 6), which is the backstop
// under the hand-written escaper. It must be Unavailable — a 503 — never a
// handle that silently drops the injection.
func TestRoleTable_UnparseableLiteralFailsClosed(t *testing.T) {
	eng := TestEngine(t, ordersTable())

	_, err := eng.RoleTable("orders", RoleShape{Defaults: map[string]string{"amount": "abc"}})
	require.Error(t, err)
	assert.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), "orders")
}

// TestRoleTable_ContradictoryShapeIsAnError: a default for a column the shape
// does not carry cannot be expressed. Dropping it silently would turn "force
// this value" into "whatever the caller sent".
func TestRoleTable_ContradictoryShapeIsAnError(t *testing.T) {
	eng := TestEngine(t, ordersTable())

	_, err := eng.RoleTable("orders", RoleShape{
		Columns:  []string{"id", "amount"},
		Defaults: map[string]string{"tenant": "acme"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant")

	_, err = eng.RoleTable("orders", RoleShape{Defaults: map[string]string{"nosuch": "acme"}})
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
	eng := TestEngine(t, ts)

	_, err := eng.RoleTable("orders", RoleShape{Defaults: map[string]string{"double": "1"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MATERIALIZED")

	// A computed column is kept whatever the allow-list says: dropping it would
	// change what the server computes.
	tbl := roleTableFor(t, eng, RoleShape{Columns: []string{"id", "amount"}})
	assert.Equal(t, []string{"id", "amount"}, tbl.WireColumns)
	assert.Equal(t, []string{"id", "amount", "double"}, compiledColumnNames(tbl.slots[0].schema))
}

// TestRoleTable_CachedPerShapeAndGeneration: the same shape must reuse the
// handle (a compile per request is the thing this cache exists to stop), a
// different shape must not, and a rebind must invalidate both.
func TestRoleTable_CachedPerShapeAndGeneration(t *testing.T) {
	eng := TestEngine(t, ordersTable())

	shape := RoleShape{Columns: []string{"tenant", "id", "amount"}, Defaults: map[string]string{"tenant": "acme"}}
	role := func(s RoleShape) *Table {
		tbl, err := eng.RoleTable("orders", s)
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

	base, err := eng.Table("orders")
	require.NoError(t, err)
	assert.Equal(t, 2, base.roles.len())
	base.Release()

	// A rebind closes every projection; the next lookup compiles a fresh one.
	changed := ordersTable()
	changed.Columns[3].Type = "UInt32"
	eng.Bind(TestServerVersion, "UTC", []*discovery.TableSchema{changed})

	after := role(shape)
	assert.NotSame(t, first, after)
	assert.Equal(t, uint64(2), after.Generation)
}

// TestRoleTable_NegativeEntryStopsRecompiling: a shape that will not compile
// costs one compile and one log line per generation, like filterCache.
func TestRoleTable_NegativeEntryStopsRecompiling(t *testing.T) {
	eng := TestEngine(t, ordersTable())

	for range 3 {
		_, err := eng.RoleTable("orders", RoleShape{Defaults: map[string]string{"amount": "abc"}})
		require.Error(t, err)
	}
	base, err := eng.Table("orders")
	require.NoError(t, err)
	defer base.Release()
	assert.Equal(t, 1, base.roles.len())
}

// TestRoleTable_BoundedUnderTenantValueChurn: the injected values come from
// tenant claims and are baked into compiled handles, so the cache must be
// bounded exactly like filterCache.
func TestRoleTable_BoundedUnderTenantValueChurn(t *testing.T) {
	eng := TestEngine(t, ordersTable())

	base, err := eng.Table("orders")
	require.NoError(t, err)
	base.Release()
	base.mu.Lock()
	base.roles = newRoleCache(4)
	base.mu.Unlock()

	for i := range 20 {
		tbl, err := eng.RoleTable("orders", RoleShape{
			Defaults: map[string]string{"tenant": string(rune('a' + i))},
		})
		require.NoError(t, err)
		tbl.Release()
	}

	base, err = eng.Table("orders")
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

// TestRoleTable_HasItsOwnHandlePool: the role handle is the ingest path's hot
// handle, so it gets the pool too.
func TestRoleTable_HasItsOwnHandlePool(t *testing.T) {
	eng := TestEngine(t, ordersTable())
	tbl := roleTableFor(t, eng, RoleShape{Defaults: map[string]string{"tenant": "acme"}})
	assert.Equal(t, poolSize(), len(tbl.slots))
	assert.Nil(t, tbl.roles, "a projection is never itself projected")
}

func (c *roleCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
