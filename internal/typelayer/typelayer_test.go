package typelayer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// eventsTable reproduces M1's measured table: every column kind the ingest path
// has to answer for, including the two that never reach the wire.
func eventsTable() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "events",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt8", Position: 1},
			{Name: "name", Type: "String", Position: 2},
			{Name: "ts", Type: "DateTime64(3)", Position: 3},
			{Name: "tags", Type: "Array(String)", Position: 4},
			{Name: "score", Type: "Nullable(Int32)", IsNullable: true, Position: 5},
			{Name: "created", Type: "DateTime", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "now()", Position: 6},
			{Name: "id_plus", Type: "UInt16", HasDefault: true, DefaultKind: "MATERIALIZED", DefaultExpression: "id + 1", Position: 7},
		},
	}
}

func TestBind_CompilesAndExposesWireColumns(t *testing.T) {
	eng := TestEngine(t, eventsTable())

	tbl, err := eng.Table("events")
	require.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, uint64(1), tbl.Generation)
	// MATERIALIZED never crosses the wire: RowsExport does not serialize it and
	// the envelope's column list must match the exported line's arity.
	assert.Equal(t, []string{"id", "name", "ts", "tags", "score", "created"}, tbl.WireColumns)
}

func TestTable_UnknownTableIsUnavailable(t *testing.T) {
	eng := TestEngine(t, eventsTable())
	_, err := eng.Table("nosuch")
	require.Error(t, err)
	assert.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), "nosuch")
}

// TestBind_GenerationBumpsOnlyOnSignatureChange: a refresh that discovers the
// same columns must not invalidate handles or cached filters, and one that
// discovers a new column must.
func TestBind_GenerationBumpsOnlyOnSignatureChange(t *testing.T) {
	eng := TestEngine(t, eventsTable())

	generation := func() uint64 {
		tbl, err := eng.Table("events")
		require.NoError(t, err)
		defer tbl.Release()
		return tbl.Generation
	}
	require.Equal(t, uint64(1), generation())

	eng.Bind(TestServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	assert.Equal(t, uint64(1), generation(), "identical columns keep the handle")

	changed := eventsTable()
	changed.Columns = append(changed.Columns, discovery.Column{Name: "extra", Type: "String", Position: 8})
	eng.Bind(TestServerVersion, "UTC", []*discovery.TableSchema{changed})

	tbl, err := eng.Table("events")
	require.NoError(t, err)
	defer tbl.Release()
	assert.Equal(t, uint64(2), tbl.Generation)
	assert.Contains(t, tbl.WireColumns, "extra")
}

// TestBind_DroppedTableBecomesUnavailable: a table that leaves the database
// must stop answering rather than serve a handle for a schema that is gone.
func TestBind_DroppedTableBecomesUnavailable(t *testing.T) {
	eng := TestEngine(t, eventsTable())
	eng.Bind(TestServerVersion, "UTC", nil)

	_, err := eng.Table("events")
	require.Error(t, err)
	assert.True(t, IsUnavailable(err))
}

// TestBind_MissingArtifact_UnavailableWithSDKMessage: the SDK's own text names
// every directory it searched and the command that installs the artifact —
// that is the whole diagnostic, so it is passed through verbatim.
func TestBind_MissingArtifact_UnavailableWithSDKMessage(t *testing.T) {
	eng := TestEngine(t, eventsTable())

	eng.Bind("1.2.3.4", "UTC", []*discovery.TableSchema{eventsTable()})
	_, err := eng.Table("events")
	require.Error(t, err)
	require.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), "no artifact for ClickHouse 1.2")
	assert.Contains(t, err.Error(), "Looked in:")

	// Rebinding a version that does resolve clears the global cause.
	eng.Bind(TestServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	tbl, err := eng.Table("events")
	require.NoError(t, err)
	tbl.Release()
}

// TestBind_TimezoneMismatchIsGlobalAndNamesBothZones: chtypes reads its
// Timezone global once per dlopen, so a server that changes zone cannot be
// adopted in-process. Every table must stop answering, loudly.
func TestBind_TimezoneMismatchIsGlobalAndNamesBothZones(t *testing.T) {
	eng := TestEngine(t, eventsTable())

	eng.Bind(TestServerVersion, "Europe/Berlin", []*discovery.TableSchema{eventsTable()})
	_, err := eng.Table("events")
	require.Error(t, err)
	require.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), "Europe/Berlin")
	assert.Contains(t, err.Error(), "UTC")
	assert.Contains(t, err.Error(), "restart")
}

// TestBind_CompileRefusalIsPerTable: one undeclarable table must not take the
// rest of the database down with it.
func TestBind_CompileRefusalIsPerTable(t *testing.T) {
	broken := &discovery.TableSchema{
		Name:    "broken",
		Columns: []discovery.Column{{Name: "x", Type: "NotAType(9)", Position: 1}},
	}
	eng := TestEngine(t, eventsTable(), broken)

	good, err := eng.Table("events")
	require.NoError(t, err)
	good.Release()

	_, err = eng.Table("broken")
	require.Error(t, err)
	require.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), "broken")
}

func TestSignatureDistinguishesEveryField(t *testing.T) {
	t.Parallel()
	base := &discovery.TableSchema{Columns: []discovery.Column{
		{Name: "a", Type: "UInt8", DefaultKind: "DEFAULT", DefaultExpression: "1", Position: 1},
	}}
	sig := signature(base)
	for _, mutate := range []func(c *discovery.Column){
		func(c *discovery.Column) { c.Name = "b" },
		func(c *discovery.Column) { c.Type = "UInt16" },
		func(c *discovery.Column) { c.DefaultKind = "MATERIALIZED" },
		func(c *discovery.Column) { c.DefaultExpression = "2" },
		func(c *discovery.Column) { c.Position = 2 },
	} {
		other := &discovery.TableSchema{Columns: []discovery.Column{base.Columns[0]}}
		mutate(&other.Columns[0])
		assert.NotEqual(t, sig, signature(other))
	}
}

// renderExpr is what a test asserts against: the expression typelayer hands
// chtypes, so a change in quoting or parameter naming is visible.
func renderExpr(t *testing.T, tbl *Table, preds ...Predicate) (string, map[string]string) {
	t.Helper()
	expr, params, ok := tbl.render(preds)
	require.True(t, ok)
	return expr, params
}

func TestRender_QuotesIdentifiersAndBindsEveryValue(t *testing.T) {
	eng := TestEngine(t, eventsTable())
	tbl, err := eng.Table("events")
	require.NoError(t, err)
	defer tbl.Release()

	expr, params := renderExpr(t, tbl,
		Predicate{Column: "name", Op: "=", Values: []string{"acme"}},
		Predicate{Column: "id", Op: "in", Values: []string{"1", "7"}},
	)
	// Every identifier is backticked — chsql.QuoteIdent, the same rule the SQL
	// path uses, rather than chtypes.QuoteIdentifier, which leaves reserved
	// words like `all` bare. Every value binds as String whatever the column's
	// declared type (id is UInt8 here), and is a bound parameter, never text.
	assert.Equal(t, "`name` = {p0:String} AND `id` IN ({p1:String}, {p2:String})", expr)
	assert.Equal(t, map[string]string{"p0": "acme", "p1": "1", "p2": "7"}, params)

	hostile := Predicate{Column: "name", Op: "=", Values: []string{"' OR 1=1 --"}}
	expr, params = renderExpr(t, tbl, hostile)
	assert.Equal(t, "`name` = {p0:String}", expr)
	assert.Equal(t, "' OR 1=1 --", params["p0"])
	assert.NotContains(t, expr, "OR 1=1")
}

// TestRender_QuotesEveryIdentifier: a column whose name is a reserved word is
// a syntax error unquoted, and chtypes.QuoteIdentifier would leave it bare.
func TestRender_QuotesEveryIdentifier(t *testing.T) {
	reserved := &discovery.TableSchema{
		Name:    "reserved",
		Columns: []discovery.Column{{Name: "all", Type: "String", Position: 1}},
	}
	eng := TestEngine(t, reserved)
	tbl, err := eng.Table("reserved")
	require.NoError(t, err)
	defer tbl.Release()

	expr, _ := renderExpr(t, tbl, Predicate{Column: "all", Op: "=", Values: []string{"x"}})
	assert.Equal(t, "`all` = {p0:String}", expr)
}

func TestRender_BackticksAnIdentifierThatNeedsIt(t *testing.T) {
	odd := &discovery.TableSchema{
		Name:    "odd",
		Columns: []discovery.Column{{Name: "weird name", Type: "String", Position: 1}},
	}
	eng := TestEngine(t, odd)
	tbl, err := eng.Table("odd")
	require.NoError(t, err)
	defer tbl.Release()

	expr, _ := renderExpr(t, tbl, Predicate{Column: "weird name", Op: "=", Values: []string{"x"}})
	assert.Equal(t, "`weird name` = {p0:String}", expr)
}

func TestRender_RefusesWhatItCannotExpress(t *testing.T) {
	eng := TestEngine(t, eventsTable())
	tbl, err := eng.Table("events")
	require.NoError(t, err)
	defer tbl.Release()

	for name, pred := range map[string]Predicate{
		"unknown column":    {Column: "nosuch", Op: "=", Values: []string{"x"}},
		"empty values":      {Column: "name", Op: "=", Values: nil},
		"unknown operator":  {Column: "name", Op: "like", Values: []string{"x"}},
		"multi-value equal": {Column: "name", Op: "=", Values: []string{"a", "b"}},
	} {
		_, _, ok := tbl.render([]Predicate{pred})
		assert.False(t, ok, name)
	}
}

func TestFilterCache_EvictsAndClosesOldest(t *testing.T) {
	t.Parallel()
	c := newFilterCache(2)
	for i := range 3 {
		key := fmt.Sprintf("k%d", i)
		c.index[key] = c.order.PushFront(&filterEntry{key: key})
		if c.order.Len() > c.cap {
			c.evictOldestLocked()
		}
	}
	assert.Equal(t, 2, c.order.Len())
	assert.NotContains(t, c.index, "k0")
}

// TestBind_WireColumnsComeFromTheCompiledHandle: the wire list is read off
// LoadedSchema.Columns, so it is a property of the handle that produced the
// bytes rather than a second derivation from discovery that could drift from
// it. All four default kinds are present so the filter is exercised whole.
func TestBind_WireColumnsComeFromTheCompiledHandle(t *testing.T) {
	kinds := &discovery.TableSchema{
		Name: "kinds",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "plain", Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'zzz'", Position: 2},
			{Name: "mat", Type: "UInt32", HasDefault: true, DefaultKind: "MATERIALIZED", DefaultExpression: "id + 1", Position: 3},
			{Name: "ali", Type: "UInt32", HasDefault: true, DefaultKind: "ALIAS", DefaultExpression: "id + 2", Position: 4},
			{Name: "eph", Type: "UInt8", DefaultKind: "EPHEMERAL", Position: 5},
		},
	}
	eng := TestEngine(t, kinds)
	tbl, err := eng.Table("kinds")
	require.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, []string{"id", "plain"}, tbl.WireColumns)
	assert.Equal(t, []string{"id", "plain", "mat", "ali", "eph"}, compiledColumnNames(tbl.slots[0].schema),
		"every declared column is known to the handle, of every kind")
	// render tests against the handle's own column set, not the wire list: a
	// filter may name a MATERIALIZED column.
	_, _, ok := tbl.render([]Predicate{{Column: "mat", Op: "=", Values: []string{"2"}}})
	assert.True(t, ok)
	_, _, ok = tbl.render([]Predicate{{Column: "nosuch", Op: "=", Values: []string{"2"}}})
	assert.False(t, ok)
}

// TestBind_HandlePoolPerTable: every table gets the pool, and a rebind
// replaces all of it.
func TestBind_HandlePoolPerTable(t *testing.T) {
	eng := TestEngine(t, eventsTable())
	tbl, err := eng.Table("events")
	require.NoError(t, err)
	assert.Equal(t, poolSize(), len(tbl.slots))
	first := tbl.slots[0]
	tbl.Release()

	changed := eventsTable()
	changed.Columns[0].Type = "UInt16"
	eng.Bind(TestServerVersion, "UTC", []*discovery.TableSchema{changed})

	tbl, err = eng.Table("events")
	require.NoError(t, err)
	defer tbl.Release()
	assert.Equal(t, poolSize(), len(tbl.slots))
	assert.NotSame(t, first, tbl.slots[0])
}
