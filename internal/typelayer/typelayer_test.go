package typelayer

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// eventsTable is the measured fixture: every column kind the ingest path has
// to answer for, including the one that never reaches the wire.
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
	eng := testEngine(t, eventsTable())

	tbl, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, uint64(1), tbl.Generation)
	// MATERIALIZED never crosses the wire: the export does not serialize it and
	// the envelope's column list must match the exported line's arity.
	assert.Equal(t, []string{"id", "name", "ts", "tags", "score", "created"}, tbl.WireColumns)
}

func TestTable_UnknownTableIsUnavailable(t *testing.T) {
	eng := testEngine(t, eventsTable())
	_, err := eng.Table(tenant.Default, "nosuch")
	require.Error(t, err)
	assert.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), "nosuch")
}

// TestBind_GenerationBumpsOnlyOnSignatureChange: a refresh that discovers the
// same columns must not invalidate handles or cached filters, and one that
// discovers a new column must.
func TestBind_GenerationBumpsOnlyOnSignatureChange(t *testing.T) {
	eng := testEngine(t, eventsTable())

	generation := func() uint64 {
		tbl, err := eng.Table(tenant.Default, "events")
		require.NoError(t, err)
		defer tbl.Release()
		return tbl.Generation
	}
	require.Equal(t, uint64(1), generation())

	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	assert.Equal(t, uint64(1), generation(), "identical columns keep the handle")

	changed := eventsTable()
	changed.Columns = append(changed.Columns, discovery.Column{Name: "extra", Type: "String", Position: 8})
	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})

	tbl, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	defer tbl.Release()
	assert.Equal(t, uint64(2), tbl.Generation)
	assert.Contains(t, tbl.WireColumns, "extra")
}

// TestBind_DroppedTableBecomesUnavailable: a table that leaves the database
// must stop answering rather than serve a handle for a schema that is gone.
func TestBind_DroppedTableBecomesUnavailable(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind(tenant.Default, testServerVersion, "UTC", nil)

	_, err := eng.Table(tenant.Default, "events")
	require.Error(t, err)
	assert.True(t, IsUnavailable(err))
}

// TestBind_MissingArtifact_UnavailableWithSDKMessage: the SDK's own text names
// the line, the platform and the artifact code — that is the whole diagnostic,
// so it is passed through verbatim.
func TestBind_MissingArtifact_UnavailableWithSDKMessage(t *testing.T) {
	eng := testEngine(t, eventsTable())

	eng.Bind(tenant.Default, "1.2.3.4", "UTC", []*discovery.TableSchema{eventsTable()})
	_, err := eng.Table(tenant.Default, "events")
	require.Error(t, err)
	require.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), "no installed artifact for ClickHouse 1.2")
	assert.Contains(t, err.Error(), string(chtypes.CodeArtifactMissing))

	// Rebinding a version that does resolve clears the tenant's cause.
	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	tbl, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	tbl.Release()
}

// TestBind_ZoneRefusalIsPerTableAndNamesBothZones: a tenant in another zone
// than the image's is served, except a table whose expressions would compute
// over a zone-less DateTime in the image zone. That table alone stops
// answering, loudly, and a rebind back into the image zone serves it again.
func TestBind_ZoneRefusalIsPerTableAndNamesBothZones(t *testing.T) {
	plain := &discovery.TableSchema{Name: "plain", Columns: []discovery.Column{
		{Name: "id", Type: "UInt8", Position: 1},
		{Name: "ts", Type: "DateTime", Position: 2},
	}}
	eng := testEngine(t, eventsTable(), plain)

	eng.Bind(tenant.Default, testServerVersion, "Europe/Berlin", []*discovery.TableSchema{eventsTable(), plain})
	assert.Empty(t, eng.TenantCause(tenant.Default), "the zone is not tenant-wide")
	_, err := eng.Table(tenant.Default, "events")
	require.Error(t, err)
	require.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), `"Europe/Berlin"`)
	assert.Contains(t, err.Error(), `"UTC"`)
	assert.Contains(t, err.Error(), "Wave-RF/chtypes#419")

	tbl, err := eng.Table(tenant.Default, "plain")
	require.NoError(t, err, "a zone-less DateTime with no expression over it is read in the tenant's zone")
	tbl.Release()

	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{eventsTable(), plain})
	tbl, err = eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	tbl.Release()
}

// TestBind_CompileRefusalIsPerTable: one undeclarable table must not take the
// rest of the database down with it.
func TestBind_CompileRefusalIsPerTable(t *testing.T) {
	broken := &discovery.TableSchema{
		Name:    "broken",
		Columns: []discovery.Column{{Name: "x", Type: "NotAType(9)", Position: 1}},
	}
	eng := testEngine(t, eventsTable(), broken)

	good, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	good.Release()

	_, err = eng.Table(tenant.Default, "broken")
	require.Error(t, err)
	require.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), "broken")
}

func TestSignatureDistinguishesEveryField(t *testing.T) {
	t.Parallel()
	base := &discovery.TableSchema{Columns: []discovery.Column{
		{Name: "a", Type: "UInt8", DefaultKind: "DEFAULT", DefaultExpression: "1", Position: 1},
	}}
	sig := signature(base, "UTC")
	assert.NotEqual(t, sig, signature(base, "Asia/Tokyo"), "the zone is part of the signature")
	for _, mutate := range []func(c *discovery.Column){
		func(c *discovery.Column) { c.Name = "b" },
		func(c *discovery.Column) { c.Type = "UInt16" },
		func(c *discovery.Column) { c.DefaultKind = "MATERIALIZED" },
		func(c *discovery.Column) { c.DefaultExpression = "2" },
		func(c *discovery.Column) { c.Position = 2 },
	} {
		other := &discovery.TableSchema{Columns: []discovery.Column{base.Columns[0]}}
		mutate(&other.Columns[0])
		assert.NotEqual(t, sig, signature(other, "UTC"))
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
	eng := testEngine(t, eventsTable())
	tbl, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	defer tbl.Release()

	expr, params := renderExpr(t, tbl,
		Predicate{Column: "name", Op: "=", Values: []string{"acme"}},
		Predicate{Column: "id", Op: "in", Values: []string{"1", "7"}},
	)
	// Every identifier is backticked by the library's own QuoteIdentifier,
	// which always quotes. Every value is a bound {pN:String} parameter, never
	// text; on an integer column (id is UInt8 here) each one is compared
	// through the strict cast, element by element.
	assert.Equal(t, "`name` = {p0:String} AND `id` IN ("+
		chsql.StrictInt("p1", "UInt8")+", "+chsql.StrictInt("p2", "UInt8")+")", expr)
	assert.Equal(t, map[string]string{"p0": "acme", "p1": "1", "p2": "7"}, params)

	hostile := Predicate{Column: "name", Op: "=", Values: []string{"' OR 1=1 --"}}
	expr, params = renderExpr(t, tbl, hostile)
	assert.Equal(t, "`name` = {p0:String}", expr)
	assert.Equal(t, "' OR 1=1 --", params["p0"])
	assert.NotContains(t, expr, "OR 1=1")
}

// TestRender_IntegerColumnsBindThroughTheStrictCast: every operator on an
// integer column (Nullable included, cast to the bare type) renders the same
// chsql.StrictInt expression the query path renders; every other column keeps
// the plain {pN:String} form.
func TestRender_IntegerColumnsBindThroughTheStrictCast(t *testing.T) {
	eng := testEngine(t, eventsTable())
	tbl, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	defer tbl.Release()

	for _, op := range []string{"=", "!=", ">", "<"} {
		expr, params := renderExpr(t, tbl, Predicate{Column: "score", Op: op, Values: []string{"-4"}})
		assert.Equal(t, "`score` "+op+" "+chsql.StrictInt("p0", "Int32"), expr, "Nullable(Int32) %s", op)
		assert.Equal(t, map[string]string{"p0": "-4"}, params)

		expr, _ = renderExpr(t, tbl, Predicate{Column: "id", Op: op, Values: []string{"7"}})
		assert.Equal(t, "`id` "+op+" "+chsql.StrictInt("p0", "UInt8"), expr, "UInt8 %s", op)

		for _, col := range []string{"name", "ts", "created"} {
			expr, _ = renderExpr(t, tbl, Predicate{Column: col, Op: op, Values: []string{"x"}})
			assert.Equal(t, "`"+col+"` "+op+" {p0:String}", expr, "%s %s", col, op)
		}
	}
	expr, params := renderExpr(t, tbl, Predicate{Column: "score", Op: "in", Values: []string{"1", "2", "3"}})
	assert.Equal(t, "`score` IN ("+chsql.StrictInt("p0", "Int32")+", "+chsql.StrictInt("p1", "Int32")+", "+
		chsql.StrictInt("p2", "Int32")+")", expr)
	assert.Equal(t, map[string]string{"p0": "1", "p1": "2", "p2": "3"}, params)

	// The MATERIALIZED UInt16 column is declared too, so a filter over it is
	// rendered the same way.
	expr, _ = renderExpr(t, tbl, Predicate{Column: "id_plus", Op: "=", Values: []string{"8"}})
	assert.Equal(t, "`id_plus` = "+chsql.StrictInt("p0", "UInt16"), expr)
}

// TestRender_QuotesEveryIdentifier: a column whose name is a reserved word is
// a syntax error unquoted, so the always-quoting spelling is the one render
// uses.
func TestRender_QuotesEveryIdentifier(t *testing.T) {
	reserved := &discovery.TableSchema{
		Name:    "reserved",
		Columns: []discovery.Column{{Name: "all", Type: "String", Position: 1}},
	}
	eng := testEngine(t, reserved)
	tbl, err := eng.Table(tenant.Default, "reserved")
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
	eng := testEngine(t, odd)
	tbl, err := eng.Table(tenant.Default, "odd")
	require.NoError(t, err)
	defer tbl.Release()

	expr, _ := renderExpr(t, tbl, Predicate{Column: "weird name", Op: "=", Values: []string{"x"}})
	assert.Equal(t, "`weird name` = {p0:String}", expr)
}

func TestRender_RefusesWhatItCannotExpress(t *testing.T) {
	eng := testEngine(t, eventsTable())
	tbl, err := eng.Table(tenant.Default, "events")
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
// Schema.Describe, so it is a property of the handle that produced the
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
	eng := testEngine(t, kinds)
	tbl, err := eng.Table(tenant.Default, "kinds")
	require.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, []string{"id", "plain"}, tbl.WireColumns)
	assert.Equal(t, []string{"id", "plain", "mat", "ali", "eph"}, compiledColumnNames(tbl.c.desc),
		"every declared column is known to the handle, of every kind")
	// render tests against the handle's own column set, not the wire list: a
	// filter may name a MATERIALIZED column.
	_, _, ok := tbl.render([]Predicate{{Column: "mat", Op: "=", Values: []string{"2"}}})
	assert.True(t, ok)
	_, _, ok = tbl.render([]Predicate{{Column: "nosuch", Op: "=", Values: []string{"2"}}})
	assert.False(t, ok)
}

// TestBind_OneHandlePerTable: every table gets one handle, and a rebind
// replaces it.
func TestBind_OneHandlePerTable(t *testing.T) {
	eng := testEngine(t, eventsTable())
	tbl, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	require.NotNil(t, tbl.c)
	first := tbl.c
	tbl.Release()

	changed := eventsTable()
	changed.Columns[0].Type = "UInt16"
	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})

	tbl, err = eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	defer tbl.Release()
	require.NotNil(t, tbl.c)
	assert.NotSame(t, first, tbl.c)
	_, err = first.schema.Describe()
	var ue *chtypes.UsageError
	assert.ErrorAs(t, err, &ue, "the replaced handle is closed")
}

// compiledColumnNames is the column list a handle compiled to, in declaration
// order.
func compiledColumnNames(desc []chtypes.Column) []string {
	out := make([]string, 0, len(desc))
	for _, c := range desc {
		out = append(out, c.Name)
	}
	return out
}

// TestQuoteIdentifier_AgreesWithChsql: the stream (render, through the
// library's QuoteIdentifier) and the SQL path (chsql.QuoteIdent, which has no
// library to ask) must spell every name byte for byte alike, so a divergence
// fails here rather than in a customer's query.
func TestQuoteIdentifier_AgreesWithChsql(t *testing.T) {
	eng := testEngine(t, eventsTable())
	lib := boundLib(eng, tenant.Default)
	require.NotNil(t, lib)

	corpus := []string{
		"x", "a`b", `a\b`, "`", `\`, "\\`", "a\\`b", "it's", `"q"`, "", "null", "NULL", "all",
		"select", "from", "where", "weird name", "n.a", "Ünï", "日本", "\xff\xfe", "1abc", "?", "--", "/*",
		"a\x00b", "\x00", "\\\n", "\n\\",
	}
	for c := range 256 {
		corpus = append(corpus, string([]byte{byte(c)}), "a"+string([]byte{byte(c)})+"b")
	}
	for _, name := range corpus {
		ours, err := lib.QuoteIdentifier(name)
		require.NoError(t, err, "%q", name)
		assert.Equal(t, ours, chsql.QuoteIdent(name), "name %q", name)
	}
}

// corruptLayout is a v1 layout whose install record for the test line is the
// real one, verbatim, beside a library that is not one: what a truncated
// download or a bad disk leaves.
func corruptLayout(t *testing.T) string {
	t.Helper()
	testEngine(t) // skips (or fails under WAVEHOUSE_TEST_REQUIRE_CHTYPES) without the real artifact
	reg, err := chtypes.NewRegistry(chtypes.WithAutoFetch(false))
	require.NoError(t, err)
	installed, err := reg.Installed()
	require.NoError(t, err)
	var good *chtypes.Resolved
	for i, r := range installed {
		if r.Platform == runtime.GOOS+"-"+runtime.GOARCH && strings.HasPrefix(r.Version, testLine+".") {
			good = &installed[i]
		}
	}
	require.NotNil(t, good)
	rel, err := filepath.Rel(good.Dir, good.LibraryPath)
	require.NoError(t, err)

	dir := t.TempDir()
	entry := filepath.Join(dir, "unpacked", "sha256", filepath.Base(good.Dir))
	require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(entry, rel)), 0o750))
	for _, name := range []string{"verified.json", "manifest.json"} {
		b, err := os.ReadFile(filepath.Join(good.Dir, name)) //nolint:gosec // G304: the SDK's own install record
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(entry, name), b, 0o600)) //nolint:gosec // G703: rooted in t.TempDir()
	}
	require.NoError(t, os.WriteFile(filepath.Join(entry, rel), []byte("not a library"), 0o600))
	b, err := os.ReadFile(filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(good.Dir))), "oci-layout"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "oci-layout"), b, 0o600)) //nolint:gosec // G703: rooted in t.TempDir()
	return dir
}

// TestNewEngine_OpensNoLibraryAtConstruction: the registry is lazy, so an
// artifact that cannot load is not a boot failure; the first Bind for its line
// reports the SDK's own error as that tenant's Unavailable.
func TestNewEngine_OpensNoLibraryAtConstruction(t *testing.T) {
	dir := corruptLayout(t)

	eng, err := NewEngine(Config{CacheDir: dir})
	require.NoError(t, err, "a broken artifact is not a construction error")
	t.Cleanup(eng.Close)
	assert.Empty(t, eng.reg.Libraries(), "construction opens no library")

	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	tbl, err := eng.Table(tenant.Default, "events")
	if err == nil {
		tbl.Release() // so the failure below is reported instead of Close deadlocking on it
	}
	require.Error(t, err)
	require.True(t, IsUnavailable(err))
	assert.Contains(t, err.Error(), dir, "the SDK's message names the library that failed")
	assert.Contains(t, err.Error(), "[CHTYPES_ARTIFACT_", "with the SDK's artifact code")
}

// TestNewEngine_UnreadableDirectoryFailsAtConstruction: a directory somebody
// named and that does not exist is a typo, reported at boot.
func TestNewEngine_UnreadableDirectoryFailsAtConstruction(t *testing.T) {
	t.Parallel()
	_, err := NewEngine(Config{CacheDir: filepath.Join(t.TempDir(), "nosuch")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nosuch")
}

// TestNewEngine_V0RegistryIsNamedAsOne: a chtypes 0.x registry directory never
// loads under v1, and an empty answer would hide why.
func TestNewEngine_V0RegistryIsNamedAsOne(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, testLine), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, testLine, "manifest.json"), []byte(`{}`), 0o600))
	_, err := NewEngine(Config{CacheDir: dir})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "0.x registry")
}

// TestNewEngine_NoArtifactFailsAtConstruction: an API process with nothing to
// judge with refuses to boot, naming where it looked.
func TestNewEngine_NoArtifactFailsAtConstruction(t *testing.T) {
	dir := t.TempDir()
	_, err := NewEngine(Config{CacheDir: dir})
	if err == nil {
		t.Skip("a chtypes artifact is installed in a system layout, so this host has no layout set without one")
	}
	assert.Contains(t, err.Error(), "no artifact installed for "+runtime.GOOS+"-"+runtime.GOARCH)
	assert.Contains(t, err.Error(), dir)
}

// boundLib is the library a tenant's handles were compiled against.
func boundLib(eng *Engine, id tenant.ID) *chtypes.Library {
	eng.mu.RLock()
	set := eng.tenants[id]
	eng.mu.RUnlock()
	if set == nil {
		return nil
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return set.lib
}
