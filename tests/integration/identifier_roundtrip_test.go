//go:build integration

package tests

// Identifier round-trip tests: ClickHouse allows arbitrary characters in quoted
// identifiers — table names, column names, AND aliases may contain dots, spaces,
// unicode, SQL keywords, even embedded backticks and backslashes. Customers point
// WaveHouse at existing schemas that use such names, so the query builder must
// (1) ACCEPT any name the schema actually contains and (2) escape it correctly so
// it round-trips against a real server.
//
// These tests run the builder's output against a real ClickHouse, which is the
// only way to prove escaping correctness. They deliberately do NOT assert the
// generated SQL string, so they stay valid whether the eventual fix quotes
// identifiers client-side or delegates to server-side {name:Identifier} params.
//
// Fixtures are created with chQuoteIdent — an INDEPENDENT quoter that mirrors
// ClickHouse's own backQuote() (escape backslash, then backtick, each with a
// backslash). Using a different routine than the production builder is the point:
// a bug in the builder cannot hide by "agreeing" with an identical bug here.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/query"
)

// chQuoteIdent quotes a ClickHouse identifier the way the server's backQuote()
// does: escape backslash, then backtick, each with a leading backslash, and wrap
// in backticks. This is the test's trusted oracle for CREATING fixtures with
// arbitrary names — it is intentionally not the routine under test.
func chQuoteIdent(name string) string {
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(name) + "`"
}

// chQuoteString renders a ClickHouse single-quoted string literal for a
// test-controlled fixture value (backslash and single-quote escaped).
func chQuoteString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// createRawTable creates a String-typed table whose name and column names may
// contain arbitrary characters, seeds exactly one row, refreshes the schema
// registry so the API discovers it, and registers drop-cleanup. cols maps each
// column name to its single row value. Returns the raw (unescaped) table name to
// hand to query.Build.
func createRawTable(t *testing.T, rawName string, cols map[string]string) string {
	t.Helper()
	ctx := context.Background()
	e := env(t)

	// Deterministic column order for the DDL and INSERT column lists.
	colNames := make([]string, 0, len(cols))
	for c := range cols {
		colNames = append(colNames, c)
	}
	sort.Strings(colNames)

	ddlCols := make([]string, len(colNames))
	insCols := make([]string, len(colNames))
	insVals := make([]string, len(colNames))
	for i, c := range colNames {
		ddlCols[i] = chQuoteIdent(c) + " String"
		insCols[i] = chQuoteIdent(c)
		insVals[i] = chQuoteString(cols[c])
	}

	qName := chQuoteIdent(rawName)
	if err := e.chConn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s (%s) ENGINE = MergeTree() ORDER BY tuple()",
		qName, strings.Join(ddlCols, ", "),
	)); err != nil {
		t.Fatalf("create raw table %q: %v", rawName, err)
	}
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.chConn.Exec(dctx, "DROP TABLE IF EXISTS "+qName)
	})

	// Seed one row with a literal INSERT parsed by the SERVER. We avoid the
	// driver's PrepareBatch path on purpose: its client-side column-list parser is
	// not backtick/backslash-aware (the very escaping under test), whereas the
	// server is. Values are test-controlled and quoted as CH string literals.
	if err := e.chConn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		qName, strings.Join(insCols, ", "), strings.Join(insVals, ", "),
	)); err != nil {
		t.Fatalf("seed raw table %q: %v", rawName, err)
	}

	if err := e.registry.Refresh(ctx); err != nil {
		t.Fatalf("refresh registry: %v", err)
	}
	return rawName
}

// queryOneString executes a single-column BuildResult against ClickHouse and
// returns the one string value of the first row.
func queryOneString(t *testing.T, res *query.BuildResult) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := env(t).chConn.Query(ctx, res.SQL, res.Params...)
	require.NoErrorf(t, err, "built SQL must execute: %s", res.SQL)
	defer func() { _ = rows.Close() }()
	require.Truef(t, rows.Next(), "expected a row from: %s", res.SQL)
	var got string
	require.NoError(t, rows.Scan(&got))
	return got
}

// TestIntegration_WeirdColumnNamesRoundTrip is the core column regression: a
// legal-in-ClickHouse column name that the safe-identifier regex would reject (or
// that, interpolated raw, would parse as a keyword) must still be queryable. The
// weird column is referenced in BOTH the projection and a filter, so the value it
// returns proves the builder both selected and matched the right physical column.
func TestIntegration_WeirdColumnNamesRoundTrip(t *testing.T) {
	const want = "round-trip-value"
	weirdCols := []string{
		"user.id",    // dotted (nested-column lookalike)
		"2024",       // leading digit
		"select",     // SQL keyword — raw interpolation parses as SELECT
		"gross $",    // space + symbol
		"naïve",      // unicode
		"tick`col",   // embedded backtick
		`back\slash`, // embedded backslash — raw escaping drops it
		`dq"col`,     // embedded double quote
	}
	for i, col := range weirdCols {
		t.Run(col, func(t *testing.T) {
			table := createRawTable(t,
				fmt.Sprintf("it_weirdcol_%d", tableCounter.Add(1)),
				map[string]string{"id": "row1", col: want},
			)
			schema := env(t).registry.Get(table)
			require.NotNilf(t, schema, "registry must discover table %q (case %d)", table, i)
			require.Containsf(t, schema.ColumnNames(), col, "discovery must surface column %q", col)

			res, err := query.Build(table, &query.StructuredQuery{
				Columns: []string{col},
				Filters: []query.Filter{{Column: "id", Op: "eq", Value: "row1"}},
			}, schema, nil, 0, query.DefaultMaxRows)
			require.NoErrorf(t, err, "builder must accept legal ClickHouse column %q", col)

			assert.Equalf(t, want, queryOneString(t, res),
				"column %q must round-trip through ClickHouse", col)
		})
	}
}

// TestIntegration_WeirdTableNamesRoundTrip exercises the table-name escaping the
// builder already attempts (backtick-doubling) plus the cases it currently
// mishandles. The backslash case is the known gap: the builder doubles backticks
// but leaves backslashes raw, and ClickHouse treats backslash as an escape
// introducer inside backticks, so the emitted name no longer matches the real
// table.
func TestIntegration_WeirdTableNamesRoundTrip(t *testing.T) {
	const want = "table-value"
	weirdNames := []string{
		"tick`tbl",      // embedded backtick
		`back\slash`,    // embedded backslash — the current susceptibility
		"space tbl",     // space
		"Naïve_таблица", // unicode
		"select",        // SQL keyword
	}
	for _, base := range weirdNames {
		t.Run(base, func(t *testing.T) {
			rawName := fmt.Sprintf("it_%s_%d", base, tableCounter.Add(1))
			table := createRawTable(t, rawName, map[string]string{"id": "row1", "val": want})
			schema := env(t).registry.Get(table)
			require.NotNilf(t, schema, "registry must discover table %q", table)

			res, err := query.Build(table, &query.StructuredQuery{
				Columns: []string{"val"},
				Filters: []query.Filter{{Column: "id", Op: "eq", Value: "row1"}},
			}, schema, nil, 0, query.DefaultMaxRows)
			require.NoError(t, err)

			assert.Equalf(t, want, queryOneString(t, res),
				"table %q must round-trip through ClickHouse", rawName)
		})
	}
}

// TestIntegration_WeirdEverythingCombined stresses every identifier surface at
// once: a table name containing both a backtick and a backslash, a dotted
// aggregation-argument column, and a unicode+space alias — the kind of combination
// a real foreign schema produces.
func TestIntegration_WeirdEverythingCombined(t *testing.T) {
	rawName := fmt.Sprintf("it`weird\\combo_%d", tableCounter.Add(1))
	table := createRawTable(t, rawName, map[string]string{
		"id":           "row1",
		"weird.metric": "7",
	})
	schema := env(t).registry.Get(table)
	require.NotNilf(t, schema, "registry must discover table %q", table)

	res, err := query.Build(table, &query.StructuredQuery{
		Aggregations: []query.Aggregation{{Fn: "max", Column: "weird.metric", Alias: "naïve max"}},
		Filters:      []query.Filter{{Column: "id", Op: "eq", Value: "row1"}},
	}, schema, nil, 0, query.DefaultMaxRows)
	require.NoError(t, err, "builder must accept weird table + weird agg column + weird alias")

	assert.Equal(t, "7", queryOneString(t, res),
		"max(`weird.metric`) aliased to a unicode name must round-trip")
}

// TestIntegration_AliasInjectionContained is the security counterpart to making
// aliases permissive: an injection-shaped alias must be treated as a literal
// (weird) column label, NOT spliced into the SQL as syntax. With correct quoting
// the statement is a single, contained count over the one-row table — so it
// returns exactly 1. A raw-interpolated alias would instead reparent the FROM
// (e.g. count system.tables) or split the statement, yielding a different count
// or an error.
func TestIntegration_AliasInjectionContained(t *testing.T) {
	table := createRawTable(t,
		fmt.Sprintf("it_aliascontain_%d", tableCounter.Add(1)),
		map[string]string{"id": "row1", "val": "v"},
	)
	schema := env(t).registry.Get(table)
	require.NotNil(t, schema)

	const evil = "c FROM system.tables; --"
	res, err := query.Build(table, &query.StructuredQuery{
		Aggregations: []query.Aggregation{{Fn: "count", Column: "*", Alias: evil}},
	}, schema, nil, 0, query.DefaultMaxRows)
	require.NoError(t, err, "a permissive alias must be accepted, not rejected")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := env(t).chConn.Query(ctx, res.SQL, res.Params...)
	require.NoErrorf(t, err, "alias must stay contained, query must execute as one statement: %s", res.SQL)
	defer func() { _ = rows.Close() }()
	require.True(t, rows.Next())
	var n uint64
	require.NoError(t, rows.Scan(&n))
	assert.Equalf(t, uint64(1), n,
		"count(*) over a 1-row table; an injection-shaped alias must not change the query: %s", res.SQL)
}

// TestIntegration_StarColumnSemantics pins the "*" contract: columns:["*"] is the
// LITERAL column named "*" (quoted, read like any other column), while select_all
// is the all-columns wildcard. A table that really has a column named "*" lets us
// exercise both against a live server.
func TestIntegration_StarColumnSemantics(t *testing.T) {
	table := createRawTable(t,
		fmt.Sprintf("it_starcol_%d", tableCounter.Add(1)),
		map[string]string{"id": "row1", "*": "star-value"},
	)
	schema := env(t).registry.Get(table)
	require.NotNil(t, schema)
	require.Contains(t, schema.ColumnNames(), "*",
		"discovery should surface a real column literally named *")

	// columns:["*"] reads the literal column named "*" — not all columns.
	res, err := query.Build(table, &query.StructuredQuery{Columns: query.Columns{"*"}}, schema, nil, 0, query.DefaultMaxRows)
	require.NoError(t, err)
	assert.Equalf(t, "star-value", queryOneString(t, res),
		`columns:["*"] must read the literal column named *: %s`, res.SQL)

	// select_all is the all-columns wildcard (returns every column, including *).
	res, err = query.Build(table, &query.StructuredQuery{SelectAll: true}, schema, nil, 0, query.DefaultMaxRows)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := env(t).chConn.Query(ctx, res.SQL, res.Params...)
	require.NoErrorf(t, err, "select_all must execute: %s", res.SQL)
	defer func() { _ = rows.Close() }()
	assert.ElementsMatchf(t, []string{"id", "*"}, rows.Columns(),
		"select_all must return ALL columns, including the one named *")
}
