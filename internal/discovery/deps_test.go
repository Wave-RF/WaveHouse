package discovery

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tbl(name string, cols ...string) *TableSchema {
	ts := &TableSchema{Name: name}
	for _, c := range cols {
		ts.Columns = append(ts.Columns, Column{Name: c, Type: "UInt64"})
	}
	return ts
}

// IsKnown reports whether a first-level name is safe to fold directly: a base table
// or a view that flattens cleanly to base tables. An unfoldable view (a source it
// can't resolve) and an unknown name are not — the caller TTL-caps those.
func TestIsKnown(t *testing.T) {
	tables := []*TableSchema{tbl("base_a", "x"), tbl("base_b", "x"), tbl("v_norm", "x"), tbl("mv1", "x"), tbl("mv2", "x"), tbl("mv_bad", "x")}
	viewSources := map[string][]string{
		"v_norm": {"base_a"},           // normal view over a base table
		"mv1":    {"base_a", "base_b"}, // MV over two sources
		"mv2":    {"mv1"},              // chained: mv2 -> mv1 -> {base_a, base_b}
		"mv_bad": {"ghost"},            // a view over an UNDISCOVERED source: unfoldable
	}
	sr := NewSchemaRegistryForTest(tables, viewSources)

	known := []string{"base_a", "base_b", "v_norm", "mv1", "mv2"}
	for _, n := range known {
		assert.True(t, sr.IsKnown(n), "%s should be foldable", n)
	}
	notKnown := []string{"mv_bad", "ghost", "who_dis"}
	for _, n := range notKnown {
		assert.False(t, sr.IsKnown(n), "%s must not be foldable", n)
	}
}

// A view we KNOW is a view (engine = View) but whose definition did not parse —
// present in `views`, absent from mvSources — must be reported not-known, never
// mistaken for a base table (which would fold a version nothing maintains). It sits
// in `tables` too (system.columns lists view columns), which is the trap.
func TestIsKnown_UnparsedViewNotFoldable(t *testing.T) {
	sr := NewSchemaRegistryForTest([]*TableSchema{tbl("weird_view", "x"), tbl("base", "x")}, map[string][]string{})
	sr.tables["weird_view"].isView = true // a view with no mapped edges (unparsed definition)

	assert.False(t, sr.IsKnown("weird_view"), "an unparsed view must not be foldable")
	assert.True(t, sr.IsKnown("base"), "a real base table is foldable")
}

// computeChangedViews evicts a view redefined in place AND the downstream closure
// of views that transitively read it — staleness no base-table write would catch.
func TestComputeChangedViews(t *testing.T) {
	enc := chsql.SafeEncodeNATS
	view := func(asSelect string, sources ...string) *tableInfo {
		return &tableInfo{isView: true, asSelect: asSelect, sources: sources}
	}
	base := &tableInfo{schema: &TableSchema{}}

	tests := []struct {
		name     string
		old, new map[string]*tableInfo
		want     []string // pre-encode names, in sorted order
	}{
		{
			name: "no change yields nothing",
			old:  map[string]*tableInfo{"base": base, "v": view("SELECT * FROM base", "base")},
			new:  map[string]*tableInfo{"base": base, "v": view("SELECT * FROM base", "base")},
			want: nil,
		},
		{
			name: "direct redefinition only",
			old:  map[string]*tableInfo{"base": base, "v": view("SELECT a FROM base", "base")},
			new:  map[string]*tableInfo{"base": base, "v": view("SELECT b FROM base", "base")},
			want: []string{"v"},
		},
		{
			name: "redefining the middle view also evicts the top (the #1 fix)",
			old: map[string]*tableInfo{
				"base":  base,
				"v_mid": view("SELECT a FROM base", "base"), "v_top": view("SELECT * FROM v_mid", "v_mid"),
			},
			new: map[string]*tableInfo{
				"base":  base,
				"v_mid": view("SELECT b FROM base", "base"), "v_top": view("SELECT * FROM v_mid", "v_mid"),
			},
			want: []string{"v_mid", "v_top"},
		},
		{
			name: "deep chain fans all the way up",
			old: map[string]*tableInfo{
				"base": base,
				"v1":   view("X", "base"), "v2": view("q2", "v1"), "v3": view("q3", "v2"),
			},
			new: map[string]*tableInfo{
				"base": base,
				"v1":   view("Y", "base"), "v2": view("q2", "v1"), "v3": view("q3", "v2"),
			},
			want: []string{"v1", "v2", "v3"},
		},
		{
			name: "a view cycle terminates",
			old:  map[string]*tableInfo{"a": view("A", "b"), "b": view("B", "a")},
			new:  map[string]*tableInfo{"a": view("A2", "b"), "b": view("B", "a")},
			want: []string{"a", "b"},
		},
		{
			name: "a brand-new downstream reader is still evicted (harmless)",
			old:  map[string]*tableInfo{"base": base, "v_mid": view("SELECT a FROM base", "base")},
			new: map[string]*tableInfo{
				"base": base, "v_mid": view("SELECT b FROM base", "base"),
				"v_new": view("SELECT * FROM v_mid", "v_mid"),
			},
			want: []string{"v_mid", "v_new"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var want []string
			for _, n := range tt.want {
				want = append(want, enc(n))
			}
			assert.Equal(t, want, computeChangedViews(tt.old, tt.new))
		})
	}
}

// Dependents is the write-side cascade: base table -> the views a write to it must
// also evict, transitively, both NATS-encoded. It is the precomputed reverse of the
// view->source edges; an unfoldable view contributes no entry.
func TestDependents(t *testing.T) {
	enc := chsql.SafeEncodeNATS
	tests := []struct {
		name        string
		tables      []*TableSchema
		viewSources map[string][]string
		want        map[string][]string // base -> dependent views (pre-encode)
	}{
		{
			name:        "single view over a base",
			tables:      []*TableSchema{tbl("base", "x"), tbl("v", "x")},
			viewSources: map[string][]string{"v": {"base"}},
			want:        map[string][]string{"base": {"v"}},
		},
		{
			name:        "chained views both cascade off the base",
			tables:      []*TableSchema{tbl("base", "x"), tbl("mv1", "x"), tbl("mv2", "x")},
			viewSources: map[string][]string{"mv1": {"base"}, "mv2": {"mv1"}},
			want:        map[string][]string{"base": {"mv1", "mv2"}},
		},
		{
			name:        "view over two sources cascades off each",
			tables:      []*TableSchema{tbl("a", "x"), tbl("b", "x"), tbl("v", "x")},
			viewSources: map[string][]string{"v": {"a", "b"}},
			want:        map[string][]string{"a": {"v"}, "b": {"v"}},
		},
		{
			name:        "unfoldable view yields no cascade",
			tables:      []*TableSchema{tbl("base", "x"), tbl("v", "x")},
			viewSources: map[string][]string{"v": {"ghost"}},
			want:        map[string][]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sr := NewSchemaRegistryForTest(tt.tables, tt.viewSources)
			got := sr.Dependents()
			want := make(map[string][]string, len(tt.want))
			for base, views := range tt.want {
				ev := make([]string, len(views))
				for i, v := range views {
					ev[i] = enc(v)
				}
				want[enc(base)] = ev
			}
			require.Len(t, got, len(want))
			for base, views := range want {
				assert.ElementsMatch(t, views, got[base], "cascade for %s", base)
			}
		})
	}
}

// A materialized view's TO target is an ordinary base table ClickHouse populates
// whenever the MV's source takes an insert — nothing else ever writes it, so the
// write-side cascade must carry a source write through the MV into the target,
// to the foldable views over it, and through chained MVs — or a pipe reading the
// target directly would sit at full TTL with nothing ever bumping its version.
// Trigger edges (attached), not the resolved read set, drive the flow: they stay
// authoritative even for an MV whose definition didn't resolve (defExternal).
func TestDependents_MVTargets(t *testing.T) {
	enc := chsql.SafeEncodeNATS
	base := func() *tableInfo { return &tableInfo{schema: &TableSchema{}} }

	tests := []struct {
		name   string
		tables map[string]*tableInfo
		want   map[string][]string // base -> full dependent set (pre-encode)
	}{
		{
			name: "source write reaches the MV target",
			tables: map[string]*tableInfo{
				"src": {schema: &TableSchema{}, attached: []string{"mv"}},
				"tgt": base(),
				"mv":  {isView: true, sources: []string{"src"}, toTarget: "tgt"},
			},
			want: map[string][]string{"src": {"mv", "tgt"}},
		},
		{
			name: "views over the target ride along",
			tables: map[string]*tableInfo{
				"src": {schema: &TableSchema{}, attached: []string{"mv"}},
				"tgt": base(),
				"mv":  {isView: true, sources: []string{"src"}, toTarget: "tgt"},
				"v_t": {isView: true, sources: []string{"tgt"}},
			},
			want: map[string][]string{
				"src": {"mv", "tgt", "v_t"},
				"tgt": {"v_t"},
			},
		},
		{
			name: "chained MVs propagate transitively",
			tables: map[string]*tableInfo{
				"src":  {schema: &TableSchema{}, attached: []string{"mv1"}},
				"tgt1": {schema: &TableSchema{}, attached: []string{"mv2"}},
				"tgt2": base(),
				"mv1":  {isView: true, sources: []string{"src"}, toTarget: "tgt1"},
				"mv2":  {isView: true, sources: []string{"tgt1"}, toTarget: "tgt2"},
			},
			want: map[string][]string{
				"src":  {"mv1", "tgt1", "mv2", "tgt2"},
				"tgt1": {"mv2", "tgt2"},
			},
		},
		{
			name: "an unresolvable MV definition still feeds its target (trigger edges are authoritative)",
			tables: map[string]*tableInfo{
				"src": {schema: &TableSchema{}, attached: []string{"mv"}},
				"tgt": base(),
				"mv":  {isView: true, sources: []string{"src"}, toTarget: "tgt", defExternal: true},
			},
			// mv itself is unfoldable (its readers TTL-cap), so only the target cascades.
			want: map[string][]string{"src": {"tgt"}},
		},
		{
			name: "a missing or non-base target contributes no edge",
			tables: map[string]*tableInfo{
				"src": {schema: &TableSchema{}, attached: []string{"mv", "mv2"}},
				"mv":  {isView: true, sources: []string{"src"}, toTarget: "ghost"},
				"mv2": {isView: true, sources: []string{"src"}, toTarget: "v"},
				"v":   {isView: true, sources: []string{"src"}},
			},
			want: map[string][]string{"src": {"mv", "mv2", "v"}},
		},
		{
			name: "an MV targeting its own source terminates",
			tables: map[string]*tableInfo{
				"src": {schema: &TableSchema{}, attached: []string{"mv"}},
				"mv":  {isView: true, sources: []string{"src"}, toTarget: "src"},
			},
			want: map[string][]string{"src": {"mv"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDeps(tt.tables)
			want := make(map[string][]string, len(tt.want))
			for b, deps := range tt.want {
				ed := make([]string, len(deps))
				for i, d := range deps {
					ed[i] = enc(d)
				}
				want[enc(b)] = ed
			}
			require.Len(t, got, len(want))
			for b, deps := range want {
				assert.ElementsMatch(t, deps, got[b], "cascade for %s", b)
			}
		})
	}
}

// parseMVTarget reads the TO target off ClickHouse's normalized DDL renderings —
// the exact strings a 26.6 server produces (pinned live by the integration
// MV-target test). It must never scan past the column list / ENGINE clause /
// SELECT body, so definition text (a TTL ... TO DISK, a string literal) can't
// fabricate a target.
func TestParseMVTarget(t *testing.T) {
	tests := []struct {
		name      string
		q         string
		db, table string
		ok        bool
	}{
		{
			name: "plain TO target",
			q:    "CREATE MATERIALIZED VIEW mvtest.mv TO mvtest.tgt (`id` UInt64, `val` String) AS SELECT id, val FROM mvtest.src",
			db:   "mvtest", table: "tgt", ok: true,
		},
		{
			name: "quoted target with an embedded dot stays one identifier",
			q:    "CREATE MATERIALIZED VIEW mvtest.mv TO mvtest.`dot.ted` (`id` UInt64) AS SELECT id FROM mvtest.src",
			db:   "mvtest", table: "dot.ted", ok: true,
		},
		{
			name: "quoted target with a space",
			q:    "CREATE MATERIALIZED VIEW mvtest.mv TO mvtest.`we ird` (`id` UInt64) AS SELECT id FROM mvtest.src",
			db:   "mvtest", table: "we ird", ok: true,
		},
		{
			name: "backslash-escaped backtick decodes",
			q:    "CREATE MATERIALIZED VIEW mvtest.mv TO mvtest.`tick\\`name` (`id` UInt64) AS SELECT id FROM mvtest.src",
			db:   "mvtest", table: "tick`name", ok: true,
		},
		{
			name: "refreshable MV: REFRESH clause precedes TO",
			q:    "CREATE MATERIALIZED VIEW mvtest.mv_refresh REFRESH EVERY 1 HOUR TO mvtest.tgt2 (`id` UInt64, `val` String) DEFINER = default SQL SECURITY DEFINER AS SELECT id, val FROM mvtest.src",
			db:   "mvtest", table: "tgt2", ok: true,
		},
		{
			name: "implicit-inner MV (no TO; column list first)",
			q:    "CREATE MATERIALIZED VIEW mvtest.mv_inner (`id` UInt64) ENGINE = MergeTree ORDER BY id AS SELECT id FROM mvtest.src",
			ok:   false,
		},
		{
			name: "TTL ... TO DISK after ENGINE is never a target",
			q:    "CREATE MATERIALIZED VIEW db.m ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY TO DISK 'cold' AS SELECT id FROM db.src",
			ok:   false,
		},
		{
			name: "plain view has no TO clause",
			q:    "CREATE VIEW test.v (`x` UInt64) AS SELECT x FROM base",
			ok:   false,
		},
		{
			name: "a view literally named TO is skipped as a quoted identifier",
			q:    "CREATE MATERIALIZED VIEW db.`TO` TO db.tgt (`x` UInt64) AS SELECT x FROM db.src",
			db:   "db", table: "tgt", ok: true,
		},
		{
			name: "a string literal containing TO does not arm",
			q:    "CREATE MATERIALIZED VIEW db.m REFRESH EVERY 1 HOUR SETTINGS note = 'go TO disk' TO db.t (`x` UInt64) AS SELECT 1",
			db:   "db", table: "t", ok: true,
		},
		{
			name: "unqualified target maps to the configured database",
			q:    "CREATE MATERIALIZED VIEW mv TO tgt AS SELECT 1",
			db:   "", table: "tgt", ok: true,
		},
		{
			name: "unterminated quote is malformed",
			q:    "CREATE MATERIALIZED VIEW db.mv TO db.`broken AS SELECT 1",
			ok:   false,
		},
		{name: "empty", q: "", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, table, ok := parseMVTarget(tt.q)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.db, db)
			assert.Equal(t, tt.table, table)
		})
	}
}

// mkInfos assembles a tableInfo map the way Refresh would, for contentHash tests:
// base tables carry a schema; a name in sources/defs is marked a view.
func mkInfos(tables map[string]*TableSchema, sources map[string][]string, defs map[string]string) map[string]*tableInfo {
	m := make(map[string]*tableInfo)
	for n, ts := range tables {
		m[n] = &tableInfo{schema: ts}
	}
	mark := func(n string) *tableInfo {
		t := m[n]
		if t == nil {
			t = &tableInfo{}
			m[n] = t
		}
		t.isView = true
		return t
	}
	for n, s := range sources {
		mark(n).sources = s
	}
	for n, d := range defs {
		mark(n).asSelect = d
	}
	return m
}

func TestContentHash_DeterministicAndSensitive(t *testing.T) {
	tables := map[string]*TableSchema{
		"a": tbl("a", "x", "y"),
		"b": tbl("b", "z"),
	}
	mv := map[string][]string{"v": {"a"}}
	defs := map[string]string{"v": "SELECT x, y FROM a"}

	base := contentHash(mkInfos(tables, mv, defs))

	// Determinism: a re-run over equal content (maps built independently) matches.
	assert.Equal(t, base, contentHash(mkInfos(
		map[string]*TableSchema{"b": tbl("b", "z"), "a": tbl("a", "x", "y")},
		map[string][]string{"v": {"a"}},
		map[string]string{"v": "SELECT x, y FROM a"},
	)), "hash must be stable regardless of map iteration order")

	// Sensitivity: each input changing flips the hash.
	assert.NotEqual(t, base, contentHash(mkInfos(map[string]*TableSchema{"a": tbl("a", "x", "y", "extra"), "b": tbl("b", "z")}, mv, defs)), "a new column must change the hash")
	assert.NotEqual(t, base, contentHash(mkInfos(tables, map[string][]string{"v": {"a", "b"}}, defs)), "a new view->source edge must change the hash")
	assert.NotEqual(t, base, contentHash(mkInfos(tables, mv, map[string]string{"v": "SELECT x, y FROM a WHERE z > 1"})), "a redefined view body (same sources) must change the hash")

	// An MV re-pointed at a different TO target (DROP + CREATE with an identical
	// SELECT) changes neither columns, sources, nor asSelect — only this fold.
	withTarget := func(target string) uint64 {
		infos := mkInfos(tables, mv, defs)
		infos["v"].toTarget = target
		return contentHash(infos)
	}
	assert.NotEqual(t, base, withTarget("t1"), "gaining a TO target must change the hash")
	assert.NotEqual(t, withTarget("t1"), withTarget("t2"), "a re-pointed TO target must change the hash")
}

// scriptedConn returns canned rows for the system.columns and system.tables
// queries (distinguished by SQL substring), and can be told to fail either,
// driving Refresh's change-detection and atomic-no-op behavior without ClickHouse.
type scriptedConn struct {
	driver.Conn
	columnRows     [][]any  // [table, name, type, default_kind]
	tableRows      [][]any  // [name, engine, as_select, dependencies_table([]string), create_table_query]
	explainSources []string // tables ResolveTables (EXPLAIN QUERY TREE) reports for a view
	explainCalls   int      // EXPLAIN queries issued (pass-2 runs) — proves the no-op skip
	failTables     bool
}

func (c *scriptedConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	switch {
	case strings.HasPrefix(query, "EXPLAIN"):
		// Stand in for ClickHouse resolving a view's definition: emit one
		// "table_name: <db>.<table>" line per configured source.
		c.explainCalls++
		rows := make([][]any, len(c.explainSources))
		for i, t := range c.explainSources {
			rows[i] = []any{"  TABLE id: 1, table_name: test." + t}
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(query, "system.columns"):
		return &scriptedRows{rows: c.columnRows}, nil
	case strings.Contains(query, "system.tables"):
		if c.failTables {
			return nil, assert.AnError
		}
		return &scriptedRows{rows: c.tableRows}, nil
	}
	return &emptyRows{}, nil
}

type scriptedRows struct {
	driver.Rows
	rows [][]any
	i    int
}

func (r *scriptedRows) Next() bool                       { return r.i < len(r.rows) }
func (r *scriptedRows) Close() error                     { return nil }
func (r *scriptedRows) Err() error                       { return nil }
func (r *scriptedRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *scriptedRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	for k := range dest {
		switch d := dest[k].(type) {
		case *string:
			d2, _ := row[k].(string)
			*d = d2
		case *[]string:
			if row[k] == nil {
				*d = nil
			} else {
				*d = row[k].([]string)
			}
		}
	}
	return nil
}

func newScriptedRegistry(conn *scriptedConn) *SchemaRegistry {
	return NewSchemaRegistry(conn, "test", 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// Refresh notifies onRefresh only when the schema content actually changed, hands
// over the up-to-date cascade, and flags a redefined view (same sources, new body)
// in ChangedViews so the cache can evict it directly.
func TestRefresh_NotifiesOnChange(t *testing.T) {
	enc := chsql.SafeEncodeNATS
	conn := &scriptedConn{
		columnRows:     [][]any{{"base", "x", "UInt64", ""}},
		tableRows:      [][]any{{"v", "View", "SELECT x FROM base", nil, "CREATE VIEW test.v (`x` UInt64) AS SELECT x FROM base"}},
		explainSources: []string{"base"},
	}
	sr := newScriptedRegistry(conn)
	ctx := context.Background()

	var snaps []DependencySnapshot
	sr.SetOnRefresh(func(s DependencySnapshot) { snaps = append(snaps, s) })

	require.NoError(t, sr.Refresh(ctx))
	require.Len(t, snaps, 1, "first refresh must notify")
	assert.ElementsMatch(t, []string{enc("v")}, snaps[0].Cascade[enc("base")], "cascade must map base -> v")
	assert.Empty(t, snaps[0].ChangedViews, "no views changed on the first refresh")
	explainsAfterFirst := conn.explainCalls
	require.Equal(t, 1, explainsAfterFirst, "first refresh resolves the one view via EXPLAIN")

	require.NoError(t, sr.Refresh(ctx))
	require.Len(t, snaps, 1, "an identical refresh must NOT notify")
	assert.Equal(t, explainsAfterFirst, conn.explainCalls, "an identical refresh must NOT re-run EXPLAIN — pass 2 is skipped on a no-op")

	// Redefine the view body (same source set) — must be detected and reported.
	conn.tableRows = [][]any{{"v", "View", "SELECT x FROM base WHERE x > 1", nil, "CREATE VIEW test.v (`x` UInt64) AS SELECT x FROM base WHERE x > 1"}}
	require.NoError(t, sr.Refresh(ctx))
	require.Len(t, snaps, 2, "a view-body redefinition must notify")
	assert.Greater(t, conn.explainCalls, explainsAfterFirst, "a redefinition changes the cheap signals, so EXPLAIN re-runs")
	assert.Equal(t, []string{enc("v")}, snaps[1].ChangedViews, "the redefined view must be flagged for direct eviction")

	// The view still resolves to its base after the redefinition.
	assert.True(t, sr.IsKnown("v"))
	assert.ElementsMatch(t, []string{enc("v")}, sr.Dependents()[enc("base")])
}

// An MV's TO target rides the cascade end-to-end: pass 1 parses the target off
// create_table_query, buildDeps carries a source write into it, and re-pointing
// the MV at a different target (DROP + CREATE, SAME SELECT — only the DDL
// differs) is a content change that re-fires onRefresh with the new cascade —
// the one change only the toTarget hash fold can catch.
func TestRefresh_MVTargetCascade(t *testing.T) {
	enc := chsql.SafeEncodeNATS
	mvRow := func(target string) []any {
		return []any{
			"mv", "MaterializedView", "SELECT x FROM src", nil,
			"CREATE MATERIALIZED VIEW test.mv TO test." + target + " (`x` UInt64) AS SELECT x FROM src",
		}
	}
	conn := &scriptedConn{
		columnRows: [][]any{{"src", "x", "UInt64", ""}, {"tgt_a", "x", "UInt64", ""}, {"tgt_b", "x", "UInt64", ""}},
		tableRows: [][]any{
			{"src", "MergeTree", "", []string{"mv"}, "CREATE TABLE test.src (`x` UInt64) ENGINE = MergeTree ORDER BY x"},
			{"tgt_a", "MergeTree", "", nil, "CREATE TABLE test.tgt_a (`x` UInt64) ENGINE = MergeTree ORDER BY x"},
			{"tgt_b", "MergeTree", "", nil, "CREATE TABLE test.tgt_b (`x` UInt64) ENGINE = MergeTree ORDER BY x"},
			mvRow("tgt_a"),
		},
		explainSources: []string{"src"},
	}
	sr := newScriptedRegistry(conn)
	var snaps []DependencySnapshot
	sr.SetOnRefresh(func(s DependencySnapshot) { snaps = append(snaps, s) })

	require.NoError(t, sr.Refresh(context.Background()))
	require.Len(t, snaps, 1)
	assert.ElementsMatch(t, []string{enc("mv"), enc("tgt_a")}, snaps[0].Cascade[enc("src")],
		"a source write must bump the MV and its TO target")

	conn.tableRows[3] = mvRow("tgt_b")
	require.NoError(t, sr.Refresh(context.Background()))
	require.Len(t, snaps, 2, "a re-pointed TO target must notify")
	assert.ElementsMatch(t, []string{enc("mv"), enc("tgt_b")}, snaps[1].Cascade[enc("src")])
	assert.Empty(t, snaps[1].ChangedViews, "the MV body is unchanged — no direct eviction")
}

func TestRefresh_AtomicNoOpOnViewDiscoveryError(t *testing.T) {
	conn := &scriptedConn{
		columnRows:     [][]any{{"base", "x", "UInt64", ""}},
		tableRows:      [][]any{{"v", "View", "SELECT x FROM base", nil, "CREATE VIEW test.v (`x` UInt64) AS SELECT x FROM base"}},
		explainSources: []string{"base"},
	}
	sr := newScriptedRegistry(conn)
	ctx := context.Background()

	notifies := 0
	sr.SetOnRefresh(func(DependencySnapshot) { notifies++ })

	require.NoError(t, sr.Refresh(ctx))
	require.Equal(t, 1, notifies)

	// The tables query now errors; the whole refresh must be a no-op.
	conn.failTables = true
	require.Error(t, sr.Refresh(ctx))
	assert.Equal(t, 1, notifies, "a failed refresh must not notify")

	// Last-good tree is fully intact: the view still resolves to its base.
	assert.True(t, sr.IsKnown("v"), "the previous good tree must survive a transient discovery error")
	assert.ElementsMatch(t, []string{chsql.SafeEncodeNATS("v")}, sr.Dependents()[chsql.SafeEncodeNATS("base")])
}
