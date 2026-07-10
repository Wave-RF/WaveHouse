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
}

// scriptedConn returns canned rows for the system.columns and system.tables
// queries (distinguished by SQL substring), and can be told to fail either,
// driving Refresh's change-detection and atomic-no-op behavior without ClickHouse.
type scriptedConn struct {
	driver.Conn
	columnRows     [][]any  // [table, name, type, default_kind]
	tableRows      [][]any  // [name, engine, as_select, dependencies_table([]string)]
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
		tableRows:      [][]any{{"v", "View", "SELECT x FROM base", nil}},
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
	conn.tableRows = [][]any{{"v", "View", "SELECT x FROM base WHERE x > 1", nil}}
	require.NoError(t, sr.Refresh(ctx))
	require.Len(t, snaps, 2, "a view-body redefinition must notify")
	assert.Greater(t, conn.explainCalls, explainsAfterFirst, "a redefinition changes the cheap signals, so EXPLAIN re-runs")
	assert.Equal(t, []string{enc("v")}, snaps[1].ChangedViews, "the redefined view must be flagged for direct eviction")

	// The view still resolves to its base after the redefinition.
	assert.True(t, sr.IsKnown("v"))
	assert.ElementsMatch(t, []string{enc("v")}, sr.Dependents()[enc("base")])
}

func TestRefresh_AtomicNoOpOnViewDiscoveryError(t *testing.T) {
	conn := &scriptedConn{
		columnRows:     [][]any{{"base", "x", "UInt64", ""}},
		tableRows:      [][]any{{"v", "View", "SELECT x FROM base", nil}},
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
