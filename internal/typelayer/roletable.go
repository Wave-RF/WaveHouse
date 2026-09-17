package typelayer

import (
	"container/list"
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// roleCacheSize bounds the per-role shapes held per table. A shape's Defaults
// values come from tenant claims and are baked into the compiled handle, so an
// unbounded cache is a memory and CPU denial of service — the same reason
// filterCache is bounded. Each entry costs poolSize() compiled handles.
const roleCacheSize = 256

// RoleShape is the projection of a table a role may insert through. It is the
// whole input to Engine.RoleTable and the whole cache key, so two roles with
// the same shape share one compiled handle.
//
// Columns is the set of columns the role may write. nil means "every column"
// and is the identity shape; a non-nil, EMPTY slice means "no column", which
// compiles to nothing and fails closed. Order is irrelevant — the DDL always
// follows the table's own declaration order — and a name the table does not
// have is ignored. Columns the role cannot supply anyway (MATERIALIZED, ALIAS,
// EPHEMERAL) are always kept: dropping one would change what the server
// computes, and a MATERIALIZED expression over a dropped column would not
// compile at all.
//
// Defaults maps a column to a literal value injected when a record omits it,
// rendered into the column's DEFAULT clause. A value the record DOES supply
// still wins (measured, AUDIT §A.3). Every key must name a column the shape
// keeps and must be an ordinary column (no DEFAULT, or a plain DEFAULT) —
// anything else is a programming error and returns an error rather than
// silently reshaping the table.
type RoleShape struct {
	Columns  []string
	Defaults map[string]string
}

// identity reports whether the shape asks for nothing the base table does not
// already answer, in which case no second handle is compiled.
func (s RoleShape) identity() bool {
	return s.Columns == nil && len(s.Defaults) == 0
}

// key is the cache key: the generation that owns the schema plus a canonical
// hash of the shape. Every component is length-prefixed, so no column name or
// claim value can spell another shape's encoding.
func (s RoleShape) key(generation uint64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "g%d\x1e", generation)
	if s.Columns == nil {
		b.WriteString("*\x1e")
	} else {
		cols := slices.Clone(s.Columns)
		slices.Sort(cols)
		cols = slices.Compact(cols)
		for _, c := range cols {
			fmt.Fprintf(&b, "%d:%s", len(c), c)
		}
		b.WriteByte(0x1e)
	}
	for _, k := range slices.Sorted(maps.Keys(s.Defaults)) {
		v := s.Defaults[k]
		fmt.Fprintf(&b, "%d:%s%d:%s", len(k), k, len(v), v)
	}
	// Hashed rather than kept verbatim: the Defaults values are tenant claims,
	// so the key's size must not be the caller's to choose.
	sum := sha256.Sum256([]byte(b.String()))
	return string(sum[:])
}

// RoleTable returns the compiled handle for one role's projection of a table,
// read-locked exactly like Engine.Table: the caller must Release it, and the
// handle stays alive and stable until it does.
//
// The shape is answered by ClickHouse's own parser rather than by a Go walk
// over the record's keys (AUDIT §A.1): a column the role may not write is
// simply absent from the compiled DDL, so a record naming it is refused
// per-row with ClickHouse's own code 117 "Unknown field found while parsing
// JSONEachRow format: x"; a Defaults column is declared DEFAULT '<literal>',
// so an absent value is filled and a supplied one still wins.
//
// The identity shape (no column restriction, no defaults) returns the base
// table itself — no second handle, no cache entry.
//
// A shape that does not compile is cached as a negative entry and reported as
// *Unavailable, so a broken policy costs one compile and one log line per
// generation rather than one per request.
func (e *Engine) RoleTable(table string, shape RoleShape) (*Table, error) {
	base, err := e.Table(table)
	if err != nil {
		return nil, err
	}
	if shape.identity() {
		return base, nil // still read-locked; the caller's Release covers it
	}
	rt, evicted, err := base.roleTable(shape)
	base.Release()
	// Closed after the cache lock and the base read lock are both gone: it
	// waits for the evicted shape's own readers, which are requests in flight.
	if evicted != nil {
		evicted.close()
	}
	if err != nil {
		return nil, err
	}
	return rt, nil
}

// roleTable is RoleTable's cache half, run under the base table's read lock.
// It returns the projection read-locked, plus the entry its insertion evicted
// (to be closed by the caller, outside the cache lock).
func (t *Table) roleTable(shape RoleShape) (*Table, *Table, error) {
	key := shape.key(t.Generation)
	c := t.roles
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, hit := c.index[key]; hit {
		c.order.MoveToFront(el)
		e := el.Value.(*roleEntry)
		if e.table == nil {
			return nil, nil, &Unavailable{Table: t.Name, Cause: e.cause}
		}
		// Taken while the base read lock is still held, so a rebind cannot be
		// closing this projection underneath us.
		e.table.mu.RLock()
		return e.table, nil, nil
	}

	rt, cause := t.compileRole(shape)
	if cause != "" {
		t.log.Error("chtypes could not compile a per-role schema; every insert for this role fails closed",
			"table", t.Name, "generation", t.Generation,
			"allowed_columns", shape.Columns, "default_columns", slices.Sorted(maps.Keys(shape.Defaults)),
			"cause", cause)
	}
	el := c.order.PushFront(&roleEntry{key: key, table: rt, cause: cause})
	c.index[key] = el
	var evicted *Table
	if c.order.Len() > c.cap {
		evicted = c.evictOldestLocked()
	}
	if rt == nil {
		return nil, evicted, &Unavailable{Table: t.Name, Cause: cause}
	}
	rt.mu.RLock()
	return rt, evicted, nil
}

// compileRole builds and compiles the role's declaration list. It returns
// (nil, cause) for every refusal, including the defensive column check.
func (t *Table) compileRole(shape RoleShape) (*Table, string) {
	cols, expected, wire, err := roleColumns(t.discovered, shape)
	if err != nil {
		return nil, err.Error()
	}
	ddl, rerr := chtypes.ReconstructDDL(cols)
	if rerr != nil {
		return nil, "cannot reconstruct role column declarations: " + rerr.Error()
	}
	slots, cause := compileDDL(t.lib, ddl)
	if cause != "" {
		return nil, cause
	}

	// Defensive: the Defaults values are the one thing in this DDL that is
	// TEXT rather than structure, so verify the compiler saw the column set we
	// meant. An escaping mistake that closed the literal early would show up
	// here as an extra or missing column, and must never be able to reshape
	// the table silently.
	got := compiledColumnNames(slots[0].schema)
	if got == nil {
		closeSlots(slots)
		return nil, "this chtypes artifact does not report compiled columns, so a per-role schema cannot be verified"
	}
	if !slices.Equal(got, expected) {
		closeSlots(slots)
		return nil, fmt.Sprintf("per-role schema compiled to columns %v, expected %v", got, expected)
	}

	return &Table{
		Name:        t.Name,
		Generation:  t.Generation,
		WireColumns: deriveWireColumns(slots[0].schema, wire),
		log:         t.log,
		slots:       slots,
		cols:        declaredColumns(slots[0].schema, nil),
		lib:         t.lib,
	}, ""
}

// roleColumns projects the table's discovered columns onto a shape. It returns
// the declaration list, the full column name list the compile must produce,
// and the wire column names (the declaration list minus the three kinds a
// positional INSERT never carries).
func roleColumns(src []discovery.Column, shape RoleShape) ([]chtypes.DiscoveredColumn, []string, []string, error) {
	var allowed map[string]struct{}
	if shape.Columns != nil {
		allowed = make(map[string]struct{}, len(shape.Columns))
		for _, c := range shape.Columns {
			allowed[c] = struct{}{}
		}
	}

	cols := make([]chtypes.DiscoveredColumn, 0, len(src))
	expected := make([]string, 0, len(src))
	wire := make([]string, 0, len(src))
	kept := make(map[string]discovery.Column, len(src))
	for _, c := range src {
		computed := c.DefaultKind == "MATERIALIZED" || c.DefaultKind == "ALIAS" || c.DefaultKind == "EPHEMERAL"
		if allowed != nil && !computed {
			if _, ok := allowed[c.Name]; !ok {
				continue
			}
		}
		dc := chtypes.DiscoveredColumn{
			Name:              c.Name,
			Type:              c.Type,
			DefaultKind:       c.DefaultKind,
			DefaultExpression: c.DefaultExpression,
			Position:          c.Position,
		}
		if v, inject := shape.Defaults[c.Name]; inject {
			if computed {
				return nil, nil, nil, fmt.Errorf(
					"cannot inject a default into column %q: it is %s", c.Name, c.DefaultKind)
			}
			dc.DefaultKind, dc.DefaultExpression = "DEFAULT", quoteLiteral(v)
		}
		cols = append(cols, dc)
		expected = append(expected, c.Name)
		if !computed {
			wire = append(wire, c.Name)
		}
		kept[c.Name] = c
	}

	// A default for a column this shape does not carry cannot be expressed,
	// and silently dropping it would turn "force this value" into "whatever
	// the caller sent". Fail loudly instead.
	for _, name := range slices.Sorted(maps.Keys(shape.Defaults)) {
		if _, ok := kept[name]; !ok {
			return nil, nil, nil, fmt.Errorf(
				"cannot inject a default into column %q: the role's schema does not carry it", name)
		}
	}
	return cols, expected, wire, nil
}

// compiledColumnNames is the column list the handle actually compiled to, in
// declaration order. nil when the artifact reports no columns at all.
func compiledColumnNames(schema *chtypes.LoadedSchema) []string {
	if schema == nil || len(schema.Columns) == 0 {
		return nil
	}
	out := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		out = append(out, c.Name)
	}
	return out
}

// literalEscaper escapes a ClickHouse string literal exactly as ClickHouse's
// own quoteString() does: a backslash becomes \\ and a single quote becomes \'.
// Both replacements run in one left-to-right pass, so neither re-processes the
// other's output.
var literalEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

// quoteLiteral renders s as a single-quoted ClickHouse string literal.
//
// This is the ONE place in WaveHouse that turns a value into SQL text, and it
// exists only because the chtypes SDK ships QuoteIdentifier but no literal
// quoter, so a per-role DEFAULT clause has to be spelled by hand — precisely
// the thing the SDK's own docs tell callers never to write. It is retired the
// day chtypes FR I‑2 (per-call column value overrides / injected literals,
// AUDIT §I‑2) lands: the value then goes to the library as a value and this
// function, its test corpus and the defensive column check in compileRole all
// go with it.
//
// The backstop, if this were ever wrong: a literal the compiler cannot parse
// is a compile refusal (ClickHouse code 6) and compileRole re-reads the
// compiled column list, so a botched escape fails closed rather than reshaping
// the table.
func quoteLiteral(s string) string {
	return "'" + literalEscaper.Replace(s) + "'"
}

type roleEntry struct {
	key   string
	table *Table // nil: this shape does not compile
	cause string
}

// roleCache is an LRU of per-role projections, with the same discipline as
// filterCache: bounded, negative entries cached, and every evicted handle
// closed.
type roleCache struct {
	mu    sync.Mutex
	cap   int
	order *list.List // front = most recently used
	index map[string]*list.Element
}

func newRoleCache(capacity int) *roleCache {
	return &roleCache{cap: capacity, order: list.New(), index: make(map[string]*list.Element)}
}

// evictOldestLocked drops the least recently used entry and returns the handle
// the caller must close. Closing is the caller's job because it waits for that
// projection's readers, and waiting under the cache lock would stall every
// other role on the table.
func (c *roleCache) evictOldestLocked() *Table {
	el := c.order.Back()
	if el == nil {
		return nil
	}
	c.order.Remove(el)
	e := el.Value.(*roleEntry)
	delete(c.index, e.key)
	return e.table
}

// closeAll drops every projection. Called with the base table's write lock
// held, so no new lookup can be in flight; each projection's own write lock
// waits for the requests already holding it.
func (c *roleCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.order.Front(); el != nil; el = el.Next() {
		if rt := el.Value.(*roleEntry).table; rt != nil {
			rt.close()
		}
	}
	c.order.Init()
	c.index = make(map[string]*list.Element)
}
