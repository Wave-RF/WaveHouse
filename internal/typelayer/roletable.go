package typelayer

import (
	"container/list"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// roleCacheSize bounds the per-role shapes held per table. A shape's Defaults
// values come from tenant claims and are baked into the compiled handle, so an
// unbounded cache is a memory and CPU denial of service — the same reason
// filterCache is bounded. Each entry holds one compiled handle, and grows to
// rolePoolSize only under contention.
const roleCacheSize = 256

// RoleShape is the projection of a table a role may insert through. It is the
// whole input to Engine.RoleTable and the whole cache key, so two roles with
// the same shape share one compiled handle.
//
// Columns is the set of columns the role may write. nil means "every column"
// and is the identity shape. Order is irrelevant — the DDL always follows the
// table's own declaration order — and a name the table does not have is
// ignored. A column the role may not write stays declared, re-declared
// MATERIALIZED with what the server stores when an INSERT omits it (its own
// DEFAULT expression, or its type's default): a record naming it is then
// refused like any column no INSERT may name (117), it is never on the wire,
// and every expression reading it still compiles and sees the value the
// server will store. A shape that leaves the role no plain or DEFAULT column
// to write is refused: it would publish rows with no columns. MATERIALIZED
// and ALIAS columns are declared as the table declares them; an EPHEMERAL one
// too, and a record may supply it only if the role may write it (and only
// where inputColumns lists it).
//
// Defaults maps a column to a literal value injected when a record omits it,
// rendered into the column's DEFAULT clause. A value the record DOES supply
// still wins (measured on the 26.6 and 26.8 artifacts; see
// TestRoleTable_DefaultInjectsWhenAbsentAndLosesToASuppliedValue). Every key
// must name a column the role may write and must be an ordinary column (no
// DEFAULT, or a plain DEFAULT) — anything else is refused rather than
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

// writable reports whether the role may supply column name.
func (s RoleShape) writable(name string) bool {
	return s.Columns == nil || slices.Contains(s.Columns, name)
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
// over the record's keys: a column the role may not write is declared
// MATERIALIZED, which no INSERT may name, so a record naming it is refused
// per-row with ClickHouse's own code 117 "Unknown field found while parsing
// JSONEachRow format: x"; a Defaults column is declared DEFAULT '<literal>',
// quoted by the library's own QuoteLiteral, so an absent value is filled and a
// supplied one still wins.
//
// The identity shape (no column restriction, no defaults) returns the base
// table itself — no second handle, no cache entry.
//
// A shape that does not compile is cached as a negative entry and reported as
// *RoleRefused, so a broken policy costs one compile and one log line per
// generation rather than one per request. An error from resolving the base
// table itself is Table's *Unavailable.
func (e *Engine) RoleTable(id tenant.ID, table string, shape RoleShape) (*Table, error) {
	base, err := e.Table(id, table)
	if err != nil {
		return nil, err
	}
	if shape.identity() {
		return base, nil // still read-locked; the caller's Release covers it
	}
	rt, evicted, err := func() (*Table, *Table, error) {
		defer base.Release() // a panicking compile must not leave a rebind waiting
		return base.roleTable(shape)
	}()
	// Closed after the cache lock and the base read lock are both gone: it
	// waits for the evicted shape's own readers, which are requests in flight.
	if evicted != nil {
		evicted.close("evicted from the role cache")
	}
	if err != nil {
		return nil, err
	}
	return rt, nil
}

// roleTable is RoleTable's cache half, run under the base table's read lock.
// It returns the projection read-locked, plus the entry its insertion evicted
// (to be closed by the caller, outside the cache lock).
//
// A miss compiles outside the cache lock, which every role lookup on the
// table needs: a compile takes ~100 µs, and the shape key carries claim
// values, so a table whose roles inject one value per end user misses on
// every request once it has more users than roleCacheSize, and after every
// rebind. Misses on different shapes therefore compile in parallel, while
// concurrent misses on one shape wait for the first one's compile rather
// than repeat it. The base read lock stays held throughout, so a rebind
// waits for a compile in flight instead of closing the cache under it.
func (t *Table) roleTable(shape RoleShape) (*Table, *Table, error) {
	key := shape.key(t.Generation)
	c := t.roles
	c.mu.Lock()
	for {
		if el, hit := c.index[key]; hit {
			rt, err := t.hitLocked(el)
			c.mu.Unlock()
			return rt, nil, err
		}
		f := c.inflight[key]
		if f == nil {
			break
		}
		f.waiters++
		c.mu.Unlock()
		<-f.done
		// Landed, or withdrawn by a panic. Look again; a shape evicted in the
		// meantime is compiled afresh below.
		c.mu.Lock()
	}
	f := &roleFlight{done: make(chan struct{})}
	c.inflight[key] = f
	c.mu.Unlock()

	rt, cause := c.compileFlight(t, shape, key, f)
	if cause != "" {
		slog.Error("chtypes could not compile a per-role schema; every insert for this role fails closed",
			"tenant", t.tenant, "table", t.Name, "generation", t.Generation,
			"allowed_columns", shape.Columns, "default_columns", slices.Sorted(maps.Keys(shape.Defaults)),
			"cause", cause)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	el := c.order.PushFront(&roleEntry{key: key, table: rt, cause: cause})
	c.index[key] = el
	delete(c.inflight, key)
	close(f.done)
	var evicted *Table
	if c.order.Len() > c.cap {
		evicted = c.evictOldestLocked()
	}
	if rt == nil {
		return nil, evicted, &RoleRefused{Tenant: t.tenant, Table: t.Name, Cause: cause}
	}
	// Under the cache lock, like a hit: nothing can have evicted it yet.
	rt.mu.RLock()
	return rt, evicted, nil
}

// hitLocked answers a cached entry. The caller holds the cache lock and the
// base read lock.
func (t *Table) hitLocked(el *list.Element) (*Table, error) {
	t.roles.order.MoveToFront(el)
	e := el.Value.(*roleEntry)
	if e.table == nil {
		return nil, &RoleRefused{Tenant: t.tenant, Table: t.Name, Cause: e.cause}
	}
	// Taken under the cache lock, while the entry is still indexed, so no
	// eviction is closing it; and under the base read lock, so no rebind is.
	// Neither close has started, so this never waits.
	e.table.mu.RLock()
	return e.table, nil
}

// compileFlight runs the compile for flight f, which this lookup registered.
// If the compile panics, f is withdrawn before the panic propagates, so its
// waiters wake, find neither an entry nor a flight, and compile for
// themselves rather than park forever.
func (c *roleCache) compileFlight(t *Table, shape RoleShape, key string, f *roleFlight) (*Table, string) {
	landed := false
	defer func() {
		if !landed {
			c.mu.Lock()
			delete(c.inflight, key)
			close(f.done)
			c.mu.Unlock()
		}
	}()
	rt, cause := c.compile(t, shape)
	landed = true
	return rt, cause
}

// compileRole builds and compiles the role's declaration list. It returns
// (nil, cause) for every refusal.
func (t *Table) compileRole(shape RoleShape) (*Table, string) {
	cols, wire, err := roleColumns(t.lib, t.discovered, shape)
	if err != nil {
		return nil, err.Error()
	}
	ddl, rerr := t.lib.ReconstructDDL(cols)
	if rerr != nil {
		return nil, "cannot reconstruct role column declarations: " + rerr.Error()
	}
	p, cause := newPool(t.lib, ddl, rolePoolSize())
	if cause != "" {
		return nil, cause
	}
	schema := p.first().schema
	declared, cause := declaredColumns(t.lib, schema, nil)
	if cause != "" {
		p.close()
		return nil, cause
	}
	wire = deriveWireColumns(schema, wire)

	return &Table{
		Name:        t.Name,
		Generation:  t.Generation,
		WireColumns: wire,
		tenant:      t.tenant,
		pool:        p,
		cols:        declared,
		inputs:      inputColumns(t.lib, cols, wire, shape.writable),
		lib:         t.lib,
	}, ""
}

// roleColumns projects the table's discovered columns onto a shape. It returns
// the declaration list and the wire column names (the plain and DEFAULT
// columns the role may write — what RowsExport emits). An injected value is
// quoted by lib.QuoteLiteral, ClickHouse's own quoteString, so it reaches the
// compiler as one string literal whatever bytes it holds.
//
// A column the role may not write is re-declared MATERIALIZED rather than
// dropped. Dropped, every DEFAULT, MATERIALIZED or ALIAS expression reading it
// would stop compiling, and the whole role would be refused for a column it
// never asked to write. MATERIALIZED with what the server stores when the
// worker's INSERT omits it keeps it off the wire and unnamable (117), and
// keeps what those expressions compute here equal to what the server will
// store.
func roleColumns(lib *chtypes.Library, src []discovery.Column, shape RoleShape) ([]chtypes.DiscoveredColumn, []string, error) {
	cols := make([]chtypes.DiscoveredColumn, 0, len(src))
	wire := make([]string, 0, len(src))
	declared := make(map[string]struct{}, len(src))
	for _, c := range src {
		declared[c.Name] = struct{}{}
		dc := discoveredColumn(c)
		v, inject := shape.Defaults[c.Name]
		switch {
		case c.DefaultKind == "MATERIALIZED" || c.DefaultKind == "ALIAS" || c.DefaultKind == "EPHEMERAL":
			if inject {
				return nil, nil, fmt.Errorf(
					"cannot inject a default into column %q: it is %s", c.Name, c.DefaultKind)
			}
		case !shape.writable(c.Name):
			// A default the role may not write cannot be expressed, and
			// dropping it would turn "force this value" into "whatever the
			// server defaults to". Fail loudly instead.
			if inject {
				return nil, nil, fmt.Errorf(
					"cannot inject a default into column %q: the role may not write it", c.Name)
			}
			expr, err := storedDefault(lib, c)
			if err != nil {
				return nil, nil, err
			}
			dc.DefaultKind, dc.DefaultExpression = "MATERIALIZED", expr
		default:
			if inject {
				lit, err := lib.QuoteLiteral(v)
				if err != nil {
					return nil, nil, fmt.Errorf("cannot quote the default for column %q: %w", c.Name, err)
				}
				dc.DefaultKind, dc.DefaultExpression = "DEFAULT", lit
			}
			wire = append(wire, c.Name)
		}
		cols = append(cols, dc)
	}

	for _, name := range slices.Sorted(maps.Keys(shape.Defaults)) {
		if _, ok := declared[name]; !ok {
			return nil, nil, fmt.Errorf(
				"cannot inject a default into column %q: the table does not have it", name)
		}
	}
	if len(wire) == 0 {
		return nil, nil, errors.New("the role may write no plain or DEFAULT column of this table, so it has no row to publish")
	}
	return cols, wire, nil
}

// storedDefault is an expression for what ClickHouse stores in column c when an
// INSERT omits it: its own DEFAULT expression, or its type's default value
// (defaultValueOfTypeName, ClickHouse's own answer, measured to compile for
// every type family the 26.8 artifact accepts, Nullable and LowCardinality
// included).
func storedDefault(lib *chtypes.Library, c discovery.Column) (string, error) {
	if c.DefaultKind == "DEFAULT" && c.DefaultExpression != "" {
		return c.DefaultExpression, nil
	}
	typ, err := lib.QuoteLiteral(c.Type)
	if err != nil {
		return "", fmt.Errorf("cannot quote the type of column %q: %w", c.Name, err)
	}
	return "defaultValueOfTypeName(" + typ + ")", nil
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
	// inflight holds the shapes being compiled, outside mu, by a lookup that
	// missed (see Table.roleTable).
	inflight map[string]*roleFlight
	// compile builds a shape's projection: Table.compileRole, which a test
	// may wrap to hold one compile open.
	compile func(*Table, RoleShape) (*Table, string)
}

// roleFlight is one shape's compile in progress. done is closed, under the
// cache lock, once the shape's entry is in the cache or the compile panicked.
type roleFlight struct {
	done chan struct{}
	// waiters counts the lookups that parked on done; written under the
	// cache lock, read by tests.
	waiters int
}

func newRoleCache(capacity int) *roleCache {
	return &roleCache{
		cap:      capacity,
		order:    list.New(),
		index:    make(map[string]*list.Element),
		inflight: make(map[string]*roleFlight),
		compile:  (*Table).compileRole,
	}
}

// capacity is the cache's bound, for a rebind's fresh cache; roleCacheSize
// for a nil cache.
func (c *roleCache) capacity() int {
	if c == nil {
		return roleCacheSize
	}
	return c.cap
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

// closeAll drops every projection. Called once the cache is detached from its
// base table (see Table.detachLocked), so no new lookup can reach it; each
// projection's own write lock waits for the requests already holding it.
func (c *roleCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.order.Front(); el != nil; el = el.Next() {
		if rt := el.Value.(*roleEntry).table; rt != nil {
			rt.close("base table rebound or closed")
		}
	}
	c.order.Init()
	c.index = make(map[string]*list.Element)
}
