// Package typelayer owns every call into the chtypes library. It compiles one
// ClickHouse schema handle per discovered table against the artifact for the
// server's own version line, and answers the questions the gateway would
// otherwise have to re-derive in Go, with the server's own parser:
//
//   - "would this record insert, and does it satisfy a role's insert check
//     clauses?" — Table.Ingest
//   - "does this stored row match a role's row filter?" — Table.ParseRow /
//     Row.Visible
//   - "what would this record insert for a role that may not write every
//     column, and whose absent check columns must be filled?" —
//     Engine.RoleTable
//
// Nothing outside this package imports github.com/wave-rf/chtypes/go/chtypes.
package typelayer

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// Format is the wire format a body is parsed as. It is chtypes' own enum,
// aliased so no other package has to import the SDK to name one; only the
// constants below are formats Ingest accepts.
type Format = chtypes.Format

// The formats Ingest accepts. JSONEachRow is name-addressed (an NDJSON body is
// the same format, byte for byte); CSV and TSV are positional in declaration
// order with no header line; the WithNames pair open with a header line that
// names the columns, in any order.
const (
	FormatJSONEachRow  = chtypes.JSONEachRow
	FormatCSV          = chtypes.CSV
	FormatTSV          = chtypes.TSV
	FormatCSVWithNames = chtypes.CSVWithNames
	FormatTSVWithNames = chtypes.TSVWithNames
)

// processTZ guards the chtypes.Timezone package global across Engines.
var processTZ struct {
	mu   sync.Mutex
	zone string
	set  bool
}

// compileSettings is the fixed parsing profile every table handle is compiled
// with. allow_errors_ratio turns "first bad row ends the batch" into
// skip-and-continue so every record gets its own verdict; skip_unknown_fields=0
// makes an unknown field a real per-row rejection (ClickHouse code 117) instead
// of silent data loss. Neither is ever forwarded to the real INSERT.
var compileSettings = map[string]string{
	"input_format_allow_errors_ratio":  "1",
	"input_format_skip_unknown_fields": "0",
}

// maxPoolSize caps the identically-compiled handles a base table holds.
//
// A LoadedSchema serializes its own calls, so one handle per table is a
// ceiling. Measured on a Linux arm64 VM (BenchmarkIngest_HandlePool, artifacts
// 26.6 and 25.8), own handles scale 4-6x at 8 goroutines, while a
// process-wide serialization gate never beats one thread (0.83-1.0x). A
// compiled handle costs ~40 KiB (~96 KiB warm), so a pool of 8 is cheap.
// Role tables keep one handle (roleHandles): hot tenants already spread over
// distinct role handles, and 256 shapes x 8 warm handles would be ~200 MiB.
// An earlier darwin run showed a flat curve, so treat the size as
// hardware-dependent and re-measure it on the deployment hardware.
const maxPoolSize = 8

// roleHandles is the handle count of a role-shape table.
const roleHandles = 1

// poolSize is how many identical handles one base-table shape gets.
func poolSize() int {
	n := runtime.GOMAXPROCS(0)
	if n > maxPoolSize {
		n = maxPoolSize
	}
	if n < 1 {
		n = 1
	}
	return n
}

// schemaSlot is one compiled handle plus the filters compiled against it. A
// filter is bound to the schema it was compiled from — evaluating it against a
// block from another handle takes BOTH handles' locks — so each slot owns its
// own cache and one evaluation stays on one slot.
type schemaSlot struct {
	schema  *chtypes.LoadedSchema
	filters *filterCache
}

// closeSlots tears down a pool. Filters go before the schema, which the C
// layer requires, and the cache index is emptied so no freed pointer survives.
func closeSlots(slots []*schemaSlot) {
	for _, s := range slots {
		if s == nil {
			continue
		}
		if s.filters != nil {
			s.filters.closeAll()
		}
		if s.schema != nil {
			s.schema.Close()
		}
	}
}

// Config is boot-tier: the registry directory is read once at process start.
// "" means the SDK's own search path ($CHTYPES_REGISTRY, the per-user cache,
// then the system directories); an explicit directory is searched first, then
// the rest of that path. Either way a library is opened lazily, by the first
// Bind for its line (~120 MB resident each).
type Config struct {
	RegistryDir string
}

// Engine wraps one chtypes.Registry for the process. Opened once at boot and
// rebound by discovery after every successful refresh.
type Engine struct {
	reg    *chtypes.Registry
	logger *slog.Logger

	// bindMu serializes Bind. Refresh normally runs on one goroutine, but the
	// manual refresh endpoint can overlap the auto-refresh loop, and two binds
	// racing would leak a handle neither of them installed.
	bindMu sync.Mutex

	mu sync.RWMutex
	// lib is the library the current handles were compiled against; a change
	// invalidates every handle, because a filter or block only answers for the
	// library its schema came from.
	lib *chtypes.Library
	// tz is the zone chtypes.Timezone was initialised with. chtypes reads that
	// package global once per dlopen, so it cannot be changed afterwards.
	tz     string
	tzSet  bool
	global string // non-empty: the cause every Table() reports
	tables map[string]*Table
}

// NewEngine opens the registry, which reads manifests and opens no library. It
// fails only when an explicit directory cannot be read or nothing on the search
// path holds an artifact, and returns the SDK's own message, which names the
// directories it looked in. Anything an artifact itself can be wrong about — a
// missing line, a refused ABI revision, a truncated library — surfaces at the
// first Bind for that line, as a global Unavailable.
//
// WithPreload is deliberately not used: it opens a library at construction,
// and chtypes.Timezone must be set before that from the server's own zone,
// which only discovery knows (see resolve).
func NewEngine(cfg Config, logger *slog.Logger) (*Engine, error) {
	reg, err := chtypes.NewRegistry(cfg.RegistryDir, chtypes.WithAutoFetch(false))
	if err != nil {
		return nil, err
	}
	return &Engine{reg: reg, logger: logger, tables: make(map[string]*Table)}, nil
}

// Table returns the current compiled handle for a table, read-locked. The
// caller must Release it when done; the handle stays alive and stable for the
// whole time it is held, so a concurrent rebind waits rather than pulling the
// schema out from under a request.
func (e *Engine) Table(name string) (*Table, error) {
	e.mu.RLock()
	global, t := e.global, e.tables[name]
	e.mu.RUnlock()

	if global != "" {
		return nil, &Unavailable{Table: name, Cause: global}
	}
	if t == nil {
		return nil, &Unavailable{Table: name, Cause: "no compiled schema (table not discovered)"}
	}
	t.mu.RLock()
	if len(t.slots) == 0 {
		cause := t.cause
		t.mu.RUnlock()
		return nil, &Unavailable{Table: name, Cause: cause}
	}
	return t, nil
}

// Close releases every compiled handle. Libraries are never unloaded — chtypes
// deliberately has no dlclose path — so this is only for tests and shutdown.
func (e *Engine) Close() {
	e.mu.Lock()
	tables := e.tables
	e.tables = make(map[string]*Table)
	e.mu.Unlock()
	for _, t := range tables {
		t.close()
	}
}

// Table is one compiled shape: a pool of identical schema handles plus the
// caches built over them. Fields are written only under the exclusive lock
// Bind takes, so a holder of a Release-pending read lock sees a consistent set.
//
// A Table is either the discovered table's own schema (Engine.Table) or a
// per-role projection of it (Engine.RoleTable). The two have the same method
// set; a role Table's WireColumns are the ROLE's columns, which is what makes
// the exported row per-role.
type Table struct {
	Name       string
	Generation uint64
	// WireColumns is declaration order minus MATERIALIZED/ALIAS/EPHEMERAL —
	// exactly the columns RowsExport emits, and therefore exactly what the NATS
	// envelope's Columns must carry. It is read off the COMPILED handle
	// (LoadedSchema.Columns), not recomputed from discovery, so it is a
	// property of the thing that produced the bytes.
	WireColumns []string

	log *slog.Logger
	mu  sync.RWMutex
	// slots holds the identically-compiled handles; empty means unavailable.
	slots []*schemaSlot
	next  atomic.Uint64
	// cols is every column the compiled schema declares, of every kind — what
	// render tests a predicate's column against.
	// cols maps every column the compiled schema declares, of every kind, to
	// its identifier as the library quotes it — what render tests a
	// predicate's column against, and what it writes.
	cols  map[string]string
	cause string // why slots is empty
	sig   string
	// lib and discovered are what a per-role recompile needs: the library the
	// handles came from, and the column list the DDL was built from.
	lib        *chtypes.Library
	discovered []discovery.Column
	// roles caches per-role projections of this table; nil on a role Table,
	// which is never itself projected.
	roles *roleCache
}

// Release drops the read lock taken by Engine.Table or Engine.RoleTable.
func (t *Table) Release() { t.mu.RUnlock() }

// slot picks the handle this call runs on. Round-robin rather than a real
// sync.Pool: a LoadedSchema is safe for concurrent use (it serializes
// internally), so there is nothing to check out and return, and a stable slot
// keeps a parsed block and every filter evaluated against it on ONE handle.
func (t *Table) slot() *schemaSlot {
	n := len(t.slots)
	if n == 1 {
		return t.slots[0]
	}
	return t.slots[t.next.Add(1)%uint64(n)]
}

// close tears every handle down, waiting for this shape's readers first.
func (t *Table) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeLocked()
}

func (t *Table) closeLocked() {
	// Role projections go first, and each waits for its own readers: a role
	// handle outlives a slot swap only for as long as a request holds it.
	if t.roles != nil {
		t.roles.closeAll()
	}
	closeSlots(t.slots)
	t.slots = nil
}

// Bind resolves the library for serverVersion and (re)compiles a handle per
// table. It is called synchronously from discovery's refresh hook, so it must
// never be fatal: a failure is recorded as a cause — per table for a compile
// refusal, process-wide for a missing artifact or a timezone mismatch — and
// surfaces as *Unavailable from Table.
func (e *Engine) Bind(serverVersion, serverTZ string, tables []*discovery.TableSchema) {
	e.bindMu.Lock()
	defer e.bindMu.Unlock()

	tz := serverTZ
	if tz == "" {
		tz = "UTC" // chtypes' own default; never leak the host's zone
	}

	lib, ok := e.resolve(serverVersion, tz)
	if !ok {
		return
	}

	// Compile outside every lock: a handle costs milliseconds and Table()
	// readers are on the request path.
	type pending struct {
		name       string
		sig        string
		slots      []*schemaSlot
		cause      string
		wire       []string
		cols       map[string]string
		discovered []discovery.Column
	}

	e.mu.RLock()
	current := make(map[string]*Table, len(e.tables))
	for k, v := range e.tables {
		current[k] = v
	}
	libChanged := e.lib != lib
	e.mu.RUnlock()

	fresh := make([]pending, 0, len(tables))
	keep := make(map[string]struct{}, len(tables))
	for _, ts := range tables {
		keep[ts.Name] = struct{}{}
		sig := signature(ts)
		if !libChanged {
			if old, exists := current[ts.Name]; exists && old.sig == sig && len(old.slots) > 0 {
				continue // same columns, same library: the handle still answers
			}
		}
		p := pending{name: ts.Name, sig: sig, discovered: ts.Columns}
		p.slots, p.cause = compile(lib, ts)
		if p.cause == "" {
			p.wire = deriveWireColumns(p.slots[0].schema, wireColumns(ts))
			if p.cols, p.cause = declaredColumns(lib, p.slots[0].schema, ts.Columns); p.cause != "" {
				closeSlots(p.slots)
				p.slots = nil
			}
		}
		if p.cause != "" {
			e.logger.Error("chtypes could not compile table schema", "table", ts.Name, "cause", p.cause)
		}
		fresh = append(fresh, p)
	}

	// Swap each slot under its own lock and NOT under e.mu: taking a slot's
	// write lock waits for that table's in-flight requests, and holding the
	// engine lock through that wait would stall lookups for every other table.
	added := make(map[string]*Table, len(fresh))
	for _, p := range fresh {
		t, exists := current[p.name]
		if !exists {
			// Nobody holds a pointer to a new slot yet, so it is filled before
			// it is published rather than swapped.
			t = &Table{Name: p.name, log: e.logger, roles: newRoleCache(roleCacheSize), Generation: 1}
			t.WireColumns, t.cols, t.sig = p.wire, p.cols, p.sig
			t.lib, t.discovered = lib, p.discovered
			t.slots, t.cause = p.slots, p.cause
			added[p.name] = t
			continue
		}
		t.mu.Lock()
		t.closeLocked()
		t.Generation++
		t.WireColumns, t.cols, t.sig = p.wire, p.cols, p.sig
		t.lib, t.discovered = lib, p.discovered
		t.slots, t.cause = p.slots, p.cause
		t.mu.Unlock()
	}

	e.mu.Lock()
	e.lib = lib
	for name, t := range added {
		e.tables[name] = t
	}
	var dropped []*Table
	for name, t := range e.tables {
		if _, still := keep[name]; !still {
			dropped = append(dropped, t)
			delete(e.tables, name)
		}
	}
	e.mu.Unlock()

	for _, t := range dropped {
		t.close()
	}
}

// resolve sets the process timezone on the first bind and looks up the library
// for the server's version line. It reports false when the engine is now
// globally unavailable.
func (e *Engine) resolve(serverVersion, tz string) (*chtypes.Library, bool) {
	e.mu.Lock()
	// chtypes.Timezone is read once per dlopen, so it is a fact about the
	// process, not this Engine: the first Engine to bind sets it under a
	// package lock and every later bind (any Engine) must agree with it.
	processTZ.mu.Lock()
	if !processTZ.set {
		chtypes.Timezone = tz
		processTZ.zone, processTZ.set = tz, true
	}
	e.tz, e.tzSet = processTZ.zone, true
	processTZ.mu.Unlock()
	if e.tz != tz {
		e.global = fmt.Sprintf(
			"ClickHouse reports timezone %q but this process initialised chtypes with %q; restart wavehouse to adopt the new zone",
			tz, e.tz)
		e.mu.Unlock()
		e.logger.Error("chtypes timezone mismatch", "process_tz", e.tz, "server_tz", tz)
		return nil, false
	}
	e.mu.Unlock()

	lib, err := e.reg.For(chtypes.Version(serverVersion))
	if err != nil {
		e.mu.Lock()
		e.global = err.Error()
		e.mu.Unlock()
		e.logger.Error("no chtypes artifact for this ClickHouse version", "server_version", serverVersion, "error", err)
		return nil, false
	}

	e.mu.Lock()
	e.global = ""
	e.mu.Unlock()
	return lib, true
}

// compile reconstructs the column-declaration list chtypes wants (not a CREATE
// TABLE) and compiles the table's pool of handles. The engine and TTL clauses
// are deliberately not declared: chtypes declines engines it cannot model, and
// neither affects the insert verdicts or filter semantics this package asks
// for.
func compile(lib *chtypes.Library, ts *discovery.TableSchema) ([]*schemaSlot, string) {
	cols := make([]chtypes.DiscoveredColumn, 0, len(ts.Columns))
	for _, c := range ts.Columns {
		cols = append(cols, chtypes.DiscoveredColumn{
			Name:              c.Name,
			Type:              c.Type,
			DefaultKind:       c.DefaultKind,
			DefaultExpression: c.DefaultExpression,
			Position:          c.Position,
		})
	}
	ddl, err := lib.ReconstructDDL(cols)
	if err != nil {
		return nil, "cannot reconstruct column declarations: " + err.Error()
	}
	return compileDDL(lib, ddl, poolSize())
}

// compileDDL compiles n identical handles for one declaration list. A
// refusal on any of them is a refusal for the whole shape: the handles are
// interchangeable by construction, so half a pool would be a table that
// answers differently depending on which slot a request landed on.
//
// The per-table filter budget is SPLIT across the pool rather than multiplied
// by it: values are tenant-controlled and baked into a compiled handle, so the
// bound that makes the cache not-a-DoS has to be a bound on the table.
func compileDDL(lib *chtypes.Library, ddl string, n int) ([]*schemaSlot, string) {
	perSlot := max(filterCacheSize/n, 1)
	slots := make([]*schemaSlot, 0, n)
	for range n {
		schema, err := lib.CompileDDL(ddl, chtypes.WithCompileSettings(compileSettings))
		if err != nil {
			closeSlots(slots)
			return nil, describeCompileError(err)
		}
		slots = append(slots, &schemaSlot{schema: schema, filters: newFilterCache(perSlot)})
	}
	return slots, ""
}

// describeCompileError keeps ClickHouse's own refusal (a real code) distinct
// from chtypes declining to answer — the two mean different things to an
// operator and to the HTTP status a caller picks.
func describeCompileError(err error) string {
	var se *chtypes.SchemaError
	if errors.As(err, &se) {
		return fmt.Sprintf("compile refused with ClickHouse code %d: %s", se.Code, se.Msg)
	}
	var ue *chtypes.UnsupportedError
	if errors.As(err, &ue) {
		return "chtypes declined: " + ue.Msg
	}
	return err.Error()
}

// signature is the column shape a handle was compiled from. An unchanged
// signature keeps the handle and the generation, so a refresh that discovers
// nothing new costs no compiles and invalidates no cached filter.
func signature(ts *discovery.TableSchema) string {
	var b strings.Builder
	for _, c := range ts.Columns {
		fmt.Fprintf(&b, "%d\x1f%s\x1f%s\x1f%s\x1f%s\x1e",
			c.Position, c.Name, c.Type, c.DefaultKind, c.DefaultExpression)
	}
	return b.String()
}

// deriveWireColumns is what RowsExport emits, read off the compiled handle:
// declaration order minus the three kinds a positional INSERT never carries.
// MATERIALIZED and ALIAS are computed by the server; EPHEMERAL is insert-only
// and never stored.
//
// Taking it from LoadedSchema.Columns rather than recomputing it from
// discovery makes the wire list a property of the handle that produced the
// bytes, which is what ParseRow's drift check actually wants. An artifact
// without column introspection reports no Columns at all; the fallback is then
// the discovery-side computation, which answers identically on every artifact
// measured (AUDIT §C.2).
func deriveWireColumns(schema *chtypes.LoadedSchema, fallback []string) []string {
	if schema == nil || len(schema.Columns) == 0 {
		return fallback
	}
	out := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		switch c.DefaultKind {
		case chtypes.KindMaterialized, chtypes.KindAlias, chtypes.KindEphemeral:
			// Never serialized by RowsExport.
		case chtypes.KindNone, chtypes.KindDefault:
			out = append(out, c.Name)
		default:
			// A kind this build does not know is treated as ordinary, matching
			// the discovery-side fallback: dropping it would silently change
			// the arity of every exported line.
			out = append(out, c.Name)
		}
	}
	return out
}

// wireColumns is deriveWireColumns' discovery-side fallback.
func wireColumns(ts *discovery.TableSchema) []string {
	out := make([]string, 0, len(ts.Columns))
	for _, c := range ts.Columns {
		switch c.DefaultKind {
		case "MATERIALIZED", "ALIAS", "EPHEMERAL":
		default:
			out = append(out, c.Name)
		}
	}
	return out
}

// declaredColumns maps every column name the compiled schema knows, of every
// kind, to its identifier as lib.QuoteIdentifier spells it (ClickHouse's own
// backQuote, always quoted). It is the set render tests a predicate's column
// against: answering "no such column" here keeps a misspelled policy from
// costing a compile and a log line per generation, and on a ROLE table it is
// what makes a filter over a denied column fail closed instead of compiling
// against a column that is not there. Quoting once per compile keeps a C call
// off render's per-event path. A non-empty second return is the cause.
func declaredColumns(lib *chtypes.Library, schema *chtypes.LoadedSchema, fallback []discovery.Column) (map[string]string, string) {
	var names []string
	if schema != nil && len(schema.Columns) > 0 {
		for _, c := range schema.Columns {
			names = append(names, c.Name)
		}
	} else {
		for _, c := range fallback {
			names = append(names, c.Name)
		}
	}
	m := make(map[string]string, len(names))
	for _, n := range names {
		q, err := lib.QuoteIdentifier(n)
		if err != nil {
			return nil, fmt.Sprintf("cannot quote column %q: %s", n, err)
		}
		m[n] = q
	}
	return m, ""
}
