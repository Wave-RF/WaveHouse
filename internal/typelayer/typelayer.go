// Package typelayer owns every call into the chtypes library. It compiles one
// ClickHouse schema handle per discovered table, per tenant, against the
// artifact for that tenant's server version line, and answers the questions
// the gateway would otherwise have to re-derive in Go, with the server's own
// parser:
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
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Format is the wire format a body is parsed as. It is chtypes' own enum,
// aliased so no other package has to import the SDK to name one; only the
// constants below are formats Ingest accepts.
type Format = chtypes.Format

// The formats Ingest accepts. JSONEachRow is name-addressed (an NDJSON body is
// the same format, byte for byte); CSV and TSV are positional in declaration
// order, with ClickHouse's header auto-detection unless IngestOptions turns it
// off; the WithNames pair open with a header line that names the columns, in
// any order.
const (
	FormatJSONEachRow  = chtypes.JSONEachRow
	FormatCSV          = chtypes.CSV
	FormatTSV          = chtypes.TSV
	FormatCSVWithNames = chtypes.CSVWithNames
	FormatTSVWithNames = chtypes.TSVWithNames
)

// Causes an Unavailable carries for states of the Engine rather than of a
// table or an artifact.
const (
	causeUnbound = "no schema is bound for this tenant yet (discovery has not completed a refresh)"
	causeRetired = "tenant retired"
	causeDropped = "table no longer discovered"
	causeClosed  = "type layer closed"
)

// Config is boot-tier, read once at process start. CacheDir is a chtypes v1
// artifact layout, the directory itself (CHTYPES_CACHE semantics); "" means
// $CHTYPES_CACHE, else ${XDG_CACHE_HOME:-~/.cache}/chtypes/v1. The read-only
// system layouts (systemLayouts) are searched after it either way. A library
// is opened lazily, by the first Bind for its line (~120 MB resident each).
type Config struct {
	CacheDir string
}

// systemLayouts are the read-only layouts the SDK searches after the cache.
var systemLayouts = []string{"/usr/local/share/chtypes/v1", "/opt/chtypes/v1"}

// Engine is the process's one chtypes registry plus every tenant's compiled
// tables. It is opened once at boot; discovery rebinds a tenant after each of
// that tenant's successful refreshes (Bind), and the tenant's teardown drops
// it (Forget).
//
// Tenants are independent: a tenant that is not bound yet or has no artifact
// for its server line is Unavailable on its own, a table that does not compile
// (or that its server zone keeps from being served, see zoneCause) is
// Unavailable alone, and every other tenant keeps answering. Two tenants on
// the same server and database still compile separate handles.
type Engine struct {
	reg *chtypes.Registry

	mu      sync.RWMutex
	tenants map[tenant.ID]*tenantSet
	closed  bool
	// retiring tracks Forget's background teardowns, so Close (and a test)
	// can wait for them.
	retiring sync.WaitGroup
}

// tenantSet is one tenant's compiled tables.
type tenantSet struct {
	id  tenant.ID
	reg *chtypes.Registry

	// bindMu serializes Bind for this tenant: a manual refresh can overlap the
	// auto-refresh loop, and two binds racing would leak a handle neither of
	// them installed. retire takes it too, so a teardown waits for a bind in
	// flight instead of racing it.
	bindMu sync.Mutex
	// retired is set, under bindMu, once the set is torn down; a Bind that
	// finds it set starts over on a fresh set.
	retired bool

	mu sync.RWMutex
	// lib is the library the current handles were compiled against; a change
	// invalidates every handle, because a filter or block only answers for the
	// library its schema came from.
	lib *chtypes.Library
	// cause, when non-empty, is what every Table() of this tenant reports.
	cause  string
	tables map[string]*Table
}

// NewEngine opens the registry, which reads the fetch layer's install records
// and opens no library. It fails only when an explicit directory cannot be read
// or is a chtypes 0.x registry, or when no layout searched holds an artifact
// for this host, naming the layouts it looked in. Anything an artifact itself
// can be wrong about — a missing line, a refused ABI, a truncated library —
// surfaces at the first Bind for that line, as that tenant's Unavailable.
//
// WithPreload is deliberately not used: it opens a library at construction,
// and the process's image zone must come from a server's own zone, which only
// discovery knows (see openLine).
func NewEngine(cfg Config) (*Engine, error) {
	if cfg.CacheDir != "" {
		if err := checkLayout(cfg.CacheDir); err != nil {
			return nil, err
		}
	}
	reg, err := chtypes.NewRegistry(chtypes.WithAutoFetch(false),
		chtypes.WithFetchOptions(chtypes.FetchOptions{CacheDir: cfg.CacheDir}))
	if err != nil {
		return nil, err
	}
	installed, err := reg.Installed()
	if err != nil {
		return nil, err
	}
	platform := runtime.GOOS + "-" + runtime.GOARCH
	if !slices.ContainsFunc(installed, func(r chtypes.Resolved) bool { return r.Platform == platform }) {
		return nil, fmt.Errorf("chtypes: no artifact installed for %s (looked in: %s)",
			platform, strings.Join(append([]string{cacheRoot(cfg.CacheDir)}, systemLayouts...), ", "))
	}
	return &Engine{reg: reg, tenants: make(map[tenant.ID]*tenantSet)}, nil
}

// checkLayout refuses an explicit directory that cannot be read, or that holds
// a chtypes 0.x registry (<line>/manifest.json) and no v1 layout: a 0.x
// artifact never loads under v1, and an empty answer would hide why.
func checkLayout(dir string) error {
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("chtypes: cannot read the artifact directory: %w", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "oci-layout")); err == nil || !errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if v0, _ := filepath.Glob(filepath.Join(dir, "*", "manifest.json")); len(v0) > 0 {
		return fmt.Errorf("chtypes: %s is a chtypes 0.x registry directory; chtypes v1 reads an OCI layout "+
			"(fetch the artifact with the v1 chtypes CLI, which writes one)", dir)
	}
	return nil
}

// cacheRoot is the cache layout the SDK reads for dir, in its own precedence.
func cacheRoot(dir string) string {
	if dir != "" {
		return dir
	}
	if env := os.Getenv("CHTYPES_CACHE"); env != "" {
		return env
	}
	base := os.Getenv("XDG_CACHE_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "${XDG_CACHE_HOME:-~/.cache}/chtypes/v1"
		}
		base = filepath.Join(home, ".cache")
	}
	return filepath.Join(base, "chtypes", "v1")
}

// Table returns tenant id's current compiled handle for a table, read-locked.
// The caller must Release it when done; the handle stays alive and stable for
// the whole time it is held, so a concurrent rebind waits rather than pulling
// the schema out from under a request.
//
// The lock is not reentrant: a caller holding a Table must not ask for the
// same table again (Table or RoleTable) before releasing it, or it deadlocks
// against a rebind waiting between the two.
func (e *Engine) Table(id tenant.ID, name string) (*Table, error) {
	e.mu.RLock()
	set, closed := e.tenants[id], e.closed
	e.mu.RUnlock()
	if closed {
		return nil, &Unavailable{Tenant: id, Cause: causeClosed}
	}
	if set == nil {
		return nil, &Unavailable{Tenant: id, Cause: causeUnbound}
	}

	set.mu.RLock()
	cause, t := set.cause, set.tables[name]
	set.mu.RUnlock()
	if cause != "" {
		return nil, &Unavailable{Tenant: id, Cause: cause}
	}
	if t == nil {
		return nil, &Unavailable{Tenant: id, Table: name, Cause: "no compiled schema (table not discovered)"}
	}
	t.mu.RLock()
	if t.c == nil {
		cause := t.cause
		t.mu.RUnlock()
		return nil, &Unavailable{Tenant: id, Table: name, Cause: cause}
	}
	return t, nil
}

// Bind resolves the library for the tenant's serverVersion and (re)compiles a
// handle per table, keeping every compiled table whose columns, zone and
// library are unchanged. It is called synchronously from discovery's refresh
// hook, so it must never be fatal: a failure is recorded as a cause — per
// table for a compile refusal or a zone the table cannot be read in,
// tenant-wide for a missing artifact — and surfaces as *Unavailable from
// Table. A table whose compile was refused
// is compiled again at every Bind; a table the tenant no longer has is closed;
// a tenant whose line resolves again is answering again.
//
// serverTZ is the server's default zone name as ClickHouse reports it; ""
// means UTC, chtypes' own default, never the host's zone. The first tenant
// bound in the process fixes the image zone (see zone.go).
func (e *Engine) Bind(id tenant.ID, serverVersion, serverTZ string, tables []*discovery.TableSchema) {
	for {
		set := e.tenantForBind(id)
		if set == nil {
			return // closed
		}
		set.bindMu.Lock()
		if set.retired {
			// Forgotten between the lookup and the lock: start over, on the
			// fresh set a lookup now creates.
			set.bindMu.Unlock()
			continue
		}
		set.bind(serverVersion, serverTZ, tables)
		set.bindMu.Unlock()
		return
	}
}

// tenantForBind returns id's set, creating it, or nil once the engine is
// closed.
func (e *Engine) tenantForBind(id tenant.ID) *tenantSet {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	set := e.tenants[id]
	if set == nil {
		set = &tenantSet{id: id, reg: e.reg, tables: make(map[string]*Table)}
		e.tenants[id] = set
	}
	return set
}

// Forget drops a tenant: every later Table for it is Unavailable until a Bind
// binds it afresh. Its handles are closed in the background — closing a
// handle waits for the requests holding it, and the caller may be holding a
// lock that must not wait on I/O — so Forget itself never blocks on them.
func (e *Engine) Forget(id tenant.ID) {
	e.mu.Lock()
	set := e.tenants[id]
	delete(e.tenants, id)
	if set != nil && !e.closed {
		e.retiring.Add(1)
	}
	closed := e.closed
	e.mu.Unlock()
	if set == nil || closed {
		return
	}
	go func() {
		defer e.retiring.Done()
		set.retire(causeRetired)
	}()
}

// Close releases every compiled handle, waiting for the requests holding them
// and for any background Forget. Libraries are never unloaded — chtypes
// deliberately has no dlclose path — so this is only for tests and shutdown;
// every later Table is Unavailable and every later Bind does nothing.
func (e *Engine) Close() {
	e.mu.Lock()
	sets := e.tenants
	e.tenants = make(map[tenant.ID]*tenantSet)
	e.closed = true
	e.mu.Unlock()
	for _, set := range sets {
		set.retire(causeClosed)
	}
	e.retiring.Wait()
}

// TenantCause is the tenant-wide cause every Table of tenant id would report,
// "" when the tenant is bound and answering. A table-level cause (a table
// that did not compile) is not tenant-wide and is not reported here.
func (e *Engine) TenantCause(id tenant.ID) string {
	e.mu.RLock()
	set := e.tenants[id]
	e.mu.RUnlock()
	if set == nil {
		return causeUnbound
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	return set.cause
}

// retire tears the set down, after any bind in flight finishes.
func (s *tenantSet) retire(cause string) {
	s.bindMu.Lock()
	defer s.bindMu.Unlock()
	s.retired = true
	s.mu.Lock()
	tables := s.tables
	s.tables = make(map[string]*Table)
	s.cause = cause
	s.mu.Unlock()
	for _, t := range tables {
		t.close(cause)
	}
}

// bind is Bind's body, run under s.bindMu.
func (s *tenantSet) bind(serverVersion, serverTZ string, tables []*discovery.TableSchema) {
	tz := serverTZ
	if tz == "" {
		tz = "UTC"
	}
	lib, cause := openLine(s.reg, serverVersion, tz)
	if cause != "" {
		// The tables stay: if the tenant's line resolves again in the same
		// library, unchanged handles are kept rather than recompiled.
		s.mu.Lock()
		s.cause = cause
		s.mu.Unlock()
		slog.Error("chtypes cannot serve this tenant; its ingest and row filters are unavailable",
			"tenant", s.id, "server_version", serverVersion, "server_tz", tz, "cause", cause)
		return
	}

	// Compile outside every lock: a handle costs milliseconds and Table()
	// readers are on the request path.
	type pending struct {
		ts     *discovery.TableSchema
		sig    string
		c      *compiled
		cause  string
		wire   []string
		inputs []string
		cols   map[string]filterColumn
	}

	s.mu.RLock()
	current := make(map[string]*Table, len(s.tables))
	for k, v := range s.tables {
		current[k] = v
	}
	libChanged := s.lib != lib
	s.mu.RUnlock()
	if libChanged {
		logLibrary(s.id, serverVersion, lib)
	}

	session := sessionZone(tz)
	fresh := make([]pending, 0, len(tables))
	keep := make(map[string]struct{}, len(tables))
	for _, ts := range tables {
		keep[ts.Name] = struct{}{}
		sig := signature(ts, tz)
		if !libChanged {
			if old, exists := current[ts.Name]; exists && old.answers(sig) {
				continue // same columns, zone and library: the handle still answers
			}
		}
		p := pending{ts: ts, sig: sig}
		decl := declsOf(ts.Columns)
		if p.cause = zoneCause(session, decl); p.cause == "" {
			p.c, p.cause = compileColumns(lib, decl)
		}
		if p.cause == "" {
			p.wire = wireColumns(p.c.desc)
			p.inputs = inputColumns(lib, decl, p.wire, nil)
			if p.cols, p.cause = declaredColumns(lib, p.c.desc); p.cause != "" {
				p.c.close()
				p.c = nil
			}
		}
		if p.cause != "" {
			slog.Error("chtypes could not compile table schema", "tenant", s.id, "table", ts.Name, "cause", p.cause)
		}
		fresh = append(fresh, p)
	}

	// Swap each table under its own lock and NOT under s.mu: taking a table's
	// write lock waits for that table's in-flight requests, and holding the
	// tenant lock through that wait would stall lookups for every other table.
	added := make(map[string]*Table, len(fresh))
	for _, p := range fresh {
		t, exists := current[p.ts.Name]
		if !exists {
			// Nobody holds a pointer to a new table yet, so it is filled before
			// it is published rather than swapped.
			t = &Table{Name: p.ts.Name, tenant: s.id, Generation: 1, roles: newRoleCache(roleCacheSize)}
			t.install(p.c, p.cause, p.wire, p.inputs, p.cols, p.sig, lib, p.ts.Columns, tz)
			added[p.ts.Name] = t
			continue
		}
		t.mu.Lock()
		old := t.detachLocked()
		t.Generation++
		t.roles = newRoleCache(old.roles.capacity())
		t.install(p.c, p.cause, p.wire, p.inputs, p.cols, p.sig, lib, p.ts.Columns, tz)
		t.mu.Unlock()
		old.close()
	}

	s.mu.Lock()
	s.lib = lib
	s.cause = ""
	for name, t := range added {
		s.tables[name] = t
	}
	var dropped []*Table
	for name, t := range s.tables {
		if _, still := keep[name]; !still {
			dropped = append(dropped, t)
			delete(s.tables, name)
		}
	}
	s.mu.Unlock()

	for _, t := range dropped {
		t.close(causeDropped)
	}
}

// logLibrary records, once each time a tenant's library changes, which
// artifact answers for the tenant's server. A server on another patch of the
// line is answered by the line's artifact, and patches can differ.
func logLibrary(id tenant.ID, serverVersion string, lib *chtypes.Library) {
	if res := lib.Resolved(); res != nil {
		if _, seen := warnedLibs.LoadOrStore(lib, true); !seen {
			for _, w := range res.Warnings {
				slog.Warn("chtypes SDK warning", "chtypes_version", lib.Version, "message", w)
			}
		}
	}
	attrs := []any{"tenant", id, "server_version", serverVersion, "chtypes_version", lib.Version}
	if lib.Version == strings.TrimPrefix(strings.TrimSpace(serverVersion), "v") {
		slog.Info("chtypes library bound", attrs...)
		return
	}
	slog.Warn("chtypes library bound from another patch of the server's line; verdicts follow the artifact's patch",
		attrs...)
}

// warnedLibs is the libraries whose fetch warnings were logged: once each.
var warnedLibs sync.Map

// Table is one compiled shape: one schema handle every request shares, plus
// the caches built over it. Fields are written only under the exclusive lock a
// rebind takes, so a holder of a Release-pending read lock sees a consistent
// set.
//
// A Table is either the discovered table's own schema (Engine.Table) or a
// per-role projection of it (Engine.RoleTable). The two have the same method
// set; a role Table's WireColumns are the ROLE's columns, which is what makes
// the exported row per-role.
type Table struct {
	Name       string
	Generation uint64
	// WireColumns is declaration order minus MATERIALIZED/ALIAS/EPHEMERAL —
	// exactly the columns the export emits, and therefore exactly what the
	// message envelope's Columns must carry. It is read off the COMPILED handle
	// (Schema.Describe), not recomputed from discovery, so it is a property of
	// the thing that produced the bytes.
	WireColumns []string

	tenant tenant.ID
	mu     sync.RWMutex
	// inputs is the INSERT column list a name-addressed body is parsed with
	// (see inputColumns); nil parses with no list, which reads WireColumns.
	inputs []string
	// c is the compiled handle; nil means unavailable.
	c *compiled
	// zone is the tenant's server zone, and session the session_timezone
	// every call for this table carries (sessionZone; "" for none). See
	// zoneOpts.
	zone, session string
	// cols maps every column the compiled schema declares, of every kind, to
	// how render writes a predicate over it — what render tests a predicate's
	// column against.
	cols  map[string]filterColumn
	cause string // why c is nil
	sig   string
	// lib and discovered are what a per-role recompile needs: the library the
	// handle came from, and the column list its CREATE was built from.
	lib        *chtypes.Library
	discovered []discovery.Column
	// roles caches per-role projections of this table; nil on a role Table,
	// which is never itself projected.
	roles *roleCache
}

// Release drops the read lock taken by Engine.Table or Engine.RoleTable.
func (t *Table) Release() { t.mu.RUnlock() }

// answers reports whether the table already serves sig, compiled.
func (t *Table) answers(sig string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sig == sig && t.c != nil
}

// install sets a freshly compiled shape. The caller holds t.mu exclusively,
// or is the only goroutine that can see t.
func (t *Table) install(c *compiled, cause string, wire, inputs []string, cols map[string]filterColumn, sig string,
	lib *chtypes.Library, discovered []discovery.Column, zone string,
) {
	t.c, t.cause = c, cause
	t.WireColumns, t.inputs, t.cols, t.sig = wire, inputs, cols, sig
	t.lib, t.discovered = lib, discovered
	t.zone, t.session = zone, sessionZone(zone)
}

// zoneOpts is the per-call zone of every filter create and every parse for
// this table, the one place either takes it from: chtypes declines a batch
// whose filter was created in another zone, so the two must never differ.
// Empty when the table's zone is the image zone.
func (t *Table) zoneOpts() []chtypes.SettingsOption {
	if t.session == "" {
		return nil
	}
	return []chtypes.SettingsOption{chtypes.WithSessionTimezone(t.session)}
}

// detached is what a table held before a swap or a close: closed once the
// table's own lock is released, so its holders never wait behind it.
type detached struct {
	c     *compiled
	roles *roleCache
}

// detachLocked takes the handle and the role projections off the table. The
// caller holds t.mu exclusively — so no request is using the detached handle
// — and closes the result after unlocking.
func (t *Table) detachLocked() detached {
	d := detached{c: t.c, roles: t.roles}
	t.c, t.roles = nil, nil
	return d
}

// close releases what d held. A role projection waits for its own readers.
func (d detached) close() {
	if d.roles != nil {
		d.roles.closeAll()
	}
	d.c.close()
}

// close tears every handle down, waiting for this shape's readers first; a
// later Table() reports cause.
func (t *Table) close(cause string) {
	t.mu.Lock()
	d := t.detachLocked()
	t.cause = cause
	t.mu.Unlock()
	d.close()
}

// signature is the column shape and zone a handle was compiled for. An
// unchanged signature keeps the handle and the generation, so a refresh that
// discovers nothing new costs no compiles and invalidates no cached filter; a
// zone change is a new generation, so no filter or role shape built under the
// old zone survives it.
func signature(ts *discovery.TableSchema, tz string) string {
	var b strings.Builder
	for _, c := range ts.Columns {
		fmt.Fprintf(&b, "%d\x1f%s\x1f%s\x1f%s\x1f%s\x1e",
			c.Position, c.Name, c.Type, c.DefaultKind, c.DefaultExpression)
	}
	b.WriteString(tz)
	return b.String()
}

// wireColumns is what the export emits, read off the compiled handle:
// declaration order minus the three kinds a positional INSERT never carries.
// MATERIALIZED and ALIAS are computed by the server; EPHEMERAL is insert-only
// and never stored. Taking it from the handle rather than from discovery makes
// the wire list a property of the handle that produced the bytes, which is
// what ParseRow's drift check actually wants.
func wireColumns(desc []chtypes.Column) []string {
	out := make([]string, 0, len(desc))
	for _, c := range desc {
		switch c.DefaultKind {
		case chtypes.KindMaterialized, chtypes.KindAlias, chtypes.KindEphemeral:
			// Never serialized by the export.
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

// WireColumnsOf is the wire column list of a discovered table — what a
// full-width envelope's Columns carry — computed from discovery alone, for a
// caller that holds no compiled handle (the stream's connect-time schema
// frame). It answered identically to the handle on every artifact measured.
func WireColumnsOf(ts *discovery.TableSchema) []string {
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

// inputColumns is the INSERT column list a name-addressed body (JSONEachRow,
// CSVWithNames, TSVWithNames) is parsed with: the wire columns plus every
// EPHEMERAL column a record may supply, in declaration order. nil when no
// EPHEMERAL column qualifies — the body is then parsed with no list, which
// reads exactly the wire columns. writable reports whether the role may
// supply a column; nil means every column.
//
// Listing an EPHEMERAL column is what lets a record supply it at all. With no
// list, ClickHouse's readers know only the stored columns and refuse it as an
// unknown field (117). Listed, its value is read and is in scope for the
// DEFAULT expressions over it, which chtypes computes and exports, while the
// value itself is never exported (measured on the 26.8 artifact). Positional
// CSV and TSV never get the list: their fields map to the wire columns by
// position, and an extra slot would shift every field after it.
//
// An EPHEMERAL column qualifies only when a DEFAULT expression reads it and
// no expression the server computes does:
//
//   - The worker inserts the exported row, so ClickHouse computes each
//     MATERIALIZED column itself, with every EPHEMERAL column at its own
//     default. A value one of those reads would be silently ignored, so the
//     column stays unlisted and a record naming it is refused instead.
//   - A DEFAULT is the only place an ephemeral value can go. And listing one
//     no DEFAULT reads makes the 26.8 artifact reject, with no code, every
//     record that omits it whenever the table computes any column (measured):
//     an unread column would cost every record its verdict to accept a value
//     that changes nothing.
func inputColumns(lib *chtypes.Library, cols []colDecl, wire []string, writable func(string) bool) []string {
	var ephemeral []string
	for _, c := range cols {
		if c.DefaultKind == "EPHEMERAL" && (writable == nil || writable(c.Name)) &&
			readBy(lib, cols, c.Name, "DEFAULT") && !readBy(lib, cols, c.Name, "MATERIALIZED", "ALIAS", "EPHEMERAL") {
			ephemeral = append(ephemeral, c.Name)
		}
	}
	if len(ephemeral) == 0 {
		return nil
	}
	listed := make(map[string]bool, len(wire)+len(ephemeral))
	for _, n := range wire {
		listed[n] = true
	}
	for _, n := range ephemeral {
		listed[n] = true
	}
	out := make([]string, 0, len(listed))
	for _, c := range cols {
		if listed[c.Name] {
			out = append(out, c.Name)
		}
	}
	return out
}

// readBy reports whether an expression of one of kinds reads column name,
// asking the compiler rather than parsing SQL in Go: the table is compiled
// without name, every other kind's expression dropped, so a reference to name
// fails the compile. Any refusal counts as a read. It compiles only when a
// column of those kinds has an expression at all.
func readBy(lib *chtypes.Library, cols []colDecl, name string, kinds ...string) bool {
	probe := make([]colDecl, 0, len(cols))
	reads := false
	for _, c := range cols {
		switch {
		case c.Name == name:
			continue
		case c.DefaultExpression == "":
		case slices.Contains(kinds, c.DefaultKind):
			reads = true
		case c.DefaultKind == "EPHEMERAL":
			c.DefaultExpression = "" // still EPHEMERAL, at its type's default
		default:
			c.DefaultKind, c.DefaultExpression = "", ""
		}
		probe = append(probe, c)
	}
	if !reads {
		return false
	}
	stmt, err := createTable(lib, probe)
	if err != nil {
		return true
	}
	s, err := lib.CompileTable(stmt, chtypes.WithSettings(compileSettings))
	if err != nil {
		return true
	}
	_ = s.Close()
	return false
}

// filterColumn is how render writes a predicate over one column.
type filterColumn struct {
	ident   string        // lib.QuoteIdentifier's spelling
	intType chsql.IntType // "" unless the claim binds through chsql.StrictInt
}

// declaredColumns maps every column name the compiled schema knows, of every
// kind, to its identifier as lib.QuoteIdentifier spells it (ClickHouse's own
// backQuote, always quoted) and, for an integer column, the type its claims
// are strictly cast to. It is the set render tests a predicate's column
// against: answering "no such column" here keeps a misspelled policy from
// costing a compile and a log line per generation. On a ROLE table a column
// the role may not write is declared too (MATERIALIZED, see roleColumns), so a
// check over it tests the value the server will store. Quoting once per
// compile keeps a C call off render's per-event path. A non-empty second
// return is the cause.
func declaredColumns(lib *chtypes.Library, desc []chtypes.Column) (map[string]filterColumn, string) {
	m := make(map[string]filterColumn, len(desc))
	for _, c := range desc {
		q, err := lib.QuoteIdentifier(c.Name)
		if err != nil {
			return nil, fmt.Sprintf("cannot quote column %q: %s", c.Name, err)
		}
		col := filterColumn{ident: q}
		if it, ok := chsql.IntegerType(c.Type); ok {
			col.intType = it
		}
		m[c.Name] = col
	}
	return m, ""
}
