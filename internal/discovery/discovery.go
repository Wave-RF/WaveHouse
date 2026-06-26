package discovery

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/chsql"

	"go.opentelemetry.io/otel"
)

// Column describes a single ClickHouse column.
type Column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	IsNullable bool   `json:"is_nullable"`
	HasDefault bool   `json:"has_default"`
}

// TableSchema holds the discovered schema for one ClickHouse table.
type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// ColumnNames returns the table's column names in their discovered order
// (system.columns position; see Refresh). The query builder uses this to expand
// a SELECT * into a role's allowed projection, so the order is stable and
// matches the physical column order. Returns an empty (non-nil) slice for a
// schema with no columns.
func (ts *TableSchema) ColumnNames() []string {
	names := make([]string, 0, len(ts.Columns))
	for _, c := range ts.Columns {
		names = append(names, c.Name)
	}
	return names
}

// tableInfo is everything the registry knows about ONE name in the database. Base
// tables and views share this one map (system.columns lists a view's columns too,
// so a view carries a schema as well as its view-only fields). One entry replaces
// what used to be five parallel maps. Rebuilt wholesale on every successful Refresh
// and guarded by SchemaRegistry.mu.
type tableInfo struct {
	schema   *TableSchema // columns in physical order; drives Get/List/ColumnNames
	isView   bool         // engine is a View family — a view, which ingest never writes
	sources  []string     // a view's immediate source tables (nil for a base table)
	asSelect string       // a view's SELECT text — diffed across refreshes to catch a redefinition
	foldable bool         // derived: a view that flattens cleanly to known base tables
}

// SchemaRegistry discovers and caches ClickHouse table schemas, and derives the
// pipe-cache dependency tree from them.
type SchemaRegistry struct {
	conn            driver.Conn
	database        string
	refreshInterval time.Duration
	logger          *slog.Logger
	mu              sync.RWMutex
	// tables maps every name in the configured database — base tables AND views —
	// to its schema, view-ness, sources, definition, and derived foldability (see
	// tableInfo). The single source of per-name truth; rebuilt and swapped atomically
	// on each successful Refresh. Guarded by mu.
	tables map[string]*tableInfo
	// cascade maps a base table to the dependent views a write to it must ALSO
	// invalidate, both NATS-encoded to match the read/write sides. It is the
	// precomputed transitive reverse of the view->source edges: the graph walk
	// happens once at refresh (buildDeps), never per request. Pushed into the cache
	// via onRefresh so Cache.Invalidate can fan a base-table bump out to its views.
	// Guarded by mu.
	cascade map[string][]string
	// onRefresh, if set, is invoked after every CONTENT-CHANGED refresh with the new
	// dependency snapshot, so the cache can install the updated cascade and bump any
	// redefined views. Set once via SetOnRefresh. Guarded by mu.
	onRefresh func(DependencySnapshot)
	// metaHash fingerprints the CHEAP schema signals — columns, view-ness, view
	// definition text, and reverse dependencies_table edges — read WITHOUT EXPLAIN.
	// Each view's EXPLAIN resolution is a deterministic function of these, so when
	// metaHash is unchanged (and the last resolve fully succeeded) Refresh skips the
	// per-view EXPLAINs entirely and keeps the last-good tree. Guarded by mu.
	metaHash uint64
	// contentHash fingerprints the FULL resolved tree (metaHash's inputs plus the
	// EXPLAIN-resolved source edges). onRefresh fires only when it changes, so a
	// refresh resolving to the same tree neither re-pushes the cascade nor bumps
	// anything. hasRefreshed distinguishes the first refresh from a steady-state one.
	// Guarded by mu.
	contentHash uint64
	// lastResolveOK is false when the previous refresh's EXPLAIN pass had any failure
	// (a broken view, or ClickHouse dropping mid-refresh); it forces the next refresh
	// to re-run the EXPLAINs even when metaHash is unchanged, so a transient failure
	// self-heals instead of freezing missing edges behind the hash. Guarded by mu.
	lastResolveOK bool
	hasRefreshed  bool
}

// DependencySnapshot is the per-refresh hand-off from the schema registry to the
// cache. Cascade is the full base-table -> dependent-views map (NATS-encoded) to
// install; ChangedViews are the (NATS-encoded) views whose definition changed this
// refresh and must be invalidated directly (a redefinition with the same sources
// changes results but no base-table write would signal it). Both are ready to use
// as-is — the cache neither parses nor re-encodes them.
type DependencySnapshot struct {
	Cascade      map[string][]string
	ChangedViews []string
}

// NewSchemaRegistry creates a registry that discovers schemas from system.columns.
func NewSchemaRegistry(conn driver.Conn, database string, refreshInterval time.Duration, logger *slog.Logger) *SchemaRegistry {
	return &SchemaRegistry{
		conn:            conn,
		database:        database,
		refreshInterval: refreshInterval,
		logger:          logger,
		tables:          make(map[string]*tableInfo),
	}
}

// Refresh re-reads the schema from ClickHouse, recomputes the derived foldable/
// cascade sets, and atomically swaps the in-memory caches. It runs in two passes so
// the expensive work is gated on change:
//
//   - Pass 1 reads the CHEAP signals (system.columns, plus system.tables metadata:
//     view-ness, definitions, reverse dependency edges) and fingerprints them
//     (metaHash). If nothing moved since the last refresh — and that refresh fully
//     resolved — Refresh stops here: no per-view EXPLAINs, no cascade rebuild, no
//     onRefresh. This is the steady-state path.
//   - Pass 2 runs only on a change: it resolves each view's sources via EXPLAIN (the
//     per-view round-trips), rebuilds the cascade, swaps the tree, and — when the
//     resolved tree actually differs — notifies onRefresh so the cache installs the
//     new cascade and bumps any redefined views.
//
// It is ALL-OR-NOTHING: a read failure aborts with nothing swapped, leaving the
// last-good tree intact. A pass-2 EXPLAIN failure (a broken view, or ClickHouse
// dropping mid-refresh) is recorded (lastResolveOK) so the next refresh re-resolves
// even when the cheap signals are unchanged, rather than freezing missing edges.
func (sr *SchemaRegistry) Refresh(ctx context.Context) error {
	tracer := otel.GetTracerProvider().Tracer("wavehouse-discovery")
	ctx, span := tracer.Start(ctx, "SchemaRegistry.Refresh")
	defer span.End()

	infos, err := sr.discoverColumns(ctx)
	if err != nil {
		return err
	}
	// Pass 1 (cheap, no EXPLAIN): view-ness, definitions, and reverse dependency
	// edges — enough to fingerprint the schema and decide whether anything moved.
	viewDefs, err := sr.discoverViewMeta(ctx, infos)
	if err != nil {
		// Atomic: a metadata-read failure aborts the whole refresh with nothing
		// swapped, so the previous good tree stays in effect. Retried next tick.
		return err
	}

	// Each view's source edges are a deterministic function of the definitions and
	// column schema just read, so metaHash captures every input to the EXPLAIN step.
	// If it's unchanged AND the last resolve fully succeeded, the EXPLAIN results
	// would be identical — skip the per-view round-trips and keep the last-good tree.
	metaHash := contentHash(infos)
	sr.mu.RLock()
	skip := sr.hasRefreshed && metaHash == sr.metaHash && sr.lastResolveOK
	sr.mu.RUnlock()
	if skip {
		sr.logger.Info("schema registry refreshed", "tables", len(infos), "changed", false)
		return nil
	}

	// Pass 2 (expensive): something changed, or a prior EXPLAIN failed — resolve the
	// view source edges via EXPLAIN and rebuild the derived sets.
	resolveOK := sr.resolveViewSources(ctx, infos, viewDefs)
	treeHash := contentHash(infos) // now folds in the EXPLAIN-resolved edges
	cascade := buildDeps(infos)    // sets foldable on each view; returns the cascade
	sr.mu.Lock()
	changed := !sr.hasRefreshed || treeHash != sr.contentHash
	// changedViews drives direct eviction of views redefined in place (and the
	// downstream views built on them) — staleness no base-table write would catch.
	var changedViews []string
	if sr.hasRefreshed {
		changedViews = computeChangedViews(sr.tables, infos)
	}
	sr.tables = infos
	sr.cascade = cascade
	sr.metaHash = metaHash
	sr.contentHash = treeHash
	sr.lastResolveOK = resolveOK
	sr.hasRefreshed = true
	onRefresh := sr.onRefresh
	sr.mu.Unlock()

	if changed && onRefresh != nil {
		// Fire outside the lock: the callback reaches into the cache, and must not
		// be able to deadlock against a concurrent reader holding sr.mu.RLock.
		onRefresh(DependencySnapshot{Cascade: cascade, ChangedViews: changedViews})
	}
	sr.logger.Info("schema registry refreshed", "tables", len(infos), "changed", changed)
	return nil
}

// buildDeps walks the view->source graph ONCE — the only place it is walked, done
// here at refresh so neither read nor write does it per call. It marks each view
// foldable (flattens cleanly to known base tables) directly on its tableInfo, and
// returns the write-side cascade: each base table mapped to the foldable views
// reading it (transitively), NATS-encoded to match the worker (write) and handler
// (read) sides. Only sets the foldable bit on the entries; no lock, no query.
func buildDeps(tables map[string]*tableInfo) map[string][]string {
	cascade := make(map[string][]string)

	// flatten returns the base tables `name` transitively reads and whether that
	// flatten is COMPLETE (every leaf is a real base table). A view recurses into its
	// sources; a base table is itself; a view with no edges (unparsed), an edge-only
	// entry with no schema, or an unknown name is incomplete. path is the cycle guard.
	var flatten func(name string, path map[string]struct{}) ([]string, bool)
	flatten = func(name string, path map[string]struct{}) ([]string, bool) {
		if _, cycle := path[name]; cycle {
			return nil, false
		}
		t := tables[name]
		if t == nil {
			return nil, false // unknown name
		}
		if t.isView {
			if len(t.sources) == 0 {
				return nil, false // a view we couldn't map to sources
			}
			path[name] = struct{}{}
			defer delete(path, name)
			baseSet := make(map[string]struct{})
			complete := true
			for _, s := range t.sources {
				sb, sok := flatten(s, path)
				if !sok {
					complete = false
				}
				for _, b := range sb {
					baseSet[b] = struct{}{}
				}
			}
			bases := make([]string, 0, len(baseSet))
			for b := range baseSet {
				bases = append(bases, b)
			}
			return bases, complete
		}
		if t.schema != nil {
			return []string{name}, true // a real base table ingest writes
		}
		return nil, false // tracked only as an edge target — no schema, not a base table
	}

	for name, t := range tables {
		if !t.isView {
			continue
		}
		bases, complete := flatten(name, make(map[string]struct{}))
		if !complete {
			continue // unfoldable: IsKnown reports it not-known so the caller TTL-floors
		}
		t.foldable = true
		ev := chsql.SafeEncodeNATS(name)
		for _, b := range bases {
			eb := chsql.SafeEncodeNATS(b)
			cascade[eb] = append(cascade[eb], ev)
		}
	}
	for b := range cascade {
		slices.Sort(cascade[b])
		cascade[b] = slices.Compact(cascade[b])
	}
	return cascade
}

// computeChangedViews returns the NATS-encoded views to evict after a refresh: the
// views whose definition (as_select) changed in place since oldTables, PLUS the
// downstream closure of views that transitively read them. A redefined view's
// readers produce stale results too, but their own bodies are unchanged and the
// base-keyed cascade can't reach them — so the redefinition must fan out here.
// Returns nil when nothing changed. `changed` doubles as the visited set, so a
// view cycle terminates. A newly-discovered downstream reader is harmless (nothing
// folds its version yet), so the closure needs no present-before filter.
func computeChangedViews(oldTables, newInfos map[string]*tableInfo) []string {
	changed := map[string]struct{}{}
	for name, t := range newInfos {
		if t.asSelect == "" {
			continue // only a view has a definition to compare
		}
		if old := oldTables[name]; old != nil && old.asSelect != "" && old.asSelect != t.asSelect {
			changed[name] = struct{}{}
		}
	}
	if len(changed) == 0 {
		return nil
	}

	readers := map[string][]string{} // name -> views that DIRECTLY read it
	for name, t := range newInfos {
		if t.isView {
			for _, s := range t.sources {
				readers[s] = append(readers[s], name)
			}
		}
	}
	queue := make([]string, 0, len(changed))
	for n := range changed {
		queue = append(queue, n)
	}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, r := range readers[n] {
			if _, seen := changed[r]; seen {
				continue
			}
			changed[r] = struct{}{}
			queue = append(queue, r)
		}
	}

	out := make([]string, 0, len(changed))
	for name := range changed {
		out = append(out, chsql.SafeEncodeNATS(name))
	}
	slices.Sort(out)
	return out
}

// discoverColumns reads system.columns and builds the per-name map with schemas
// populated (views included — system.columns lists their columns too).
func (sr *SchemaRegistry) discoverColumns(ctx context.Context) (map[string]*tableInfo, error) {
	rows, err := sr.conn.Query(
		ctx,
		`SELECT table, name, type, default_kind
		 FROM system.columns
		 WHERE database = ?
		   AND table NOT LIKE '.%'
		 ORDER BY table, position`,
		sr.database,
	)
	if err != nil {
		return nil, fmt.Errorf("query system.columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	infos := make(map[string]*tableInfo)
	for rows.Next() {
		var tableName, colName, colType, defaultKind string
		if err := rows.Scan(&tableName, &colName, &colType, &defaultKind); err != nil {
			return nil, fmt.Errorf("scan column row: %w", err)
		}
		t, ok := infos[tableName]
		if !ok {
			t = &tableInfo{schema: &TableSchema{Name: tableName}}
			infos[tableName] = t
		}
		t.schema.Columns = append(t.schema.Columns, Column{
			Name:       colName,
			Type:       colType,
			IsNullable: isNullable(colType),
			HasDefault: defaultKind != "",
		})
	}
	return infos, rows.Err()
}

// viewDef pairs a view's name with its SELECT text, carried from pass 1
// (discoverViewMeta) to pass 2 (resolveViewSources) for EXPLAIN resolution.
type viewDef struct{ name, asSelect string }

// discoverViewMeta is Refresh's PASS 1: it reads the CHEAP view signals from
// system.tables — view-ness, the SELECT definition, and the reverse
// dependencies_table edges (a source -> its attached MV) — and returns the view
// definitions for pass 2 to resolve. No EXPLAIN runs here, so Refresh can fingerprint
// the schema and skip pass 2 entirely when nothing changed.
func (sr *SchemaRegistry) discoverViewMeta(ctx context.Context, infos map[string]*tableInfo) ([]viewDef, error) {
	// get returns the entry for name, creating a schema-less one if the name appeared
	// only as an edge target (so a view/edge is never silently dropped; a schema-less
	// entry is never treated as a base table — see buildDeps/IsKnown).
	get := func(name string) *tableInfo {
		t := infos[name]
		if t == nil {
			t = &tableInfo{}
			infos[name] = t
		}
		return t
	}

	var toResolve []viewDef
	rows, err := sr.conn.Query(ctx,
		"SELECT name, engine, as_select, dependencies_table FROM system.tables WHERE database = ?",
		sr.database)
	if err != nil {
		return nil, fmt.Errorf("query system.tables: %w", err)
	}
	for rows.Next() {
		var name, engine, asSelect string
		var dependents []string
		if err := rows.Scan(&name, &engine, &asSelect, &dependents); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan table row: %w", err)
		}
		t := get(name)
		if strings.Contains(engine, "View") {
			t.isView = true
		}
		if asSelect != "" {
			t.asSelect = asSelect
			toResolve = append(toResolve, viewDef{name, asSelect})
		}
		// Reverse edge: this row is a SOURCE table; each dependent is an attached MV.
		for _, d := range dependents {
			get(d).sources = append(get(d).sources, name)
		}
	}
	rerr := rows.Err()
	_ = rows.Close()
	if rerr != nil {
		return nil, rerr
	}
	return toResolve, nil
}

// resolveViewSources is Refresh's PASS 2 — the expensive one, run only when pass 1's
// cheap fingerprint changed. It asks ClickHouse (EXPLAIN QUERY TREE, see
// ResolveTables) for each view's source tables, unions them with the reverse edges
// pass 1 recorded, and de-dups. A view whose definition can't be resolved (it reads a
// table function, a cross-database table) simply gets no forward edge — a pipe reading
// it then over-resolves or goes stale per the documented rules, never a silent wrong
// answer. It returns false if ANY EXPLAIN errored (a broken view, or ClickHouse
// dropping mid-refresh) so Refresh re-resolves next tick instead of trusting a partial
// tree behind an unchanged metaHash.
func (sr *SchemaRegistry) resolveViewSources(ctx context.Context, infos map[string]*tableInfo, toResolve []viewDef) bool {
	allResolved := true
	for _, v := range toResolve {
		srcs, perr := ResolveTables(ctx, sr.conn, sr.database, v.asSelect)
		if perr != nil {
			sr.logger.Debug("view definition not resolvable via EXPLAIN; relying on dependencies_table", "view", v.name, "error", perr)
			allResolved = false
			continue
		}
		infos[v.name].sources = append(infos[v.name].sources, srcs...)
	}

	for _, t := range infos {
		if len(t.sources) > 1 {
			slices.Sort(t.sources)
			t.sources = slices.Compact(t.sources)
		}
	}
	return allResolved
}

// AllBaseTables returns the name of every base table (not a view) — the tables
// ClickHouse writes go to. It is the over-resolve fallback: a pipe whose exact
// tables can't be resolved is treated as depending on all of them, so any write
// evicts it. Coarse, but never a stale read.
func (sr *SchemaRegistry) AllBaseTables() []string {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	out := make([]string, 0, len(sr.tables))
	for name, t := range sr.tables {
		if !t.isView && t.schema != nil {
			out = append(out, name)
		}
	}
	return out
}

// Database returns the configured ClickHouse database this registry tracks.
func (sr *SchemaRegistry) Database() string { return sr.database }

// contentHash fingerprints everything dependency resolution derives from the schema:
// per name, its columns, view-ness, source edges, and definition. Names are sorted
// so the hash is stable regardless of map iteration order (an unsorted hash would
// flap and re-fire onRefresh every refresh). Conservative by construction — a false
// "changed" only costs a recompute; a real change can never present as "unchanged"
// because every relevant input is folded in.
func contentHash(infos map[string]*tableInfo) uint64 {
	h := fnv.New64a()
	write := func(s string) { _, _ = h.Write([]byte(s)); _, _ = h.Write([]byte{0}) }

	names := make([]string, 0, len(infos))
	for n := range infos {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		t := infos[n]
		write("n")
		write(n)
		if t.schema != nil {
			for _, c := range t.schema.Columns {
				write(c.Name)
				write(c.Type)
				if c.HasDefault {
					write("d")
				}
			}
		}
		if t.isView {
			write("v")
		}
		// Sort/de-dup a copy before hashing so the fingerprint is order-independent:
		// pass 1 leaves reverse edges in system.tables row order, which would
		// otherwise flap metaHash between refreshes (pass 2's edges are already sorted).
		srcs := slices.Clone(t.sources)
		slices.Sort(srcs)
		for _, s := range slices.Compact(srcs) {
			write("s")
			write(s)
		}
		if t.asSelect != "" {
			write("q")
			write(t.asSelect)
		}
	}
	return h.Sum64()
}

// IsKnown reports whether name is SAFE to fold directly into a cache key — i.e.
// its version is reliably maintained on writes. That is true for a real base
// table (ingest writes it) and for a foldable view (a write to a source bumps it
// via the cascade), but NOT for an unfoldable view (unparsed definition, or a
// cross-database/unknown source) nor an unknown name: the caller treats those as
// unresolved and TTL-floors the result rather than trust an unmaintained version.
// Pure in-memory lookup over the map built during Refresh — no query.
func (sr *SchemaRegistry) IsKnown(name string) bool {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	t := sr.tables[name]
	if t == nil {
		return false // unknown name
	}
	if t.isView {
		return t.foldable // a view: safe only if it flattens cleanly to base tables
	}
	return t.schema != nil // a real base table ingest writes
}

// Dependents returns a copy of the current cascade: each (NATS-encoded) base table
// mapped to the (NATS-encoded) views a write to it must also invalidate. The cache
// installs this so a base-table bump fans out to the views reading it. main pushes
// it through onRefresh; an out-of-band caller (e.g. a test wiring its own cache)
// can pull the current value directly.
func (sr *SchemaRegistry) Dependents() map[string][]string {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	out := make(map[string][]string, len(sr.cascade))
	for k, v := range sr.cascade {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// SetOnRefresh registers the callback invoked after every content-changed Refresh
// with the new dependency snapshot. Set once at wiring time, before the periodic
// refresh loop starts, so the cache stays in lock-step with the schema.
func (sr *SchemaRegistry) SetOnRefresh(fn func(DependencySnapshot)) {
	sr.mu.Lock()
	sr.onRefresh = fn
	sr.mu.Unlock()
}

// Get returns the schema for a table, or nil if not found.
func (sr *SchemaRegistry) Get(name string) *TableSchema {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	if t := sr.tables[name]; t != nil {
		return t.schema
	}
	return nil
}

// List returns all discovered table schemas.
func (sr *SchemaRegistry) List() []*TableSchema {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	result := make([]*TableSchema, 0, len(sr.tables))
	for _, t := range sr.tables {
		if t.schema != nil {
			result = append(result, t.schema)
		}
	}
	return result
}

// clampBackoff guards RetryRefresh against busy-looping when callers pass
// zero/negative durations. Any non-positive initialBackoff falls back to 1s,
// and maxBackoff is widened to at least initialBackoff. Extracted so the
// clamp behaviour is unit-testable without observing real wall-clock sleeps.
func clampBackoff(initialBackoff, maxBackoff time.Duration) (time.Duration, time.Duration) {
	if initialBackoff <= 0 {
		initialBackoff = time.Second
	}
	if maxBackoff < initialBackoff {
		maxBackoff = initialBackoff
	}
	return initialBackoff, maxBackoff
}

// RetryRefresh repeatedly calls Refresh with exponential backoff until it
// succeeds or ctx is cancelled. onAttempt is invoked after each failed
// attempt with the resulting error, letting callers surface the latest
// diagnostic (e.g. via /livez) while the registry is still degraded.
//
// The first attempt fires immediately. After a failure the loop sleeps for
// initialBackoff, then doubles up to maxBackoff between attempts. Returns
// nil on success or ctx.Err() on cancellation. Zero/negative bounds are
// clamped via clampBackoff rather than busy-looping.
func (sr *SchemaRegistry) RetryRefresh(ctx context.Context, initialBackoff, maxBackoff time.Duration, onAttempt func(err error)) error {
	initialBackoff, maxBackoff = clampBackoff(initialBackoff, maxBackoff)
	backoff := initialBackoff
	for {
		if err := sr.Refresh(ctx); err == nil {
			return nil
		} else if ctx.Err() == nil && onAttempt != nil {
			// Skip the callback when Refresh's error is just a downstream
			// reflection of ctx cancellation — that's a shutdown signal,
			// not a real diagnostic. Without this guard, a clean shutdown
			// fires onAttempt with `context.Canceled`, which the wavehouse
			// boot path then writes into BootState as
			//   "schema discovery: context canceled"
			// — visible to anyone curl'ing /livez during the shutdown
			// window. Not wrong, just noise.
			onAttempt(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// StartAutoRefresh runs a background goroutine that refreshes schemas
// at the configured interval. Blocks until ctx is cancelled.
func (sr *SchemaRegistry) StartAutoRefresh(ctx context.Context) {
	ticker := time.NewTicker(sr.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sr.Refresh(ctx); err != nil {
				sr.logger.Error("schema auto-refresh failed", "error", err)
			}
		}
	}
}

// isNullable checks if a ClickHouse type string is Nullable.
func isNullable(chType string) bool {
	return len(chType) > 9 && chType[:9] == "Nullable("
}

// NewSchemaRegistryFromMap creates a SchemaRegistry pre-loaded with the given
// table schemas. Intended for testing — no ClickHouse connection is required.
func NewSchemaRegistryFromMap(tables []*TableSchema) *SchemaRegistry {
	m := make(map[string]*tableInfo, len(tables))
	for _, t := range tables {
		m[t.Name] = &tableInfo{schema: t}
	}
	return &SchemaRegistry{
		tables: m,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// NewSchemaRegistryForTest creates a registry pre-loaded with table schemas AND
// view->source edges, with the foldable/cascade sets derived from them, for testing
// dependency resolution without a ClickHouse connection. Marked as already
// refreshed, mimicking a once-refreshed registry.
func NewSchemaRegistryForTest(tables []*TableSchema, viewSources map[string][]string) *SchemaRegistry {
	sr := NewSchemaRegistryFromMap(tables)
	for name, srcs := range viewSources {
		t := sr.tables[name]
		if t == nil {
			t = &tableInfo{}
			sr.tables[name] = t
		}
		t.isView = true
		t.sources = srcs
	}
	sr.cascade = buildDeps(sr.tables)
	sr.hasRefreshed = true
	return sr
}
