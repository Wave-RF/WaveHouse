package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var (
	// ErrNotLoaded is Lookup's answer before the first successful Refresh:
	// the table may well exist, the registry just cannot say yet.
	ErrNotLoaded = errors.New("schema not loaded yet")
	// ErrUnknownTable is Lookup's answer for a table the loaded schema lacks.
	ErrUnknownTable = errors.New("unknown table")
	// ErrNoConnection is Refresh's answer when the connection getter yields
	// none: the tenant has no open ClickHouse pool.
	ErrNoConnection = errors.New("no ClickHouse connection")
)

// refreshFailures counts the refresh loops' failed attempts per tenant: a
// tenant's ClickHouse outage after its first discovery is this counter and a
// log line, never a probe failure.
var refreshFailures, _ = otel.Meter("wavehouse-discovery").Int64Counter(
	"wavehouse_schema_refresh_failures_total",
	metric.WithDescription("Failed schema refresh attempts of the boot retry and auto-refresh loops, per tenant"),
)

// Column describes a single ClickHouse column.
type Column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	IsNullable bool   `json:"is_nullable"`
	HasDefault bool   `json:"has_default"`
	// DefaultKind is how the column's default is declared, verbatim from
	// system.columns.default_kind: "" (none), "DEFAULT", "MATERIALIZED",
	// "ALIAS", or "EPHEMERAL". It is what decides whether a record may carry a
	// value for the column — see IsInsertable. HasDefault is the boolean
	// reading of the same field, so the two never disagree about whether a
	// default exists.
	DefaultKind string `json:"default_kind,omitempty"`
	// DefaultExpression is the column's DEFAULT/MATERIALIZED/ALIAS expression
	// as ClickHouse stores it (system.columns.default_expression), empty when
	// the column declares none.
	DefaultExpression string `json:"default_expression,omitempty"`
	// Position is the column's 1-based ordinal in the table's declaration order
	// (system.columns.position). Columns is already ordered by it; the field
	// carries the ordinal itself so a caller holding a lone Column still knows
	// where it sits.
	Position uint64 `json:"position"`

	// tsSpec is a DateTime/DateTime64 column's canonicalization spec, resolved
	// once at schema build (Refresh). nil for non-timestamp columns, hand-built
	// Column literals, and unresolvable zones — CanonicalizeTimestamps passes
	// those through untouched (fail-open, #372).
	tsSpec *timestampSpec
}

// TableSchema holds the discovered schema for one ClickHouse table. Columns is
// in declaration order (system.columns.position; see Refresh), which is the
// order positional row formats such as JSONCompactEachRow use.
type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	// DDL is the table's CREATE TABLE statement as ClickHouse renders it
	// (system.tables.create_table_query). Deliberately NOT serialized: the
	// schema HTTP endpoint marshals TableSchema straight to the client, and an
	// external-engine table (S3, MySQL, PostgreSQL, Kafka) renders its wiring
	// there unconditionally — endpoint, bucket or host, database, username, S3
	// access key id, ZooKeeper replica paths. The password specifically is
	// rendered `[HIDDEN]` by ClickHouse >= ~23.9 (verified on 26.7.3), so the
	// leak is the topology rather than the secret — except on an older server,
	// or one with `display_secrets_in_show_and_select` enabled, where the
	// password is in there too. Internal consumers read the field directly.
	// Empty when the table was discovered from system.columns but had gone from
	// system.tables by the time the second query ran — the two scans are not one
	// snapshot, so a consumer must not treat an empty DDL as "no such table".
	DDL string `json:"-"`

	// insertable/insertableNames memoize InsertableColumns/InsertableColumnNames,
	// which are per-table constants that the ingest path would otherwise rebuild
	// once per record — on a 20-column table that is ~2 KB of garbage per record.
	// Filled once by cacheInsertable when the registry builds the schema, and
	// never written again, so concurrent readers need no lock. A TableSchema
	// built as a literal (tests) leaves them nil and takes the uncached path.
	insertable      []Column
	insertableNames []string
}

// ColumnNames returns the table's column names in their discovered order
// (system.columns position; see Refresh). The query builder uses this to expand
// a SELECT * into a role's allowed projection, so the order is stable and
// matches the physical column order. Returns an empty (non-nil) slice for a
// schema with no columns.
func (ts *TableSchema) ColumnNames() []string { return columnNames(ts.Columns) }

// IsInsertable reports whether a record may carry a value for this column —
// whether naming it in an INSERT's column list is legal.
//
// ClickHouse refuses exactly two kinds, verified against a live server
// (26.6.3): a MATERIALIZED column is `Cannot insert column …, because it is
// MATERIALIZED column` (code 44, and insert_allow_materialized_columns
// defaults to 0), and an ALIAS column is `No such column …` (code 16) — it has
// no storage to write. Everything else takes a value: a plain column, a
// DEFAULT column, and an EPHEMERAL one, which exists precisely to be inserted
// into (it is insert-only — never stored, never selected).
func (c Column) IsInsertable() bool {
	switch c.DefaultKind {
	case "MATERIALIZED", "ALIAS":
		return false
	default:
		return true
	}
}

// InsertableColumns returns the columns a record may carry, in declaration
// order — the positional contract for a row on the ingest path. Computed
// columns are left out because naming one in an INSERT is an error, not
// because they are uninteresting: they stay in Columns, so the schema endpoint
// and the query path still see the whole table.
func (ts *TableSchema) InsertableColumns() []Column {
	if ts.insertable != nil {
		return ts.insertable
	}
	return computeInsertable(ts.Columns)
}

// cacheInsertable fills the memoized subsets. Called once per table per refresh,
// before the schema is published to readers.
func (ts *TableSchema) cacheInsertable() {
	ts.insertable = computeInsertable(ts.Columns)
	ts.insertableNames = columnNames(ts.insertable)
}

// computeInsertable filters to the columns a record may carry, preserving
// declaration order — the positional contract the wire envelope depends on.
func computeInsertable(cols []Column) []Column {
	out := make([]Column, 0, len(cols))
	for _, c := range cols {
		if c.IsInsertable() {
			out = append(out, c)
		}
	}
	// Cap the slice to its length. The memo is handed to every caller by
	// reference, and on a table with a computed column cap > len — so an append
	// by some future caller would write into the shared, concurrently-read
	// backing array instead of copying. Capping forces that append to allocate.
	return out[:len(out):len(out)]
}

// columnNames projects a column slice to its names, shared by ColumnNames and
// InsertableColumnNames so the two cannot drift.
func columnNames(cols []Column) []string {
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.Name)
	}
	return names
}

// Lookup returns the named column and whether the table declares it. Matching
// is exact, as ClickHouse's own column resolution is. Linear over Columns, which
// is the right shape for the per-record call sites: schemas are small and the
// caller asks about one or two columns.
//
// It returns the Column rather than a bool because "does the table have it" is
// rarely the whole question — a caller on the ingest path also has to know
// whether a record may carry a value for it (IsInsertable).
func (ts *TableSchema) Lookup(name string) (Column, bool) {
	for _, c := range ts.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// InsertableColumnNames is InsertableColumns reduced to names, for the wire
// envelope's column list. Returns an empty (non-nil) slice when none qualify.
func (ts *TableSchema) InsertableColumnNames() []string {
	if ts.insertableNames != nil {
		return ts.insertableNames
	}
	return columnNames(ts.InsertableColumns())
}

// SchemaRegistry discovers and caches one tenant's ClickHouse table schemas.
type SchemaRegistry struct {
	// source supplies the tenant's connection and the database to discover
	// from, read together once per Refresh, so a settings reload that moves
	// the tenant to another pool or database is honored by the next refresh
	// (the tenant's chconn.Pools entry in production, where a tenant moved
	// to another address or database gets a fresh registry instead).
	source Source
	// tenant is whose tables the registry discovers.
	tenant tenant.ID
	// refreshInterval supplies the tenant's auto-refresh interval on each
	// tick, so a settings reload retunes the cadence without restarting the
	// loop (settings.Store.SchemaRefreshInterval in production).
	refreshInterval func(tenant.ID) time.Duration
	// firstTick picks how long StartAutoRefresh waits before its first
	// refresh, within the interval; rand.N, substituted by tests.
	firstTick func(interval time.Duration) time.Duration
	// retryDelay picks how long RetryRefresh sleeps before its next attempt,
	// within the current backoff; rand.N, substituted by tests.
	retryDelay func(backoff time.Duration) time.Duration
	// loaded is set by the first successful Refresh and never cleared: the
	// line between "no schema known yet" and "this table is unknown".
	loaded atomic.Bool
	mu     sync.RWMutex
	tables map[string]*TableSchema
	// serverVersion is the ClickHouse version string from the last successful
	// Refresh, guarded by mu alongside tables.
	serverVersion string
	// serverTZ is the server's default time zone name from the last successful
	// Refresh, guarded by mu; kept to report a change, which moves how
	// zone-less timestamps are canonicalized.
	serverTZ string
}

// Source yields a tenant's connection and the database it discovers from,
// one snapshot: the database is the one the connection's own pool was
// opened for, so the schema discovered always describes the database the
// tenant's queries and inserts run against. A nil connection is a tenant
// with no open pool, which Refresh reports as ErrNoConnection.
type Source func() (driver.Conn, string)

// NewSchemaRegistry creates the registry of tenant id, which discovers
// schemas from system.columns over the connection and database source
// yields.
func NewSchemaRegistry(source Source, id tenant.ID, refreshInterval func(tenant.ID) time.Duration) *SchemaRegistry {
	return &SchemaRegistry{
		source:          source,
		tenant:          id,
		refreshInterval: refreshInterval,
		firstTick:       rand.N[time.Duration],
		retryDelay:      rand.N[time.Duration],
		tables:          make(map[string]*TableSchema),
	}
}

// Refresh rebuilds the in-memory schema cache: it discovers the server's default
// time zone and version, queries system.columns, attaches each table's DDL from
// system.tables, and precomputes timestamp column specs.
func (sr *SchemaRegistry) Refresh(ctx context.Context) error {
	return sr.refreshThen(ctx, nil)
}

// refreshThen hands a failure to onFail inside the still-open span, so its log
// line carries the span's trace_id (#899). A canceled ctx (shutdown, tenant
// dropped, caller gone) is not a failure: it is returned but neither recorded
// nor handed to onFail.
func (sr *SchemaRegistry) refreshThen(ctx context.Context, onFail func(context.Context, error)) error {
	tracer := otel.GetTracerProvider().Tracer("wavehouse-discovery")
	spanCtx, span := tracer.Start(ctx, "SchemaRegistry.Refresh",
		trace.WithAttributes(attribute.String(observability.TenantKey, string(sr.tenant))))
	defer span.End()

	err := sr.refresh(spanCtx)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return err
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	if onFail != nil {
		onFail(spanCtx, err)
	}
	return err
}

// refresh is Refresh's work, run in the span refreshThen opened.
func (sr *SchemaRegistry) refresh(ctx context.Context) error {
	// One connection and one database per refresh, read together: a reload
	// that moves the tenant to another pool or database applies to the NEXT
	// refresh, so every query of this one runs against the same server and
	// database — reading the database twice would let a reconfigure land
	// between the system.columns and system.tables queries and attach DDL
	// from the new database to same-named schemas discovered from the old
	// one. (The pool's own connection can still be swapped underneath
	// mid-refresh by a resize, which stays on the same server.)
	conn, database := sr.source()
	if conn == nil {
		return fmt.Errorf("%w for tenant %s", ErrNoConnection, sr.tenant)
	}
	// Every query of the refresh carries the refresh span's trace context.
	ctx = chconn.WithTraceContext(ctx)

	// ClickHouse interprets zone-less timestamp strings in the server's default
	// zone; canonicalization applies the same rule so the instant never changes (#372).
	var tzName string
	if err := conn.QueryRow(ctx, "SELECT timezone()").Scan(&tzName); err != nil {
		return fmt.Errorf("query server timezone: %w", err)
	}
	// The server version is metadata about the schema source, probed on the same
	// refresh so a stale version cannot outlive the schemas it describes.
	//
	// The read side is not atomic: ServerVersion() and Get()/List() take
	// separate RLocks, so a caller doing both across a refresh boundary pairs
	// version N with schemas N+1. They are published together; nothing reads
	// them together.
	var serverVersion string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&serverVersion); err != nil {
		return fmt.Errorf("query server version: %w", err)
	}

	var serverTZ *time.Location
	if loc, err := loadLocation(tzName); err == nil {
		serverTZ = loc
	} else {
		// Unresolvable — warn, not fatal, and no UTC fallback (that could move
		// instants). A nil server zone means zone-less values pass through.
		slog.WarnContext(ctx, "cannot resolve server timezone; zone-less timestamps will pass through un-canonicalized",
			"timezone", tzName, "error", err)
	}

	rows, err := conn.Query(ctx,
		`SELECT table, name, type, default_kind, default_expression, position
		 FROM system.columns
		 WHERE database = ?
		   AND table NOT LIKE '.%'
		 ORDER BY table, position`,
		database,
	)
	if err != nil {
		return fmt.Errorf("query system.columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tables := make(map[string]*TableSchema)
	for rows.Next() {
		var tableName, colName, colType, defaultKind, defaultExpr string
		var position uint64
		if err := rows.Scan(&tableName, &colName, &colType, &defaultKind, &defaultExpr, &position); err != nil {
			return fmt.Errorf("scan column row: %w", err)
		}
		ts, ok := tables[tableName]
		if !ok {
			ts = &TableSchema{Name: tableName}
			tables[tableName] = ts
		}
		col := Column{
			Name:              colName,
			Type:              colType,
			IsNullable:        isNullable(colType),
			HasDefault:        defaultKind != "",
			DefaultKind:       defaultKind,
			DefaultExpression: defaultExpr,
			Position:          position,
		}
		ts.Columns = append(ts.Columns, col)
	}
	// rows.Next() returns false on a mid-stream driver error too — check Err()
	// so a truncated scan fails the refresh (callers keep the prior cache and
	// retry) instead of publishing a partial registry.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate system.columns: %w", err)
	}

	if err := attachDDL(ctx, conn, database, tables); err != nil {
		return err
	}

	for _, ts := range tables {
		resolveTimestampSpecs(ctx, ts, serverTZ)
		ts.cacheInsertable()
	}

	sr.mu.Lock()
	prev, prevVersion, prevTZ := sr.tables, sr.serverVersion, sr.serverTZ
	sr.tables = tables
	sr.serverVersion = serverVersion
	sr.serverTZ = tzName
	sr.mu.Unlock()
	first := !sr.loaded.Swap(true)

	// A refresh that found nothing new runs once per tenant per interval on
	// every replica, so it logs at DEBUG; INFO is kept for the first load and
	// for a change, naming what changed (#888).
	attrs := []slog.Attr{
		slog.Any("tenant", sr.tenant), slog.Int("tables", len(tables)),
		slog.String("server_tz", tzName), slog.String("server_version", serverVersion),
	}
	level := slog.LevelDebug
	if first {
		level = slog.LevelInfo
	} else if added, removed, changed := schemaChanges(prev, tables); len(added)+len(removed)+len(changed) > 0 || serverVersion != prevVersion || tzName != prevTZ {
		level = slog.LevelInfo
		for _, a := range []struct {
			key   string
			names []string
		}{{"tables_added", added}, {"tables_removed", removed}, {"tables_changed", changed}} {
			if len(a.names) > 0 {
				attrs = append(attrs, slog.Any(a.key, a.names))
			}
		}
		if serverVersion != prevVersion {
			attrs = append(attrs, slog.String("previous_server_version", prevVersion))
		}
		if tzName != prevTZ {
			attrs = append(attrs, slog.String("previous_server_tz", prevTZ))
		}
	}
	slog.LogAttrs(ctx, level, "schema registry refreshed", attrs...)

	return nil
}

// schemaChanges names, each sorted, the tables next has that prev lacks, the
// tables prev had that next lacks, and the tables in both whose columns or DDL
// (a TTL, engine setting or index) differ.
func schemaChanges(prev, next map[string]*TableSchema) (added, removed, changed []string) {
	for name, ts := range next {
		old, ok := prev[name]
		switch {
		case !ok:
			added = append(added, name)
		case !slices.EqualFunc(old.Columns, ts.Columns, sameColumn),
			// An empty DDL is a table gone from system.tables between the two
			// scans (see TableSchema.DDL), not a change to it.
			old.DDL != "" && ts.DDL != "" && old.DDL != ts.DDL:
			changed = append(changed, name)
		}
	}
	for name := range prev {
		if _, ok := next[name]; !ok {
			removed = append(removed, name)
		}
	}
	slices.Sort(added)
	slices.Sort(removed)
	slices.Sort(changed)
	return added, removed, changed
}

// sameColumn compares the discovered fields. The derived ones follow from them
// and the server time zone, which Refresh compares on its own.
func sameColumn(a, b Column) bool {
	return a.Name == b.Name && a.Type == b.Type && a.DefaultKind == b.DefaultKind &&
		a.DefaultExpression == b.DefaultExpression && a.Position == b.Position
}

// attachDDL fills each discovered table's DDL from system.tables. A table listed
// there but absent from tables — a view or table whose columns the system.columns
// scan didn't return, and one created between the two queries — is skipped rather
// than added: a TableSchema with no columns is not a schema, and the two queries
// are not a snapshot.
func attachDDL(ctx context.Context, conn driver.Conn, database string, tables map[string]*TableSchema) error {
	rows, err := conn.Query(ctx,
		`SELECT name, create_table_query
		 FROM system.tables
		 WHERE database = ?
		   AND name NOT LIKE '.%'`,
		database,
	)
	if err != nil {
		return fmt.Errorf("query system.tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var name, ddl string
		if err := rows.Scan(&name, &ddl); err != nil {
			return fmt.Errorf("scan table row: %w", err)
		}
		if ts, ok := tables[name]; ok {
			ts.DDL = ddl
		}
	}
	// Same reasoning as the system.columns scan: a mid-stream driver error
	// leaves rows.Next() reporting false, so consult Err() rather than publish
	// a registry whose tables are missing their DDL.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate system.tables: %w", err)
	}
	return nil
}

// ServerVersion returns the ClickHouse version string captured by the last
// successful Refresh, or "" before the first one.
func (sr *SchemaRegistry) ServerVersion() string {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return sr.serverVersion
}

// Get returns the schema for a table, or nil if not found — before the first
// refresh as much as for a table the schema lacks, which is the fail-closed
// reading the stream hub wants. A handler that answers 404 uses Lookup.
func (sr *SchemaRegistry) Get(name string) *TableSchema {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return sr.tables[name]
}

// Loaded reports whether a Refresh has ever succeeded: until one has, the
// registry cannot tell an unknown table from one it has not seen yet.
func (sr *SchemaRegistry) Loaded() bool { return sr.loaded.Load() }

// Lookup is Get for a caller that answers the two misses differently:
// ErrNotLoaded before the first successful Refresh (the table may exist —
// a 503 with Retry-After, not a 404), ErrUnknownTable for a table the loaded
// schema lacks.
func (sr *SchemaRegistry) Lookup(name string) (*TableSchema, error) {
	if !sr.Loaded() {
		return nil, ErrNotLoaded
	}
	ts := sr.Get(name)
	if ts == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownTable, name)
	}
	return ts, nil
}

// List returns all discovered table schemas.
func (sr *SchemaRegistry) List() []*TableSchema {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	result := make([]*TableSchema, 0, len(sr.tables))
	for _, ts := range sr.tables {
		result = append(result, ts)
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
// diagnostic (e.g. via /livez) while the registry is still degraded. It runs
// under the failed attempt's span context, so a line it logs carries that
// attempt's trace_id.
//
// The first attempt fires immediately. After a failure the loop sleeps a
// uniformly random time below the backoff ("full jitter"), which starts at
// initialBackoff and doubles up to maxBackoff, so instances retrying against
// one recovering ClickHouse spread over the whole window rather than firing
// in lockstep (#141). Returns nil on success or ctx.Err() on cancellation.
// Zero/negative bounds are clamped via clampBackoff rather than busy-looping.
func (sr *SchemaRegistry) RetryRefresh(ctx context.Context, initialBackoff, maxBackoff time.Duration, onAttempt func(ctx context.Context, err error)) error {
	initialBackoff, maxBackoff = clampBackoff(initialBackoff, maxBackoff)
	backoff := initialBackoff
	for {
		// refreshThen skips canceled attempts, so a shutdown does not write
		// "schema discovery: context canceled" into BootState (/livez).
		err := sr.refreshThen(ctx, func(spanCtx context.Context, err error) {
			sr.countFailure(spanCtx)
			if onAttempt != nil {
				onAttempt(spanCtx, err)
			}
		})
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sr.retryDelay(backoff)):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// unresolvedRefreshInterval paces StartAutoRefresh while its tenant's interval
// cannot be read.
const unresolvedRefreshInterval = time.Minute

// StartAutoRefresh runs a background goroutine that refreshes schemas
// at the configured interval. Blocks until ctx is cancelled. The first
// refresh fires at a random point within the interval, so tenants adopted
// together do not refresh together; the cadence runs from there. The
// interval is re-read after every tick, so a changed setting applies from
// the next cycle — an in-flight wait finishes at the old cadence rather
// than resetting, which keeps a reload from ever deferring an imminent
// refresh.
//
// A validated setting is at least a second, so a non-positive interval is a
// tenant the settings registry could not resolve, read as the zero value.
// NewTicker and Reset panic on one, so the loop keeps the cadence it has —
// unresolvedRefreshInterval when it has none yet — and picks the setting up
// on the first tick that resolves.
func (sr *SchemaRegistry) StartAutoRefresh(ctx context.Context) {
	interval := sr.refreshInterval(sr.tenant)
	if interval <= 0 {
		interval = unresolvedRefreshInterval
	}
	first := time.NewTimer(sr.firstTick(interval))
	defer first.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-first.C:
			// The cadence starts from the first refresh, not from the loop.
			ticker.Reset(interval)
		case <-ticker.C:
		}
		_ = sr.refreshThen(ctx, func(ctx context.Context, err error) {
			sr.countFailure(ctx)
			slog.ErrorContext(ctx, "schema auto-refresh failed", "tenant", sr.tenant, "error", err)
		})
		if next := sr.refreshInterval(sr.tenant); next > 0 && next != interval {
			interval = next
			ticker.Reset(interval)
		}
	}
}

// countFailure bumps the tenant's refresh-failure counter.
func (sr *SchemaRegistry) countFailure(ctx context.Context) {
	refreshFailures.Add(ctx, 1, metric.WithAttributes(observability.TenantLabel(sr.tenant.String())))
}

// isNullable checks if a ClickHouse type string is Nullable.
func isNullable(chType string) bool {
	return len(chType) > 9 && chType[:9] == "Nullable("
}
