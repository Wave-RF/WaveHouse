package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"go.opentelemetry.io/otel"
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

// SchemaRegistry discovers and caches ClickHouse table schemas.
type SchemaRegistry struct {
	conn driver.Conn
	// database supplies the database to discover from on each Refresh, so a
	// ClickHouse reconfigure that changes clickhouse.database is honored by
	// the next refresh (chconn.Manager.Database in production).
	database func() string
	// refreshInterval supplies the auto-refresh interval on each tick, so a
	// settings reload retunes the cadence without restarting the loop
	// (settings.Store.SchemaRefreshInterval in production).
	refreshInterval func() time.Duration
	logger          *slog.Logger
	mu              sync.RWMutex
	tables          map[string]*TableSchema
	// serverVersion is the ClickHouse version string from the last successful
	// Refresh, guarded by mu alongside tables.
	serverVersion string
}

// NewSchemaRegistry creates a registry that discovers schemas from system.columns.
func NewSchemaRegistry(conn driver.Conn, database func() string, refreshInterval func() time.Duration, logger *slog.Logger) *SchemaRegistry {
	return &SchemaRegistry{
		conn:            conn,
		database:        database,
		refreshInterval: refreshInterval,
		logger:          logger,
		tables:          make(map[string]*TableSchema),
	}
}

// Refresh rebuilds the in-memory schema cache: it discovers the server's default
// time zone and version, queries system.columns, attaches each table's DDL from
// system.tables, and precomputes timestamp column specs.
func (sr *SchemaRegistry) Refresh(ctx context.Context) error {
	tracer := otel.GetTracerProvider().Tracer("wavehouse-discovery")
	ctx, span := tracer.Start(ctx, "SchemaRegistry.Refresh")
	defer span.End()

	// ClickHouse interprets zone-less timestamp strings in the server's default
	// zone; canonicalization applies the same rule so the instant never changes (#372).
	var tzName string
	if err := sr.conn.QueryRow(ctx, "SELECT timezone()").Scan(&tzName); err != nil {
		return fmt.Errorf("query server timezone: %w", err)
	}
	// The server version is metadata about the schema source, probed on the same
	// refresh so a stale version cannot outlive the schemas it describes.
	//
	// It is NOT a same-server guarantee: chconn.Manager resolves the connection
	// per call, so a reload changing clickhouse.addr between this probe and the
	// system.columns query below would pair a version from one server with
	// schemas from another. Narrow, self-correcting on the next refresh, and
	// shared with the timezone probe above — but do not read this as atomic.
	//
	// Nor is the read side: ServerVersion() and Get()/List() take separate
	// RLocks, so a caller doing both across a refresh boundary pairs version N
	// with schemas N+1. They are published together; nothing reads them together.
	var serverVersion string
	if err := sr.conn.QueryRow(ctx, "SELECT version()").Scan(&serverVersion); err != nil {
		return fmt.Errorf("query server version: %w", err)
	}

	var serverTZ *time.Location
	if loc, err := loadLocation(tzName); err == nil {
		serverTZ = loc
	} else {
		// Unresolvable — warn, not fatal, and no UTC fallback (that could move
		// instants). A nil server zone means zone-less values pass through.
		sr.logger.Warn("cannot resolve server timezone; zone-less timestamps will pass through un-canonicalized",
			"timezone", tzName, "error", err)
	}

	// One database per refresh. `database` is a live getter so a ClickHouse
	// reconfigure is honored on the NEXT refresh — reading it twice would let a
	// reconfigure land between the system.columns and system.tables queries and
	// attach DDL from the new database to same-named schemas discovered from the
	// old one.
	database := sr.database()

	rows, err := sr.conn.Query(ctx,
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

	if err := sr.attachDDL(ctx, database, tables); err != nil {
		return err
	}

	for _, ts := range tables {
		resolveTimestampSpecs(ts, serverTZ, sr.logger)
		ts.cacheInsertable()
	}

	sr.mu.Lock()
	sr.tables = tables
	sr.serverVersion = serverVersion
	sr.mu.Unlock()
	sr.logger.Info("schema registry refreshed", "tables", len(tables), "server_tz", tzName, "server_version", serverVersion)

	return nil
}

// attachDDL fills each discovered table's DDL from system.tables. A table listed
// there but absent from tables — a view or table whose columns the system.columns
// scan didn't return, and one created between the two queries — is skipped rather
// than added: a TableSchema with no columns is not a schema, and the two queries
// are not a snapshot.
func (sr *SchemaRegistry) attachDDL(ctx context.Context, database string, tables map[string]*TableSchema) error {
	rows, err := sr.conn.Query(ctx,
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

// Get returns the schema for a table, or nil if not found.
func (sr *SchemaRegistry) Get(name string) *TableSchema {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return sr.tables[name]
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
// at the configured interval. Blocks until ctx is cancelled. The interval is
// re-read after every tick, so a changed setting applies from the next cycle
// — an in-flight wait finishes at the old cadence rather than resetting,
// which keeps a reload from ever deferring an imminent refresh.
func (sr *SchemaRegistry) StartAutoRefresh(ctx context.Context) {
	interval := sr.refreshInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sr.Refresh(ctx); err != nil {
				sr.logger.Error("schema auto-refresh failed", "error", err)
			}
			if next := sr.refreshInterval(); next != interval {
				interval = next
				ticker.Reset(interval)
			}
		}
	}
}

// isNullable checks if a ClickHouse type string is Nullable.
func isNullable(chType string) bool {
	return len(chType) > 9 && chType[:9] == "Nullable("
}
