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

	// tsSpec is a DateTime/DateTime64 column's canonicalization spec, resolved
	// once at schema build (Refresh). nil for non-timestamp columns, hand-built
	// Column literals, and unresolvable zones — CanonicalizeTimestamps passes
	// those through untouched (fail-open, #372).
	tsSpec *timestampSpec
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

// SchemaRegistry discovers and caches ClickHouse table schemas.
type SchemaRegistry struct {
	conn            driver.Conn
	database        string
	refreshInterval time.Duration
	// intervalFn, when set via SetIntervalSource, supplies the current
	// auto-refresh interval on each tick so a settings reload retunes the
	// cadence without restarting the loop.
	intervalFn func() time.Duration
	logger     *slog.Logger
	mu         sync.RWMutex
	tables     map[string]*TableSchema
}

// NewSchemaRegistry creates a registry that discovers schemas from system.columns.
func NewSchemaRegistry(conn driver.Conn, database string, refreshInterval time.Duration, logger *slog.Logger) *SchemaRegistry {
	return &SchemaRegistry{
		conn:            conn,
		database:        database,
		refreshInterval: refreshInterval,
		logger:          logger,
		tables:          make(map[string]*TableSchema),
	}
}

// Refresh rebuilds the in-memory schema cache: it discovers the server's default
// time zone, queries system.columns, and precomputes timestamp column specs.
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
	var serverTZ *time.Location
	if loc, err := loadLocation(tzName); err == nil {
		serverTZ = loc
	} else {
		// Unresolvable — warn, not fatal, and no UTC fallback (that could move
		// instants). A nil server zone means zone-less values pass through.
		sr.logger.Warn("cannot resolve server timezone; zone-less timestamps will pass through un-canonicalized",
			"timezone", tzName, "error", err)
	}

	rows, err := sr.conn.Query(ctx,
		`SELECT table, name, type, default_kind
		 FROM system.columns
		 WHERE database = ?
		   AND table NOT LIKE '.%'
		 ORDER BY table, position`,
		sr.database,
	)
	if err != nil {
		return fmt.Errorf("query system.columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tables := make(map[string]*TableSchema)
	for rows.Next() {
		var tableName, colName, colType, defaultKind string
		if err := rows.Scan(&tableName, &colName, &colType, &defaultKind); err != nil {
			return fmt.Errorf("scan column row: %w", err)
		}
		ts, ok := tables[tableName]
		if !ok {
			ts = &TableSchema{Name: tableName}
			tables[tableName] = ts
		}
		col := Column{
			Name:       colName,
			Type:       colType,
			IsNullable: isNullable(colType),
			HasDefault: defaultKind != "",
		}
		ts.Columns = append(ts.Columns, col)
	}
	// rows.Next() returns false on a mid-stream driver error too — check Err()
	// so a truncated scan fails the refresh (callers keep the prior cache and
	// retry) instead of publishing a partial registry.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate system.columns: %w", err)
	}

	for _, ts := range tables {
		resolveTimestampSpecs(ts, serverTZ, sr.logger)
	}

	sr.mu.Lock()
	sr.tables = tables
	sr.mu.Unlock()
	sr.logger.Info("schema registry refreshed", "tables", len(tables), "server_tz", tzName)

	return nil
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

// SetIntervalSource installs a dynamic source for the auto-refresh interval
// (settings.Store.SchemaRefreshInterval in production), polled after each tick.
// Call before StartAutoRefresh; the constructor's interval remains the
// fallback whenever fn is nil or returns a non-positive duration.
func (sr *SchemaRegistry) SetIntervalSource(fn func() time.Duration) {
	sr.intervalFn = fn
}

// currentInterval resolves the effective auto-refresh interval.
func (sr *SchemaRegistry) currentInterval() time.Duration {
	if sr.intervalFn != nil {
		if d := sr.intervalFn(); d > 0 {
			return d
		}
	}
	return sr.refreshInterval
}

// StartAutoRefresh runs a background goroutine that refreshes schemas
// at the configured interval. Blocks until ctx is cancelled. The interval is
// re-read after every tick, so a changed setting applies from the next cycle
// — an in-flight wait finishes at the old cadence rather than resetting,
// which keeps a reload from ever deferring an imminent refresh.
func (sr *SchemaRegistry) StartAutoRefresh(ctx context.Context) {
	interval := sr.currentInterval()
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
			if next := sr.currentInterval(); next != interval {
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
