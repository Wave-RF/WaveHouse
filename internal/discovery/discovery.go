package discovery

import (
	"context"
	"fmt"
	"io"
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
}

// TableSchema holds the discovered schema for one ClickHouse table.
type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// SchemaRegistry discovers and caches ClickHouse table schemas.
type SchemaRegistry struct {
	conn            driver.Conn
	database        string
	refreshInterval time.Duration
	logger          *slog.Logger
	mu              sync.RWMutex
	tables          map[string]*TableSchema
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

// Refresh queries system.columns and rebuilds the in-memory schema cache.
func (sr *SchemaRegistry) Refresh(ctx context.Context) error {

	tracer := otel.GetTracerProvider().Tracer("wavehouse-discovery")
    ctx, span := tracer.Start(ctx, "SchemaRegistry.Refresh")
    defer span.End()

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
	defer rows.Close()

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

	sr.mu.Lock()
	sr.tables = tables
	sr.mu.Unlock()
	sr.logger.Info("schema registry refreshed", "tables", len(tables))

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
	m := make(map[string]*TableSchema, len(tables))
	for _, t := range tables {
		m[t.Name] = t
	}
	return &SchemaRegistry{
		tables: m,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}
