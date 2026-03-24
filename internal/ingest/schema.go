package ingest

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// EnsureSchema creates the events table in ClickHouse if it does not already exist.
// The column list must match the INSERT in BufferConsumer.insertBatch.
func EnsureSchema(ctx context.Context, conn driver.Conn) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS events (
			tenant_id   String,
			event_id    String,
			timestamp   DateTime64(3, 'UTC'),
			type        String,
			map_keys    Array(String),
			map_values  Array(String)
		) ENGINE = MergeTree()
		ORDER BY (tenant_id, timestamp, event_id)`
	if err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("ensure clickhouse schema: %w", err)
	}
	return nil
}
