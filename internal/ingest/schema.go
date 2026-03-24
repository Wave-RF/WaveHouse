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
			tenant_id          UUID,
			event_id           UUID,
			received_timestamp DateTime64(3, 'UTC'),
			ingested_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
			timestamp          DateTime64(3, 'UTC'),
			table_name         String,
			str_data           Map(String, String),
			num_data           Map(String, Float64),
			bool_data          Map(String, Bool)
		) ENGINE = ReplacingMergeTree(ingested_timestamp)
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (tenant_id, table_name, toDate(timestamp), event_id)`
	if err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("ensure clickhouse schema: %w", err)
	}
	return nil
}
