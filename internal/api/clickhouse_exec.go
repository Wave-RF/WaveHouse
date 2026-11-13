package api

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/pkg/chsql"
	"github.com/google/uuid"
)

// connOf is conn's answer for store — the tenant's pool — and nil for an
// unwired source or a tenant on no pool. The nil is untyped: a nil *Manager
// inside a non-nil driver.Conn would pass a nil check and panic on use.
func connOf(conn func(*settings.Store) driver.Conn, store *settings.Store) driver.Conn {
	if conn == nil {
		return nil
	}
	return conn(store)
}

// targetOf is target's answer for store, the zero Target for an unwired
// source.
func targetOf(target func(*settings.Store) chconn.Target, store *settings.Store) chconn.Target {
	if target == nil {
		return chconn.Target{}
	}
	return target(store)
}

// timeoutOf is timeout's answer for store, zero for an unwired source.
func timeoutOf(timeout func(*settings.Store) time.Duration, store *settings.Store) time.Duration {
	if timeout == nil {
		return 0
	}
	return timeout(store)
}

// executeCHQuery runs sql against the native-protocol driver conn,
// classifying by leading SQL verb to pick the Exec-vs-Query path —
// clickhouse-go's driver.Query() errors on statements that return no
// result set, so the dispatch is correctness, not optimisation. Returns
// a row-array suitable for JSON marshalling; mutations marshal to `[]`,
// preserving the "always-an-array" response shape callers depend on.
//
// Used by the structured-query and pipes handlers — those are the cached
// read paths that need explicit Query/Exec dispatch and per-row scanning.
// The raw-SQL endpoint (/v1/ops/query) proxies straight to ClickHouse
// over HTTP and never calls this; see internal/api/query.go.
func executeCHQuery(ctx context.Context, conn driver.Conn, sql string, params []any) ([]map[string]any, error) {
	ctx = chconn.WithTraceContext(ctx)
	if chsql.IsMutation(sql) {
		if err := conn.Exec(ctx, sql, params...); err != nil {
			return nil, fmt.Errorf("clickhouse exec: %w", err)
		}
		return []map[string]any{}, nil
	}

	rows, err := conn.Query(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columns := rows.ColumnTypes()
	// Initialize as empty (not nil) so a zero-row result marshals to `[]`,
	// not `null`. SDK consumers do `data!.length` on the response; a `null`
	// crashes the client on every empty fetch.
	results := []map[string]any{}

	for rows.Next() {
		valPtrs := make([]any, len(columns))
		for i, col := range columns {
			valPtrs[i] = reflect.New(col.ScanType()).Interface()
		}
		if err := rows.Scan(valPtrs...); err != nil {
			return nil, fmt.Errorf("scan clickhouse row: %w", err)
		}
		row := make(map[string]any)
		for i, col := range columns {
			row[col.Name()] = reflect.ValueOf(valPtrs[i]).Elem().Interface()
		}
		results = append(results, transformRow(row))
	}
	// rows.Next() returns false both when iteration completes successfully
	// AND when the driver hits an error mid-stream (network drop, decode
	// failure on a row past the first). Without this check, a partial
	// result set silently masquerades as a complete one.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clickhouse rows: %w", err)
	}
	return results, nil
}

// transformRow converts ClickHouse-specific types to JSON-friendly values.
func transformRow(row map[string]any) map[string]any {
	for k, v := range row {
		switch val := v.(type) {
		case uuid.UUID:
			row[k] = val.String()
		case [16]byte:
			row[k] = uuid.UUID(val).String()
		case time.Time:
			row[k] = val.UTC().Format(time.RFC3339Nano)
		case *time.Time:
			// Nullable(DateTime…) scans as a pointer; NULL stays nil (JSON null).
			if val != nil {
				row[k] = val.UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return row
}
