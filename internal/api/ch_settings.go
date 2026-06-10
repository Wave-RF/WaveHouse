package api

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// chReadSettings builds the per-query ClickHouse Settings that enforce a role's
// resource caps SERVER-SIDE, so a structured read can't outrun its policy
// budget during a server-side scan / merge / aggregation phase (#316).
//
// Without these, the only budget reaching ClickHouse is whatever clickhouse-go
// derives from the context deadline — and it only appends max_execution_time
// for deadlines > 1s, never bounds memory or rows scanned, and a context cancel
// only fires while the client goroutine is in the block-read loop (a long
// server-side aggregation runs to completion first). So a heavy aggregation can
// allocate GBs of state, or scan an entire table, well within the time budget.
//
// Mapping (a 0 cap means "no policy limit" and is omitted):
//
//   - MaxExecutionTimeMs → max_execution_time (fractional seconds of the
//     effective timeout). The driver already derives max_execution_time from
//     the context deadline for multi-second budgets, but SKIPS deadlines ≤ 1s,
//     so a sub-second policy cap would otherwise reach the server with no time
//     bound. Emitting it explicitly closes that hole; for >1s budgets the
//     driver overwrites it with deadline+5s, which is a fine backstop.
//   - MaxRows → max_result_rows + result_overflow_mode=throw. Defense-in-depth
//     behind the SQL LIMIT the builder already applied — a hard ceiling that
//     survives a future query-shape change. Never trips in practice because the
//     LIMIT keeps the result at or under the cap.
//   - MaxRowsToRead → max_rows_to_read + read_overflow_mode=throw (rows scanned
//     from storage; the lever that stops a full-table scan).
//   - MaxMemoryUsageBytes → max_memory_usage (peak per-query memory; the lever
//     that stops a heavy aggregation from exhausting the box).
//
// timeout is the effective execution budget the caller already computed (min of
// the role's max_execution_time_ms and the server query_timeout). Returns nil
// when the role sets no caps, so the caller can skip wrapping the context.
func chReadSettings(perms *policy.ResolvedPermissions, timeout time.Duration) clickhouse.Settings {
	if perms == nil {
		return nil
	}
	settings := clickhouse.Settings{}
	if perms.MaxExecutionTimeMs > 0 {
		// Fractional seconds — ClickHouse accepts them, and they preserve a
		// sub-second cap that a whole-second representation would round away.
		settings["max_execution_time"] = timeout.Seconds()
	}
	if perms.MaxRows > 0 {
		settings["max_result_rows"] = perms.MaxRows
		settings["result_overflow_mode"] = "throw"
	}
	if perms.MaxRowsToRead > 0 {
		settings["max_rows_to_read"] = perms.MaxRowsToRead
		settings["read_overflow_mode"] = "throw"
	}
	if perms.MaxMemoryUsageBytes > 0 {
		settings["max_memory_usage"] = perms.MaxMemoryUsageBytes
	}
	if len(settings) == 0 {
		return nil
	}
	return settings
}
