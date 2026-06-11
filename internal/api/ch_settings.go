package api

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// chQueryLimits is the per-request resource budget a single read runs under,
// taken from the role's resolved policy caps. A zero field means "no limit" for
// that dimension and is omitted from the settings. Server-wide backstops are
// ClickHouse's job (settings profiles / quotas), not WaveHouse's — so an admin,
// whose policy resolves to no caps, sends no settings here and is bounded only
// by ClickHouse's own config.
type chQueryLimits struct {
	// ExecutionTime is the wall-clock budget, emitted as max_execution_time in
	// fractional seconds. clickhouse-go already derives max_execution_time from
	// the context deadline, but only for deadlines > 1s — so a sub-second cap
	// would otherwise reach the server with no time bound, and a context cancel
	// can't interrupt an already-running server-side phase. Emitting it
	// explicitly closes that hole; for >1s budgets the driver overwrites it with
	// deadline+5s, a fine backstop.
	ExecutionTime time.Duration
	// MaxResultRows caps rows RETURNED (max_result_rows + result_overflow_mode=
	// throw) — defense-in-depth behind the SQL LIMIT the structured builder
	// applies; not used on the pipe path (a pipe may legitimately return many).
	MaxResultRows int
	// MaxRowsToRead caps rows SCANNED from storage (max_rows_to_read +
	// read_overflow_mode=throw) — the lever that stops a full-table scan.
	MaxRowsToRead int64
	// MaxMemoryBytes caps peak query memory (max_memory_usage) — the lever that
	// stops a heavy aggregation from exhausting the box.
	MaxMemoryBytes int64
}

// chReadSettings builds the per-query ClickHouse Settings that enforce a read's
// resource budget SERVER-SIDE, so it can't outrun the budget during a
// server-side scan / merge / aggregation phase (#316). Without these, the only
// budget reaching ClickHouse is whatever clickhouse-go derives from the context
// deadline — which never bounds memory or rows scanned. Returns nil when no cap
// applies, so the caller can skip wrapping the context.
func chReadSettings(l chQueryLimits) clickhouse.Settings {
	settings := clickhouse.Settings{}
	if l.ExecutionTime > 0 {
		// Fractional seconds — ClickHouse accepts them, preserving a sub-second
		// cap that a whole-second representation would round away.
		settings["max_execution_time"] = l.ExecutionTime.Seconds()
	}
	if l.MaxResultRows > 0 {
		settings["max_result_rows"] = l.MaxResultRows
		settings["result_overflow_mode"] = "throw"
	}
	if l.MaxRowsToRead > 0 {
		settings["max_rows_to_read"] = l.MaxRowsToRead
		settings["read_overflow_mode"] = "throw"
	}
	if l.MaxMemoryBytes > 0 {
		settings["max_memory_usage"] = l.MaxMemoryBytes
	}
	if len(settings) == 0 {
		return nil
	}
	return settings
}
