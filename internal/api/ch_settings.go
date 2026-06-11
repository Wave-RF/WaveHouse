package api

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// QueryLimits holds the server-wide default resource caps applied to non-admin
// reads when a role sets no tighter cap of its own. It mirrors
// config.QueryLimits (translated in cmd/wavehouse) so the api package stays
// free of the config import. A zero field means "no server-imposed default" for
// that dimension. DefaultMaxRows is the structured-query result LIMIT applied
// when the caller and policy specify none (formerly the hard-coded
// query.DefaultMaxRows).
type QueryLimits struct {
	DefaultMaxRows        int
	DefaultMaxRowsToRead  int64
	DefaultMaxMemoryBytes int64
}

// resolveReadBudget returns the effective rows-scanned and memory caps for a
// read: the per-role override when set, else the server-wide default. Admin
// bypasses both — admins run heavy queries, and the structured LIMIT plus the
// unbounded /v1/admin/query path are their guardrails, not these DoS backstops.
// A returned 0 means "no cap" for that dimension. The pipe path has no per-role
// caps, so it passes 0 for both per-role values and gets the defaults.
func (d QueryLimits) resolveReadBudget(perRoleRowsToRead, perRoleMemory int64, isAdmin bool) (rowsToRead, memory int64) {
	if isAdmin {
		return 0, 0
	}
	rowsToRead = perRoleRowsToRead
	if rowsToRead == 0 {
		rowsToRead = d.DefaultMaxRowsToRead
	}
	memory = perRoleMemory
	if memory == 0 {
		memory = d.DefaultMaxMemoryBytes
	}
	return rowsToRead, memory
}

// chQueryLimits is the resolved, per-request resource budget a single read runs
// under. A zero field means "no limit" for that dimension and is omitted from
// the settings. The caller resolves these from the role's policy caps and the
// server-wide defaults (see resolveReadBudget).
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
