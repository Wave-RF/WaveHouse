package api

import (
	"strconv"
	"time"
)

// chQueryLimits is the per-request resource budget a single read runs under,
// taken from the role's resolved policy caps. A zero field means "no limit" for
// that dimension and is omitted from the settings. Server-wide backstops are
// ClickHouse's job (settings profiles / quotas), not WaveHouse's — so an admin,
// whose policy resolves to no caps, sends no settings here and is bounded only
// by ClickHouse's own config.
type chQueryLimits struct {
	// ExecutionTime is the wall-clock budget, emitted as max_execution_time in
	// fractional seconds. Cancelling the HTTP request cannot interrupt a
	// server-side phase that has already started, so the bound has to reach
	// ClickHouse as a setting rather than only as a client deadline.
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

// chReadSettings builds the per-query ClickHouse settings that enforce a
// read's resource budget SERVER-SIDE, so it can't outrun the budget during a
// server-side scan / merge / aggregation phase (#316). Without these, the only
// budget reaching ClickHouse is the HTTP request's own deadline — which never
// bounds memory or rows scanned. Returns nil when no cap applies.
//
// The values are text because they ride on the ClickHouse HTTP interface's
// query string, but they are the numeric spellings ClickHouse expects:
// max_execution_time in fractional seconds, the rest as plain integers.
func chReadSettings(l chQueryLimits) map[string]string {
	settings := map[string]string{}
	if l.ExecutionTime > 0 {
		// Fractional seconds — ClickHouse accepts them, preserving a sub-second
		// cap that a whole-second representation would round away.
		settings["max_execution_time"] = strconv.FormatFloat(l.ExecutionTime.Seconds(), 'f', -1, 64)
	}
	if l.MaxResultRows > 0 {
		settings["max_result_rows"] = strconv.Itoa(l.MaxResultRows)
		settings["result_overflow_mode"] = "throw"
	}
	if l.MaxRowsToRead > 0 {
		settings["max_rows_to_read"] = strconv.FormatInt(l.MaxRowsToRead, 10)
		settings["read_overflow_mode"] = "throw"
	}
	if l.MaxMemoryBytes > 0 {
		settings["max_memory_usage"] = strconv.FormatInt(l.MaxMemoryBytes, 10)
	}
	if len(settings) == 0 {
		return nil
	}
	return settings
}
