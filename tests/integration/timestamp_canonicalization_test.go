//go:build integration

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// TestTimestampCanonicalization_DifferentialAgainstClickHouse pins the #372
// invariant with ClickHouse itself as the oracle (PR #402 review): for every
// corpus input × timestamp column shape, inserting the raw producer value and
// inserting CanonicalizeTimestamps' output must both succeed or both fail, and
// when both succeed store the same instant — anything else means the rewrite
// changed what ClickHouse stores or accepts. Inserts go through the worker's
// exact HTTP surface (JSONEachRow, date_time_input_format=best_effort), so the
// oracle is the production parse, not a lookalike.
func TestTimestampCanonicalization_DifferentialAgainstClickHouse(t *testing.T) {
	colTypes := []struct {
		name string
		ddl  string
	}{
		{"datetime", "DateTime"},
		{"datetime_nyc", "DateTime('America/New_York')"},
		{"datetime64_3", "DateTime64(3)"},
		{"datetime64_6_tokyo", "DateTime64(6, 'Asia/Tokyo')"},
		// Precision 9 has its own ceiling (Int64 nanoseconds end 2262-04-11, past
		// which an insert fails outright instead of saturating), and precision 0
		// pins the DateTime64-kind rules on a second-granular column.
		{"datetime64_9", "DateTime64(9)"},
		{"datetime64_0_utc", "DateTime64(0, 'UTC')"},
	}

	// One entry per spelling family, each found or pinned by differentially
	// fuzzing the canonicalizer against a live ClickHouse (PR #402 review). The
	// digit runs, number shapes, out-of-range instants, and garbage document the
	// pass-through side: they must reach ClickHouse verbatim and get its
	// verdict, never a rewrite.
	corpus := []any{
		"2026-06-21T04:00:00Z",       // canonical already
		"2026-06-21T06:30:00+02:30",  // offset form
		"2026-06-21 04:00:00",        // zone-less space form
		"2026-06-21T04:00:00",        // zone-less T form
		"2026-06-21",                 // date-only
		"2026-06-21 04:00:00.123456", // zone-less with fraction
		"2026-06-21T04:00:00.9999Z",  // fraction beyond column precision
		"2026-06-21T04:00:00,999Z",   // comma fraction: ISO 8601 yes, ClickHouse no
		"1750478400",                 // 10-digit Unix string
		"999999999",                  // 9-digit Unix string
		"1750478400.5",               // fractional Unix string: DateTime64-only to ClickHouse
		"1750478400.123456789",       // ns-exact fraction (float64 would corrupt it)
		"1750478400.9999999995",      // >9 fraction digits: ClickHouse truncates, never rounds
		float64(1750478400),          // integer number: seconds to DateTime, *ticks* to DateTime64
		float64(1750478400.5),        // non-integer number: ClickHouse rejects for every kind
		float64(1750478400500),       // epoch-ms number: DateTime64(3)'s natural ticks shape
		"20260711",                   // 8 digits: YYYYMMDD to ClickHouse
		"20260711150000",             // 14 digits: YYYYMMDDhhmmss
		"202607111500",               // 12 digits: ClickHouse rejects
		"1752278400000",              // 13 digits: epoch milliseconds to ClickHouse
		"1750478400123456",           // 16 digits: epoch microseconds to ClickHouse
		"2026",                       // 4 digits: a year to ClickHouse
		"1e9",                        // not a timestamp
		"-100",                       // not a timestamp
		"banana",                     // garbage
		"2026-11-01 01:30:00",        // DST fall-back: ambiguous local time
		"2026-03-08 02:30:00",        // DST spring-forward: nonexistent local time
		"1960-01-01T00:00:00Z",       // pre-range: ClickHouse saturates, spelling-dependently
		"2300-06-30 12:30:00",        // beyond DateTime64's ceiling, zone-less
	}

	for _, ct := range colTypes {
		t.Run(ct.name, func(t *testing.T) {
			// Independent tables per shape; parallel keeps the six shapes from
			// serializing ~350 single-row inserts against the suite timeout.
			t.Parallel()
			rawTable := createTable(t, "id UInt32, v "+ct.ddl, "ORDER BY id")
			canonTable := createTable(t, "id UInt32, v "+ct.ddl, "ORDER BY id")
			schema := env(t).registry.Get(rawTable)
			require.NotNil(t, schema, "registry must discover the raw table")

			for i, input := range corpus {
				id := uint32(i)
				rawErr := insertBestEffort(t, rawTable, map[string]any{"id": id, "v": input})

				canonData := map[string]any{"id": id, "v": input}
				discovery.CanonicalizeTimestamps(schema, canonData)
				canonErr := insertBestEffort(t, canonTable, canonData)

				if (rawErr == nil) != (canonErr == nil) {
					t.Errorf("input %v: asymmetric insertability — raw err=%v, canonicalized (%v) err=%v",
						input, rawErr, canonData["v"], canonErr)
					continue
				}
				if rawErr != nil {
					continue // both rejected — consistent
				}
				rawStored := selectInstant(t, rawTable, id)
				canonStored := selectInstant(t, canonTable, id)
				if !rawStored.Equal(canonStored) {
					t.Errorf("input %v: stored instants differ — raw %s vs canonicalized (%v) %s",
						input, rawStored.UTC().Format(time.RFC3339Nano),
						canonData["v"], canonStored.UTC().Format(time.RFC3339Nano))
				}
			}
		})
	}
}

// insertBestEffort inserts one JSONEachRow row over ClickHouse HTTP with
// date_time_input_format=best_effort — the exact settings the ingest worker
// uses (see IngestWorker.insertToClickHouse) — and returns ClickHouse's
// verdict as an error (nil on 2xx).
func insertBestEffort(t *testing.T, table string, row map[string]any) error {
	t.Helper()
	body, err := json.Marshal(row)
	require.NoError(t, err)

	q := url.Values{}
	q.Set("database", testCHDatabase)
	q.Set("param_target_table", table)
	q.Set("query", "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow")
	q.Set("date_time_input_format", "best_effort")

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		env(t).chHTTPURL+"?"+q.Encode(), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", testCHUser)
	req.Header.Set("X-ClickHouse-Key", testCHPassword)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// selectInstant reads back the single stored timestamp as a time.Time instant.
func selectInstant(t *testing.T, table string, id uint32) time.Time {
	t.Helper()
	var v time.Time
	require.NoError(t, env(t).chConn.QueryRow(context.Background(),
		fmt.Sprintf("SELECT v FROM %s WHERE id = ?", table), id).Scan(&v))
	return v
}
