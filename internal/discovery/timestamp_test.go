package discovery

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsTimestampType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		chType string
		want   bool
	}{
		{"DateTime", true},
		{"DateTime('UTC')", true},
		{"DateTime64(3)", true},
		{"DateTime64(6, 'America/New_York')", true},
		{"Nullable(DateTime)", true},
		{"LowCardinality(Nullable(DateTime('UTC')))", true},
		{"Date", false},   // day-precision, no spelling ambiguity — deliberately excluded
		{"Date32", false}, // as above
		{"String", false},
		{"UInt64", false},
	}

	for _, tt := range tests {
		t.Run(tt.chType, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isTimestampType(tt.chType))
		})
	}
}

// tsSchema builds a one-column schema of the given ClickHouse type, so each case
// exercises exactly one column's canonicalization.
func tsSchema(colType string) *TableSchema {
	return &TableSchema{Name: "t", Columns: []Column{{Name: "ts", Type: colType}}}
}

func TestCanonicalizeTimestamps(t *testing.T) {
	t.Parallel()
	nyc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	tests := []struct {
		name     string
		colType  string
		serverTZ *time.Location
		value    any
		want     any
	}{
		{"canonical passes through", "DateTime('UTC')", nil, "2026-06-21T04:00:00Z", "2026-06-21T04:00:00Z"},
		{"offset converts to Z", "DateTime('UTC')", nil, "2026-06-21T06:30:00+02:30", "2026-06-21T04:00:00Z"},
		{"naive space form, column zone", "DateTime('America/New_York')", nil, "2026-06-21 00:00:00", "2026-06-21T04:00:00Z"},
		{"naive T form, column zone", "DateTime('America/New_York')", nil, "2026-06-21T00:00:00", "2026-06-21T04:00:00Z"},
		{"naive form, Etc/UTC column zone", "DateTime('Etc/UTC')", nil, "2026-06-21 04:00:00", "2026-06-21T04:00:00Z"},
		{"naive form, server zone", "DateTime", nyc, "2026-06-21 00:00:00", "2026-06-21T04:00:00Z"},
		{"naive form, unknown server zone ⇒ passes through", "DateTime", nil, "2026-06-21 04:00:00", "2026-06-21 04:00:00"},
		{"offset form, unknown server zone still canonicalizes", "DateTime", nil, "2026-06-21T06:00:00+02:00", "2026-06-21T04:00:00Z"},
		{"date-only ⇒ midnight in zone", "DateTime('America/New_York')", nil, "2026-06-21", "2026-06-21T04:00:00Z"},
		{"unix seconds number", "DateTime('UTC')", nil, float64(1782014400), "2026-06-21T04:00:00Z"},
		{"unix seconds json.Number", "DateTime('UTC')", nil, json.Number("1782014400"), "2026-06-21T04:00:00Z"},
		{"unix seconds digit-string", "DateTime('UTC')", nil, "1782014400", "2026-06-21T04:00:00Z"},
		{"9-digit unix string", "DateTime('UTC')", nil, "999999999", "2001-09-09T01:46:39Z"},
		{"fractional unix digit-string", "DateTime64(3, 'UTC')", nil, "1782014400.5", "2026-06-21T04:00:00.5Z"},
		{"nanosecond fraction parsed exactly, not via float64", "DateTime64(9, 'UTC')", nil, "1782014400.123456789", "2026-06-21T04:00:00.123456789Z"},
		{"fraction digits beyond nine truncate, never round", "DateTime64(9, 'UTC')", nil, "1782014400.9999999995", "2026-06-21T04:00:00.999999999Z"},
		// Integer numbers are ClickHouse *ticks* at the column scale on DateTime64
		// — the ms epoch is the natural producer shape there, and an epoch-seconds
		// number really is a 1970 instant (what the insert stores either way).
		{"epoch-ms number is ticks on DateTime64(3)", "DateTime64(3, 'UTC')", nil, float64(1782014400000), "2026-06-21T04:00:00Z"},
		{"epoch-ms json.Number with sub-second ticks", "DateTime64(3, 'UTC')", nil, json.Number("1782014400123"), "2026-06-21T04:00:00.123Z"},
		{"epoch-seconds number on DateTime64(3) is a 1970 instant", "DateTime64(3, 'UTC')", nil, float64(1782014400), "1970-01-21T15:00:14.4Z"},
		{"number on DateTime64(0) is seconds (scale 1)", "DateTime64(0, 'UTC')", nil, float64(1782014400), "2026-06-21T04:00:00Z"},
		{"fraction truncated to column precision", "DateTime64(3, 'UTC')", nil, "2026-06-21T04:00:00.123456Z", "2026-06-21T04:00:00.123Z"},
		{"fraction truncated off a second-precision column", "DateTime('UTC')", nil, "2026-06-21 04:00:00.999", "2026-06-21T04:00:00Z"},
		{"trailing fractional zeros trimmed, like /v1/query", "DateTime64(3, 'UTC')", nil, "2026-06-21T04:00:00.120Z", "2026-06-21T04:00:00.12Z"},
		{"Nullable unwraps", "Nullable(DateTime('UTC'))", nil, "2026-06-21 04:00:00", "2026-06-21T04:00:00Z"},
		{"null left for ClickHouse", "Nullable(DateTime)", nil, nil, nil},
		{"String column untouched", "String", nil, "2026-06-21 04:00:00", "2026-06-21 04:00:00"},
		{"Date column untouched (excluded)", "Date", nil, "2026-06-21", "2026-06-21"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The production path: specs resolved once at schema-build time.
			schema := tsSchema(tt.colType)
			resolveTimestampSpecs(schema, tt.serverTZ, discardLogger())
			data := map[string]any{"ts": tt.value}
			CanonicalizeTimestamps(schema, data)
			assert.Equal(t, tt.want, data["ts"])
		})
	}
}

// TestColumnTimeParser: the stream row-filter's per-column parser (#381) is the
// canonicalization grammar exactly — same spellings, zone rule, and Unix forms —
// truncated to the column's precision and bounded by the rewrite range, so a
// filter constant and a canonicalized payload always meet on the instant
// ClickHouse stores.
func TestColumnTimeParser(t *testing.T) {
	t.Parallel()
	utc4 := time.Date(2026, 6, 21, 4, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		colType string
		value   any
		want    time.Time
		ok      bool
	}{
		{"canonical RFC 3339", "DateTime('UTC')", "2026-06-21T04:00:00Z", utc4, true},
		{"zone-less read in column zone", "DateTime('UTC')", "2026-06-21 04:00:00", utc4, true},
		{"explicit offset, same instant", "DateTime('UTC')", "2026-06-21T06:00:00+02:00", utc4, true},
		{"unix seconds string", "DateTime('UTC')", "1782014400", utc4, true},
		{"unix seconds number", "DateTime('UTC')", json.Number("1782014400"), utc4, true},
		{"fraction truncated to column precision", "DateTime64(1, 'UTC')", "2026-06-21T04:00:00.19Z", utc4.Add(100 * time.Millisecond), true},
		{"junk refused", "DateTime('UTC')", "not a timestamp", time.Time{}, false},
		{"out of range refused (insert-time saturation would move it)", "DateTime('UTC')", "2400-01-01T00:00:00Z", time.Time{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			schema := tsSchema(tt.colType)
			resolveTimestampSpecs(schema, nil, discardLogger())
			parse := schema.Columns[0].TimeParser()
			require.NotNil(t, parse)
			got, ok := parse(tt.value)
			assert.Equal(t, tt.ok, ok)
			if tt.ok {
				assert.True(t, got.Equal(tt.want), "got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestColumnTimeParser_NilOrZoneLimited: only DateTime/DateTime64 columns with a
// resolved spec carry a parser — String/Date columns and hand-built literals
// return nil (byte-equality semantics on the stream). A timestamp column whose
// zone is unknown still parses zone-explicit forms but refuses zone-less strings:
// the zone would be a guess, and a guessed instant could move a row across a
// filter boundary.
func TestColumnTimeParser_NilOrZoneLimited(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{Name: "t", Columns: []Column{
		{Name: "s", Type: "String"},
		{Name: "d", Type: "Date"},
		{Name: "ts", Type: "DateTime"},
	}}
	resolveTimestampSpecs(schema, nil, discardLogger())
	assert.Nil(t, schema.Columns[0].TimeParser(), "String column: no parser")
	assert.Nil(t, schema.Columns[1].TimeParser(), "Date column: excluded from timestamp handling")
	assert.Nil(t, tsSchema("DateTime").Columns[0].TimeParser(), "hand-built literal without spec resolution: no parser")

	unknownZone := schema.Columns[2].TimeParser()
	require.NotNil(t, unknownZone, "zone-less DateTime with unknown server zone still has a (zone-explicit-only) parser")
	_, ok := unknownZone("2026-06-21 04:00:00")
	assert.False(t, ok, "zone-less string with unknown column zone: refused, never guessed")
	got, ok := unknownZone("2026-06-21T04:00:00Z")
	assert.True(t, ok)
	assert.True(t, got.Equal(time.Date(2026, 6, 21, 4, 0, 0, 0, time.UTC)))
}

// TestCanonicalizeTimestamps_NoPrecomputedSpec: a schema that skipped spec
// resolution (hand-built literals) passes through untouched.
func TestCanonicalizeTimestamps_NoPrecomputedSpec(t *testing.T) {
	t.Parallel()
	data := map[string]any{"ts": "2026-06-21 04:00:00"}
	CanonicalizeTimestamps(tsSchema("DateTime"), data)
	assert.Equal(t, "2026-06-21 04:00:00", data["ts"])
}

// TestCanonicalizeTimestamps_AbsentColumn: a column not in the payload (DEFAULT-
// filled by ClickHouse) is left absent, never invented.
func TestCanonicalizeTimestamps_AbsentColumn(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{Name: "t", Columns: []Column{
		{Name: "ts", Type: "DateTime", HasDefault: true},
		{Name: "page", Type: "String"},
	}}
	resolveTimestampSpecs(schema, time.UTC, discardLogger())
	data := map[string]any{"page": "/home"}
	CanonicalizeTimestamps(schema, data)
	assert.Equal(t, map[string]any{"page": "/home"}, data)
}

// TestCanonicalizeTimestamps_Unparseable_PassThrough: fail-open — unparseable
// values and unresolvable column specs pass through verbatim.
func TestCanonicalizeTimestamps_Unparseable_PassThrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		colType string
		value   any
	}{
		{"unrecognized string", "DateTime('UTC')", "banana"},
		{"non-finite numeric string is not an instant", "DateTime('UTC')", "NaN"},
		{"wrong value type", "DateTime('UTC')", true},
		{"unknown zone in type", "DateTime('Not/AZone')", "2026-06-21 04:00:00"},
		{"malformed DateTime64 precision", "DateTime64(x)", "2026-06-21 04:00:00"},
		// Digit-strings outside the 9–10 digit Unix shape mean calendar forms
		// (or nothing) to ClickHouse best_effort — parsing them as Unix seconds
		// would store a different instant than the insert (PR #402 review).
		{"8-digit string is YYYYMMDD to ClickHouse", "DateTime('UTC')", "20260711"},
		{"12-digit string (ClickHouse rejects)", "DateTime('UTC')", "202607111500"},
		{"14-digit string is YYYYMMDDhhmmss to ClickHouse", "DateTime('UTC')", "20260711150000"},
		{"4-digit string is a year to ClickHouse", "DateTime('UTC')", "2026"},
		{"11-digit string", "DateTime('UTC')", "17504784000"},
		{"13-digit string is ClickHouse's ms epoch, not ours", "DateTime('UTC')", "1752278400000"},
		{"16-digit string is ClickHouse's µs epoch, not ours", "DateTime('UTC')", "1750478400123456"},
		{"scientific notation is not a timestamp", "DateTime('UTC')", "1e9"},
		{"negative digit-string", "DateTime('UTC')", "-100"},
		{"empty fraction", "DateTime('UTC')", "1750478400."},
		// ClickHouse consumes a fraction after a Unix epoch only for DateTime64
		// targets; on plain DateTime the leftover fraction fails the row.
		{"fractional unix string on DateTime", "DateTime('UTC')", "1782014400.5"},
		// ClickHouse has no ',' decimal separator (Go's RFC3339Nano accepts one
		// per ISO 8601) — rewriting would insert a row ClickHouse rejects raw.
		{"comma fraction", "DateTime('UTC')", "2026-06-21T04:00:00,999Z"},
		{"comma fraction on DateTime64", "DateTime64(3, 'UTC')", "2026-06-21T04:00:00,9Z"},
		// ClickHouse rejects non-integer JSON numbers for every DateTime kind.
		{"non-integer number", "DateTime64(3, 'UTC')", 1782014400.5},
		{"non-integer number on DateTime", "DateTime('UTC')", 1782014400.5},
		{"negative number", "DateTime('UTC')", float64(-100)},
		{"json.Number with exponent", "DateTime('UTC')", json.Number("1.5e9")},
		// Out of the column kind's range: ClickHouse saturates, and saturation is
		// spelling-dependent (local time-of-day is kept while the date clamps), so
		// no rewrite is safe — the raw spelling must be the one that saturates.
		{"pre-epoch instant on DateTime", "DateTime('UTC')", "1960-01-01T00:00:00Z"},
		{"beyond UInt32 seconds on DateTime", "DateTime('UTC')", "2107-01-01T00:00:00Z"},
		{"number beyond UInt32 seconds on DateTime", "DateTime('UTC')", float64(4294967296)},
		{"beyond 2299 on DateTime64", "DateTime64(3, 'UTC')", "2300-06-30 12:30:00"},
		{"beyond the Int64-ns ceiling on DateTime64(9)", "DateTime64(9, 'UTC')", "2280-01-01T00:00:00Z"},
		// Valid RFC 3339 the subset deliberately omits (Go rejects :60).
		{"leap-second spelling", "DateTime('UTC')", "2016-12-31T23:59:60Z"},
		// "" and "Local" are Go LoadLocation quirks (UTC / process env), not
		// zone declarations — strict: unresolvable, pass through.
		{"empty zone name in type", "DateTime('')", "2026-06-21 04:00:00"},
		{"Local zone in type", "DateTime('Local')", "2026-06-21 04:00:00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			schema := tsSchema(tt.colType)
			resolveTimestampSpecs(schema, time.UTC, discardLogger())
			data := map[string]any{"ts": tt.value}
			CanonicalizeTimestamps(schema, data)
			assert.Equal(t, tt.value, data["ts"])
		})
	}
}

// TestResolveTimestampSpecs: timestamp columns get a spec (own zone, else server
// default); others don't; an unresolvable zone degrades to nil, not a failure.
func TestResolveTimestampSpecs(t *testing.T) {
	t.Parallel()
	nyc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	schema := &TableSchema{Name: "t", Columns: []Column{
		{Name: "plain", Type: "DateTime"},
		{Name: "zoned", Type: "DateTime64(3, 'America/New_York')"},
		{Name: "page", Type: "String"},
		{Name: "broken", Type: "DateTime('Not/AZone')"},
		{Name: "etc_utc", Type: "DateTime('Etc/UTC')"},
	}}
	resolveTimestampSpecs(schema, nyc, discardLogger())

	require.NotNil(t, schema.Columns[0].tsSpec)
	assert.Equal(t, nyc, schema.Columns[0].tsSpec.loc, "zone-less column takes the server zone")
	assert.False(t, schema.Columns[0].tsSpec.isDT64, "DateTime is not kind DateTime64")
	require.NotNil(t, schema.Columns[1].tsSpec)
	assert.Equal(t, nyc, schema.Columns[1].tsSpec.loc)
	assert.Equal(t, 3, schema.Columns[1].tsSpec.precision)
	assert.True(t, schema.Columns[1].tsSpec.isDT64, "numbers and unix fractions follow the DateTime64 rules")
	assert.Nil(t, schema.Columns[2].tsSpec, "non-timestamp column gets no spec")
	assert.Nil(t, schema.Columns[3].tsSpec, "unresolvable zone degrades to nil, not a failed build")
	require.NotNil(t, schema.Columns[4].tsSpec)
	assert.Same(t, time.UTC, schema.Columns[4].tsSpec.loc, "Etc/UTC maps to UTC without a tzdata lookup")

	// The degraded column passes through untouched — fail-open, never a rejection.
	data := map[string]any{"broken": "2026-06-21 04:00:00"}
	CanonicalizeTimestamps(schema, data)
	assert.Equal(t, "2026-06-21 04:00:00", data["broken"])
}

// TestRefresh_PrecomputesSpecs: schema builds resolve timestamp specs with the
// server zone from SELECT timezone(), so registry consumers — production and
// the testutil mock-conn path alike — exercise the same precomputed path.
func TestRefresh_PrecomputesSpecs(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{columns: []fakeColumn{{table: "t", name: "ts", chType: "DateTime", position: 1}}}
	reg := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())
	require.NoError(t, reg.Refresh(context.Background()))
	col := reg.Get("t").Columns[0]
	require.NotNil(t, col.tsSpec)
	assert.Equal(t, time.UTC, col.tsSpec.loc)
}

// discardLogger mirrors the registries' test logger: spec-resolution warnings are
// asserted via behavior (nil specs), not log output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
