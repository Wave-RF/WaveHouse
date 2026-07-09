package discovery

import (
	"encoding/json"
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
			assert.Equal(t, tt.want, IsTimestampType(tt.chType))
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
		{"naive form, server zone", "DateTime", nyc, "2026-06-21 00:00:00", "2026-06-21T04:00:00Z"},
		{"naive form, no zone known ⇒ UTC", "DateTime", nil, "2026-06-21 04:00:00", "2026-06-21T04:00:00Z"},
		{"date-only ⇒ midnight in zone", "DateTime('America/New_York')", nil, "2026-06-21", "2026-06-21T04:00:00Z"},
		{"unix seconds number", "DateTime('UTC')", nil, float64(1782014400), "2026-06-21T04:00:00Z"},
		{"unix seconds json.Number", "DateTime('UTC')", nil, json.Number("1782014400"), "2026-06-21T04:00:00Z"},
		{"unix seconds digit-string", "DateTime('UTC')", nil, "1782014400", "2026-06-21T04:00:00Z"},
		{"fractional unix digit-string", "DateTime64(3, 'UTC')", nil, "1782014400.5", "2026-06-21T04:00:00.5Z"},
		{"fractional unix on DateTime64", "DateTime64(3, 'UTC')", nil, 1782014400.5, "2026-06-21T04:00:00.5Z"},
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
			data := map[string]any{"ts": tt.value}
			require.NoError(t, CanonicalizeTimestamps(tsSchema(tt.colType), data, tt.serverTZ))
			assert.Equal(t, tt.want, data["ts"])
		})
	}
}

// TestCanonicalizeTimestamps_AbsentColumn: a column not in the payload (DEFAULT-
// filled by ClickHouse) is left absent, never invented.
func TestCanonicalizeTimestamps_AbsentColumn(t *testing.T) {
	t.Parallel()
	data := map[string]any{"page": "/home"}
	require.NoError(t, CanonicalizeTimestamps(
		&TableSchema{Name: "t", Columns: []Column{
			{Name: "ts", Type: "DateTime", HasDefault: true},
			{Name: "page", Type: "String"},
		}}, data, nil))
	assert.Equal(t, map[string]any{"page": "/home"}, data)
}

func TestCanonicalizeTimestamps_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		colType string
		value   any
		wantIn  string
	}{
		{"unrecognized string", "DateTime('UTC')", "banana", `column "ts"`},
		{"non-finite numeric string is not an instant", "DateTime('UTC')", "NaN", "unrecognized timestamp"},
		{"wrong value type", "DateTime('UTC')", true, "must be a string or Unix-seconds number"},
		{"unknown zone in type", "DateTime('Not/AZone')", "2026-06-21 04:00:00", `unknown time zone "Not/AZone"`},
		{"malformed DateTime64 precision", "DateTime64(x)", "2026-06-21 04:00:00", "bad precision"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := CanonicalizeTimestamps(tsSchema(tt.colType), map[string]any{"ts": tt.value}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantIn)
		})
	}
}

// TestServerTimezone_DefaultsToUTC: a registry that has never refreshed (the
// map-built test form) reports UTC rather than nil.
func TestServerTimezone_DefaultsToUTC(t *testing.T) {
	t.Parallel()
	reg := NewSchemaRegistryFromMap(nil)
	assert.Equal(t, time.UTC, reg.ServerTimezone())
}
