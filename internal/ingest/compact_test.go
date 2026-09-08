package ingest

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

func cols(names ...string) []discovery.Column {
	out := make([]discovery.Column, 0, len(names))
	for i, n := range names {
		out = append(out, discovery.Column{Name: n, Type: "String", Position: uint64(i + 1)})
	}
	return out
}

// TestEncodeCompactRow_DeclarationOrder: the array follows the SCHEMA's order,
// not the record's — a Go map has none, which is the whole reason the column
// list travels separately from the row.
func TestEncodeCompactRow_DeclarationOrder(t *testing.T) {
	t.Parallel()
	line, err := EncodeCompactRow(cols("page", "button", "country"), map[string]any{
		"country": "US",
		"page":    "/home",
		"button":  "signup",
	})
	require.NoError(t, err)
	assert.JSONEq(t, `["/home","signup","US"]`, string(line))
	assert.Equal(t, `["/home","signup","US"]`, string(line), "positional output must be byte-exact, not merely equivalent")
}

// TestEncodeCompactRow_MissingColumnIsNull: a column the record omits holds a
// position in the array — dropping it would shift every value after it.
func TestEncodeCompactRow_MissingColumnIsNull(t *testing.T) {
	t.Parallel()
	line, err := EncodeCompactRow(cols("page", "button", "country"), map[string]any{
		"page":    "/home",
		"country": "US",
	})
	require.NoError(t, err)
	assert.Equal(t, `["/home",null,"US"]`, string(line))
}

// TestEncodeCompactRow_ExplicitNullAndMissingAgree: a column present as JSON
// null encodes the same as one that is absent, so the insert path treats them
// alike.
func TestEncodeCompactRow_ExplicitNullAndMissingAgree(t *testing.T) {
	t.Parallel()
	explicit, err := EncodeCompactRow(cols("page", "button"), map[string]any{"page": "/a", "button": nil})
	require.NoError(t, err)
	absent, err := EncodeCompactRow(cols("page", "button"), map[string]any{"page": "/a"})
	require.NoError(t, err)
	assert.Equal(t, string(absent), string(explicit))
}

// TestEncodeCompactRow_NumberFidelity: a json.Number keeps its exact digits, so
// a 64-bit id past 2^53 survives the round trip a float64 would round off.
func TestEncodeCompactRow_NumberFidelity(t *testing.T) {
	t.Parallel()
	const bigID = "9007199254740993" // 2^53 + 1: not representable as a float64
	line, err := EncodeCompactRow(cols("id", "ratio"), map[string]any{
		"id":    json.Number(bigID),
		"ratio": json.Number("1.500"),
	})
	require.NoError(t, err)
	assert.Equal(t, `[9007199254740993,1.500]`, string(line),
		"json.Number writes its exact digits, trailing zeros and all")

	// The same value decoded as a float64 would not survive — the guard the
	// UseNumber decoders on the ingest path exist for.
	lossy, err := EncodeCompactRow(cols("id"), map[string]any{"id": float64(9007199254740993)})
	require.NoError(t, err)
	assert.NotEqual(t, "["+bigID+"]", string(lossy))
}

// TestEncodeCompactRow_EmptySchema: no columns is an empty array, never a bare
// or malformed line.
func TestEncodeCompactRow_EmptySchema(t *testing.T) {
	t.Parallel()
	for _, record := range []map[string]any{nil, {}, {"stray": 1}} {
		line, err := EncodeCompactRow(nil, record)
		require.NoError(t, err)
		assert.Equal(t, `[]`, string(line))
	}
}

// TestEncodeCompactRow_NoTrailingNewline: the caller joins lines, so a line
// must not carry its own terminator.
func TestEncodeCompactRow_NoTrailingNewline(t *testing.T) {
	t.Parallel()
	line, err := EncodeCompactRow(cols("page"), map[string]any{"page": "/a"})
	require.NoError(t, err)
	assert.NotContains(t, string(line), "\n")
	assert.Equal(t, byte(']'), line[len(line)-1])
}

// TestEncodeCompactRow_StructuredAndUnicodeValues: arrays, maps, and non-ASCII
// text pass through as the JSON they are — the encoder judges no value.
func TestEncodeCompactRow_StructuredAndUnicodeValues(t *testing.T) {
	t.Parallel()
	line, err := EncodeCompactRow(cols("tags", "attrs", "label"), map[string]any{
		"tags":  []any{"a", "b"},
		"attrs": map[string]any{"k": "v"},
		"label": "héllo · 世界",
	})
	require.NoError(t, err)

	var decoded []any
	require.NoError(t, json.Unmarshal(line, &decoded))
	require.Len(t, decoded, 3)
	assert.Equal(t, []any{"a", "b"}, decoded[0])
	assert.Equal(t, map[string]any{"k": "v"}, decoded[1])
	assert.Equal(t, "héllo · 世界", decoded[2])
}

// TestEncodeCompactRow_UnmarshalableValue: a value encoding/json cannot render
// is an error naming the column, not a silently dropped or shifted field.
func TestEncodeCompactRow_UnmarshalableValue(t *testing.T) {
	t.Parallel()
	_, err := EncodeCompactRow(cols("page", "score"), map[string]any{
		"page":  "/a",
		"score": math.Inf(1), // JSON has no infinity
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"score"`)
}
