package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFirstNonSpace: the array-or-object byte is found the way ClickHouse's
// JSON reader starts a body — a byte order mark skipped at the very start only,
// and all six ASCII whitespace bytes skipped.
func TestFirstNonSpace(t *testing.T) {
	t.Parallel()
	for body, want := range map[string]byte{
		"[":                         '[',
		" \t\r\n\f\v[":              '[',
		"\xEF\xBB\xBF[":             '[',
		"\xEF\xBB\xBF\f\n {":        '{',
		" \xEF\xBB\xBF[":            0xEF, // a mark after layout is not skipped, by ClickHouse either
		"\xEF\xBB\xBF\xEF\xBB\xBF[": 0xEF,
		"\xEF\xBBx":                 0xEF,
	} {
		got, ok := firstNonSpace([]byte(body))
		assert.True(t, ok, "%q", body)
		assert.Equal(t, want, got, "%q", body)
	}
	for _, body := range []string{"", " \f\v\r\n\t", "\xEF\xBB\xBF", "\xEF\xBB\xBF\n", strings.Repeat(" ", maxSniffBytes) + "["} {
		_, ok := firstNonSpace([]byte(body))
		assert.False(t, ok, "%q reads as empty", body)
	}
}

func TestCellAt(t *testing.T) {
	t.Parallel()
	const line = `["a, b", "c\"d", 42, null, ["x", "y"], {"k": 1}, "last"]`
	for i, want := range []string{`"a, b"`, `"c\"d"`, `42`, `null`, `["x", "y"]`, `{"k": 1}`, `"last"`} {
		got, ok := cellAt([]byte(line), i)
		require.True(t, ok, "cell %d", i)
		assert.Equal(t, want, string(got), "cell %d", i)
	}
	_, ok := cellAt([]byte(line), 7)
	assert.False(t, ok, "past the end")
	_, ok = cellAt([]byte(line), -1)
	assert.False(t, ok, "before the start")
	_, ok = cellAt([]byte(`[`), 0)
	assert.False(t, ok, "an unterminated row yields nothing")
}

// TestEventIDAt: the id is the STORED value, a string cell is JSON-decoded —
// JSON's escapes, "\/" included, which Go's own string-literal unquoting
// refuses — and a null cell is no id at all.
func TestEventIDAt(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		line string
		idx  int
		want string
		ok   bool
	}{
		{"a quoted string is decoded", `["/a", "evt-1", 0]`, 1, "evt-1", true},
		{"a slash escape survives", `["\/a\/b", 0]`, 0, "/a/b", true},
		{"a number is its digits", `["x", 18446744073709551615]`, 1, "18446744073709551615", true},
		{"an empty string is no id", `["x", ""]`, 1, "", false},
		// #370: an explicit null is as missing as an absent column — keying on
		// its spelling would make every null record one id, and collide with
		// the literal id "null".
		{"a null cell is no id", `["x", null]`, 1, "", false},
		{"the string null is an id", `["x", "null"]`, 1, "null", true},
		{"a column that is not on the wire is no id", `["x", "y"]`, -1, "", false},
		{"past the end is no id", `["x"]`, 3, "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := eventIDAt([]byte(tt.line), tt.idx)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
