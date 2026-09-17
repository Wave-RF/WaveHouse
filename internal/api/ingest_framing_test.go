package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReframeArray covers the one place ingest still looks at the body's own
// bytes. The rewrite is string- and escape-aware and runs ONLY on a declared
// JSON body whose first non-whitespace byte is '[' — the gate matters as much as
// the scan, because a bare object's commas are at depth 1 too and rewriting them
// destroys the record (measured, AUDIT §A.0a).
func TestReframeArray(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		body  string
		want  string
		count int
		ok    bool
	}{
		{
			name:  "compact array becomes one record per line",
			body:  `[{"a":1},{"a":2},{"a":3}]`,
			want:  " {\"a\":1}\n{\"a\":2}\n{\"a\":3} ",
			count: 3, ok: true,
		},
		{
			name:  "a comma inside a string is data",
			body:  `[{"a":"x,y"},{"a":"z"}]`,
			want:  " {\"a\":\"x,y\"}\n{\"a\":\"z\"} ",
			count: 2, ok: true,
		},
		{
			name:  "brackets inside a string do not move the depth",
			body:  `[{"a":"]["},{"a":"[[["}]`,
			want:  " {\"a\":\"][\"}\n{\"a\":\"[[[\"} ",
			count: 2, ok: true,
		},
		{
			name:  "an escaped quote does not end the string",
			body:  `[{"a":"he said \"a,b\""},{"a":"z"}]`,
			want:  " {\"a\":\"he said \\\"a,b\\\"\"}\n{\"a\":\"z\"} ",
			count: 2, ok: true,
		},
		{
			name:  "nested arrays and objects keep their commas",
			body:  `[{"a":[1,2],"b":{"c":3,"d":4}},{"a":[5]}]`,
			want:  " {\"a\":[1,2],\"b\":{\"c\":3,\"d\":4}}\n{\"a\":[5]} ",
			count: 2, ok: true,
		},
		{
			// A multi-line array keeps its meaning AND stops carrying layout
			// newlines, which is what keeps the record count exact.
			name:  "a pretty-printed array is one record per line and nothing else",
			body:  "[\n  {\"a\":1},\n  {\"a\":2}\n]",
			want:  "    {\"a\":1}\n   {\"a\":2}  ",
			count: 2, ok: true,
		},
		{
			name:  "an empty array holds no records",
			body:  `[]`,
			want:  `  `,
			count: 0, ok: true,
		},
		{
			name:  "whitespace inside an empty array is still no records",
			body:  "[\n ]",
			want:  "    ",
			count: 0, ok: true,
		},
		{
			name:  "a one-element array is one record",
			body:  `[{"a":1}]`,
			want:  ` {"a":1} `,
			count: 1, ok: true,
		},
		{
			name: "scalar elements are still elements",
			body: `[1,"x",[2]]`,
			want: " 1\n\"x\"\n[2] ",
			// They will each be a per-record parse refusal; the framing's job is
			// only to keep them separate so the objects around them survive.
			count: 3, ok: true,
		},
		{name: "a truncated array does not balance", body: `[{"a":1}`, ok: false},
		{name: "a trailing comma cut off does not balance", body: `[{"a":1},`, ok: false},
		{name: "a bare open bracket does not balance", body: `[`, ok: false},
		{name: "a cut-off element does not balance", body: `[{"a":1},{"b`, ok: false},
		{name: "a structural syntax error does not balance", body: `[{"a":1}, {bad]`, ok: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := []byte(tt.body)
			count, ok := reframeArray(b)
			require.Equal(t, tt.ok, ok, "balance")
			if !tt.ok {
				return
			}
			assert.Equal(t, tt.count, count, "element count")
			assert.Equal(t, tt.want, string(b), "rewritten body")
		})
	}
}

// TestReframeArray_DestroysWhatItMustNotSee is the gate, not the scan: these
// bodies are the reason reframeArray runs only behind a leading '['. Running it
// on either would corrupt the record, so the test asserts the damage — if the
// handler ever stops gating, this is the shape of the bug.
func TestReframeArray_DestroysWhatItMustNotSee(t *testing.T) {
	t.Parallel()

	obj := []byte(`{"a":1,"b":2}`)
	reframeArray(obj)
	assert.Equal(t, "{\"a\":1\n\"b\":2}", string(obj),
		"a bare object's own commas are at depth 1 — the handler must never send one here")

	ndjson := []byte("{\"a\":1,\"b\":2}\n{\"a\":3,\"b\":4}\n")
	reframeArray(ndjson)
	assert.NotContains(t, string(ndjson), `{"a":1,"b":2}`,
		"an NDJSON body is destroyed too — same reason, same gate")
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

// TestEventIDAt: the id is the STORED value, and a string cell is JSON-decoded
// because ClickHouse's writer escapes "/" as "\/" — Go's own string-literal
// unquoting refuses that.
func TestEventIDAt(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		line string
		idx  int
		want string
		ok   bool
	}{
		{"a quoted string is decoded", `["\/a", "evt-1", 0]`, 1, "evt-1", true},
		{"a slash escape survives", `["\/a\/b", 0]`, 0, "/a/b", true},
		{"a number is its digits", `["x", 18446744073709551615]`, 1, "18446744073709551615", true},
		{"an empty string is no id", `["x", ""]`, 1, "", false},
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
