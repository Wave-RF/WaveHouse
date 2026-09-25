package cache

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		deps []Namespace
		want []string
	}{
		{"no deps is the tenant token alone", nil, []string{"wh:{acme}:T"}},
		{
			"a dep folds its table and scope tokens",
			[]Namespace{{Tenant: "acme", Table: "events", Scope: "org_1"}},
			[]string{"wh:{acme}:T", "wh:{acme}:B:events", "wh:{acme}:S:events:org_1"},
		},
		{
			"a scopeless dep reads the whole-table view",
			[]Namespace{{Tenant: "acme", Table: "events"}},
			[]string{"wh:{acme}:T", "wh:{acme}:B:events", "wh:{acme}:S:events:"},
		},
		{
			"shared table tokens and duplicate deps appear once, sorted",
			[]Namespace{
				{Tenant: "acme", Table: "orders"},
				{Tenant: "acme", Table: "events", Scope: "b"},
				{Tenant: "acme", Table: "events", Scope: "a"},
				{Tenant: "acme", Table: "orders"},
			},
			[]string{
				"wh:{acme}:T", "wh:{acme}:B:events", "wh:{acme}:B:orders",
				"wh:{acme}:S:events:a", "wh:{acme}:S:events:b", "wh:{acme}:S:orders:",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tokenKeys("wh", "acme", tt.deps))
		})
	}
}

func TestBumpKeys(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"p:{acme}:B:events"}, bumpKeys("p", Namespace{Tenant: "acme", Table: "events"}))
	assert.Equal(t, []string{"p:{acme}:S:events:org_1", "p:{acme}:S:events:"},
		bumpKeys("p", Namespace{Tenant: "acme", Table: "events", Scope: "org_1"}))
}

// Every key a bump writes is one some lookup reads: otherwise the bump
// orphans nothing.
func TestBumpKeysAreReadByLookups(t *testing.T) {
	t.Parallel()
	for _, ns := range []Namespace{{Tenant: "acme", Table: "events"}, {Tenant: "acme", Table: "events", Scope: "org_1"}} {
		read := map[string]bool{}
		for _, scope := range []string{"", ns.Scope} {
			for _, k := range tokenKeys("wh", "acme", []Namespace{{Tenant: "acme", Table: ns.Table, Scope: scope}}) {
				read[k] = true
			}
		}
		for _, k := range bumpKeys("wh", ns) {
			assert.True(t, read[k], k)
		}
	}
}

func TestValueKey(t *testing.T) {
	t.Parallel()
	a, b := Namespace{Tenant: "acme", Table: "events"}, Namespace{Tenant: "acme", Table: "orders", Scope: "x"}
	k := valueKey("wh", "acme", "acme:query:abc", []Namespace{a, b})
	assert.True(t, strings.HasPrefix(k, "wh:q:acme:"), k)
	assert.NotContains(t, k, "{", "values carry no hash tag, so they spread across shards")
	assert.Equal(t, k, valueKey("wh", "acme", "acme:query:abc", []Namespace{b, a, b}), "order and duplicates do not matter")
	assert.NotEqual(t, k, valueKey("wh", "acme", "acme:query:abc", []Namespace{a}))
	assert.NotEqual(t, k, valueKey("wh", "acme", "acme:query:abd", []Namespace{a, b}))
	assert.NotEqual(t,
		valueKey("wh", "acme", "q", []Namespace{{Tenant: "acme", Table: "ab", Scope: "c"}}),
		valueKey("wh", "acme", "q", []Namespace{{Tenant: "acme", Table: "a", Scope: "bc"}}))
}

func newTestCodec(t *testing.T, compressMin, maxDecoded int) *codec {
	t.Helper()
	c, err := newCodec(compressMin, maxDecoded)
	require.NoError(t, err)
	t.Cleanup(c.close)
	return c
}

func TestCodec_RoundTrip(t *testing.T) {
	t.Parallel()
	c := newTestCodec(t, 64, 1<<20)
	tokens := append(newToken(), newToken()...)
	exp := time.UnixMilli(time.Now().Add(time.Minute).UnixMilli())
	tests := []struct {
		name       string
		payload    []byte
		compressed bool
	}{
		{"empty", []byte{}, false},
		{"below the threshold", []byte(`[{"a":1}]`), false},
		{"compressible", bytes.Repeat([]byte(`{"user":"u1","n":42},`), 200), true},
		{"incompressible stays raw", randomBytes(4096), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := c.encode(tokens, exp, tt.payload)
			assert.Equal(t, tt.compressed, b[1]&flagZstd != 0)
			if tt.compressed {
				assert.Less(t, len(b), len(tt.payload))
			}
			gotTokens, gotExp, gotPayload, err := c.decode(b)
			require.NoError(t, err)
			assert.Equal(t, tokens, gotTokens)
			assert.True(t, exp.Equal(gotExp))
			assert.Equal(t, tt.payload, gotPayload)
		})
	}
}

func TestCodec_CompressionOff(t *testing.T) {
	t.Parallel()
	c := newTestCodec(t, 0, 1<<20)
	b := c.encode(newToken(), time.Now(), bytes.Repeat([]byte("a"), 4096))
	assert.Zero(t, b[1])
}

func TestCodec_RefusesBadValues(t *testing.T) {
	t.Parallel()
	c := newTestCodec(t, 1, 1<<10)
	good := c.encode(newToken(), time.Now(), []byte("rows"))
	bomb := newTestCodec(t, 1, 1<<30).encode(newToken(), time.Now(), make([]byte, 1<<20))
	require.Less(t, len(bomb), 1<<10, "the bomb is small when stored")

	withByte := func(i int, v byte) []byte {
		b := bytes.Clone(good)
		b[i] = v
		return b
	}
	tests := []struct {
		name string
		b    []byte
	}{
		{"shorter than the header", good[:headerLen-1]},
		{"unknown format", withByte(0, 2)},
		{"unknown flags", withByte(1, 0x80)},
		{"fewer tokens than it declares", withByte(11, 200)},
		{"not zstd", append(withByte(1, flagZstd)[:headerLen+tokenLen], "not zstd"...)},
		{"decodes past the limit", bomb},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, _, err := c.decode(tt.b)
			require.ErrorIs(t, err, errCorruptValue)
		})
	}
}

func TestJitter(t *testing.T) {
	t.Parallel()
	d := 100 * time.Second
	for range 1000 {
		j := jitter(d, newToken())
		assert.GreaterOrEqual(t, j, 90*time.Second)
		assert.Less(t, j, 110*time.Second)
	}
	assert.Equal(t, time.Nanosecond, jitter(time.Nanosecond, newToken()))
}

func TestNewToken(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for range 1000 {
		tok := newToken()
		require.Len(t, tok, tokenLen)
		require.False(t, seen[string(tok)])
		seen[string(tok)] = true
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, 0, n)
	for len(b) < n {
		b = append(b, newToken()...)
	}
	return b[:n]
}
