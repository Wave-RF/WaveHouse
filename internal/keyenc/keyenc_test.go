package keyenc_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// v010Escape is the encoder v0.1.0 shipped as query.SafeEncodeNATS, copied
// verbatim. Escape differs from it only in keeping '-'.
func v010Escape(raw string) string {
	var buf bytes.Buffer
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' {
			buf.WriteByte(b)
		} else {
			fmt.Fprintf(&buf, "%%%02X", b)
		}
	}
	return buf.String()
}

func TestEscape_Golden(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]string{
		"":               "",
		"my_table123":    "my_table123",
		"default.clicks": "default%2Eclicks",
		"my table":       "my%20table",
		"a-b/c":          "a-b%2Fc",
		"a.*.>":          "a%2E%2A%2E%3E",
		"100%":           "100%25",
		"{acme}:x|y":     "%7Bacme%7D%3Ax%7Cy",
		"a\x00b":         "a%00b",
		"café":           "caf%C3%A9",
		"\xff":           "%FF",
		"tab\tnewline\n": "tab%09newline%0A",
		"#hash":          "%23hash",
		"ABCxyz_0189":    "ABCxyz_0189",
		"evt-123":        "evt-123",
		"日本":             "%E6%97%A5%E6%9C%AC",
	} {
		assert.Equal(t, want, keyenc.Escape(raw), "%q", raw)
		assert.Equal(t, want, string(keyenc.AppendEscape([]byte("x"), raw))[1:], "%q", raw)
	}
}

// Every byte value, alone and between kept bytes, encodes as v0.1.0 did,
// but for '-'.
func TestEscape_MatchesV010ButDash(t *testing.T) {
	t.Parallel()
	for b := range 256 {
		for _, s := range []string{string([]byte{byte(b)}), "a" + string([]byte{byte(b)}) + "Z"} {
			want := v010Escape(s)
			if b == '-' {
				want = s
			}
			require.Equal(t, want, keyenc.Escape(s), "byte %#x", b)
		}
	}
}

// A tenant id is its own escaped form: the kept bytes are its grammar.
func TestEscape_KeepsExactlyTheTenantGrammar(t *testing.T) {
	t.Parallel()
	for b := range 256 {
		s := string([]byte{byte(b)})
		_, err := tenant.Parse(s)
		assert.Equal(t, err == nil, keyenc.Escape(s) == s, "byte %#x", b)
	}
}

func TestEscape_NoAllocWhenNothingToEscape(t *testing.T) {
	s := "events_2026-09"
	assert.Zero(t, testing.AllocsPerRun(100, func() { _ = keyenc.Escape(s) }))
}

// Distinct names never share an escaped form, even names that look escaped:
// '%' is itself escaped.
func TestEscape_LookalikesStayDistinct(t *testing.T) {
	t.Parallel()
	names := []string{"b-c", "b%2Dc", "b%2dc", "b.c", "b%2Ec"}
	seen := map[string]string{}
	for _, n := range names {
		e := keyenc.Escape(n)
		require.NotContains(t, seen, e, "%q and %q", seen[e], n)
		seen[e] = n
		back, err := keyenc.Unescape(e)
		require.NoError(t, err)
		assert.Equal(t, n, back)
	}
}

func TestUnescape(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"":                 "",
		"plain":            "plain",
		"default%2Eclicks": "default.clicks",
		"lower%2ecase":     "lower.case",
		"evt%2D123":        "evt-123", // an earlier build's form
		"%00%FF":           "\x00\xff",
		"a+b":              "a+b",
	} {
		got, err := keyenc.Unescape(in)
		require.NoError(t, err, "%q", in)
		assert.Equal(t, want, got, "%q", in)
	}
	for _, bad := range []string{"%", "%2", "a%2Gb", "%%41", "x%"} {
		_, err := keyenc.Unescape(bad)
		require.Error(t, err, "%q", bad)
	}
}

func TestJoinSplit(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "acme/clicks/evt-123", keyenc.Join('/', "acme", "clicks", "evt-123"))
	assert.Equal(t, "a%2Fb/c", keyenc.Join('/', "a/b", "c"), "a separator inside a field is escaped")
	assert.Equal(t, "a..", keyenc.Join('.', "a", "", ""))
	assert.Equal(t, "", keyenc.Join('.', ""), "one empty field")
	assert.Equal(t, "p:a", string(keyenc.AppendJoin([]byte("p:"), '/', "a")))

	for _, fields := range [][]string{{"acme", "a/b", "id"}, {"", "", ""}, {""}, {"%", "/", "%2F"}, {"x"}} {
		got, err := keyenc.Split(keyenc.Join('/', fields...), '/')
		require.NoError(t, err)
		assert.Equal(t, fields, got)
	}
	_, err := keyenc.Split("a/%zz", '/')
	require.Error(t, err)
}

func TestJoin_RefusesZeroFields(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() { keyenc.Join('/') })
	assert.Panics(t, func() { keyenc.AppendJoin(nil, '/') })
}

// A separator Escape could write, or one outside ASCII, is refused by Join
// and Split alike; every other byte separates.
func TestSeparators(t *testing.T) {
	t.Parallel()
	for b := range 256 {
		sep := byte(b)
		if keyenc.Escape(string([]byte{sep})) == string([]byte{sep}) || sep == '%' || sep >= 0x80 {
			assert.Panics(t, func() { keyenc.Join(sep, "x") }, "%#x", b)
			assert.Panics(t, func() { _, _ = keyenc.Split("x", sep) }, "%#x", b)
			continue
		}
		fields := []string{"a", string([]byte{sep}), "b" + string([]byte{sep}) + "c"}
		got, err := keyenc.Split(keyenc.Join(sep, fields...), sep)
		require.NoError(t, err, "%#x", b)
		assert.Equal(t, fields, got, "%#x", b)
	}
}

func FuzzEscapeRoundTrip(f *testing.F) {
	for _, s := range []string{"", "a.b", "\x00", "café", "%", "a/b c", "b-c", "b%2Dc"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		enc := keyenc.Escape(s)
		require.Equal(t, strings.ReplaceAll(v010Escape(s), "%2D", "-"), enc)
		require.False(t, strings.ContainsAny(enc, "./:{}|# *>\x00"), enc)
		dec, err := keyenc.Unescape(enc)
		require.NoError(t, err)
		require.Equal(t, s, dec)
	})
}
