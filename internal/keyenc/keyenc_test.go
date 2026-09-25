package keyenc_test

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
)

// v010Escape is the encoder v0.1.0 shipped as query.SafeEncodeNATS, copied
// verbatim: the reference Escape must match byte for byte.
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
		"a-b/c":          "a%2Db%2Fc",
		"a.*.>":          "a%2E%2A%2E%3E",
		"100%":           "100%25",
		"{acme}:x|y":     "%7Bacme%7D%3Ax%7Cy",
		"a\x00b":         "a%00b",
		"café":           "caf%C3%A9",
		"\xff":           "%FF",
		"tab\tnewline\n": "tab%09newline%0A",
		"#hash":          "%23hash",
		"ABCxyz_0189":    "ABCxyz_0189",
		"evt-123":        "evt%2D123",
		"日本":             "%E6%97%A5%E6%9C%AC",
	} {
		assert.Equal(t, want, keyenc.Escape(raw), "%q", raw)
		assert.Equal(t, want, string(keyenc.AppendEscape([]byte("x"), raw))[1:], "%q", raw)
	}
}

// Every byte value, alone and between kept bytes, encodes as v0.1.0 did.
func TestEscape_MatchesV010EveryByte(t *testing.T) {
	t.Parallel()
	for b := 0; b < 256; b++ {
		for _, s := range []string{string([]byte{byte(b)}), "a" + string([]byte{byte(b)}) + "Z"} {
			require.Equal(t, v010Escape(s), keyenc.Escape(s), "byte %#x", b)
		}
	}
}

func TestEscape_NoAllocWhenNothingToEscape(t *testing.T) {
	s := "events_2026"
	assert.Zero(t, testing.AllocsPerRun(100, func() { _ = keyenc.Escape(s) }))
}

func TestUnescape(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"":                 "",
		"plain":            "plain",
		"default%2Eclicks": "default.clicks",
		"lower%2ecase":     "lower.case",
		"left-as-is":       "left-as-is",
		"%00%FF":           "\x00\xff",
		"a+b":              "a+b",
	} {
		got, err := keyenc.Unescape(in)
		require.NoError(t, err, "%q", in)
		assert.Equal(t, want, got, "%q", in)
	}
	for _, bad := range []string{"%", "%2", "a%2Gb", "%%41", "x%"} {
		_, err := keyenc.Unescape(bad)
		require.ErrorIs(t, err, keyenc.ErrBadEscape, "%q", bad)
	}
}

func TestJoinSplit(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "acme/clicks/evt%2D123", keyenc.Join('/', "acme", "clicks", "evt-123"))
	assert.Equal(t, "a%2Fb/c", keyenc.Join('/', "a/b", "c"), "a separator inside a field is escaped")
	assert.Equal(t, "a..", keyenc.Join('.', "a", "", ""))
	assert.Equal(t, "p:a", string(keyenc.AppendJoin([]byte("p:"), '/', "a")))

	for _, fields := range [][]string{{"acme", "a/b", "id"}, {"", "", ""}, {"%", "/", "%2F"}, {"x"}} {
		got, err := keyenc.Split(keyenc.Join('/', fields...), '/')
		require.NoError(t, err)
		assert.Equal(t, fields, got)
	}
	_, err := keyenc.Split("a/%zz", '/')
	require.ErrorIs(t, err, keyenc.ErrBadEscape)

	for _, sep := range []byte{'a', 'Z', '5', '_', '%'} {
		assert.Panics(t, func() { keyenc.Join(sep, "x") }, "%q", sep)
	}
}

// Unescape accepts exactly what url.PathUnescape did, so readers moved onto
// it decode every subject they decoded before.
func FuzzUnescapeMatchesPathUnescape(f *testing.F) {
	for _, s := range []string{"", "a%2Eb", "%2e", "%", "%zz", "a+b", "%E6%97%A5", "100%25"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		want, wantErr := url.PathUnescape(s)
		got, err := keyenc.Unescape(s)
		if wantErr != nil {
			require.Error(t, err)
			return
		}
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}

func FuzzEscapeRoundTrip(f *testing.F) {
	for _, s := range []string{"", "a.b", "\x00", "café", "%", "a/b c"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		enc := keyenc.Escape(s)
		require.Equal(t, v010Escape(s), enc)
		require.False(t, strings.ContainsAny(enc, "./:{}|# *>\x00"), enc)
		dec, err := keyenc.Unescape(enc)
		require.NoError(t, err)
		require.Equal(t, s, dec)
	})
}
