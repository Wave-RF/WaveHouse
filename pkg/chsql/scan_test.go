package chsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsMutation_ClickHouseWhitespace pins every character ClickHouse 26.6's
// lexer accepts as whitespace, each checked against a live server: ahead of a
// write it must not hide the verb, and ahead of a read it must not make one.
// The classifier's statement cases are internal/testutil/mutationtest, run
// from internal/api, which may import it.
func TestIsMutation_ClickHouseWhitespace(t *testing.T) {
	t.Parallel()
	spaces := []rune{' ', '\t', '\n', '\v', '\f', '\r', 0x85, 0xA0, 0x180E, 0x2028, 0x2029, 0x202F, 0x205F, 0x2060, 0x3000, 0xFEFF}
	for r := rune(0x2000); r <= 0x200D; r++ {
		spaces = append(spaces, r)
	}
	for _, r := range spaces {
		ws := string(r)
		assert.True(t, IsMutation(ws+"INSERT INTO t VALUES (1)"), "U+%04X before INSERT", r)
		assert.False(t, IsMutation(ws+"SELECT 1"), "U+%04X before SELECT", r)
		assert.True(t, IsMutation("WITH x AS (SELECT 1)"+ws+"INSERT INTO t SELECT * FROM x"), "U+%04X before a WITH's INSERT", r)
		assert.True(t, IsMutation("WITH 1 AS x INSERT"+ws+"INTO t SELECT x"), "U+%04X between a WITH's INSERT and INTO", r)
	}
	// Not whitespace to ClickHouse (it rejects the statement), so not skipped.
	assert.False(t, IsMutation(" INSERT INTO t VALUES (1)"))
}

// TestOpaque pins the byte ranges ClickHouse reads as text: every comment
// and quoted-token form its lexer knows, each closed, nested or unclosed as
// the lexer treats it, and the shapes that look like one but are not.
func TestOpaque(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
		want []Span
	}{
		{"plain", "SELECT 1", nil},
		{"line comment to the end of its line", "SELECT 1 -- a\nFROM t", []Span{{Start: 9, End: 14, Comment: true}}},
		{"line comment at the end", "SELECT 1 -- a", []Span{{Start: 9, End: 13, Comment: true}}},
		{"hash and slash comments", "# a\n// b\nSELECT 1", []Span{{Start: 0, End: 4, Comment: true}, {Start: 4, End: 9, Comment: true}}},
		{"block comment nests", "SELECT /* a /* b */ c */ 1", []Span{{Start: 7, End: 24, Comment: true}}},
		{"unclosed block comment runs to the end", "SELECT /* a", []Span{{Start: 7, End: 11, Comment: true}}},
		{"string literal, comment marker inside", "SELECT 'a -- b' AS s", []Span{{Start: 7, End: 15}}},
		{"doubled and escaped quotes stay inside", `SELECT 'it''s \' ok' AS s`, []Span{{Start: 7, End: 20}}},
		{"quoted identifiers", "SELECT \"a b\", `c -- d` FROM t", []Span{{Start: 7, End: 12}, {Start: 14, End: 22}}},
		{"heredoc", "SELECT $tag$ 'x' -- y $tag$ AS s", []Span{{Start: 7, End: 27}}},
		{"dollar inside a bareword opens no heredoc", "SELECT a$b$ FROM t WHERE c = $b$", nil},
		{"curly quotes", "SELECT ‘a’, “b” FROM t", []Span{{Start: 7, End: 14}, {Start: 16, End: 23}}},
		{"unclosed string runs to the end", "SELECT 'a", []Span{{Start: 7, End: 9}}},
		{"number led by a dot, then a string", "SELECT .5e3, 'x'", []Span{{Start: 13, End: 16}}},
		{"string after a comment line", "-- c\nSELECT 'a'", []Span{{Start: 0, End: 5, Comment: true}, {Start: 12, End: 15}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, Opaque(tt.sql))
		})
	}
}
