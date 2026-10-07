package api

import (
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil/mutationtest"
	"github.com/stretchr/testify/assert"
)

// TestIsMutation runs the shared cases; the integration suite checks the
// same cases against ClickHouse's parser.
func TestIsMutation(t *testing.T) {
	t.Parallel()
	for _, tc := range mutationtest.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.Mutation, IsMutation(tc.SQL))
		})
	}
}

// TestIsMutation_ClickHouseWhitespace pins every character ClickHouse 26.6's
// lexer accepts as whitespace, each checked against a live server: ahead of a
// write it must not hide the verb, and ahead of a read it must not make one.
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
	assert.False(t, IsMutation("\u1680INSERT INTO t VALUES (1)"))
}
