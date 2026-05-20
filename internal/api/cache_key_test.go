package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// queryCacheKey is consumed by structured_query.go and pipes.go, so its
// contract has to stay stable even though /v1/admin/query no longer caches.
// Table-driven so future collision regressions drop in as additional rows
// without growing the assertion flow.
func TestQueryCacheKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sqlA        string
		paramsA     []any
		sqlB        string
		paramsB     []any
		expectEqual bool
	}{
		{
			name:        "same sql, nil params → deterministic",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 1",
			paramsB:     nil,
			expectEqual: true,
		},
		{
			name:        "different sql → distinct",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 2",
			paramsB:     nil,
			expectEqual: false,
		},
		{
			name:        "param presence changes key",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 1",
			paramsB:     []any{"a"},
			expectEqual: false,
		},
		{
			name:        "embedded NUL byte does not collide with split params",
			sqlA:        "SELECT 1",
			paramsA:     []any{"foo\x00bar"},
			sqlB:        "SELECT 1",
			paramsB:     []any{"foo", "bar"},
			expectEqual: false,
		},
		{
			name:        "string and int with same textual value are distinct",
			sqlA:        "SELECT 1",
			paramsA:     []any{"42"},
			sqlB:        "SELECT 1",
			paramsB:     []any{42},
			expectEqual: false,
		},
		{
			name:        "nil and empty slice params produce the same key",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 1",
			paramsB:     []any{},
			expectEqual: true,
		},
		{
			// Constructed as an actual collision pair under the old
			// "raw sql + framed params" format. The param frame for
			// `"y"` is 0x00 + 8-byte BE length (0x1D = 29) +
			// `{"type":"string","value":"y"}` (29 bytes), so the byte
			// stream `("X", ["y"])` produces under the old framing is
			// `"X" + 0x00 + 0x00…0x1D + {"type":"string","value":"y"}`.
			// Setting sqlA to exactly those bytes and paramsA to nil
			// reproduces that stream — under the old framing the two
			// inputs hashed identically. The new 0x01-marker + 8-byte
			// length prefix on sql forces them apart.
			name:        "sql crafted to mimic a param-frame stream does not collide with shorter sql + real param",
			sqlA:        "X\x00\x00\x00\x00\x00\x00\x00\x00\x1d{\"type\":\"string\",\"value\":\"y\"}",
			paramsA:     nil,
			sqlB:        "X",
			paramsB:     []any{"y"},
			expectEqual: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := queryCacheKey(tt.sqlA, tt.paramsA)
			b := queryCacheKey(tt.sqlB, tt.paramsB)
			if tt.expectEqual {
				assert.Equal(t, a, b)
			} else {
				assert.NotEqual(t, a, b)
			}
		})
	}
}
