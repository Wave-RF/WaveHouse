package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// queryCacheKey is consumed by structured_query.go and pipes.go, so its
// contract has to stay stable even though /v1/ops/query no longer caches.
// Table-driven so future collision regressions drop in as additional rows
// without growing the assertion flow.
func TestQueryCacheKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sqlA        string
		paramsA     []string
		sqlB        string
		paramsB     []string
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
			paramsB:     []string{"a"},
			expectEqual: false,
		},
		{
			name:        "embedded NUL byte does not collide with split params",
			sqlA:        "SELECT 1",
			paramsA:     []string{"foo\x00bar"},
			sqlB:        "SELECT 1",
			paramsB:     []string{"foo", "bar"},
			expectEqual: false,
		},
		{
			// Every parameter is a String on the wire now, so "42" and 42
			// ARE the same read. What must still stay apart is two values
			// whose CONCATENATION matches — the framing's job.
			name:        "adjacent params are not confusable with one joined param",
			sqlA:        "SELECT 1",
			paramsA:     []string{"4", "2"},
			sqlB:        "SELECT 1",
			paramsB:     []string{"42"},
			expectEqual: false,
		},
		{
			name:        "nil and empty slice params produce the same key",
			sqlA:        "SELECT 1",
			paramsA:     nil,
			sqlB:        "SELECT 1",
			paramsB:     []string{},
			expectEqual: true,
		},
		{
			// Constructed as an actual collision pair under the pre-#315
			// "raw sql + framed params" format: sqlA is byte-for-byte the
			// stream `("X", ["y"])` used to produce (the param frame is
			// 0x00 + an 8-byte BE length + the payload), so the two inputs
			// hashed identically. The 0x01 marker + 8-byte length prefix on
			// the sql section forces them apart.
			name:        "sql crafted to mimic a param-frame stream does not collide with shorter sql + real param",
			sqlA:        "X\x00\x00\x00\x00\x00\x00\x00\x00\x01y",
			paramsA:     nil,
			sqlB:        "X",
			paramsB:     []string{"y"},
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
