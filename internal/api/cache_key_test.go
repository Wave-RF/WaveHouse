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
