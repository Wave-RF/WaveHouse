package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// queryCacheKey is consumed by structured_query.go and pipes.go, so its
// contract has to stay stable even though /v1/admin/query no longer caches.
func TestQueryCacheKey_Deterministic(t *testing.T) {
	t.Parallel()
	k1 := queryCacheKey("SELECT 1", nil)
	k2 := queryCacheKey("SELECT 1", nil)
	assert.Equal(t, k1, k2)

	k3 := queryCacheKey("SELECT 1", []any{"a"})
	assert.NotEqual(t, k1, k3)

	k4 := queryCacheKey("SELECT 2", nil)
	assert.NotEqual(t, k1, k4)
}
