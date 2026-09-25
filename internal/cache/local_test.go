package cache_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/testutil/cachetest"
)

const localMaxCost = 1 << 20

func newLocal(t *testing.T) cache.Cache {
	t.Helper()
	c, err := cache.NewLocal(localMaxCost)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestLocalCache_Conformance(t *testing.T) {
	t.Parallel()
	cachetest.Run(t, newLocal, cachetest.Options{MaxValueBytes: localMaxCost})
}
