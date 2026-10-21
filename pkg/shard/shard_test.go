package shard_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/pkg/shard"
)

// Both are pinned: their results are routed on and stored, so a change moves
// keys. The values were derived apart from this code (the paper's algorithm
// and splitmix64's finalizer, in another language), not by running it.
func TestGolden(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []int{0, 6, 87, 520}, []int{shard.Jump(0, 1), shard.Jump(1, 10), shard.Jump(0xdeadbeef, 128), shard.Jump(256, 1024)})
	assert.Equal(t, []uint64{0, 0x5692161d100b05e5, 0x4e062702ec929eea}, []uint64{shard.Mix(0), shard.Mix(1), shard.Mix(0xdeadbeef)})
}

// Growing n by one moves a key only into the new bucket, and about 1/(n+1)
// of the keys; the keys spread evenly.
func TestJump_Consistent(t *testing.T) {
	t.Parallel()
	const keys = 20_000
	for _, n := range []int{1, 3, 16, 255} {
		moved := 0
		counts := make([]int, n+1)
		for i := range uint64(keys) {
			key := shard.Mix(i)
			b0, b1 := shard.Jump(key, n), shard.Jump(key, n+1)
			require.True(t, b0 >= 0 && b0 < n, "Jump(%d, %d) = %d", key, n, b0)
			counts[b1]++
			if b0 != b1 {
				moved++
				require.Equal(t, n, b1, "key %d moved to an old bucket", key)
			}
		}
		want := float64(keys) / float64(n+1)
		assert.InDelta(t, want, float64(moved), want*0.15, "%d→%d", n, n+1)
		for b, c := range counts {
			assert.InDelta(t, want, float64(c), want*0.5, "bucket %d of %d", b, n+1)
		}
	}
}
