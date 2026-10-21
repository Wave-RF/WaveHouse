// Package shard is the hashing WaveHouse places keys with: Jump maps a key to
// one of n buckets so that changing n moves as few keys as possible, and Mix
// derives a second, independent hash from one already taken. The external
// broker's partitions and shards, the ingest processes' shard assignment and
// the DynamoDB dedupe table's buckets are all placed with them, so a tool
// outside this module can compute where any of them lives. What they return
// is routed on and stored, so it never changes: tests pin it.
package shard

// Jump is the jump consistent hash (Lamping and Veach, 2014): key's bucket
// among n, for n ≥ 1. Growing n to n+1 moves a key only into bucket n, and
// each key with probability 1/(n+1), where a modulus would move nearly all.
func Jump(key uint64, n int) int {
	var b, j int64 = -1, 0
	for j < int64(n) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// Mix is splitmix64's finalizer: a hash of z with every bit of z mixed into
// every bit of the result, independent of z itself. Placing a key twice from
// one hash needs it: Jump(h, 2) = 1 forces Jump(h, 4) ≠ 0, so a second
// placement from the same h would leave buckets empty.
func Mix(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
