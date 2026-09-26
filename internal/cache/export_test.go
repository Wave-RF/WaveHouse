package cache

import "github.com/redis/rueidis"

// Hooks for the integration tests in package cache_test.

// ClientOption is the rueidis option a RedisCache built from c dials with.
func ClientOption(c RedisConfig) (rueidis.ClientOption, error) {
	c, err := c.withDefaults()
	return c.clientOption(), err
}

// Pending reports how many token bumps r still owes the server.
func Pending(r *RedisCache) int { return r.pending.len() }

// Bypassed reports whether r is skipping the server.
func Bypassed(r *RedisCache) bool { return r.bypassed() }

// KeyPrefix is the prefix every key r writes leads with.
func KeyPrefix(r *RedisCache) string { return r.cfg.KeyPrefix }

// ZeroSnapshot reports whether s files nothing.
func ZeroSnapshot(s Snapshot) bool { return s.key == "" && s.tokens == nil }

// DecodedFactor is how many times MaxValueBytes a value may decompress to.
const DecodedFactor = decodedFactor

// Len counts the unexpired entries l holds, for the conformance suite's
// Options.Entries: no Lookup reads the key a zero snapshot would land under.
func (l *LocalCache) Len() int {
	n := 0
	l.cache.IterValues(func([]byte) bool {
		n++
		return false
	})
	return n
}
