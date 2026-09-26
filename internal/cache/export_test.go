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

// ZeroSnapshot reports whether s files nothing.
func ZeroSnapshot(s Snapshot) bool { return s.key == "" && s.tokens == nil }

// DecodedFactor is how many times MaxValueBytes a value may decompress to.
const DecodedFactor = decodedFactor
