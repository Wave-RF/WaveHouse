package cache

// Hooks for the integration tests in package cache_test.

// Pending reports how many token bumps r still owes the server.
func Pending(r *RedisCache) int { return r.pending.len() }

// Bypassed reports whether r is skipping the server.
func Bypassed(r *RedisCache) bool { return r.bypassed() }

// DecodedFactor is how many times MaxValueBytes a value may decompress to.
const DecodedFactor = decodedFactor
