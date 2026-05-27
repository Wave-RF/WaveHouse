package cache

import (
	"context"
	"fmt"
	"time"
)

// Cache provides key-value storage with TTL support.
type Cache interface {
	// Get retrieves a value and its remaining TTL. Returns nil, 0, nil on miss.
	// key is the hashed cache key value, namespace is the table or pipe, and scope applies roles
	Get(ctx context.Context, key string, namespace string, scope string) ([]byte, time.Duration, error)

	// TODO: TTL should be set based on query execution time
	// Set stores a value with the given TTL.
	// key is the hashed cache key value, namespace is the table or pipe, and scope applies roles
	Set(ctx context.Context, key string, namespace string, scope string, value []byte, ttl time.Duration) error

	// TODO: option to prefetch pipes when invalidated?
	// TODO: AST query builder needs to give us a deterministic key or bypass cache entirely

	// Version Registry
	// Version operations accept a scope (e.g., "org_123")
	// If scope is empty, it bumps the global table version

	// InvalidateCache invalidates the cache for a given list of versionKeys
	InvalidateCache(ctx context.Context, versionKeys []string) (uint64, error)

	// TODO: for local cache, we can just store the versions in memory, but for distributed/L2 cache, we will need to be able to either have stored procedures/pipelines etc to query them and attach them to a query, or sync them to each edge api server.

	// Close releases resources.
	Close() error
}

// our tunable function to determine a cache entry's TTL based on how long its queryTime took
func QueryTimeToTTL(queryTime time.Duration) time.Duration {
	// need more real-world data/metrics in order to better refine this, and likely an override option for easy local testing per-deployment
	// for now its just a made up equation

	// Base multiplier: 1000x (e.g., 50ms -> 50s; 1s -> ~16 mins)
	ttl := queryTime * 1000

	const minTTL = 10 * time.Second
	const maxTTL = 1 * time.Hour

	if ttl < minTTL {
		return minTTL
	}
	if ttl > maxTTL {
		return maxTTL
	}

	return ttl
}

func generateVersionKey(namespace string, scope string) string {
	if scope == "" {
		return namespace
	}
	return fmt.Sprintf("%s.%s", namespace, scope)
}
