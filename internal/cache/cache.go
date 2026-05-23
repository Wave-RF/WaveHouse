package cache

import (
	"context"
	"fmt"
	"time"
)

// Cache provides key-value storage with TTL support.
type Cache interface {
	// Get retrieves a value and its remaining TTL. Returns nil, 0, nil on miss.
	GetQuery(ctx context.Context, key string, table string, scope string) ([]byte, time.Duration, error)

	// Set stores a value with the given TTL.
	// TODO: TTL should be set based on query execution time
	SetQuery(ctx context.Context, key string, table string, scope string, value []byte, ttl time.Duration) error

	GetPipe(ctx context.Context, key string) ([]byte, time.Duration, error)

	SetPipe(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// TODO: option to prefetch pipes when invalidated?
	// TODO: AST query builder needs to give us a deterministic key or bypass cache entirely

	// Version Registry
	// Version operations accept a scope (e.g., "org_id:123")
	// If scope is empty, it bumps the global table version

	// InvalidateCache invalidates the cache for a given table and set of scopes. If scopes is empty, it invalidates the global table version.
	InvalidateCache(ctx context.Context, table string, scopes map[string]struct{}) (uint64, error)

	// TODO: for local cache, we can just store the versions in memory, but for distributed/L2 cache, we will need to be able to either have stored procedures/pipelines etc to query them and attach them to a query, or sync them to each edge api server.

	// Close releases resources.
	Close() error
}

// our tunable function to determine a cache entry's TTL based on how long its queryTime took
func QueryTimeToTTL(queryTime time.Duration) time.Duration {
	// need more real-world data/metrics in order to better refine this, and likely an override option for easy local testing per-deployment
	// for now its just a made up equation

	// if we want 50ms --> 5 minutes, we multiply by 6000
	return queryTime * 6000
}

func generateInvalidationKeys(table string, scopes map[string]struct{}) []string {
	if len(scopes) == 0 {
		return []string{table}
	}

	var keys []string
	for scope := range scopes {
		keys = append(keys, generateVersionKey(table, scope))
	}
	return keys
}

func generateVersionKey(table string, scope string) string {
	if scope == "" {
		return table
	}
	return fmt.Sprintf("%s.%s", table, scope)
}

type TableScope struct {
	TableName string
	Scope     string // e.g., "org_id:123" or "" for global
}
