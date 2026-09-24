package cache

import (
	"context"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Cache provides versioned query-result storage with TTL support.
type Cache interface {
	// Get retrieves a cached query result and its remaining TTL. sha is the
	// caller's key for the SQL+params, led by the tenant it was built for; deps
	// are the namespaces the result depends on (one for a structured query,
	// several for a pipe), each naming its tenant. Returns nil, 0, nil on miss.
	Get(ctx context.Context, sha string, deps []Namespace) ([]byte, time.Duration, error)

	// TODO: TTL should be set based on query execution time
	// Set stores a query result keyed by sha + its dependency namespaces.
	Set(ctx context.Context, sha string, deps []Namespace, value []byte, ttl time.Duration) error

	// TODO: option to prefetch pipes when invalidated?
	// TODO: AST query builder needs to give us a deterministic key or bypass cache entirely

	// Invalidate bumps the version for each namespace, orphaning every cached query
	// that depends on it. A namespace with an empty Scope bumps the whole table
	// (every scope); a non-empty Scope bumps just that scope plus the whole-table
	// view. A bump reaches the namespace's tenant alone: the same table under
	// another tenant keeps its versions. Returns the number of namespaces processed.
	Invalidate(ctx context.Context, namespaces []Namespace) (uint64, error)

	// InvalidateTenant orphans every cached query of one tenant that is keyed
	// by its tables in one step — every table and scope, bumped or not; a
	// pipe result names no table, so neither this nor any insert
	// invalidates it and it stays until its TTL expires (#343) — for a tenant that comes back after an absence from the
	// invalidation fan-out (its settings folder rejected or removed, #583
	// story 6), stale by every insert it missed, or that moved to another
	// ClickHouse address or database, whose cached results were read from
	// other tables.
	InvalidateTenant(ctx context.Context, id tenant.ID) error

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
