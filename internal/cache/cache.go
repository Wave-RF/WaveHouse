package cache

import (
	"context"
	"time"
)

// Cache provides versioned query-result storage with TTL support.
type Cache interface {
	// Key folds sha (the hash of the SQL+params) with every dependency namespace at
	// its CURRENT version — a point-in-time snapshot. deps are the namespaces the
	// result depends on (one for a structured query, several for a pipe).
	//
	// A caller MUST compute the key once, BEFORE executing the underlying query,
	// and use that same key for both the Get and the Set of one request. Folding
	// versions at Set time instead would race Invalidate: a write that lands while
	// the query is executing bumps the versions, and the result — computed from
	// PRE-write data — would be stored under the POST-bump key and served as fresh
	// for its full TTL. With the snapshot key, a mid-flight bump merely orphans the
	// Set (stored under a key no future reader computes): wasted work, never a
	// stale read.
	Key(sha string, deps []Namespace) string

	// Get retrieves a cached query result and its remaining TTL by its folded key
	// (see Key). Returns nil, 0, nil on miss.
	Get(ctx context.Context, key string) ([]byte, time.Duration, error)

	// Set stores a query result under the folded key snapshotted by Key before the
	// query ran. Callers derive ttl from query execution time via QueryTimeToTTL;
	// tuning that curve with real-world metrics is the remaining work (see
	// QueryTimeToTTL).
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// TODO: option to prefetch pipes when invalidated?
	// TODO: AST query builder needs to give us a deterministic key or bypass cache entirely

	// Invalidate bumps the version for each namespace, orphaning every cached query
	// that depends on it. A namespace with an empty Scope bumps the whole table
	// (every scope); a non-empty Scope bumps just that scope plus the whole-table
	// view. A write also fans out to the namespace's dependents (SetDependents —
	// the views reading it and the MV TO targets it feeds), and any non-empty batch
	// bumps the DATABASE version once (DatabaseNamespace), so a result folding only
	// that one namespace is evicted by any write at all. Returns the number of
	// namespaces processed.
	Invalidate(ctx context.Context, namespaces []Namespace) (uint64, error)

	// SetDependents installs the base-table -> dependents cascade that Invalidate
	// fans out through, so a write to a base table also evicts pipes reading a view
	// over it or a table its attached MVs populate. Both sides are NATS-encoded. The
	// schema registry pushes a fresh map on every content-changed refresh; replaces
	// any prior one.
	SetDependents(dependents map[string][]string)

	// TODO: for local cache, we can just store the versions in memory, but for distributed/L2 cache, we will need to be able to either have stored procedures/pipelines etc to query them and attach them to a query, or sync them to each edge api server.

	// Close releases resources.
	Close() error
}

// UnresolvedDepsTTLCap bounds the lifetime of a cached result whose dependencies
// are not reliably version-invalidatable, so it must self-expire quickly instead.
// Applied as min(QueryTimeToTTL, cap) at the Set call sites — and since
// QueryTimeToTTL's minimum is 10s too, the cap effectively PINS such a result's
// TTL at 10s (a ceiling on lifetime, not a floor). It fires for: a pipe with an
// unresolved dependency (an unknown or not-yet-discovered table, an unfoldable
// view), a pipe reading an external source no table version can watch (a table
// function, a cross-database table, a non-local dictionary source), a pipe whose
// dependency set was dead-branch pruned (belt-and-suspenders), and a structured
// query targeting an UNFOLDABLE view (the refresh-time cascade never bumps it; a
// base table or foldable view is unaffected). A fixed in-code backstop, not a
// tuning knob; promote to config only if real data warrants.
const UnresolvedDepsTTLCap = 10 * time.Second

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
