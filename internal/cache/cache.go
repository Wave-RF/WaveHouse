package cache

import (
	"context"
	"errors"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Entry is what a Lookup found. A nil Value is a miss.
type Entry struct {
	Value []byte
	TTL   time.Duration // remaining
}

// Snapshot is the dependency versions a Lookup observed. A caller takes it
// before choosing any input a bump invalidates — the tenant's connection as
// well as the rows its query reads — and Set files the result under it, so a
// bump that lands after the Lookup orphans the fill rather than re-homing
// what was read before the bump under the post-bump versions (#382). The
// zero Snapshot makes Set a no-op.
type Snapshot struct {
	key string // the backend's key for the entry at the observed versions
}

// ErrForeignDependency is a Lookup whose dependencies name a tenant other
// than the one it is for: a cached result is one tenant's, and so is every
// version it is filed under.
var ErrForeignDependency = errors.New("cache: dependency names another tenant")

// Cache provides versioned query-result storage with TTL support.
//
// Every entry is one tenant's and folds that tenant's version, so
// InvalidateTenant orphans all of it — a result with no dependencies (a pipe)
// included. A backend that cannot be reached is a miss on Lookup and a no-op
// on Set; the caller runs its query either way.
type Cache interface {
	// Lookup reads the entry for sha under tenant id at deps' current
	// versions, and returns the snapshot of those versions for the Set that
	// fills it on a miss. sha is the caller's key for the SQL and params;
	// deps are the namespaces the result reads (one for a structured query,
	// none yet for a pipe), each of tenant id — any other is
	// ErrForeignDependency. An error is a miss with a zero Snapshot.
	Lookup(ctx context.Context, id tenant.ID, sha string, deps []Namespace) (Entry, Snapshot, error)

	// Set stores value under snap, the Snapshot a Lookup returned before any
	// input of the value was chosen. It returns an error only when the backend failed;
	// a value the cache declines to keep — too large, refused admission, a
	// non-positive ttl, or a zero snap — is not an error.
	Set(ctx context.Context, snap Snapshot, value []byte, ttl time.Duration) error

	// TODO: option to prefetch pipes when invalidated?
	// TODO: AST query builder needs to give us a deterministic key or bypass cache entirely

	// Invalidate bumps the version for each namespace, orphaning every cached query
	// that depends on it. A namespace with an empty Scope bumps the whole table
	// (every scope); a non-empty Scope bumps just that scope plus the whole-table
	// view. A bump reaches the namespace's tenant alone: the same table under
	// another tenant keeps its versions. Returns the number of namespaces processed.
	Invalidate(ctx context.Context, namespaces []Namespace) (uint64, error)

	// InvalidateTenant orphans every cached result of one tenant in one step
	// — every table and scope, bumped or not, and every pipe result — for a
	// tenant that comes back after an absence from the invalidation fan-out
	// (its settings folder rejected or removed, #583 story 6), stale by every
	// insert it missed, or that moved to another ClickHouse address or
	// database, whose cached results were read from other tables.
	InvalidateTenant(ctx context.Context, id tenant.ID) error

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
