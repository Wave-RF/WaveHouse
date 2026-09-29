package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto/v2"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// LocalCache is an L1 in-process cache backed by Ristretto: one pool for
// every tenant (#583 story 8) — a heavier tenant holds more of it — with the
// tenant leading every key, so no entry is shared across tenants.
type LocalCache struct {
	cache          *ristretto.Cache[string, []byte]
	maxCost        int64
	versionManager *VersionManager
}

// NewLocal creates a new Ristretto-backed local cache.
func NewLocal(maxCost int64) (*LocalCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: max(maxCost/10, 1000),
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	vm := NewVersionManager()
	return &LocalCache{cache: cache, maxCost: maxCost, versionManager: vm}, nil
}

// Lookup reads a cached query RESULT by its sha (hash of SQL+params) and the
// namespaces it depends on, and snapshots the key at their current versions.
// Used by BOTH structured queries (which pass one Namespace) and pipes (none
// yet).
func (l *LocalCache) Lookup(_ context.Context, id tenant.ID, sha string, deps []Namespace) (Entry, Snapshot, error) {
	for _, d := range deps {
		if d.Tenant != id {
			return Entry{}, Snapshot{}, fmt.Errorf("%w: %q under %q", ErrForeignDependency, d.Tenant, id)
		}
	}
	key := l.versionManager.QueryKey(id, sha, deps)
	snap := Snapshot{key: key}
	val, found := l.cache.Get(key)
	if !found {
		return Entry{}, snap, nil
	}
	remaining, _ := l.cache.GetTTL(key)
	return Entry{Value: val, TTL: remaining}, snap, nil
}

// Set stores a query result under the key its Lookup snapshotted. Admission
// is asynchronous (see Wait), and Ristretto may still decline the value.
func (l *LocalCache) Set(_ context.Context, snap Snapshot, value []byte, ttl time.Duration) error {
	if snap.key == "" || ttl <= 0 || int64(len(value)) > l.maxCost {
		return nil
	}
	l.cache.SetWithTTL(snap.key, value, int64(len(value)), ttl)
	return nil
}

// Invalidate bumps the version for each namespace, instantly orphaning every
// cached query that depends on it. An empty Scope bumps the whole table (every
// scope at once); a non-empty Scope bumps just that scope plus the whole-table
// view. Returns the number of namespaces processed.
//
// This bumps exactly what it's given. A whole-table bump already subsumes every
// per-scope bump for the same table (every key that folds a scope version
// folds the table version too), so a caller that knows a whole-table bump is
// coming should drop the now-redundant scope entries itself — the ingest
// worker does this as it builds the batch, where it already loops once and
// knows it's a single table.
func (l *LocalCache) Invalidate(_ context.Context, namespaces []Namespace) (uint64, error) {
	for _, ns := range namespaces {
		if ns.Scope == "" {
			l.versionManager.BumpTable(ns.Tenant, ns.Table)
		} else {
			l.versionManager.BumpNamespace(ns)
		}
	}
	return uint64(len(namespaces)), nil
}

// InvalidateTenant orphans every cached result of tenant id, pipe results
// included: its version index is dropped, nothing enumerated (see
// VersionManager.BumpTenant).
func (l *LocalCache) InvalidateTenant(_ context.Context, id tenant.ID) error {
	l.versionManager.BumpTenant(id)
	return nil
}

// Prune drops the version index of every tenant served rejects, orphaning
// its entries as InvalidateTenant would, so a tenant removed or rejected at
// a reload stops holding memory (#262). The entries themselves go with
// their TTL or Ristretto's eviction.
func (l *LocalCache) Prune(served func(tenant.ID) bool) {
	l.versionManager.Prune(served)
}

// Wait blocks until all buffered writes have been applied.
// Exposed for testing; production callers rarely need this.
func (l *LocalCache) Wait() {
	l.cache.Wait()
}

func (l *LocalCache) Close() error {
	l.Wait()
	l.cache.Close()
	return nil
}
