package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// LocalCache is an L1 in-process cache backed by Ristretto.
type LocalCache struct {
	cache          *ristretto.Cache[string, []byte]
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
	vm := NewVersionManager(nil)
	return &LocalCache{cache: cache, versionManager: vm}, nil
}

// Key folds sha (hash of SQL+params) with each dependency namespace at its
// current version — the point-in-time snapshot both Get and Set of one request
// must share (see the Cache interface contract). Used by BOTH structured
// queries (which pass one Namespace) and pipes (which pass several).
func (l *LocalCache) Key(sha string, deps []Namespace) string {
	return l.versionManager.QueryKey(sha, deps)
}

// Get looks up a cached query RESULT by its folded key. Returns nil, 0, nil on miss.
func (l *LocalCache) Get(_ context.Context, key string) ([]byte, time.Duration, error) {
	val, found := l.cache.Get(key)
	if !found {
		return nil, 0, nil
	}
	remaining, _ := l.cache.GetTTL(key)
	return val, remaining, nil
}

// Set stores a query result under the folded key snapshotted before the query
// ran (see Cache.Key); a version bump since the snapshot simply orphans the
// entry — no future Get computes this key.
func (l *LocalCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if ok := l.cache.SetWithTTL(key, value, int64(len(value)), ttl); !ok {
		return fmt.Errorf("cache admission rejected for key %q", key)
	}
	return nil
}

// Invalidate bumps the version for each namespace, instantly orphaning every
// cached query that depends on it. An empty Scope bumps the whole table (every
// scope at once); a non-empty Scope bumps just that scope plus the whole-table
// view. Returns the number of namespaces processed.
//
// Each written table also fans out to its dependent views (SetDependents): a write
// to a base table whole-table-bumps every view reading it, so a pipe that folds a
// VIEW's namespace directly is evicted without the read path ever walking the view
// graph. The fan-out is whole-table (the view's result depends on the table, not a
// scope) and deduped, so a view fed by several written tables bumps once.
//
// This bumps exactly what it's given (plus the fan-out). A whole-table bump already
// subsumes every per-scope bump for the same table (the table version is embedded
// in every namespace key), so a caller that knows a whole-table bump is coming
// should drop the now-redundant scope entries itself — the ingest worker does this
// as it builds the batch, where it already loops once and knows it's a single table.
//
// Any non-empty batch also bumps the DATABASE version once (DatabaseVersionTable),
// continuing the subsumption hierarchy upward: a result folding only the database
// namespace — the dependency-resolution fallback — is evicted by any write, in O(1)
// instead of folding every base table.
func (l *LocalCache) Invalidate(_ context.Context, namespaces []Namespace) (uint64, error) {
	bumpedViews := make(map[string]struct{})
	for _, ns := range namespaces {
		if ns.Scope == "" {
			l.versionManager.BumpTable(ns.Table)
		} else {
			l.versionManager.BumpNamespace(ns.Table, ns.Scope)
		}
		for _, view := range l.versionManager.DependentsOf(ns.Table) {
			if _, done := bumpedViews[view]; done {
				continue
			}
			bumpedViews[view] = struct{}{}
			l.versionManager.BumpTable(view)
		}
	}
	if len(namespaces) > 0 {
		l.versionManager.BumpTable(DatabaseVersionTable)
	}
	return uint64(len(namespaces)), nil
}

// SetDependents installs the base-table -> dependent-view cascade used by
// Invalidate to fan a base-table write out to the views reading it. The schema
// registry pushes a fresh map on every content-changed refresh.
func (l *LocalCache) SetDependents(dependents map[string][]string) {
	l.versionManager.SetDependents(dependents)
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
