package dedupe

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Factory builds one tenant's store, closed: Stores calls it the first time
// a tenant is named. It is the seam a shared backend slots into later
// (#583): a factory that puts the tenant in the key of one shared store
// changes nothing that holds the Stores.
type Factory func(id tenant.ID) *Managed

// Stores is one Managed store per tenant (#583 story 7), each following its
// own tenant's dedupe.enabled through Apply. A store is built on first use
// and forgotten by Retain once its tenant is no longer served; its data
// stays on disk either way, so a tenant whose folder comes back finds its
// seen ids where it left them.
type Stores struct {
	build Factory
	mu    sync.Mutex
	byID  map[tenant.ID]*Managed
}

// NewStores returns an empty Stores that builds each tenant's store with
// build.
func NewStores(build Factory) *Stores {
	return &Stores{build: build, byID: map[tenant.ID]*Managed{}}
}

// For returns id's store, building it closed on first use. So a tenant a
// reload adopted a moment ago has a store before the hook that follows its
// switch runs — one answering ErrDisabled, the reload-window semantics
// Managed already has — rather than none.
func (s *Stores) For(id tenant.ID) *Managed {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.byID[id]
	if !ok {
		m = s.build(id)
		s.byID[id] = m
	}
	return m
}

// Retain closes and forgets every store whose tenant keep does not name — a
// tenant the registry no longer serves — and touches nothing on disk. The
// map is edited under the lock and the stores closed outside it, so one
// tenant's close (a Pebble close waits on its flushes and compactions)
// never stalls another tenant's lookup. The close failures are joined; the
// stores are forgotten either way. A store For builds for a dropped tenant
// meanwhile is closed and stays so — only the reconcile that called Retain
// opens one — so no directory is ever open twice.
func (s *Stores) Retain(keep func(tenant.ID) bool) error {
	s.mu.Lock()
	dropped := make(map[tenant.ID]*Managed)
	for id, m := range s.byID {
		if !keep(id) {
			dropped[id] = m
			delete(s.byID, id)
		}
	}
	s.mu.Unlock()
	return closeAll(dropped)
}

// Stats sums the open stores' metrics — the process's Pebble footprint,
// which is what the system gauges report — and is nil while no store is
// open, so the scraper skips the gauges as it does for one closed store. The
// stores are read outside the lock: a scrape waiting on one tenant's
// opening store stalls no other tenant's lookup.
func (s *Stores) Stats() map[string]int64 {
	s.mu.Lock()
	stores := slices.Collect(maps.Values(s.byID))
	s.mu.Unlock()
	var sum map[string]int64
	for _, m := range stores {
		stats := m.Stats()
		if stats == nil {
			continue
		}
		if sum == nil {
			sum = make(map[string]int64, len(stats))
		}
		for k, v := range stats {
			sum[k] += v
		}
	}
	return sum
}

// Close closes every store, in id order, and reports the failures joined.
// Safe to call more than once.
func (s *Stores) Close() error {
	s.mu.Lock()
	stores := maps.Clone(s.byID)
	s.mu.Unlock()
	return closeAll(stores)
}

// closeAll closes the stores in id order and reports the failures joined.
func closeAll(stores map[tenant.ID]*Managed) error {
	var errs []error
	for _, id := range slices.Sorted(maps.Keys(stores)) {
		if err := stores[id].Close(); err != nil {
			errs = append(errs, fmt.Errorf("tenant %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}
