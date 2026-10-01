package app

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// discoveries is one schema registry per served tenant (#583 story 6), each
// kept fresh by a loop of its own — RetryRefresh until its first success,
// then StartAutoRefresh at the tenant's cadence, first tick at a random
// offset so tenants adopted together do not refresh together. Reconciled
// from the settings registry's AfterAdopt hook, after the pools: a newly
// served tenant gets a registry over the pool it is on and a loop; a tenant
// no longer served — rejected or removed — has its loop stopped and its
// registry dropped, and starts over when it is back. A tenant a reload moved
// to another ClickHouse address or database starts over the same way (drop,
// #638): the schema it held describes tables it no longer reads, so its
// lookups answer not loaded, never the previous database's schema, until the
// first discovery of the new one. Every loop stops under App.Close within the
// release budget, the ones a reload stopped included. A lookup is one
// lock-free load.
type discoveries struct {
	// ctx is the loops' parent: the App's stop context.
	ctx context.Context
	// build makes a tenant's registry over the pool it is on.
	build func(tenant.ID, *settings.Store) *discovery.SchemaRegistry
	// onAttempt reports a loop's failed attempt before its first success,
	// and onLoaded the first success: what /livez is driven by.
	onAttempt func(tenant.ID, error)
	onLoaded  func(tenant.ID)
	// onRetire, when set, is told each registry a reload retires, under mu
	// and the reload lock: what it does must not wait on I/O.
	onRetire func(tenant.ID, *discovery.SchemaRegistry)

	mu  sync.Mutex // serializes reconcile, drop, adopt and close
	cur atomic.Pointer[map[tenant.ID]*tenantDiscovery]
	// retired is the loops a reload stopped that had not ended when last
	// looked at: a refresh in flight ends on its own time, and close waits
	// for it like the rest. Under mu.
	retired []*tenantDiscovery
}

// tenantDiscovery is one tenant's registry and its loop.
type tenantDiscovery struct {
	id       tenant.ID
	registry *discovery.SchemaRegistry
	cancel   context.CancelFunc
	done     chan struct{}
}

func newDiscoveries(ctx context.Context, build func(tenant.ID, *settings.Store) *discovery.SchemaRegistry, onAttempt func(tenant.ID, error), onLoaded func(tenant.ID), onRetire func(tenant.ID, *discovery.SchemaRegistry)) *discoveries {
	d := &discoveries{ctx: ctx, build: build, onAttempt: onAttempt, onLoaded: onLoaded, onRetire: onRetire}
	d.cur.Store(&map[tenant.ID]*tenantDiscovery{})
	return d
}

// For returns tenant id's registry, or nil when it has none: it is not
// served, or its registry is not built yet.
func (d *discoveries) For(id tenant.ID) *discovery.SchemaRegistry {
	if td, ok := (*d.cur.Load())[id]; ok {
		return td.registry
	}
	return nil
}

// reconcile starts a loop, over a registry built now, for every served
// tenant without one, and stops the loop of every tenant no longer served.
func (d *discoveries) reconcile(tenants *settings.Registry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur := *d.cur.Load()
	next := maps.Clone(cur)
	served := map[tenant.ID]bool{}
	for id, store := range tenants.All() {
		served[id] = true
		if _, ok := next[id]; !ok {
			next[id] = d.start(id, d.build(id, store))
		}
	}
	for id, td := range cur {
		if !served[id] {
			d.retire(td)
			delete(next, id)
		}
	}
	d.cur.Store(&next)
}

// retire stops td's loop and keeps it for close to wait for, letting go of
// the retired loops that have ended since. Under mu.
func (d *discoveries) retire(td *tenantDiscovery) {
	td.cancel()
	if d.onRetire != nil {
		d.onRetire(td.id, td.registry)
	}
	d.retired = append(slices.DeleteFunc(d.retired, func(r *tenantDiscovery) bool {
		select {
		case <-r.done:
			return true
		default:
			return false
		}
	}), td)
}

// drop stops the loop of each of ids and drops its registry, so the
// reconcile that follows builds each a fresh one, whose loop discovers at
// once and retries until it succeeds. No I/O: the hook calling it holds the
// lock that serializes reloads. A tenant with no registry — new, or back
// after a rejection or removal — is left to that reconcile alone.
func (d *discoveries) drop(ids []tenant.ID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := maps.Clone(*d.cur.Load())
	for _, id := range ids {
		if td, ok := next[id]; ok {
			d.retire(td)
			delete(next, id)
		}
	}
	d.cur.Store(&next)
}

// adopt registers reg as tenant id's registry and starts its loop: for the
// registry boot refreshed synchronously before any loop ran.
func (d *discoveries) adopt(id tenant.ID, reg *discovery.SchemaRegistry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := maps.Clone(*d.cur.Load())
	next[id] = d.start(id, reg)
	d.cur.Store(&next)
}

// start runs reg's loop: the boot retry until the first success, skipped
// for a registry already loaded, then the periodic refresh.
func (d *discoveries) start(id tenant.ID, reg *discovery.SchemaRegistry) *tenantDiscovery {
	ctx, cancel := context.WithCancel(d.ctx)
	td := &tenantDiscovery{id: id, registry: reg, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(td.done)
		if !reg.Loaded() {
			err := reg.RetryRefresh(ctx, 2*time.Second, 60*time.Second, func(err error) { d.onAttempt(id, err) })
			if err != nil {
				// ctx cancelled before success — the process is stopping, or
				// the tenant is no longer served.
				return
			}
			d.onLoaded(id)
		}
		reg.StartAutoRefresh(ctx)
	}()
	return td
}

// close stops every loop and waits for them within ctx, the release budget:
// the served tenants', then the ones a reload stopped earlier.
func (d *discoveries) close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur := *d.cur.Swap(&map[tenant.ID]*tenantDiscovery{})
	for _, td := range cur {
		td.cancel()
	}
	loops := make([]*tenantDiscovery, 0, len(cur)+len(d.retired))
	for _, id := range slices.Sorted(maps.Keys(cur)) {
		loops = append(loops, cur[id])
	}
	loops = append(loops, d.retired...)
	for i, td := range loops {
		select {
		case <-td.done:
		case <-ctx.Done():
			// A later close waits for what this one did not see end.
			d.retired = loops[i:]
			return fmt.Errorf("schema discovery loop of tenant %s not stopped: %w", td.id, ctx.Err())
		}
	}
	d.retired = nil
	return nil
}
