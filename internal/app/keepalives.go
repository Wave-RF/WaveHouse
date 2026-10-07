package app

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// keepalives is one keepalive wheel per served tenant (#597), each turning
// at its own tenant's stream.keepalive_interval over its own
// stream.keepalive_buckets, so no tenant's settings set another's cadence.
// Reconciled from the settings registry's AfterAdopt hook, as discoveries
// is: a newly served tenant gets a wheel, a served one has its wheel
// reshaped in place — its open streams carried over, and untouched when its
// settings did not move — and a tenant no longer served, rejected or removed,
// has its wheel stopped and dropped, its streams being ended by the hub
// (Hub.Prune). The wheels turn under run, the keepalive component's loop: one
// built before Run holds its streams until then. A lookup is lock-free, but
// for the one that finds a served tenant without a wheel yet (For).
type keepalives struct {
	tenants *settings.Registry
	// shape reads a tenant's wheel settings: (*settings.Store).Keepalive.
	shape func(*settings.Store) (period time.Duration, buckets int)

	mu  sync.Mutex // serializes reconcile with run's start and stop
	cur atomic.Pointer[map[tenant.ID]*tenantWheel]
	// ctx is the loops' parent, the keepalive component's: nil until Run
	// starts it. Under mu.
	ctx context.Context
}

// tenantWheel is one tenant's wheel and, once it turns, its loop. Never
// changed once it is in the map: turn replaces it.
type tenantWheel struct {
	wheel *stream.Heartbeater
	// cancel stops the loop and done is closed when it has returned; both
	// nil for a wheel that does not turn yet.
	cancel context.CancelFunc
	done   chan struct{}
}

func newKeepalives(tenants *settings.Registry, shape func(*settings.Store) (time.Duration, int)) *keepalives {
	k := &keepalives{tenants: tenants, shape: shape}
	k.cur.Store(&map[tenant.ID]*tenantWheel{})
	return k
}

// For returns tenant id's wheel, or nil when it has none: the tenant is not
// served, and the hub is ending its streams. A served tenant always has one:
// a lookup that lands after a reload adopted the tenant and before this
// collection's hook ran reconciles first. Only that lookup does, so the
// streams of a tenant no longer served cost a reload nothing as they end.
func (k *keepalives) For(id tenant.ID) *stream.Heartbeater {
	if tw, ok := (*k.cur.Load())[id]; ok {
		return tw.wheel
	}
	if _, served := k.tenants.For(id); !served {
		return nil
	}
	k.reconcile()
	if tw, ok := (*k.cur.Load())[id]; ok {
		return tw.wheel
	}
	return nil
}

// reconcile gives every served tenant a wheel in the shape its settings ask
// for, built for a tenant without one and reshaped in place otherwise, and
// stops the wheel of every tenant no longer served.
func (k *keepalives) reconcile() {
	k.mu.Lock()
	defer k.mu.Unlock()
	cur := *k.cur.Load()
	next := make(map[tenant.ID]*tenantWheel, len(cur))
	for id, store := range k.tenants.All() {
		period, buckets := k.shape(store)
		tw, ok := cur[id]
		if ok {
			tw.wheel.Reconfigure(period, buckets)
		} else {
			tw = &tenantWheel{wheel: stream.NewHeartbeater(period, buckets)}
		}
		next[id] = k.turn(tw)
	}
	k.cur.Store(&next)
	for id, tw := range cur {
		if _, served := next[id]; !served {
			tw.stop()
		}
	}
}

// turn returns tw with its loop running under run's context: tw itself when
// it already turns, and when no loop may start — before Run, or once the
// stop has begun. Under mu.
func (k *keepalives) turn(tw *tenantWheel) *tenantWheel {
	if tw.done != nil || k.ctx == nil || k.ctx.Err() != nil {
		return tw
	}
	ctx, cancel := context.WithCancel(k.ctx)
	turning := &tenantWheel{wheel: tw.wheel, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(turning.done)
		turning.wheel.Run(ctx)
	}()
	return turning
}

// stop ends tw's loop, when it has one, and waits for it to return: at once,
// since a wheel waits on nothing but its ticker, so the reload holding the
// caller is not held up.
func (tw *tenantWheel) stop() {
	if tw.done == nil {
		return
	}
	tw.cancel()
	<-tw.done
}

// run turns every wheel until ctx is done — the ones built before it, and
// through reconcile the ones a reload builds from here on — and returns once
// each has stopped.
func (k *keepalives) run(ctx context.Context) {
	k.mu.Lock()
	k.ctx = ctx
	k.mu.Unlock()
	k.reconcile()

	<-ctx.Done()
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, tw := range *k.cur.Load() {
		tw.stop()
	}
}
