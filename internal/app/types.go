package app

import (
	"sync"
	"sync/atomic"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// binder is what typeBindings drives: *typelayer.Engine, or a test's
// recorder.
type binder interface {
	Bind(id tenant.ID, serverVersion, serverTZ string, tables []*discovery.TableSchema)
	Forget(id tenant.ID)
}

// typeBindings ties each tenant's schema registry to the process's one type
// layer: every successful refresh of the registry rebinds the tenant's
// compiled tables, and the registry's retirement forgets them.
//
// A registry is retired under the reload lock while a refresh of it may be
// in flight — its loop is cancelled, not joined — and a refresh past its
// last query still runs its hooks. Unguarded, that late bind would bring a
// removed tenant's tables back for good, or, for a tenant a reload moved
// (#638), land the previous database's schema over the one its fresh
// registry bound. So each tenant has one owning registry, and only the
// owner's refreshes bind it.
type typeBindings struct {
	eng binder

	mu      sync.Mutex
	tenants map[tenant.ID]*typeBinding
}

// typeBinding is one tenant's: kept for the process lifetime, so the
// registries a tenant has over time share one bindMu.
type typeBinding struct {
	// bindMu serializes the tenant's binds across its registries, so a
	// retired registry's bind cannot interleave with its successor's.
	bindMu sync.Mutex
	// owner is the registry whose refreshes bind the tenant; nil once it is
	// retired and before its successor is attached.
	owner atomic.Pointer[discovery.SchemaRegistry]
}

func newTypeBindings(eng binder) *typeBindings {
	return &typeBindings{eng: eng, tenants: map[tenant.ID]*typeBinding{}}
}

func (b *typeBindings) of(id tenant.ID) *typeBinding {
	b.mu.Lock()
	defer b.mu.Unlock()
	tb, ok := b.tenants[id]
	if !ok {
		tb = &typeBinding{}
		b.tenants[id] = tb
	}
	return tb
}

// attach makes reg tenant id's owning registry and binds the tenant from
// each of its successful refreshes. Call it before reg's first refresh, so
// the boot refresh binds too.
func (b *typeBindings) attach(id tenant.ID, reg *discovery.SchemaRegistry) {
	tb := b.of(id)
	tb.owner.Store(reg)
	reg.OnRefresh(func(serverVersion, serverTZ string, tables []*discovery.TableSchema) {
		tb.bindMu.Lock()
		defer tb.bindMu.Unlock()
		if tb.owner.Load() != reg {
			return // a refresh that outlived its registry's retirement
		}
		b.eng.Bind(id, serverVersion, serverTZ, tables)
		if tb.owner.Load() != reg {
			// Retired mid-bind: its Forget may have run before this bind
			// created the tenant afresh. Forgotten again here, before a
			// successor's first bind, which waits on bindMu.
			b.eng.Forget(id)
		}
	})
}

// detach retires reg as tenant id's owner and forgets the tenant's compiled
// tables, when reg still owns it. It waits on nothing — a bind in flight
// finishes on its own and forgets after itself — so the reload hooks that
// retire registries stay free of I/O.
func (b *typeBindings) detach(id tenant.ID, reg *discovery.SchemaRegistry) {
	if b.of(id).owner.CompareAndSwap(reg, nil) {
		b.eng.Forget(id)
	}
}
