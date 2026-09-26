package cache

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// VersionManager is the invalidation index: one version per tenant, per
// (tenant, table) and per (tenant, table, scope), in maps keyed by name
// alone, never by another version (#262). A bump overwrites a version in
// place, so the index holds one entry per live tenant, table and scope
// however often each is bumped, and forgetting a tenant releases all of it.
//
// A query key folds all three versions of each dependency, which gives the
// lattice: a table bump orphans every scope, a scope bump that scope and the
// whole-table view, and a tenant bump everything of the tenant's.
type VersionManager struct {
	mu sync.RWMutex

	// tenants holds each tenant's index from the first query key built for
	// it until the tenant is bumped or pruned.
	tenants map[tenant.ID]*tenantVersions

	// lastGen is the last generation handed to a tenant; see tenantVersions.gen.
	lastGen uint64
}

// tenantVersions is one tenant's slice of the index.
type tenantVersions struct {
	// gen is the tenant's version: unique within the process, so a tenant
	// forgotten and recreated can never fold a generation an entry was
	// cached under. That is what makes dropping the tenant's whole index a
	// safe bump.
	gen    uint64
	tables map[string]*tableVersions
}

// tableVersions is one table's version and its scopes'. A missing table or
// scope reads as 0: an entry is only ever removed together with a bump of
// the version above it (a table bump clears the scopes, a tenant bump
// drops the tables), so a 0 read after a removal never matches an entry
// cached before it.
type tableVersions struct {
	version uint64
	scopes  map[string]uint64
}

// Namespace is one (tenant, table, scope) a cached result depends on. The
// tenant leads every key built from it, so the same table under two tenants
// is two namespaces, versioned and bumped apart (#583 story 8). Table and
// Scope are raw names: the cache escapes them where it builds a key
// (keyenc), so no caller escapes and no separator in a name can run two
// fields together.
type Namespace struct {
	Tenant tenant.ID
	Table  string
	Scope  string
}

// NewVersionManager initializes the thread-safe version store.
func NewVersionManager() *VersionManager {
	return &VersionManager{tenants: make(map[tenant.ID]*tenantVersions)}
}

// QueryKey builds the queries-table key for tenant id's result that depends
// on deps: the query's sha (hash of SQL+params) folded with the tenant's
// version and, for every dependency, its tenant's, table's and scope's
// versions, so a bump of the tenant or of any dependency misses the key — a
// result with no deps (a pipe) is orphaned by BumpTenant too. A structured
// query passes one Namespace, a pipe none yet (#343). Deps are sorted so
// their order never changes the key, and every version is read under one
// lock, so the key is one consistent snapshot. The key nests two levels:
// the escaped sha and the '.'-joined tenant and dependency segments,
// separated by '|', which no escaped field or '.' join ever holds.
//
// The first key built for a tenant creates its index at a fresh generation.
func (vm *VersionManager) QueryKey(id tenant.ID, sha string, deps []Namespace) string {
	vm.mu.RLock()
	key, ok := vm.queryKeyLocked(id, sha, deps, false)
	vm.mu.RUnlock()
	if ok {
		return key
	}
	vm.mu.Lock()
	defer vm.mu.Unlock()
	key, _ = vm.queryKeyLocked(id, sha, deps, true)
	return key
}

// queryKeyLocked renders QueryKey with vm.mu held — for writing when create
// is set, which creates the index of each tenant the key names that has
// none; otherwise such a tenant reports !ok.
func (vm *VersionManager) queryKeyLocked(id tenant.ID, sha string, deps []Namespace, create bool) (string, bool) {
	index := func(id tenant.ID) (*tenantVersions, bool) {
		tv := vm.tenants[id]
		if tv == nil && create {
			tv = vm.newTenantLocked(id)
		}
		return tv, tv != nil
	}
	own, ok := index(id)
	if !ok {
		return "", false
	}
	segs := make([]string, len(deps))
	for i, d := range deps {
		tv, ok := index(d.Tenant)
		if !ok {
			return "", false
		}
		var table, scope uint64
		if t := tv.tables[d.Table]; t != nil {
			table, scope = t.version, t.scopes[d.Scope]
		}
		segs[i] = keyenc.Join('.', string(d.Tenant), strconv.FormatUint(tv.gen, 10), d.Table, strconv.FormatUint(table, 10), d.Scope, strconv.FormatUint(scope, 10))
	}
	sort.Strings(segs)
	return keyenc.Escape(sha) + "|" + keyenc.Join('.', string(id), strconv.FormatUint(own.gen, 10)) + "|" + strings.Join(segs, "|"), true
}

func (vm *VersionManager) newTenantLocked(id tenant.ID) *tenantVersions {
	vm.lastGen++
	tv := &tenantVersions{gen: vm.lastGen, tables: make(map[string]*tableVersions)}
	vm.tenants[id] = tv
	return tv
}

// tableLocked is the entry for a tenant's table, created at version 0, or
// nil when the tenant has no index: no key folds its current generation
// yet, so there is nothing a bump could orphan. Caller holds vm.mu for
// writing.
func (vm *VersionManager) tableLocked(id tenant.ID, table string) *tableVersions {
	tv := vm.tenants[id]
	if tv == nil {
		return nil
	}
	t := tv.tables[table]
	if t == nil {
		t = &tableVersions{}
		tv.tables[table] = t
	}
	return t
}

// BumpTable advances a tenant's table version, orphaning every namespace — and
// every cached query — that depends on the table, in one step (the whole-table
// nuke). The table's scope versions are dropped with it: every key they were
// folded into also folds the old table version. The same table under another
// tenant is untouched.
func (vm *VersionManager) BumpTable(id tenant.ID, table string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if t := vm.tableLocked(id, table); t != nil {
		t.version++
		t.scopes = nil
	}
}

// BumpNamespace advances one (tenant, table, scope) namespace plus the table's
// whole-table (empty-scope) view, since a write to a named scope also changes
// the whole-table result; other scopes' cached queries stay valid.
func (vm *VersionManager) BumpNamespace(ns Namespace) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	t := vm.tableLocked(ns.Tenant, ns.Table)
	if t == nil {
		return
	}
	if t.scopes == nil {
		t.scopes = make(map[string]uint64)
	}
	t.scopes[ns.Scope]++
	if ns.Scope != "" {
		t.scopes[""]++
	}
}

// BumpTenant orphans every cached query of a tenant, whatever its deps, in
// one step (the whole-tenant nuke), by dropping the tenant's index: the next
// key built for it gets a fresh generation, which no cached entry folds.
// Nothing has to be enumerated, a table no bump ever keyed is orphaned like
// the rest, and the index the tenant held is released. Other tenants are
// untouched.
func (vm *VersionManager) BumpTenant(id tenant.ID) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	delete(vm.tenants, id)
}

// Prune drops the index of every tenant keep rejects, as BumpTenant would,
// so a tenant that stops being served stops holding memory; one served again
// starts over at a fresh generation.
func (vm *VersionManager) Prune(keep func(tenant.ID) bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	for id := range vm.tenants {
		if !keep(id) {
			delete(vm.tenants, id)
		}
	}
}

// size is the number of versions the index holds, for tests.
func (vm *VersionManager) size() int {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	n := len(vm.tenants)
	for _, tv := range vm.tenants {
		n += len(tv.tables)
		for _, t := range tv.tables {
			n += len(t.scopes)
		}
	}
	return n
}
