package cache

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// VersionManager handles the safe tracking of table + scope versioning.
// It uses a standard map because versions must NEVER be evicted under memory pressure.
// TODO: this potentially could be bad/dangerous with a low amount of RAM available/high memory pressure AND a TON of tables/scopes per table... will need to work out eventually
type VersionManager struct {
	mu sync.RWMutex

	// tenantVersions leads every key of a tenant, so BumpTenant orphans the
	// tenant's every namespace and query in one step — the ones no bump ever
	// keyed included, which is what an enumeration of the maps would miss.
	tenantVersions    map[tenant.ID]uint64 // <tenant>                                                  -> tenant_version
	tableVersions     map[string]uint64    // <tenant>.<tenant_version>.<table>                         -> table_version
	namespaceVersions map[string]uint64    // <tenant>.<tenant_version>.<table>.<table_version>.<scope> -> namespace_version
}

// NewVersionManager initializes the thread-safe version store.
func NewVersionManager() *VersionManager {
	return &VersionManager{
		tenantVersions:    make(map[tenant.ID]uint64),
		tableVersions:     make(map[string]uint64),
		namespaceVersions: make(map[string]uint64),
	}
}

// Namespace is one (tenant, table, scope) a cached result depends on. The
// tenant leads every key built from it, so the same table under two tenants
// is two namespaces, versioned and bumped apart (#583 story 8).
type Namespace struct {
	Tenant tenant.ID
	Table  string
	Scope  string
}

// tableKeyLocked renders the table-versions key,
// "<tenant>.<tenant_version>.<table>"; caller must hold vm.mu. A tenant id
// cannot contain a dot and callers encode the table dot-free, so the tokens
// can never run together.
func (vm *VersionManager) tableKeyLocked(id tenant.ID, table string) string {
	return fmt.Sprintf("%s.%d.%s", id, vm.tenantVersions[id], table)
}

// namespaceKeyLocked builds the namespace-table key; caller must hold vm.mu.
func (vm *VersionManager) namespaceKeyLocked(ns Namespace) string {
	tk := vm.tableKeyLocked(ns.Tenant, ns.Table)
	return fmt.Sprintf("%s.%d.%s", tk, vm.tableVersions[tk], ns.Scope)
}

// NamespaceKey renders the namespace-table key for ns at its tenant's and
// table's current versions:
// "<tenant>.<tenant_version>.<table>.<table_version>.<scope>" (scopeless
// scope is "", so e.g. "<tenant>.0.<table>.<v>.").
func (vm *VersionManager) NamespaceKey(ns Namespace) string {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	return vm.namespaceKeyLocked(ns)
}

// QueryKey builds the queries-table key for a result that depends on deps: the
// query's sha (hash of SQL+params) folded with every dependency's namespace key
// AND its namespace version, so a bump of any dependency misses the key. A
// structured query passes one Namespace; a pipe passes several. Deps are sorted
// so their order never changes the key.
func (vm *VersionManager) QueryKey(sha string, deps []Namespace) string {
	segs := make([]string, len(deps))
	// Lock per dependency rather than across the whole loop: each dep's table +
	// namespace versions are read together (consistent for that dep), but we don't
	// hold the lock across all deps. A concurrent bump can land between deps, but the
	// key is already a racy snapshot (versions can move between building it and using
	// it), so cross-dep consistency buys nothing. Crucially, the sort/join run with
	// no lock held.
	for i, d := range deps {
		vm.mu.RLock()
		nsKey := vm.namespaceKeyLocked(d)
		segs[i] = fmt.Sprintf("%s.%d", nsKey, vm.namespaceVersions[nsKey])
		vm.mu.RUnlock()
	}
	sort.Strings(segs)
	return sha + "|" + strings.Join(segs, "|")
}

// BumpTable advances a tenant's table version, orphaning every namespace — and
// every cached query — that depends on the table, in one step (the whole-table
// nuke). The same table under another tenant is untouched.
func (vm *VersionManager) BumpTable(id tenant.ID, table string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.tableVersions[vm.tableKeyLocked(id, table)]++
}

// BumpTenant advances a tenant's version, orphaning its every namespace —
// and every cached query keyed by one — in one step (the whole-tenant
// nuke): every namespace key of the tenant carries the version, so nothing
// has to be enumerated, and a table no bump ever keyed is orphaned like the
// rest. Other tenants are untouched.
func (vm *VersionManager) BumpTenant(id tenant.ID) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.tenantVersions[id]++
}

// BumpNamespace advances one (tenant, table, scope) namespace plus the table's
// whole-table (empty-scope) view, since a write to a named scope also changes
// the whole-table result; other scopes' cached queries stay valid.
func (vm *VersionManager) BumpNamespace(ns Namespace) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.namespaceVersions[vm.namespaceKeyLocked(ns)]++
	if ns.Scope != "" {
		vm.namespaceVersions[vm.namespaceKeyLocked(Namespace{Tenant: ns.Tenant, Table: ns.Table})]++
	}
}
