package cache

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
)

// VersionManager handles the safe tracking of table + scope versioning.
// It uses a standard map because versions must NEVER be evicted under memory pressure.
// TODO: this potentially could be bad/dangerous with a low amount of RAM available/high memory pressure AND a TON of tables/scopes per table... will need to work out eventually
type VersionManager struct {
	mu sync.RWMutex

	tableVersions     map[string]uint64 // <table>                         -> table_version
	namespaceVersions map[string]uint64 // <table>.<table_version>.<scope> -> namespace_version

	conn *nats.Conn
}

// NewVersionManager initializes the thread-safe version store.
// Optionally initialized with a NATS connection, so that each version manager on every distributed server can keep in sync – NOT IMPLEMENTED, just wired in
func NewVersionManager(conn *nats.Conn) *VersionManager {
	return &VersionManager{
		tableVersions:     make(map[string]uint64),
		namespaceVersions: make(map[string]uint64),
		conn:              conn,
	}
}

type Namespace struct {
	Table string
	Scope string
}

// namespaceKeyLocked builds the namespace-table key; caller must hold vm.mu.
func (vm *VersionManager) namespaceKeyLocked(table, scope string) string {
	return fmt.Sprintf("%s.%d.%s", table, vm.tableVersions[table], scope)
}

// NamespaceKey renders the namespace-table key for (table, scope) at the table's
// current version: "<table>.<table_version>.<scope>" (scopeless scope is "", so
// e.g. "<table>.<v>.").
func (vm *VersionManager) NamespaceKey(table, scope string) string {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	return vm.namespaceKeyLocked(table, scope)
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
		nsKey := vm.namespaceKeyLocked(d.Table, d.Scope)
		segs[i] = fmt.Sprintf("%s.%d", nsKey, vm.namespaceVersions[nsKey])
		vm.mu.RUnlock()
	}
	sort.Strings(segs)
	return sha + "|" + strings.Join(segs, "|")
}

// BumpTable advances a table's version, orphaning every namespace — and every
// cached query — that depends on the table, in one step (the whole-table nuke).
func (vm *VersionManager) BumpTable(table string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.tableVersions[table]++
}

// BumpNamespace advances one (table, scope) namespace plus the table's whole-table
// (empty-scope) view, since a write to a named scope also changes the whole-table
// result; other scopes' cached queries stay valid.
func (vm *VersionManager) BumpNamespace(table, scope string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.namespaceVersions[vm.namespaceKeyLocked(table, scope)]++
	if scope != "" {
		vm.namespaceVersions[vm.namespaceKeyLocked(table, "")]++
	}
}
