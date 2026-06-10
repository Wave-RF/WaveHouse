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
	mu       sync.RWMutex
	versions map[string]uint64

	tableVersions     map[string]uint64 // <table>                         -> table_version
	namespaceVersions map[string]uint64 // <table>.<table_version>.<scope> -> namespace_version

	conn *nats.Conn
}

// NewVersionManager initializes the thread-safe version store.
// Optionally initialized with a NATS connection, so that each version manager on every distributed server can keep in sync – NOT IMPLEMENTED, just wired in
func NewVersionManager(conn *nats.Conn) *VersionManager {
	return &VersionManager{
		versions:          make(map[string]uint64),
		tableVersions:     make(map[string]uint64),
		namespaceVersions: make(map[string]uint64),
		conn:              conn,
	}
}

func (vm *VersionManager) GetCacheKey(queryHash, namespace, scope string) string {
	versionKey := generateVersionKey(namespace, scope)
	version := vm.GetVersion(versionKey)
	return fmt.Sprintf("%s.%d:%s", versionKey, version, queryHash)
}

// Returns 0 if it has never been set.
func (vm *VersionManager) GetVersion(versionKey string) uint64 {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	version := vm.versions[versionKey] // Go maps safely return the zero-value (0) if the key doesn't exist

	return version
}

// IncrementVersion bumps the version string, instantly invalidating previous cache keys.
func (vm *VersionManager) IncrementVersion(versionKey string) {
	// TODO: NATS Core broadcasting to keep keys in sync? Or just use L2 instead (just means round trip flights for versions w/o pipelines etc)
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.versions[versionKey]++
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
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	segs := make([]string, len(deps))
	for i, d := range deps {
		nsKey := vm.namespaceKeyLocked(d.Table, d.Scope)
		segs[i] = fmt.Sprintf("%s.%d", nsKey, vm.namespaceVersions[nsKey])
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
