package cache

import (
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

// VersionManager handles the safe tracking of table + scope versioning.
// It uses a standard map because versions must NEVER be evicted under memory pressure.
type VersionManager struct {
	mu       sync.RWMutex
	versions map[string]uint64
	conn     *nats.Conn
}

// NewVersionManager initializes the thread-safe version store.
// Optionally initialized with a NATS connection, so that each version manager on every distributed server can keep in sync – NOT IMPLEMENTED, just wired in
func NewVersionManager(conn *nats.Conn) *VersionManager {
	return &VersionManager{
		versions: make(map[string]uint64),
		conn:     conn,
	}
}

func (vm *VersionManager) GetCacheKey(queryHash, table, scope string) string {
	versionKey := generateVersionKey(table, scope)
	version := vm.GetVersion(versionKey)
	return fmt.Sprintf("%s.%d:%s", versionKey, version, queryHash)
}

// Returns 0 if it has never been set.
func (vm *VersionManager) GetVersion(versionKey string) uint64 {
	vm.mu.RLock()
	version := vm.versions[versionKey] // Go maps safely return the zero-value (0) if the key doesn't exist
	vm.mu.RUnlock()

	return version
}

// IncrementVersion bumps the version string, instantly invalidating previous cache keys.
func (vm *VersionManager) IncrementVersion(versionKey string) {
	// TODO: NATS Core broadcasting to keep keys in sync? Or just use L2 instead (just means round trip flights for versions w/o pipelines etc)
	vm.mu.Lock()
	vm.versions[versionKey]++
	vm.mu.Unlock()
}
