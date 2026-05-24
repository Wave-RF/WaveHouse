package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionManager_GetVersion(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager(nil)

	// Should return 0 for uninitialized keys
	assert.Equal(t, uint64(0), vm.GetVersion("non_existent_table"))
}

func TestVersionManager_IncrementVersion(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager(nil)

	// Increment and check
	vm.IncrementVersion("users")
	assert.Equal(t, uint64(1), vm.GetVersion("users"))

	// Increment again
	vm.IncrementVersion("users")
	assert.Equal(t, uint64(2), vm.GetVersion("users"))
}

func TestVersionManager_GetCacheKey(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager(nil)

	// Default version (0)
	key1 := vm.GetCacheKey("hash123", "users", "org_1")
	assert.Equal(t, "users.org_1.0:hash123", key1)

	// Incrementing the version should change the resulting cache key
	vm.IncrementVersion("users.org_1")
	key2 := vm.GetCacheKey("hash123", "users", "org_1")
	assert.Equal(t, "users.org_1.1:hash123", key2)

	// Global scope key
	keyGlobal := vm.GetCacheKey("hash456", "users", "")
	assert.Equal(t, "users.0:hash456", keyGlobal)
}
