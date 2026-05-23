package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryTimeToTTL(t *testing.T) {
	t.Parallel()
	queryTime := 50 * time.Millisecond
	expectedTTL := queryTime * 6000

	assert.Equal(t, expectedTTL, QueryTimeToTTL(queryTime))
}

func TestGenerateVersionKey(t *testing.T) {
	t.Parallel()

	// Test empty scope
	assert.Equal(t, "users", generateVersionKey("users", ""))

	// Test populated scope
	assert.Equal(t, "users.org_123", generateVersionKey("users", "org_123"))
}

func TestGenerateInvalidationKeys(t *testing.T) {
	t.Parallel()

	// Test with no scopes
	keysEmpty := generateInvalidationKeys("users", nil)
	assert.Equal(t, []string{"users"}, keysEmpty, "should always include the global table key")

	// Test with scopes
	scopes := map[string]struct{}{
		"org_1": {},
		"org_2": {},
	}
	keys := generateInvalidationKeys("users", scopes)

	assert.Len(t, keys, 3)
	assert.Contains(t, keys, "users")
	assert.Contains(t, keys, "users.org_1")
	assert.Contains(t, keys, "users.org_2")
}
