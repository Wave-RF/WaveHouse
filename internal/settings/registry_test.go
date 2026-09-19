package settings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

func TestRegistry_For(t *testing.T) {
	t.Parallel()
	store := newLoadedStore(t, nil)
	reg := NewRegistry(store)

	got, ok := reg.For(tenant.Default)
	require.True(t, ok)
	assert.Same(t, store, got)

	got, ok = reg.For(tenant.ID("acme"))
	assert.False(t, ok, "only the default tenant exists")
	assert.Nil(t, got)
}
