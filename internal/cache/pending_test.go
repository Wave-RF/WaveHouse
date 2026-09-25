package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPendingBumps_Coalesce(t *testing.T) {
	t.Parallel()
	p := newPendingBumps("wh", 10)
	p.add("acme", "k1", "k2")
	p.add("acme", "k1")
	assert.Equal(t, 2, p.len())

	snap := p.snapshot()
	p.done(snap)
	assert.Zero(t, p.len())
}

// A key added again after the snapshot a drain worked from stays owed: the
// drain's bump may have landed before the write the new add is for.
func TestPendingBumps_ReAddedKeyStays(t *testing.T) {
	t.Parallel()
	p := newPendingBumps("wh", 10)
	p.add("acme", "k1", "k2")
	snap := p.snapshot()
	p.add("acme", "k1")
	p.done(snap)
	assert.Equal(t, map[string]uint64{"k1": 2}, p.snapshot())
}

func TestPendingBumps_OverflowCollapsesToTenants(t *testing.T) {
	t.Parallel()
	p := newPendingBumps("wh", 3)
	p.add("acme", "wh:{acme}:B:a", "wh:{acme}:B:b")
	p.add("globex", "wh:{globex}:B:a")
	assert.Equal(t, 3, p.len())

	p.add("acme", "wh:{acme}:B:c")
	got := p.snapshot()
	assert.Len(t, got, 2)
	assert.Contains(t, got, "wh:{acme}:T")
	assert.Contains(t, got, "wh:{globex}:T")
}
