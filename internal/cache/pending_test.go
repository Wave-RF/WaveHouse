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

// A lookup is held by a bump owed on any of its token keys, including the
// tenant token a set past its maximum collapses to.
func TestPendingBumps_OwesAny(t *testing.T) {
	t.Parallel()
	events := tokenKeys("wh", "acme", []Namespace{{Tenant: "acme", Table: "events", Scope: "org_1"}})
	orders := tokenKeys("wh", "acme", []Namespace{{Tenant: "acme", Table: "orders"}})
	globex := tokenKeys("wh", "globex", []Namespace{{Tenant: "globex", Table: "events"}})
	p := newPendingBumps("wh", 3)
	assert.False(t, p.owesAny(events))

	p.add("acme", bumpKeys("wh", Namespace{Tenant: "acme", Table: "events", Scope: "org_1"})...)
	assert.True(t, p.owesAny(events))
	assert.False(t, p.owesAny(orders), "another table's lookups are not held")
	assert.False(t, p.owesAny(globex))

	p.add("acme", "wh:{acme}:B:a", "wh:{acme}:B:b") // past the maximum
	assert.Equal(t, map[string]uint64{"wh:{acme}:T": 2}, p.snapshot())
	assert.True(t, p.owesAny(orders), "collapsed to the tenant token, which every lookup of the tenant reads")
	assert.True(t, p.owesAny(tokenKeys("wh", "acme", nil)))
	assert.False(t, p.owesAny(globex))

	p.done(p.snapshot())
	assert.False(t, p.owesAny(events))
	assert.Zero(t, p.len())
}
