package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

func TestVersionManager_QueryKey(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	// sha | <tenant>.<gen> | <tenant>.<gen>.<table>.<tableVer>.<scope>.<scopeVer>;
	// acme's index is created by its first key, at generation 1.
	key := vm.QueryKey("acme", "hash123", []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}})
	assert.Equal(t, "hash123|acme.1|acme.1.users.0.org_1.0", key)

	// No deps (a pipe) still folds the tenant version.
	assert.Equal(t, "hash123|acme.1|", vm.QueryKey("acme", "hash123", nil))

	// Dependency order must not change the key (segments are sorted).
	deps1 := []Namespace{{Tenant: "acme", Table: "a"}, {Tenant: "acme", Table: "b"}}
	deps2 := []Namespace{{Tenant: "acme", Table: "b"}, {Tenant: "acme", Table: "a"}}
	assert.Equal(t, vm.QueryKey("acme", "h", deps1), vm.QueryKey("acme", "h", deps2))

	// The same sha and table under two tenants fold to two keys; so does the
	// same sha with no deps.
	assert.NotEqual(t,
		vm.QueryKey("acme", "h", []Namespace{{Tenant: "acme", Table: "users"}}),
		vm.QueryKey("globex", "h", []Namespace{{Tenant: "globex", Table: "users"}}))
	assert.NotEqual(t, vm.QueryKey("acme", "h", nil), vm.QueryKey("globex", "h", nil))

	// Reading keys is stable: nothing but a bump moves a version.
	assert.Equal(t, key, vm.QueryKey("acme", "hash123", []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}))
}

func TestVersionManager_BumpTable(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	users := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}
	orders := []Namespace{{Tenant: "acme", Table: "orders", Scope: "org_1"}}
	globexUsers := []Namespace{{Tenant: "globex", Table: "users", Scope: "org_1"}}

	usersBefore := vm.QueryKey("acme", "h", users)
	ordersBefore := vm.QueryKey("acme", "h", orders)
	globexBefore := vm.QueryKey("globex", "h", globexUsers)

	// Bumping a table changes the key for that tenant's table but leaves other
	// tables — and the same table under another tenant — alone.
	vm.BumpTable("acme", "users")
	assert.NotEqual(t, usersBefore, vm.QueryKey("acme", "h", users))
	assert.Equal(t, ordersBefore, vm.QueryKey("acme", "h", orders))
	assert.Equal(t, globexBefore, vm.QueryKey("globex", "h", globexUsers))
}

func TestVersionManager_BumpNamespace(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	scoped := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}
	wholeTable := []Namespace{{Tenant: "acme", Table: "users"}}
	otherScope := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_2"}}
	otherTenant := []Namespace{{Tenant: "globex", Table: "users", Scope: "org_1"}}

	scopedBefore := vm.QueryKey("acme", "h", scoped)
	wholeBefore := vm.QueryKey("acme", "h", wholeTable)
	otherBefore := vm.QueryKey("acme", "h", otherScope)
	otherTenantBefore := vm.QueryKey("globex", "h", otherTenant)

	// Bumping (acme, users, org_1) changes that scope AND the whole-table view,
	// but leaves every other scope — and the same scope under another tenant —
	// valid.
	vm.BumpNamespace(Namespace{Tenant: "acme", Table: "users", Scope: "org_1"})
	assert.NotEqual(t, scopedBefore, vm.QueryKey("acme", "h", scoped))
	assert.NotEqual(t, wholeBefore, vm.QueryKey("acme", "h", wholeTable))
	assert.Equal(t, otherBefore, vm.QueryKey("acme", "h", otherScope))
	assert.Equal(t, otherTenantBefore, vm.QueryKey("globex", "h", otherTenant))
}

// A table bump drops the table's scope versions, which then read as 0 again
// — safe only because every key a scope version was folded into also folds
// the table version the bump moved. Pinned so a table bump that forgot to
// advance the table version would revive the scoped entry.
func TestVersionManager_BumpTableDropsScopes(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()
	scoped := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}

	fresh := vm.QueryKey("acme", "h", scoped)
	vm.BumpNamespace(scoped[0])
	bumped := vm.QueryKey("acme", "h", scoped)
	vm.BumpTable("acme", "users")
	after := vm.QueryKey("acme", "h", scoped)

	assert.NotEqual(t, fresh, after)
	assert.NotEqual(t, bumped, after)
	assert.Equal(t, 2, vm.size(), "the tenant and its table; the scopes went with the table bump")
}

// TestVersionManager_BumpTenant: a tenant's every namespace is orphaned in
// one step — a table that was never bumped included — and no other tenant's
// is touched.
func TestVersionManager_BumpTenant(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	users := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}
	orders := []Namespace{{Tenant: "acme", Table: "orders"}}
	globexUsers := []Namespace{{Tenant: "globex", Table: "users", Scope: "org_1"}}
	vm.QueryKey("acme", "h", users)
	vm.BumpTable("acme", "users")

	usersBefore := vm.QueryKey("acme", "h", users)
	ordersBefore := vm.QueryKey("acme", "h", orders)
	globexBefore := vm.QueryKey("globex", "h", globexUsers)

	vm.BumpTenant("acme")
	assert.NotEqual(t, usersBefore, vm.QueryKey("acme", "h", users))
	assert.NotEqual(t, ordersBefore, vm.QueryKey("acme", "h", orders), "a table no bump ever keyed is orphaned too")
	assert.Equal(t, globexBefore, vm.QueryKey("globex", "h", globexUsers))

	pipeBefore := vm.QueryKey("acme", "h", nil)
	vm.BumpTenant("acme")
	assert.NotEqual(t, pipeBefore, vm.QueryKey("acme", "h", nil), "a result with no deps is orphaned too")
}

// Dropping a tenant's index is a bump only because the index it gets back
// never repeats a generation: every key built before any of these drops must
// differ from every key built after it. A counter per tenant restarting at 0
// fails this, reviving the first entry.
func TestVersionManager_GenerationsNeverRepeat(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()
	deps := []Namespace{{Tenant: "acme", Table: "users"}}
	seen := map[string]bool{}
	for i := range 100 {
		key := vm.QueryKey("acme", "h", deps)
		assert.False(t, seen[key], "round %d revived %s", i, key)
		seen[key] = true
		if i%2 == 0 {
			vm.BumpTenant("acme")
		} else {
			vm.Prune(func(tenant.ID) bool { return false })
		}
	}
}

// A bump of a tenant with no index is a no-op: no key folds its next
// generation yet, so nothing needs orphaning — and an insert still in flight
// for a tenant just pruned does not bring its index back.
func TestVersionManager_BumpWithoutIndex(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()
	vm.BumpTable("acme", "users")
	vm.BumpNamespace(Namespace{Tenant: "acme", Table: "users", Scope: "org_1"})
	vm.BumpTenant("acme")
	assert.Zero(t, vm.size())
}

func TestVersionManager_Prune(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()
	acme := []Namespace{{Tenant: "acme", Table: "users"}}
	globex := []Namespace{{Tenant: "globex", Table: "users"}}
	acmeBefore := vm.QueryKey("acme", "h", acme)
	globexBefore := vm.QueryKey("globex", "h", globex)
	vm.BumpTable("acme", "users")
	vm.BumpTable("globex", "users")
	acmeBumped := vm.QueryKey("acme", "h", acme)
	globexBumped := vm.QueryKey("globex", "h", globex)

	vm.Prune(func(id tenant.ID) bool { return id == "globex" })
	assert.Equal(t, 2, vm.size(), "globex and its table; acme released")
	assert.Equal(t, globexBumped, vm.QueryKey("globex", "h", globex), "a kept tenant is untouched")

	back := vm.QueryKey("acme", "h", acme)
	assert.NotEqual(t, acmeBefore, back, "a pruned tenant never revives what it cached")
	assert.NotEqual(t, acmeBumped, back)
	assert.NotEqual(t, globexBefore, globexBumped)
}

// The index holds one version per live tenant, table and scope, however often
// each is bumped (#262): the nested index this replaced kept every table and
// scope under every tenant version it had seen.
func TestVersionManager_SizeDoesNotGrowWithBumps(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()
	touch := func() {
		for _, id := range []tenant.ID{"acme", "globex"} {
			for _, table := range []string{"users", "orders"} {
				for _, scope := range []string{"", "org_1", "org_2"} {
					vm.QueryKey(id, "h", []Namespace{{Tenant: id, Table: table, Scope: scope}})
					vm.BumpNamespace(Namespace{Tenant: id, Table: table, Scope: scope})
				}
			}
		}
	}
	touch()
	settled := vm.size()
	assert.Equal(t, 2+2*2+2*2*3, settled, "two tenants, two tables each, three scopes each")

	for i := range 10_000 {
		switch i % 3 {
		case 0:
			vm.BumpTable("acme", []string{"users", "orders"}[i%2])
		case 1:
			vm.BumpTenant("globex")
		}
		touch()
		assert.LessOrEqual(t, vm.size(), settled)
	}
	assert.Equal(t, settled, vm.size())
}
