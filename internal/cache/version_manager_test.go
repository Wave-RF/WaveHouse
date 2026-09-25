package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

func TestVersionManager_NamespaceKey(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	// The tenant leads at its default version (0), then the table at its
	// default version (0); a scopeless namespace renders a trailing dot. The
	// flat directory's tenant is "0".
	assert.Equal(t, "acme.0.users.0.", vm.NamespaceKey(Namespace{Tenant: "acme", Table: "users"}))
	assert.Equal(t, "acme.0.users.0.org_1", vm.NamespaceKey(Namespace{Tenant: "acme", Table: "users", Scope: "org_1"}))
	assert.Equal(t, "0.0.users.0.", vm.NamespaceKey(Namespace{Tenant: tenant.Default, Table: "users"}))

	// Names arrive raw and are escaped into the key, so a dot or a space in
	// one is never read as the separator.
	assert.Equal(t, "acme.0.default%2Eclicks.0.org%2E1", vm.NamespaceKey(Namespace{Tenant: "acme", Table: "default.clicks", Scope: "org.1"}))
	assert.Equal(t, "acme.0.my%20table.0.", vm.NamespaceKey(Namespace{Tenant: "acme", Table: "my table"}))

	// The table version is embedded in every namespace key for that tenant's
	// table, so a BumpTable is reflected across all its scopes at once — and
	// nowhere else: the same table under another tenant keeps its version.
	vm.BumpTable("acme", "users")
	assert.Equal(t, "acme.0.users.1.", vm.NamespaceKey(Namespace{Tenant: "acme", Table: "users"}))
	assert.Equal(t, "acme.0.users.1.org_1", vm.NamespaceKey(Namespace{Tenant: "acme", Table: "users", Scope: "org_1"}))
	assert.Equal(t, "globex.0.users.0.", vm.NamespaceKey(Namespace{Tenant: "globex", Table: "users"}))

	// The tenant version leads every key of the tenant, so a BumpTenant moves
	// every table of acme's — the never-bumped orders table included — to a
	// fresh key space, at table version 0 again, and no other tenant's.
	vm.BumpTenant("acme")
	assert.Equal(t, "acme.1.users.0.", vm.NamespaceKey(Namespace{Tenant: "acme", Table: "users"}))
	assert.Equal(t, "acme.1.orders.0.", vm.NamespaceKey(Namespace{Tenant: "acme", Table: "orders"}))
	assert.Equal(t, "globex.0.users.0.", vm.NamespaceKey(Namespace{Tenant: "globex", Table: "users"}))
}

func TestVersionManager_QueryKey(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	// One dependency at default versions:
	// sha | <tenant>.<tenantVer> | <tenant>.<tenantVer>.<table>.<tableVer>.<scope>.<nsVer>.
	key := vm.QueryKey("acme", "hash123", []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}})
	assert.Equal(t, "hash123|acme.0|acme.0.users.0.org_1.0", key)

	// No deps (a pipe) still folds the tenant version.
	assert.Equal(t, "hash123|acme.0|", vm.QueryKey("acme", "hash123", nil))

	// The sha is a field like any other: escaped, so no '|' in it can pass
	// for the separator.
	assert.Equal(t, "acme%3Aquery%3Aab|acme.0|acme.0.my%20table.0..0",
		vm.QueryKey("acme", "acme:query:ab", []Namespace{{Tenant: "acme", Table: "my table"}}))

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
}

func TestVersionManager_BumpTable(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	users := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}
	orders := []Namespace{{Tenant: "acme", Table: "orders", Scope: "org_1"}}
	globexUsers := []Namespace{{Tenant: "globex", Table: "users", Scope: "org_1"}}

	usersBefore := vm.QueryKey(users[0].Tenant, "h", users)
	ordersBefore := vm.QueryKey(orders[0].Tenant, "h", orders)
	globexBefore := vm.QueryKey(globexUsers[0].Tenant, "h", globexUsers)

	// Bumping a table changes the key for that tenant's table but leaves other
	// tables — and the same table under another tenant — alone.
	vm.BumpTable("acme", "users")
	assert.NotEqual(t, usersBefore, vm.QueryKey(users[0].Tenant, "h", users))
	assert.Equal(t, ordersBefore, vm.QueryKey(orders[0].Tenant, "h", orders))
	assert.Equal(t, globexBefore, vm.QueryKey(globexUsers[0].Tenant, "h", globexUsers))
}

func TestVersionManager_BumpNamespace(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	scoped := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}
	wholeTable := []Namespace{{Tenant: "acme", Table: "users"}}
	otherScope := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_2"}}
	otherTenant := []Namespace{{Tenant: "globex", Table: "users", Scope: "org_1"}}

	scopedBefore := vm.QueryKey(scoped[0].Tenant, "h", scoped)
	wholeBefore := vm.QueryKey(wholeTable[0].Tenant, "h", wholeTable)
	otherBefore := vm.QueryKey(otherScope[0].Tenant, "h", otherScope)
	otherTenantBefore := vm.QueryKey(otherTenant[0].Tenant, "h", otherTenant)

	// Bumping (acme, users, org_1) changes that scope AND the whole-table view,
	// but leaves every other scope — and the same scope under another tenant —
	// valid.
	vm.BumpNamespace(Namespace{Tenant: "acme", Table: "users", Scope: "org_1"})
	assert.NotEqual(t, scopedBefore, vm.QueryKey(scoped[0].Tenant, "h", scoped))
	assert.NotEqual(t, wholeBefore, vm.QueryKey(wholeTable[0].Tenant, "h", wholeTable))
	assert.Equal(t, otherBefore, vm.QueryKey(otherScope[0].Tenant, "h", otherScope))
	assert.Equal(t, otherTenantBefore, vm.QueryKey(otherTenant[0].Tenant, "h", otherTenant))
}

// TestVersionManager_BumpTenant: a tenant's every namespace is orphaned in
// one step — a table that was never bumped (so has no key of its own to bump)
// included — and no other tenant's is touched.
func TestVersionManager_BumpTenant(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	users := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}
	orders := []Namespace{{Tenant: "acme", Table: "orders"}}
	globexUsers := []Namespace{{Tenant: "globex", Table: "users", Scope: "org_1"}}
	vm.BumpTable("acme", "users")

	usersBefore := vm.QueryKey(users[0].Tenant, "h", users)
	ordersBefore := vm.QueryKey(orders[0].Tenant, "h", orders)
	globexBefore := vm.QueryKey(globexUsers[0].Tenant, "h", globexUsers)

	vm.BumpTenant("acme")
	assert.NotEqual(t, usersBefore, vm.QueryKey(users[0].Tenant, "h", users))
	assert.NotEqual(t, ordersBefore, vm.QueryKey(orders[0].Tenant, "h", orders), "a table no bump ever keyed is orphaned too")
	assert.Equal(t, globexBefore, vm.QueryKey(globexUsers[0].Tenant, "h", globexUsers))

	pipeBefore := vm.QueryKey("acme", "h", nil)
	vm.BumpTenant("acme")
	assert.NotEqual(t, pipeBefore, vm.QueryKey("acme", "h", nil), "a result with no deps is orphaned too")
}
