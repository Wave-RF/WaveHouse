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

	// One dependency at default versions: sha | <tenant>.<table>.<tableVer>.<scope>.<nsVer>.
	key := vm.QueryKey("hash123", []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}})
	assert.Equal(t, "hash123|acme.0.users.0.org_1.0", key)

	// Dependency order must not change the key (segments are sorted).
	deps1 := []Namespace{{Tenant: "acme", Table: "a"}, {Tenant: "acme", Table: "b"}}
	deps2 := []Namespace{{Tenant: "acme", Table: "b"}, {Tenant: "acme", Table: "a"}}
	assert.Equal(t, vm.QueryKey("h", deps1), vm.QueryKey("h", deps2))

	// The same sha and table under two tenants fold to two keys.
	assert.NotEqual(t,
		vm.QueryKey("h", []Namespace{{Tenant: "acme", Table: "users"}}),
		vm.QueryKey("h", []Namespace{{Tenant: "globex", Table: "users"}}))
}

func TestVersionManager_BumpTable(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	users := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}
	orders := []Namespace{{Tenant: "acme", Table: "orders", Scope: "org_1"}}
	globexUsers := []Namespace{{Tenant: "globex", Table: "users", Scope: "org_1"}}

	usersBefore := vm.QueryKey("h", users)
	ordersBefore := vm.QueryKey("h", orders)
	globexBefore := vm.QueryKey("h", globexUsers)

	// Bumping a table changes the key for that tenant's table but leaves other
	// tables — and the same table under another tenant — alone.
	vm.BumpTable("acme", "users")
	assert.NotEqual(t, usersBefore, vm.QueryKey("h", users))
	assert.Equal(t, ordersBefore, vm.QueryKey("h", orders))
	assert.Equal(t, globexBefore, vm.QueryKey("h", globexUsers))
}

func TestVersionManager_BumpNamespace(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager()

	scoped := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_1"}}
	wholeTable := []Namespace{{Tenant: "acme", Table: "users"}}
	otherScope := []Namespace{{Tenant: "acme", Table: "users", Scope: "org_2"}}
	otherTenant := []Namespace{{Tenant: "globex", Table: "users", Scope: "org_1"}}

	scopedBefore := vm.QueryKey("h", scoped)
	wholeBefore := vm.QueryKey("h", wholeTable)
	otherBefore := vm.QueryKey("h", otherScope)
	otherTenantBefore := vm.QueryKey("h", otherTenant)

	// Bumping (acme, users, org_1) changes that scope AND the whole-table view,
	// but leaves every other scope — and the same scope under another tenant —
	// valid.
	vm.BumpNamespace(Namespace{Tenant: "acme", Table: "users", Scope: "org_1"})
	assert.NotEqual(t, scopedBefore, vm.QueryKey("h", scoped))
	assert.NotEqual(t, wholeBefore, vm.QueryKey("h", wholeTable))
	assert.Equal(t, otherBefore, vm.QueryKey("h", otherScope))
	assert.Equal(t, otherTenantBefore, vm.QueryKey("h", otherTenant))
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

	usersBefore := vm.QueryKey("h", users)
	ordersBefore := vm.QueryKey("h", orders)
	globexBefore := vm.QueryKey("h", globexUsers)

	vm.BumpTenant("acme")
	assert.NotEqual(t, usersBefore, vm.QueryKey("h", users))
	assert.NotEqual(t, ordersBefore, vm.QueryKey("h", orders), "a table no bump ever keyed is orphaned too")
	assert.Equal(t, globexBefore, vm.QueryKey("h", globexUsers))
}
