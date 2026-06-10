package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionManager_NamespaceKey(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager(nil)

	// Default table version (0); a scopeless namespace renders a trailing dot.
	assert.Equal(t, "users.0.", vm.NamespaceKey("users", ""))
	assert.Equal(t, "users.0.org_1", vm.NamespaceKey("users", "org_1"))

	// The table version is embedded in every namespace key for that table, so a
	// BumpTable is reflected across all scopes at once.
	vm.BumpTable("users")
	assert.Equal(t, "users.1.", vm.NamespaceKey("users", ""))
	assert.Equal(t, "users.1.org_1", vm.NamespaceKey("users", "org_1"))
}

func TestVersionManager_QueryKey(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager(nil)

	// One dependency at default versions: sha | <table>.<tableVer>.<scope>.<nsVer>.
	key := vm.QueryKey("hash123", []Namespace{{Table: "users", Scope: "org_1"}})
	assert.Equal(t, "hash123|users.0.org_1.0", key)

	// Dependency order must not change the key (segments are sorted).
	deps1 := []Namespace{{Table: "a"}, {Table: "b"}}
	deps2 := []Namespace{{Table: "b"}, {Table: "a"}}
	assert.Equal(t, vm.QueryKey("h", deps1), vm.QueryKey("h", deps2))
}

func TestVersionManager_BumpTable(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager(nil)

	users := []Namespace{{Table: "users", Scope: "org_1"}}
	orders := []Namespace{{Table: "orders", Scope: "org_1"}}

	usersBefore := vm.QueryKey("h", users)
	ordersBefore := vm.QueryKey("h", orders)

	// Bumping a table changes the key for that table but leaves other tables alone.
	vm.BumpTable("users")
	assert.NotEqual(t, usersBefore, vm.QueryKey("h", users))
	assert.Equal(t, ordersBefore, vm.QueryKey("h", orders))
}

func TestVersionManager_BumpNamespace(t *testing.T) {
	t.Parallel()
	vm := NewVersionManager(nil)

	scoped := []Namespace{{Table: "users", Scope: "org_1"}}
	wholeTable := []Namespace{{Table: "users"}}
	otherScope := []Namespace{{Table: "users", Scope: "org_2"}}

	scopedBefore := vm.QueryKey("h", scoped)
	wholeBefore := vm.QueryKey("h", wholeTable)
	otherBefore := vm.QueryKey("h", otherScope)

	// Bumping (users, org_1) changes that scope AND the whole-table view, but leaves
	// every other scope valid.
	vm.BumpNamespace("users", "org_1")
	assert.NotEqual(t, scopedBefore, vm.QueryKey("h", scoped))
	assert.NotEqual(t, wholeBefore, vm.QueryKey("h", wholeTable))
	assert.Equal(t, otherBefore, vm.QueryKey("h", otherScope))
}
