package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdminRole_DefaultAndConfigured(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "admin", AdminRole(nil), "nil policy → default admin role")
	assert.Equal(t, "admin", AdminRole(&Policy{}), "unset admin_role → default")
	assert.Equal(t, "superuser", AdminRole(&Policy{AdminRole: "superuser"}))
}

func TestIsAdmin(t *testing.T) {
	t.Parallel()
	assert.True(t, IsAdmin(nil, "admin"), "default admin role applies to a nil policy")
	assert.False(t, IsAdmin(nil, ""), "empty role is never admin")
	assert.False(t, IsAdmin(nil, "service"), "service is no longer privileged")
	assert.False(t, IsAdmin(nil, "Admin"), "case-sensitive: Admin != admin")

	p := &Policy{AdminRole: "superuser"}
	assert.True(t, IsAdmin(p, "superuser"))
	assert.False(t, IsAdmin(p, "admin"), "with a custom admin_role, literal admin is ordinary")
	assert.False(t, IsAdmin(p, ""), "empty role is never admin even with a custom admin_role")
}

func TestRoleAllowed(t *testing.T) {
	t.Parallel()
	allow := []string{"editor", "analyst"}

	assert.True(t, RoleAllowed(nil, "admin", nil), "admin passes any allowlist, even empty")
	assert.True(t, RoleAllowed(nil, "editor", allow), "listed role passes")
	assert.False(t, RoleAllowed(nil, "viewer", allow), "unlisted role denied")
	assert.False(t, RoleAllowed(nil, "", allow), "empty role denied")
	assert.False(t, RoleAllowed(nil, "editor", nil), "no allowlist denies non-admin")
	assert.False(t, RoleAllowed(nil, "", []string{""}), "empty allowlist entry can't authorize an empty role")

	p := &Policy{AdminRole: "superuser"}
	assert.True(t, RoleAllowed(p, "superuser", nil), "configured admin bypasses the allowlist")
	assert.False(t, RoleAllowed(p, "admin", nil), "literal admin is not privileged under a custom admin_role")
}
