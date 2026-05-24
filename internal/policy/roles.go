package policy

// defaultAdminRole is the role granted full administrative access when a policy
// does not configure an explicit admin_role. It exists so a fresh deployment
// with no policy yet can still be bootstrapped by an "admin" token.
const defaultAdminRole = "admin"

// AdminRole returns the role string that grants full administrative access.
// Configurable per-policy via admin_role (case-sensitive, exact match);
// defaults to "admin" when unset or when the policy is nil.
func AdminRole(p *Policy) string {
	if p != nil && p.AdminRole != "" {
		return p.AdminRole
	}
	return defaultAdminRole
}

// IsAdmin reports whether role is the privileged admin role. An empty/absent
// role is never admin, regardless of how admin_role is configured — a roleless
// request must never inherit admin via an empty-string match. This is the
// single source of truth for the admin check; Evaluate, ResolveRole, Validate,
// the /v1/admin gate, and pipe authorization all route through it.
func IsAdmin(p *Policy, role string) bool {
	return role != "" && role == AdminRole(p)
}

// RoleAllowed reports whether role passes an allowlist gate (used by named
// pipes). The admin role always passes; otherwise role must appear in
// allowedRoles by exact, non-empty match — there is no "*" any-role wildcard.
// An empty/absent role, or an empty allowlist, authorizes nobody but admin
// (fails closed). Callers must resolve an empty role to the policy default_role
// (via ResolveRole) before calling if they want default-role access.
func RoleAllowed(p *Policy, role string, allowedRoles []string) bool {
	if IsAdmin(p, role) {
		return true
	}
	if role == "" {
		return false
	}
	for _, ar := range allowedRoles {
		if ar != "" && ar == role {
			return true
		}
	}
	return false
}
