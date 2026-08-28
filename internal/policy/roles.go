package policy

// defaultAdminRole is the role granted full administrative access when a
// (non-nil) policy does not configure an explicit admin_role. A nil policy
// grants admin to nobody (see IsAdmin) — a deployment gets its policy from the
// settings directory's policies.json (adopted server-side), not from an "admin" token.
const defaultAdminRole = "admin"

// AdminRole returns the role string that grants full administrative access for a
// given policy: the configured admin_role (case-sensitive, exact match), or
// "admin" when a real policy leaves it unset. A nil policy returns "" — with no
// policy loaded there is no admin role, so a deleted/empty policy can never hand
// back a usable admin role at the source. This is defense-in-depth behind
// IsAdmin's own nil guard: recovery from a lost policy is server-side only
// (fix policies.json; it reloads); an "admin" token cannot re-open the
// deployment over HTTP.
func AdminRole(p *Policy) string {
	if p == nil {
		return ""
	}
	if p.AdminRole != "" {
		return p.AdminRole
	}
	return defaultAdminRole
}

// IsAdmin reports whether role is the privileged admin role. A nil policy
// (policies.json empty) admits nobody — not even "admin": "no
// policy" is a total lockout, so an implicit admin grant can't re-open a
// deliberately-emptied or lost deployment, and a deployment with a custom
// admin_role can't have the literal "admin" silently re-privileged. A
// deployment's policy comes from policies.json, never over HTTP. An empty/absent
// role is never admin either, regardless of how admin_role is configured — a
// roleless request must never inherit admin via an empty-string match. This is
// the single source of truth for the admin check; Evaluate, ResolveRole,
// Validate, the /v1/ops gate, and pipe authorization all route through it.
func IsAdmin(p *Policy, role string) bool {
	if p == nil {
		return false
	}
	return role != "" && role == AdminRole(p)
}

// DefaultRoleGrantsAdmin reports whether the policy's default_role resolves to
// the admin role. When true, ResolveRole maps every roleless request (no token,
// or a token without a role claim) to the admin role, so unauthenticated callers
// receive full admin access — including /v1/ops/*. This is permitted as a
// local/dev convenience (no token needed to exercise admin surfaces), but is
// unsafe in production, so the policy store warns loudly whenever a policy with
// this setting is adopted. An empty default_role (no public access) is never
// admin. This is the single source of truth for the "default grants admin"
// condition, mirroring IsAdmin's exact, case-sensitive match.
func DefaultRoleGrantsAdmin(p *Policy) bool {
	return p != nil && p.DefaultRole != "" && p.DefaultRole == AdminRole(p)
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
