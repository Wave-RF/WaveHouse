// Package auth handles authentication: it validates JWT Bearer tokens and
// records the caller's role, claims, and any token-validation error in the
// request context. It makes no authorization decisions and writes no HTTP
// responses — authorization lives in internal/policy (role checks) and the
// gates in internal/api (which translate decisions into HTTP responses).
package auth

import (
	"context"

	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const (
	contextKeyClaims    contextKey = "jwt_claims"
	contextKeyRole      contextKey = "jwt_role"
	contextKeyAuthError contextKey = "jwt_auth_error"
	contextKeyOperator  contextKey = "operator"
)

// ClaimsFromContext extracts JWT claims from the request context. ok is false
// when no valid token established claims.
func ClaimsFromContext(ctx context.Context) (jwt.MapClaims, bool) {
	c, ok := ctx.Value(contextKeyClaims).(jwt.MapClaims)
	return c, ok
}

// RoleFromContext extracts the role string from the request context. An empty
// string means no role was established — no token, a token without a role
// claim, or an invalid/expired token — which callers resolve to the policy
// default_role via policy.ResolveRole.
func RoleFromContext(ctx context.Context) string {
	r, _ := ctx.Value(contextKeyRole).(string)
	return r
}

// AuthErrorFromContext returns the token-validation error recorded when a
// present token failed to validate (bad signature, malformed, expired). It is
// nil for a valid token or for no token at all. Gates use it to fail loud —
// surfacing the token problem instead of a bare "forbidden" — when a request
// that fell back to the default role is denied for lacking permission.
func AuthErrorFromContext(ctx context.Context) error {
	e, _ := ctx.Value(contextKeyAuthError).(error)
	return e
}

// WithClaims returns a copy of ctx carrying the given JWT claims. Set by the
// middleware; exported so tests (and any code constructing an authenticated
// context) can establish identity.
func WithClaims(ctx context.Context, claims jwt.MapClaims) context.Context {
	return context.WithValue(ctx, contextKeyClaims, claims)
}

// WithRole returns a copy of ctx carrying the given role string.
func WithRole(ctx context.Context, role string) context.Context {
	return context.WithValue(ctx, contextKeyRole, role)
}

// WithAuthError returns a copy of ctx carrying a token-validation error, used
// by gates to fail loud when a request that fell back to the default role is
// denied. See AuthErrorFromContext.
func WithAuthError(ctx context.Context, err error) context.Context {
	return context.WithValue(ctx, contextKeyAuthError, err)
}

// WithOperator returns a copy of ctx marked as a platform-operator request, set
// by the middleware when a valid operator key is presented (Authorization:
// Operator <key>, or the X-Operator-Key alias).
// The operator bit authorizes the admin surface independently of the policy —
// RequireAdmin honors it even when the policy is nil/deleted — so it is the
// break-glass path for restoring a wiped policy over HTTP.
func WithOperator(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyOperator, true)
}

// IsOperator reports whether the request was authenticated with the operator
// key. See WithOperator.
func IsOperator(ctx context.Context) bool {
	v, _ := ctx.Value(contextKeyOperator).(bool)
	return v
}
