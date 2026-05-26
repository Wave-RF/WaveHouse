package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// Config holds configuration for the JWT authentication middleware.
type Config struct {
	JWTSecret string
	JWKSURL   string
	RoleClaim string // dot-separated claim path, e.g. "role" or "app_metadata.role"
}

var (
	errInvalidToken = errors.New("invalid token")
	errTokenExpired = errors.New("token expired")
)

// Middleware authenticates Bearer tokens and records the caller's role, claims,
// and any validation error in the request context. Authentication is decoupled
// from authorization: this middleware NEVER rejects a request and writes no
// response. A missing token, a token with no role claim, or an
// invalid/expired/malformed token all yield an EMPTY role, which downstream
// gates resolve to the policy default_role. When a present token fails to
// validate, the (sanitized) error is stashed in the context so a gate that
// later denies the request can fail loud rather than silently treating the
// caller as the public default.
//
// Verification is JWKS-or-HMAC, not both: when JWKSURL is configured (and
// initializes) JWKS is the sole verifier and the HMAC secret is ignored;
// otherwise the HMAC secret (JWTSecret) is used. Accepted signing algorithms are
// restricted to the active verifier's family (asymmetric for JWKS, HMAC
// otherwise) so a token can't force an alg-confusion or alg:none bypass. With
// neither JWKSURL nor JWTSecret configured, no token can validate and every
// request falls back to the default role — i.e. a pure public deployment.
//
// If JWKSURL is set but its JWK Set can't be fetched at startup, Middleware
// returns an error so the caller can fail fast instead of booting into a
// degraded state where no token can validate.
func Middleware(cfg Config) (func(http.Handler) http.Handler, error) {
	roleClaim := cfg.RoleClaim
	if roleClaim == "" {
		roleClaim = "role"
	}

	var jwks keyfunc.Keyfunc
	if cfg.JWKSURL != "" {
		// Force a synchronous initial fetch (NoErrorReturnFirstHTTPReq=false) so an
		// unreachable or misconfigured JWKS endpoint fails startup loudly instead of
		// silently booting into a state where no token can validate. keyfunc keeps
		// refreshing in the background once this succeeds, so a JWKS outage *after*
		// boot is tolerated — only boot-time unreachability is fatal.
		noErrorReturnFirstHTTPReq := false
		var err error
		jwks, err = keyfunc.NewDefaultOverrideCtx(context.Background(), []string{cfg.JWKSURL}, keyfunc.Override{
			NoErrorReturnFirstHTTPReq: &noErrorReturnFirstHTTPReq,
		})
		if err != nil {
			return nil, fmt.Errorf("initialize JWKS from %q: %w", cfg.JWKSURL, err)
		}
	}

	keyFunc := func(t *jwt.Token) (any, error) {
		// JWKS-or-HMAC, JWKS first: when configured it is the sole verifier; the
		// HMAC shared secret is reached only when JWKS isn't in play.
		if jwks != nil {
			return jwks.Keyfunc(t)
		}
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(cfg.JWTSecret), nil
	}

	// Restrict accepted signing algorithms to the active verifier's family, so
	// jwt.Parse rejects an unexpected alg (including "none") before keyFunc runs
	// — defense-in-depth against alg-confusion and alg:none attacks. Keyed off the
	// runtime verifier (jwks != nil), not just cfg, so a failed JWKS init that
	// falls back to HMAC still gets the HMAC allowlist.
	var validMethods []string
	if jwks != nil {
		validMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512", "EdDSA"}
	} else {
		validMethods = []string{"HS256", "HS384", "HS512"}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenStr := bearerToken(r)
			if tokenStr == "" {
				// No token: roleless request (not an error), resolved to
				// default_role downstream.
				next.ServeHTTP(w, r)
				return
			}

			// A presented token authenticates ONLY through the explicit success
			// path below; every other outcome falls through to the fail-safe
			// default at the end (roleless + recorded error). Ordering it this way
			// means a future missing return, or a mis-set !ok/!Valid condition,
			// can never accidentally promote an unverified token to a real role.
			token, err := jwt.Parse(tokenStr, keyFunc, jwt.WithValidMethods(validMethods))
			if err == nil && token.Valid {
				if claims, ok := token.Claims.(jwt.MapClaims); ok {
					ctx := WithClaims(r.Context(), claims)
					ctx = WithRole(ctx, extractClaim(claims, roleClaim))
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}

			// Fail-safe default: present-but-unverifiable token. Fall back to the
			// roleless default_role, but record why so a gate that denies can fail
			// loud (401) instead of as a bare 403.
			r = r.WithContext(WithAuthError(r.Context(), tokenError(err)))
			next.ServeHTTP(w, r)
		})
	}, nil
}

// bearerToken extracts a JWT from the Authorization: Bearer header, or — for
// WebSocket clients that can't set headers — the ?token query parameter, which
// is stripped from the URL so it can't leak into logs. The Authorization header
// takes precedence when both are present. Returns "" if absent.
func bearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	if q := r.URL.Query().Get("token"); q != "" {
		params := r.URL.Query()
		params.Del("token")
		r.URL.RawQuery = params.Encode()
		return q
	}
	return ""
}

// tokenError maps a jwt parse failure to a stable, caller-safe error for the
// fail-loud message: expired tokens are distinguished, everything else (bad
// signature, malformed, nil) collapses to a generic invalid-token error so no
// library internals leak to clients.
func tokenError(err error) error {
	if err != nil && errors.Is(err, jwt.ErrTokenExpired) {
		return errTokenExpired
	}
	return errInvalidToken
}

// extractClaim resolves a single string claim from a dot-separated path (e.g.
// "role" or "app_metadata.role"). It walks the path one segment at a time: each
// non-final segment must index into a nested JSON object (map[string]any), so a
// missing segment — or a non-object encountered mid-path — makes the path
// unresolvable and returns "". The leaf is returned only when it is a string; a
// non-string value (number, bool, object) also returns "". Returning "" (an
// empty role) is the fail-safe: downstream it resolves to the policy
// default_role and never matches a real role key.
func extractClaim(claims jwt.MapClaims, path string) string {
	parts := strings.Split(path, ".")
	var current any = map[string]any(claims)
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = m[part]
	}
	if s, ok := current.(string); ok {
		return s
	}
	return ""
}
