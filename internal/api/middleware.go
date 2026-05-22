package api

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const (
	// ContextKeyClaims holds jwt.MapClaims in the request context.
	ContextKeyClaims contextKey = "jwt_claims"
	// ContextKeyRole holds the resolved role string.
	ContextKeyRole contextKey = "jwt_role"
)

// ClaimsFromContext extracts JWT claims from the request context.
func ClaimsFromContext(ctx context.Context) (jwt.MapClaims, bool) {
	c, ok := ctx.Value(ContextKeyClaims).(jwt.MapClaims)
	return c, ok
}

// RoleFromContext extracts the role string from the request context.
func RoleFromContext(ctx context.Context) string {
	r, _ := ctx.Value(ContextKeyRole).(string)
	return r
}

// AuthConfig holds configuration for the JWT auth middleware.
type AuthConfig struct {
	Enabled   bool
	JWTSecret string
	JWKSURL   string
	RoleClaim string // dot-separated claim path, e.g. "role" or "app_metadata.role"
	DevMode   bool
	// AllowAnonymous, when non-nil and it returns true, lets a request with NO
	// token through with an empty role instead of 401 (downstream policy /
	// allowed_roles gates then decide). It's wired to "is a usable default_role
	// configured?". Only an absent token is admitted — invalid/expired tokens
	// still 401.
	AllowAnonymous func() bool
}

// JWTAuthMiddleware validates Bearer tokens.
// When auth is disabled or dev mode is enabled, returns a no-op passthrough middleware.
// Supports HMAC signing (via JWTSecret) and/or JWKS endpoint (via JWKSURL).
func JWTAuthMiddleware(cfg AuthConfig) func(http.Handler) http.Handler {
	if !cfg.Enabled || cfg.DevMode {
		if cfg.DevMode {
			slog.Warn("auth dev mode enabled — all requests treated as admin, no JWT validation")
		}
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if cfg.DevMode {
					ctx := context.WithValue(r.Context(), ContextKeyRole, "admin")
					ctx = context.WithValue(ctx, ContextKeyClaims, jwt.MapClaims{})
					r = r.WithContext(ctx)
				}
				next.ServeHTTP(w, r)
			})
		}
	}

	roleClaim := cfg.RoleClaim
	if roleClaim == "" {
		roleClaim = "role"
	}

	// Build keyfunc for JWKS if configured.
	var jwks keyfunc.Keyfunc
	if cfg.JWKSURL != "" {
		var err error
		jwks, err = keyfunc.NewDefault([]string{cfg.JWKSURL})
		if err != nil {
			slog.Error("failed to initialize JWKS keyfunc", "url", cfg.JWKSURL, "error", err)
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var tokenStr string

			auth := r.Header.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") {
				tokenStr = strings.TrimPrefix(auth, "Bearer ")
			} else if q := r.URL.Query().Get("token"); q != "" {
				// Fallback: accept token as query parameter (required for
				// WebSocket connections where headers cannot be set).
				tokenStr = q
				// Strip the token from the URL to avoid leaking it in logs.
				params := r.URL.Query()
				params.Del("token")
				r.URL.RawQuery = params.Encode()
			} else {
				// No token presented. Proceed as a roleless request
				// (RoleFromContext == "") only if anonymous access is configured
				// — i.e. a usable default_role exists; otherwise fail closed with
				// 401. This covers only an ABSENT token; a present-but-invalid
				// token still 401s in the parse path below.
				if cfg.AllowAnonymous != nil && cfg.AllowAnonymous() {
					next.ServeHTTP(w, r)
					return
				}
				writeJSONError(w, http.StatusUnauthorized, "missing authorization")
				return
			}

			keyFunc := func(t *jwt.Token) (interface{}, error) {
				// Try JWKS first if configured.
				if jwks != nil {
					return jwks.Keyfunc(t)
				}
				// Fall back to HMAC shared secret.
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, jwt.ErrSignatureInvalid
				}
				return []byte(cfg.JWTSecret), nil
			}

			token, err := jwt.Parse(tokenStr, keyFunc)
			if err != nil || !token.Valid {
				writeJSONError(w, http.StatusUnauthorized, "invalid token")
				return
			}

			claims, ok := token.Claims.(jwt.MapClaims)
			if !ok {
				writeJSONError(w, http.StatusUnauthorized, "invalid claims")
				return
			}

			// Extract role from claims using dot-separated path.
			role := extractClaim(claims, roleClaim)

			ctx := context.WithValue(r.Context(), ContextKeyClaims, claims)
			ctx = context.WithValue(ctx, ContextKeyRole, role)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// extractClaim navigates a dot-separated claim path in JWT claims.
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
