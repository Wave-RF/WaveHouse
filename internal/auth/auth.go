package auth

import (
	"errors"
	"log/slog"
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
// HMAC (via JWTSecret) and/or JWKS (via JWKSURL) are supported. With neither
// configured, every token fails to validate and every request falls back to the
// default role — i.e. a pure public deployment.
func Middleware(cfg Config) func(http.Handler) http.Handler {
	roleClaim := cfg.RoleClaim
	if roleClaim == "" {
		roleClaim = "role"
	}

	var jwks keyfunc.Keyfunc
	if cfg.JWKSURL != "" {
		var err error
		jwks, err = keyfunc.NewDefault([]string{cfg.JWKSURL})
		if err != nil {
			slog.Error("failed to initialize JWKS keyfunc", "url", cfg.JWKSURL, "error", err)
		}
	}

	keyFunc := func(t *jwt.Token) (any, error) {
		// Try JWKS first if configured, then fall back to the HMAC shared secret.
		if jwks != nil {
			return jwks.Keyfunc(t)
		}
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(cfg.JWTSecret), nil
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenStr := bearerToken(r)
			if tokenStr == "" {
				// No token: roleless request, resolved to default_role downstream.
				next.ServeHTTP(w, r)
				return
			}

			token, err := jwt.Parse(tokenStr, keyFunc)
			if err != nil || !token.Valid {
				// Present-but-invalid token: fall back to the default role, but
				// remember why so a denied request can fail loud.
				r = r.WithContext(WithAuthError(r.Context(), tokenError(err)))
				next.ServeHTTP(w, r)
				return
			}

			claims, ok := token.Claims.(jwt.MapClaims)
			if !ok {
				r = r.WithContext(WithAuthError(r.Context(), errInvalidToken))
				next.ServeHTTP(w, r)
				return
			}

			ctx := WithClaims(r.Context(), claims)
			ctx = WithRole(ctx, extractClaim(claims, roleClaim))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// bearerToken extracts a JWT from the Authorization: Bearer header, or — for
// WebSocket clients that can't set headers — the ?token query parameter, which
// is stripped from the URL so it can't leak into logs. Returns "" if absent.
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
