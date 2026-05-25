package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// classifyJWTError maps a jwt-library error to a coarse failure-reason label
// for wavehouse_auth_failures_total via library sentinels. Unknown errors
// collapse to "invalid" so the label set stays bounded.
func classifyJWTError(err error) string {
	switch {
	case errors.Is(err, jwt.ErrTokenExpired) || errors.Is(err, jwt.ErrTokenNotValidYet):
		return "expired"
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		return "bad_signature"
	case errors.Is(err, jwt.ErrTokenMalformed):
		return "malformed"
	case errors.Is(err, jwt.ErrTokenUnverifiable):
		return "unverifiable"
	default:
		return "invalid"
	}
}

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

	authMethod := "hmac"
	if jwks != nil {
		authMethod = "jwks"
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, claims, role, ok := verifyJWT(w, r, cfg, jwks, roleClaim, authMethod)
			if !ok {
				return
			}
			ctx = context.WithValue(ctx, ContextKeyClaims, claims)
			ctx = context.WithValue(ctx, ContextKeyRole, role)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// verifyJWT runs JWT verify + claim extraction under a `jwt_verify` span.
// The span ends when this function returns; the caller's middleware doesn't
// wrap it in a defer so the span duration excludes downstream handler work.
func verifyJWT(w http.ResponseWriter, r *http.Request, cfg AuthConfig, jwks keyfunc.Keyfunc, roleClaim, authMethod string) (context.Context, jwt.MapClaims, string, bool) {
	ctx, span := observability.Tracer().Start(r.Context(), "jwt_verify",
		trace.WithAttributes(
			attribute.String("auth.method", authMethod),
			attribute.String("auth.role_claim", roleClaim),
		),
	)
	defer span.End()
	fail := func(status int, reason, msg string, recordErr error) (context.Context, jwt.MapClaims, string, bool) {
		observability.AuthFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
		span.SetAttributes(attribute.String("auth.failure_reason", reason))
		if recordErr != nil {
			span.RecordError(recordErr)
		}
		writeJSONError(w, status, msg)
		return ctx, nil, "", false
	}

	var tokenStr string
	if t, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		tokenStr = t
	} else if q := r.URL.Query().Get("token"); q != "" {
		// ?token= fallback for WebSocket connections (no headers in upgrade).
		// Strip from the URL so it doesn't leak into request logs.
		tokenStr = q
		params := r.URL.Query()
		params.Del("token")
		r.URL.RawQuery = params.Encode()
	} else {
		return fail(http.StatusUnauthorized, "no_token", "missing authorization", nil)
	}

	keyFunc := func(t *jwt.Token) (interface{}, error) {
		if jwks != nil {
			return jwks.Keyfunc(t)
		}
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(cfg.JWTSecret), nil
	}

	token, err := jwt.Parse(tokenStr, keyFunc)
	if err != nil || !token.Valid {
		return fail(http.StatusUnauthorized, classifyJWTError(err), "invalid token", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return fail(http.StatusUnauthorized, "invalid_claims", "invalid claims", nil)
	}

	// Missing role on a verified token doesn't fail the request — RequireRole
	// at the router decides downstream. Surface as `auth.role_present=false`
	// + a counter bump so /v1/admin/* 403s can be told apart from 401s.
	role := extractClaim(claims, roleClaim)
	if role == "" {
		observability.AuthFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "missing_role_claim")))
		span.SetAttributes(attribute.Bool("auth.role_present", false))
	} else {
		span.SetAttributes(
			attribute.String("auth.role", role),
			attribute.Bool("auth.role_present", true),
		)
	}
	return ctx, claims, role, true
}

// extractClaim navigates a dot-separated claim path in JWT claims.
func extractClaim(claims jwt.MapClaims, path string) string {
	var current any = map[string]any(claims)
	for _, part := range strings.Split(path, ".") {
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
