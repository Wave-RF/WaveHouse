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

// classifyJWTError maps the jwt-library error to a coarse failure-reason label
// for wavehouse_auth_failures_total. The library returns sentinel errors per
// failure mode, which gives us clean buckets without parsing messages. Falls
// back to "invalid" for any error not matching a known sentinel — keeps the
// counter label set bounded.
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
			// Span around the whole verify+claim-extract path. Kept short
			// (no body read, no upstream calls beyond JWKS rotation cache
			// hits) so the parent HTTP span stays the right unit for
			// per-request latency. The span attributes carry method (hmac vs
			// jwks) so trace queries can split out JWKS-fetch-blip latency.
			ctx, span := observability.Tracer().Start(r.Context(), "jwt_verify",
				trace.WithAttributes(
					attribute.String("auth.method", authMethod),
					attribute.String("auth.role_claim", roleClaim),
				),
			)
			defer span.End()

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
				observability.AuthFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "no_token")))
				span.SetAttributes(attribute.String("auth.failure_reason", "no_token"))
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
				reason := classifyJWTError(err)
				observability.AuthFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
				span.SetAttributes(attribute.String("auth.failure_reason", reason))
				if err != nil {
					span.RecordError(err)
				}
				writeJSONError(w, http.StatusUnauthorized, "invalid token")
				return
			}

			claims, ok := token.Claims.(jwt.MapClaims)
			if !ok {
				observability.AuthFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "invalid_claims")))
				span.SetAttributes(attribute.String("auth.failure_reason", "invalid_claims"))
				writeJSONError(w, http.StatusUnauthorized, "invalid claims")
				return
			}

			// Extract role from claims using dot-separated path.
			role := extractClaim(claims, roleClaim)
			if role == "" {
				// Successful JWT verify but no role — auth metadata pulled from
				// the wrong claim, common misconfiguration. Surface as a
				// distinct failure reason so /v1/admin/* 403s on this can be
				// distinguished from a missing/expired token.
				observability.AuthFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "missing_role_claim")))
				span.SetAttributes(attribute.String("auth.failure_reason", "missing_role_claim"))
			} else {
				span.SetAttributes(attribute.String("auth.role", role))
			}

			ctx = context.WithValue(ctx, ContextKeyClaims, claims)
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
