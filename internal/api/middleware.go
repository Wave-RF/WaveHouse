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
			ctx, claims, role, ok := verifyJWT(w, r, cfg, jwks, roleClaim, authMethod)
			if !ok {
				// verifyJWT already responded with writeJSONError + recorded
				// the auth-failure counter + ended the span.
				return
			}
			ctx = context.WithValue(ctx, ContextKeyClaims, claims)
			ctx = context.WithValue(ctx, ContextKeyRole, role)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// verifyJWT performs the JWT verification + claim extraction under a
// `jwt_verify` span. The span is ended explicitly before this function
// returns (success OR failure) so its duration reflects only the verify
// work, not the downstream handler — a deferred End() inside the
// surrounding middleware would balloon the span to cover the whole request.
//
// Returns (childCtx, claims, role, true) on success. On failure the response
// has already been written via writeJSONError, the auth-failure counter
// incremented, and the span finalized; the caller should just return.
func verifyJWT(w http.ResponseWriter, r *http.Request, cfg AuthConfig, jwks keyfunc.Keyfunc, roleClaim, authMethod string) (context.Context, jwt.MapClaims, string, bool) {
	ctx, span := observability.Tracer().Start(r.Context(), "jwt_verify",
		trace.WithAttributes(
			attribute.String("auth.method", authMethod),
			attribute.String("auth.role_claim", roleClaim),
		),
	)
	// Panic-safe span finalization: each explicit success/failure path
	// flips `ended` and calls span.End() so the span boundary reflects only
	// the verify work, not the downstream handler. If anything panics
	// before one of those paths fires (jwt.Parse with a future library
	// bump, extractClaim on a malformed claim shape, etc.), the deferred
	// branch still ends the span — otherwise it stays pinned in the OTel
	// SDK's active-span set and never exports.
	ended := false
	defer func() {
		if !ended {
			span.End()
		}
	}()
	// Local helper for the failure-return shape — keeps each return path
	// to one line and guarantees span.End() fires before writeJSONError.
	fail := func(status int, reason, msg string, recordErr error) (context.Context, jwt.MapClaims, string, bool) {
		observability.AuthFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
		span.SetAttributes(attribute.String("auth.failure_reason", reason))
		if recordErr != nil {
			span.RecordError(recordErr)
		}
		ended = true
		span.End()
		writeJSONError(w, status, msg)
		return ctx, nil, "", false
	}

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

	role := extractClaim(claims, roleClaim)
	if role == "" {
		// Successful JWT verify but no role — auth metadata pulled from the
		// wrong claim, common misconfiguration. Doesn't fail the request:
		// RequireRole at the router decides authorization downstream. The
		// missing role is surfaced as auth.role_present=false on the span (a
		// distinct dimension from `auth.failure_reason`, which stays a clean
		// failure-correlate — verify-time issues only) and as a counter
		// record with reason=missing_role_claim so /v1/admin/* 403s can be
		// distinguished from missing/expired-token 401s in metrics.
		observability.AuthFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "missing_role_claim")))
		span.SetAttributes(attribute.Bool("auth.role_present", false))
	} else {
		span.SetAttributes(
			attribute.String("auth.role", role),
			attribute.Bool("auth.role_present", true),
		)
	}
	ended = true
	span.End()
	return ctx, claims, role, true
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
