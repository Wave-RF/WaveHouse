package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJWTAuthMiddleware_Disabled(t *testing.T) {
	t.Parallel()
	mw := JWTAuthMiddleware(AuthConfig{Enabled: false})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	// No role should be injected when auth is disabled.
	assert.Empty(t, RoleFromContext(req.Context()))
}

func TestJWTAuthMiddleware_DevMode(t *testing.T) {
	t.Parallel()
	var capturedRole string
	var capturedClaims jwt.MapClaims
	mw := JWTAuthMiddleware(AuthConfig{Enabled: true, DevMode: true})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedRole = RoleFromContext(r.Context())
		capturedClaims, _ = ClaimsFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "admin", capturedRole)
	assert.NotNil(t, capturedClaims)
}

func TestJWTAuthMiddleware_MissingToken(t *testing.T) {
	t.Parallel()
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "missing authorization")
	testutil.AssertJSONErrorResponse(t, rec)
}

func TestJWTAuthMiddleware_InvalidToken(t *testing.T) {
	t.Parallel()
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer invalid.token.here")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid token")
	testutil.AssertJSONErrorResponse(t, rec)
}

func TestJWTAuthMiddleware_ExpiredToken(t *testing.T) {
	t.Parallel()
	token := testutil.MakeExpiredJWT(t, map[string]any{"role": "viewer"})
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestJWTAuthMiddleware_ValidToken_FlatRole(t *testing.T) {
	t.Parallel()
	token := testutil.MakeJWT(t, map[string]any{"role": "editor"})

	var capturedRole string
	var capturedClaims jwt.MapClaims
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
		RoleClaim: "role",
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedRole = RoleFromContext(r.Context())
		capturedClaims, _ = ClaimsFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "editor", capturedRole)
	require.NotNil(t, capturedClaims)
	assert.Equal(t, "editor", capturedClaims["role"])
}

func TestJWTAuthMiddleware_ValidToken_NestedRoleClaim(t *testing.T) {
	t.Parallel()
	token := testutil.MakeJWT(t, map[string]any{
		"app_metadata": map[string]any{
			"role": "manager",
		},
	})

	var capturedRole string
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
		RoleClaim: "app_metadata.role",
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedRole = RoleFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "manager", capturedRole)
}

func TestJWTAuthMiddleware_WrongSecret(t *testing.T) {
	t.Parallel()
	// Sign with one secret, verify with another.
	token := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: "completely-different-secret-for-testing!",
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestClaimsFromContext(t *testing.T) {
	t.Parallel()
	t.Run("present", func(t *testing.T) {
		t.Parallel()
		claims := jwt.MapClaims{"sub": "user123"}
		ctx := context.WithValue(context.Background(), ContextKeyClaims, claims)
		got, ok := ClaimsFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "user123", got["sub"])
	})
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		got, ok := ClaimsFromContext(context.Background())
		assert.False(t, ok)
		assert.Nil(t, got)
	})
}

func TestRoleFromContext(t *testing.T) {
	t.Parallel()
	t.Run("present", func(t *testing.T) {
		t.Parallel()
		ctx := context.WithValue(context.Background(), ContextKeyRole, "editor")
		assert.Equal(t, "editor", RoleFromContext(ctx))
	})
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", RoleFromContext(context.Background()))
	})
}

func TestExtractClaim(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		claims jwt.MapClaims
		path   string
		want   string
	}{
		{
			name:   "flat claim",
			claims: jwt.MapClaims{"role": "admin"},
			path:   "role",
			want:   "admin",
		},
		{
			name: "nested claim",
			claims: jwt.MapClaims{
				"app_metadata": map[string]any{"role": "viewer"},
			},
			path: "app_metadata.role",
			want: "viewer",
		},
		{
			name: "deeply nested",
			claims: jwt.MapClaims{
				"a": map[string]any{
					"b": map[string]any{
						"c": "deep",
					},
				},
			},
			path: "a.b.c",
			want: "deep",
		},
		{
			name:   "missing claim",
			claims: jwt.MapClaims{"foo": "bar"},
			path:   "role",
			want:   "",
		},
		{
			name:   "non-string claim",
			claims: jwt.MapClaims{"role": 42},
			path:   "role",
			want:   "",
		},
		{
			name:   "broken nested path",
			claims: jwt.MapClaims{"a": "not-a-map"},
			path:   "a.b",
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractClaim(tt.claims, tt.path)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestJWTAuthMiddleware_DefaultRoleClaim(t *testing.T) {
	t.Parallel()
	// Empty RoleClaim should default to "role".
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
		RoleClaim: "", // should default to "role"
	})

	var capturedRole string
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedRole = RoleFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	tok := testutil.MakeJWT(t, jwt.MapClaims{"role": "viewer"})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "viewer", capturedRole)
}

func TestJWTAuthMiddleware_QueryParamToken(t *testing.T) {
	t.Parallel()
	token := testutil.MakeJWT(t, map[string]any{"role": "viewer"})

	var capturedRole string
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedRole = RoleFromContext(r.Context())
		// Verify token was stripped from the URL.
		assert.Empty(t, r.URL.Query().Get("token"))
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?token="+token, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "viewer", capturedRole)
}

func TestJWTAuthMiddleware_HeaderTakesPrecedenceOverQueryParam(t *testing.T) {
	t.Parallel()
	headerToken := testutil.MakeJWT(t, map[string]any{"role": "admin"})
	queryToken := testutil.MakeJWT(t, map[string]any{"role": "viewer"})

	var capturedRole string
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedRole = RoleFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?token="+queryToken, nil)
	req.Header.Set("Authorization", "Bearer "+headerToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "admin", capturedRole, "header should take precedence over query param")
}

func TestJWTAuthMiddleware_InvalidQueryParamToken(t *testing.T) {
	t.Parallel()
	mw := JWTAuthMiddleware(AuthConfig{
		Enabled:   true,
		JWTSecret: testutil.TestJWTSecret,
	})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?token=invalid.token.here", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid token")
}

func TestJWTAuthMiddleware_NonBearerToken(t *testing.T) {
	t.Parallel()
	mw := JWTAuthMiddleware(AuthConfig{Enabled: true, JWTSecret: "secret"})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz") // Basic auth
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestJWTAuthMiddleware_NoHeader(t *testing.T) {
	t.Parallel()
	mw := JWTAuthMiddleware(AuthConfig{Enabled: true, JWTSecret: "secret"})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	// No Authorization header.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "missing authorization")
}
