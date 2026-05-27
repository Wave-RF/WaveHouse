package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captured records what the middleware established in the request context.
type captured struct {
	called     bool
	role       string
	claims     jwt.MapClaims
	hasClaims  bool
	authErr    error
	tokenInURL string
}

// run drives cfg's middleware over a request decorated by setup, returning what
// the downstream handler observed. The middleware never rejects — it always
// reaches the handler — so the interesting output is the captured context, not
// the status code.
func run(t *testing.T, cfg Config, setup func(*http.Request)) captured {
	t.Helper()
	var c captured
	mw, err := Middleware(cfg)
	require.NoError(t, err)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.called = true
		c.role = RoleFromContext(r.Context())
		c.claims, c.hasClaims = ClaimsFromContext(r.Context())
		c.authErr = AuthErrorFromContext(r.Context())
		c.tokenInURL = r.URL.Query().Get("token")
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	if setup != nil {
		setup(req)
	}
	h.ServeHTTP(httptest.NewRecorder(), req)
	require.True(t, c.called, "middleware must always reach the handler")
	return c
}

func bearer(tok string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+tok) }
}

func cfg() Config { return Config{JWTSecret: testutil.TestJWTSecret, RoleClaim: "role"} }

func TestMiddleware_NoToken_RolelessNoError(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), nil)
	assert.Empty(t, c.role, "no token → empty role (resolved to default_role downstream)")
	assert.False(t, c.hasClaims)
	assert.NoError(t, c.authErr, "absent token is not an auth error")
}

func TestMiddleware_NonBearerHeader_TreatedAsNoToken(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), func(r *http.Request) { r.Header.Set("Authorization", "Basic dXNlcjpwYXNz") })
	assert.Empty(t, c.role)
	assert.NoError(t, c.authErr)
}

func TestMiddleware_ValidToken_FlatRole(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), bearer(testutil.MakeJWT(t, map[string]any{"role": "editor"})))
	assert.Equal(t, "editor", c.role)
	require.True(t, c.hasClaims)
	assert.Equal(t, "editor", c.claims["role"])
	assert.NoError(t, c.authErr)
}

func TestMiddleware_ValidToken_NestedRoleClaim(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeJWT(t, map[string]any{"app_metadata": map[string]any{"role": "manager"}})
	c := run(t, Config{JWTSecret: testutil.TestJWTSecret, RoleClaim: "app_metadata.role"}, bearer(tok))
	assert.Equal(t, "manager", c.role)
}

func TestMiddleware_ValidToken_NoRoleClaim_RolelessNoError(t *testing.T) {
	t.Parallel()
	// A valid token without the role claim authenticates but carries no role —
	// it resolves to default_role downstream, and is NOT an auth error.
	c := run(t, cfg(), bearer(testutil.MakeJWT(t, map[string]any{"sub": "u1"})))
	assert.Empty(t, c.role)
	assert.True(t, c.hasClaims, "claims are still set for a valid token")
	assert.NoError(t, c.authErr)
}

func TestMiddleware_DefaultRoleClaim(t *testing.T) {
	t.Parallel()
	// Empty RoleClaim defaults to "role".
	c := run(t, Config{JWTSecret: testutil.TestJWTSecret}, bearer(testutil.MakeJWT(t, map[string]any{"role": "viewer"})))
	assert.Equal(t, "viewer", c.role)
}

func TestMiddleware_InvalidToken_FallsBackWithError(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), bearer("not.a.jwt"))
	assert.Empty(t, c.role, "invalid token falls back to the default role")
	assert.False(t, c.hasClaims)
	require.Error(t, c.authErr)
	assert.True(t, errors.Is(c.authErr, errInvalidToken))
}

func TestMiddleware_WrongSecret_FallsBackWithError(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeJWT(t, map[string]any{"role": "viewer"}) // signed with testutil secret
	c := run(t, Config{JWTSecret: "a-different-secret-entirely!", RoleClaim: "role"}, bearer(tok))
	assert.Empty(t, c.role)
	assert.True(t, errors.Is(c.authErr, errInvalidToken))
}

func TestMiddleware_ExpiredToken_FallsBackWithExpiredError(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeExpiredJWT(t, map[string]any{"role": "viewer"})
	c := run(t, cfg(), bearer(tok))
	assert.Empty(t, c.role)
	require.Error(t, c.authErr)
	assert.True(t, errors.Is(c.authErr, errTokenExpired), "expired tokens get the distinct expired error")
	assert.Equal(t, "token expired", c.authErr.Error())
}

func TestMiddleware_QueryParamToken_StrippedFromURL(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	c := run(t, cfg(), func(r *http.Request) { r.URL.RawQuery = "token=" + tok })
	assert.Equal(t, "viewer", c.role)
	assert.Empty(t, c.tokenInURL, "the ?token param must be stripped so it can't leak into logs")
}

func TestMiddleware_HeaderTakesPrecedenceOverQuery(t *testing.T) {
	t.Parallel()
	header := testutil.MakeJWT(t, map[string]any{"role": "admin"})
	query := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	c := run(t, cfg(), func(r *http.Request) {
		r.URL.RawQuery = "token=" + query
		r.Header.Set("Authorization", "Bearer "+header)
	})
	assert.Equal(t, "admin", c.role)
}

func TestMiddleware_InvalidQueryParamToken_FallsBackWithError(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), func(r *http.Request) { r.URL.RawQuery = "token=not.a.jwt" })
	assert.Empty(t, c.role)
	assert.True(t, errors.Is(c.authErr, errInvalidToken))
}

func TestMiddleware_NoneAlgToken_Rejected(t *testing.T) {
	t.Parallel()
	// A token using "alg": "none" carries claims but no signature. It must never
	// authenticate — WithValidMethods drops it before keyFunc runs (the classic
	// JWT alg:none bypass), so even a "role": "admin" claim is ignored.
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"role": "admin"})
	signed, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)
	c := run(t, cfg(), bearer(signed))
	assert.Empty(t, c.role, "alg:none token must not authenticate")
	assert.False(t, c.hasClaims)
	assert.True(t, errors.Is(c.authErr, errInvalidToken))
}

func TestMiddleware_JWKSUnreachableAtBoot_FailsLoud(t *testing.T) {
	t.Parallel()
	// A configured JWKS endpoint that errors at startup must fail Middleware
	// construction — not silently boot into a state where no token can validate.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := Middleware(Config{JWKSURL: srv.URL})
	require.Error(t, err, "an unreachable/erroring JWKS at boot must fail loudly")
}

func TestMiddleware_JWKSReachableAtBoot_OK(t *testing.T) {
	t.Parallel()
	// A reachable JWKS endpoint (even with an empty key set) constructs cleanly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	defer srv.Close()

	mw, err := Middleware(Config{JWKSURL: srv.URL})
	require.NoError(t, err, "a reachable JWKS endpoint must construct successfully")
	require.NotNil(t, mw)
}

func TestMiddleware_JWKSEmptyKeySet_TokenDoesNotAuthenticate(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	defer srv.Close()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"role": "admin",
		"exp":  jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})
	signed, err := tok.SignedString(priv)
	require.NoError(t, err)

	c := run(t, Config{JWKSURL: srv.URL, RoleClaim: "role"}, bearer(signed))
	assert.Empty(t, c.role, "empty JWKS → no key validates the token → roleless default")
	assert.False(t, c.hasClaims)
	assert.True(t, errors.Is(c.authErr, errInvalidToken), "present-but-unverifiable token records invalid-token")
}

func TestContextHelpers_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert.Empty(t, RoleFromContext(ctx))
	_, ok := ClaimsFromContext(ctx)
	assert.False(t, ok)
	assert.NoError(t, AuthErrorFromContext(ctx))

	ctx = WithRole(ctx, "editor")
	ctx = WithClaims(ctx, jwt.MapClaims{"sub": "u1"})
	ctx = WithAuthError(ctx, errInvalidToken)

	assert.Equal(t, "editor", RoleFromContext(ctx))
	claims, ok := ClaimsFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, "u1", claims["sub"])
	assert.True(t, errors.Is(AuthErrorFromContext(ctx), errInvalidToken))
}

func TestExtractClaim(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		claims jwt.MapClaims
		path   string
		want   string
	}{
		{"flat claim", jwt.MapClaims{"role": "admin"}, "role", "admin"},
		{"nested claim", jwt.MapClaims{"app_metadata": map[string]any{"role": "viewer"}}, "app_metadata.role", "viewer"},
		{"deeply nested", jwt.MapClaims{"a": map[string]any{"b": map[string]any{"c": "deep"}}}, "a.b.c", "deep"},
		{"missing claim", jwt.MapClaims{"foo": "bar"}, "role", ""},
		{"non-string claim", jwt.MapClaims{"role": 42}, "role", ""},
		{"broken nested path", jwt.MapClaims{"a": "not-a-map"}, "a.b", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, extractClaim(tt.claims, tt.path))
		})
	}
}
