package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testOperatorKey = "op-secret-key"

// captured records what the middleware established in the request context.
type captured struct {
	called     bool
	role       string
	claims     jwt.MapClaims
	hasClaims  bool
	authErr    error
	isOperator bool
	tokenInURL string
	// a non-token query param, to prove the strip is surgical
	otherQueryParam string
}

// run drives cfg's middleware over a request decorated by setup, returning what
// the downstream handler observed. The middleware never rejects — it always
// reaches the handler — so the interesting output is the captured context, not
// the status code. It uses no policy store or logger (the operator-key path is
// exercised by runOp).
func run(t *testing.T, cfg Config, setup func(*http.Request)) captured {
	t.Helper()
	return runOp(t, cfg, nil, nil, setup)
}

// runOp is run with an explicit policy store and logger, so the operator-key
// path (which reads the live admin role from the store) can be exercised.
func runOp(t *testing.T, cfg Config, store *policy.Store, logger *slog.Logger, setup func(*http.Request)) captured {
	t.Helper()
	var c captured
	mw, err := Middleware(cfg, store, logger)
	require.NoError(t, err)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.called = true
		c.role = RoleFromContext(r.Context())
		c.claims, c.hasClaims = ClaimsFromContext(r.Context())
		c.authErr = AuthErrorFromContext(r.Context())
		c.isOperator = IsOperator(r.Context())
		c.tokenInURL = r.URL.Query().Get("token")
		c.otherQueryParam = r.URL.Query().Get("table")
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

func operatorHeader(key string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("X-Operator-Key", key) }
}

func authOperatorHeader(key string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Operator "+key) }
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

// TestMiddleware_LargeIntegerClaim_ExactThroughPolicy pins jwt.WithJSONNumber
// on the parser: a numeric claim above 2^53 must survive jwt.Parse exactly as
// issued, not as a float64 — which rounds 1234567890123456789 to
// 1234567890123456768, a *different* value a policy filter would silently bind.
// The claim rides a real signed token because a hand-built claims map (or a
// string-valued test claim) passes with or without the parser option; the tail
// asserts the exact digits reach a resolved policy filter, the consumer the
// precision exists for.
func TestMiddleware_LargeIntegerClaim_ExactThroughPolicy(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), bearer(testutil.MakeJWT(t, map[string]any{"role": "viewer", "tenant_id": int64(1234567890123456789)})))
	require.True(t, c.hasClaims)
	assert.Equal(t, json.Number("1234567890123456789"), c.claims["tenant_id"])

	eq := "{{ jwt.tenant_id }}"
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {Select: map[string]policy.RolePermissions{
			"viewer": {Filter: map[string]policy.Filter{"tenant_id": {Eq: &eq}}},
		}},
	}}
	perms := policy.Evaluate(p, "viewer", "clicks", "select", c.claims)
	require.True(t, perms.Allowed)
	assert.Equal(t, "`tenant_id` = ?", perms.WhereClause)
	assert.Equal(t, []any{"1234567890123456789"}, perms.WhereParams)
}

func TestMiddleware_BearerScheme_CaseInsensitive(t *testing.T) {
	t.Parallel()
	// RFC 7235 auth-schemes are case-insensitive; a lowercase / mixed-case
	// "bearer" must validate exactly like the canonical "Bearer" (regression
	// guard — bearerToken shares the case-insensitive authScheme matcher with
	// the operator path).
	tok := testutil.MakeJWT(t, map[string]any{"role": "editor"})
	for _, scheme := range []string{"bearer", "BEARER", "BeArEr"} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()
			c := run(t, cfg(), func(r *http.Request) { r.Header.Set("Authorization", scheme+" "+tok) })
			assert.Equal(t, "editor", c.role, "the Bearer auth-scheme must be case-insensitive")
			assert.True(t, c.hasClaims)
			assert.NoError(t, c.authErr)
		})
	}
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
	assert.Empty(t, c.tokenInURL, "the ?token param must be stripped so it stays out of our own logs")
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
	// The header won, but the unused query credential must not ride along in
	// r.URL into a later handler's log line.
	assert.Empty(t, c.tokenInURL, "the losing ?token param must be stripped too")
}

// A ?token is stripped even when it authorizes nothing, so an unused credential
// can't survive in r.URL — here the header is malformed, so neither path uses it.
func TestMiddleware_QueryTokenStrippedWithUnusableHeader(t *testing.T) {
	t.Parallel()
	query := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	c := run(t, cfg(), func(r *http.Request) {
		r.URL.RawQuery = "table=clicks&token=" + query
		r.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	})
	assert.Equal(t, "viewer", c.role, "a non-Bearer header falls through to the query token")
	assert.Empty(t, c.tokenInURL)
	assert.Equal(t, "clicks", c.otherQueryParam, "unrelated query params must survive the strip")
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

	_, err := Middleware(Config{JWKSURL: srv.URL}, nil, nil)
	require.Error(t, err, "an unreachable/erroring JWKS at boot must fail loudly")
}

func TestMiddleware_JWKSReachableAtBoot_OK(t *testing.T) {
	t.Parallel()
	// A reachable JWKS endpoint (even with an empty key set) constructs cleanly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	defer srv.Close()

	mw, err := Middleware(Config{JWKSURL: srv.URL}, nil, nil)
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

func TestMiddleware_OperatorKey(t *testing.T) {
	t.Parallel()
	adminStore := func() *policy.Store { return policy.NewMemoryStore(&policy.Policy{AdminRole: "admin"}) }
	// A valid JWT (signed with the test secret) for the fall-through / precedence cases.
	editorJWT := testutil.MakeJWT(t, map[string]any{"role": "editor"})
	withBoth := func(opKey string) func(*http.Request) {
		return func(r *http.Request) {
			r.Header.Set("X-Operator-Key", opKey)
			r.Header.Set("Authorization", "Bearer "+editorJWT)
		}
	}

	tests := []struct {
		name       string
		cfg        Config
		store      *policy.Store
		logger     *slog.Logger
		setup      func(*http.Request)
		wantOp     bool
		wantRole   string
		wantClaims bool
	}{
		{
			name:     "match sets operator bit and stamps the live admin role",
			cfg:      Config{JWTSecret: testutil.TestJWTSecret, RoleClaim: "role", OperatorKey: testOperatorKey},
			store:    adminStore(),
			logger:   testutil.NopLogger(), // exercise the audit-log (logger != nil) branch
			setup:    operatorHeader(testOperatorKey),
			wantOp:   true,
			wantRole: "admin",
		},
		{
			name:     "stamped role tracks a custom admin_role, read live",
			cfg:      Config{OperatorKey: testOperatorKey},
			store:    policy.NewMemoryStore(&policy.Policy{AdminRole: "superuser"}),
			setup:    operatorHeader(testOperatorKey),
			wantOp:   true,
			wantRole: "superuser",
		},
		{
			// Break-glass: a nil/deleted policy makes the admin role resolve to "",
			// but the operator bit is still set so RequireAdmin can admit the request
			// to restore the policy over HTTP.
			name:   "nil policy sets the operator bit but an empty role (break-glass)",
			cfg:    Config{OperatorKey: testOperatorKey},
			store:  policy.NewMemoryStore(nil),
			setup:  operatorHeader(testOperatorKey),
			wantOp: true,
		},
		{
			name:   "nil store does not panic and still authorizes",
			cfg:    Config{OperatorKey: testOperatorKey},
			store:  nil,
			setup:  operatorHeader(testOperatorKey),
			wantOp: true,
		},
		{
			// A non-matching key never authenticates; the request falls through to
			// the Bearer-token path (a valid JWT → role editor, claims set).
			name:       "wrong key falls through to the JWT path",
			cfg:        Config{JWTSecret: testutil.TestJWTSecret, RoleClaim: "role", OperatorKey: testOperatorKey},
			store:      adminStore(),
			setup:      withBoth("wrong-key"),
			wantRole:   "editor",
			wantClaims: true,
		},
		{
			// The operator key is checked before the Bearer token, so it wins even
			// when a valid JWT is also present (and never parses the JWT claims).
			name:     "operator key wins over a valid JWT",
			cfg:      Config{JWTSecret: testutil.TestJWTSecret, RoleClaim: "role", OperatorKey: testOperatorKey},
			store:    adminStore(),
			setup:    withBoth(testOperatorKey),
			wantOp:   true,
			wantRole: "admin",
		},
		{
			name:  "no operator key configured → header ignored, roleless",
			cfg:   Config{JWTSecret: testutil.TestJWTSecret, RoleClaim: "role"},
			store: adminStore(),
			setup: operatorHeader("anything"),
		},
		{
			name:  "operator key configured but header absent → roleless",
			cfg:   Config{OperatorKey: testOperatorKey},
			store: adminStore(),
			setup: nil,
		},
		{
			name:     "Authorization Operator scheme grants operator",
			cfg:      Config{JWTSecret: testutil.TestJWTSecret, RoleClaim: "role", OperatorKey: testOperatorKey},
			store:    adminStore(),
			setup:    authOperatorHeader(testOperatorKey),
			wantOp:   true,
			wantRole: "admin",
		},
		{
			// RFC 7235 auth-schemes are case-insensitive.
			name:     "Authorization scheme matched case-insensitively",
			cfg:      Config{OperatorKey: testOperatorKey},
			store:    adminStore(),
			setup:    func(r *http.Request) { r.Header.Set("Authorization", "operator "+testOperatorKey) },
			wantOp:   true,
			wantRole: "admin",
		},
		{
			// A Bearer credential is a JWT, never mistaken for the operator key.
			name:       "Authorization Bearer is a JWT, not the operator key",
			cfg:        Config{JWTSecret: testutil.TestJWTSecret, RoleClaim: "role", OperatorKey: testOperatorKey},
			store:      adminStore(),
			setup:      bearer(editorJWT),
			wantRole:   "editor",
			wantClaims: true,
		},
		{
			// The Authorization header takes precedence: a wrong Operator-scheme
			// value is used (and rejected) even when a correct X-Operator-Key is
			// also present — so it never silently falls back to the alias.
			name:  "Authorization Operator takes precedence over X-Operator-Key",
			cfg:   Config{OperatorKey: testOperatorKey},
			store: adminStore(),
			setup: func(r *http.Request) {
				r.Header.Set("Authorization", "Operator wrong-key")
				r.Header.Set("X-Operator-Key", testOperatorKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := runOp(t, tt.cfg, tt.store, tt.logger, tt.setup)
			assert.Equal(t, tt.wantOp, c.isOperator, "operator bit")
			assert.Equal(t, tt.wantRole, c.role, "role")
			assert.Equal(t, tt.wantClaims, c.hasClaims, "claims present")
			// Every operator-key outcome (match, or fall-through to a valid JWT, or
			// roleless) records no auth error.
			assert.NoError(t, c.authErr)
		})
	}
}

// The operator key is the most privileged credential and returns before the
// Bearer path, so it is the easiest place for the ?token strip to be skipped —
// which it was until the bearerToken call was hoisted above the operator branch.
func TestMiddleware_OperatorKey_StripsQueryToken(t *testing.T) {
	t.Parallel()
	store := policy.NewMemoryStore(&policy.Policy{AdminRole: "admin"})
	query := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	c := runOp(t, Config{OperatorKey: testOperatorKey}, store, nil, func(r *http.Request) {
		r.URL.RawQuery = "table=clicks&token=" + query
		r.Header.Set("X-Operator-Key", testOperatorKey)
	})
	assert.True(t, c.isOperator, "the operator key still wins")
	assert.Equal(t, "admin", c.role)
	assert.Empty(t, c.tokenInURL, "the unused ?token must be stripped on the operator path too")
	assert.Equal(t, "clicks", c.otherQueryParam)
}

// infoBufLogger returns a logger writing JSON records at Info+ to buf, so a test
// can assert both that the failed-operator-attempt WARN fires and that ordinary
// traffic does not emit it. Mirrors warnBufLogger in internal/api/errors_test.go.
func infoBufLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})), &buf
}

// A presented-but-wrong operator credential is recorded at WARN (so operators
// can alert on probing of the most privileged credential) while the request
// still falls through unauthenticated — no operator bit, roleless — because the
// middleware never rejects. A request carrying no operator credential must NOT
// emit that WARN, so ordinary traffic never drowns the signal. The counter
// (wavehouse_auth_operator_key_failures_total) rides the same branch; like its
// sibling wavehouse_ingest_dedupe_missing_id_total it isn't asserted here, but
// the failed-attempt cases in TestMiddleware_OperatorKey already exercise the
// Add call (proving it's safe under the default no-op meter).
func TestMiddleware_OperatorKey_FailedAttemptLogged(t *testing.T) {
	t.Parallel()
	cfg := Config{OperatorKey: testOperatorKey}
	store := policy.NewMemoryStore(&policy.Policy{AdminRole: "admin"})

	t.Run("wrong key via X-Operator-Key logs WARN and falls through", func(t *testing.T) {
		t.Parallel()
		logger, buf := infoBufLogger()
		c := runOp(t, cfg, store, logger, operatorHeader("wrong-key"))
		assert.False(t, c.isOperator, "a wrong key never sets the operator bit")
		assert.Empty(t, c.role, "wrong key + no JWT → roleless fall-through")
		assert.NoError(t, c.authErr)
		out := buf.String()
		assert.Contains(t, out, `"level":"WARN"`)
		assert.Contains(t, out, `"msg":"operator key authentication failed"`)
		assert.Contains(t, out, `"path":"/"`)
		assert.Contains(t, out, `"method":"GET"`)
	})

	t.Run("wrong key via Authorization Operator scheme logs WARN", func(t *testing.T) {
		t.Parallel()
		logger, buf := infoBufLogger()
		c := runOp(t, cfg, store, logger, func(r *http.Request) {
			r.Header.Set("Authorization", "Operator wrong-key")
		})
		assert.False(t, c.isOperator)
		assert.Contains(t, buf.String(), `"msg":"operator key authentication failed"`)
	})

	t.Run("absent operator credential does not emit the failed-attempt WARN", func(t *testing.T) {
		t.Parallel()
		logger, buf := infoBufLogger()
		c := runOp(t, cfg, store, logger, nil) // no operator header at all
		assert.False(t, c.isOperator)
		assert.NotContains(t, buf.String(), "operator key authentication failed",
			"an absent operator credential is an ordinary request, not a failed attempt")
	})

	t.Run("successful operator auth logs the INFO audit, not the failure WARN", func(t *testing.T) {
		t.Parallel()
		logger, buf := infoBufLogger()
		c := runOp(t, cfg, store, logger, operatorHeader(testOperatorKey))
		assert.True(t, c.isOperator)
		out := buf.String()
		assert.Contains(t, out, `"level":"INFO"`)
		assert.Contains(t, out, `"msg":"operator key authenticated request"`)
		assert.NotContains(t, out, "authentication failed")
	})
}

func TestContextHelpers_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert.Empty(t, RoleFromContext(ctx))
	_, ok := ClaimsFromContext(ctx)
	assert.False(t, ok)
	assert.NoError(t, AuthErrorFromContext(ctx))
	assert.False(t, IsOperator(ctx))

	ctx = WithRole(ctx, "editor")
	ctx = WithClaims(ctx, jwt.MapClaims{"sub": "u1"})
	ctx = WithAuthError(ctx, errInvalidToken)
	ctx = WithOperator(ctx)

	assert.Equal(t, "editor", RoleFromContext(ctx))
	claims, ok := ClaimsFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, "u1", claims["sub"])
	assert.True(t, errors.Is(AuthErrorFromContext(ctx), errInvalidToken))
	assert.True(t, IsOperator(ctx))
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
