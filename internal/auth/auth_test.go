package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
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
	// the query string the handler was left with, byte for byte
	rawQuery string
}

// testTenantKey carries the tenant asTenant sets, the way the store
// api.TenantMW resolves does in production.
type testTenantKey struct{}

// testTenantOf reads the tenant asTenant set.
func testTenantOf(ctx context.Context) (tenant.ID, bool) {
	id, ok := ctx.Value(testTenantKey{}).(tenant.ID)
	return id, ok
}

// newAuth builds an Authenticator whose default tenant is wired with w,
// closed with the test.
func newAuth(t *testing.T, cfg Config, w Wiring, policies PolicySource) *Authenticator {
	t.Helper()
	a := NewAuthenticator(cfg, testTenantOf, policies)
	a.Reconfigure(tenant.Default, w)
	t.Cleanup(a.Close)
	return a
}

// run drives the middleware of cfg + w (the default tenant's wiring) over a
// request decorated by setup, returning what the downstream handler
// observed. The middleware never rejects — it always reaches the handler —
// so the interesting output is the captured context, not the status code.
// It uses no policy source (the operator-key path is exercised by runOp).
func run(t *testing.T, cfg Config, w Wiring, setup func(*http.Request)) captured {
	t.Helper()
	return runOp(t, cfg, w, nil, setup)
}

// runOp is run with an explicit policy source, so the operator-key path
// (which reads the live admin role from the request tenant's policy) can be
// exercised.
func runOp(t *testing.T, cfg Config, w Wiring, policies PolicySource, setup func(*http.Request)) captured {
	t.Helper()
	return serve(t, newAuth(t, cfg, w, policies), setup)
}

// serve drives one request through a's middleware.
func serve(t *testing.T, a *Authenticator, setup func(*http.Request)) captured {
	t.Helper()
	var c captured
	h := a.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.called = true
		c.role = RoleFromContext(r.Context())
		c.claims, c.hasClaims = ClaimsFromContext(r.Context())
		c.authErr = AuthErrorFromContext(r.Context())
		c.isOperator = IsOperator(r.Context())
		c.tokenInURL = r.URL.Query().Get("token")
		c.otherQueryParam = r.URL.Query().Get("table")
		c.rawQuery = r.URL.RawQuery
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

func cfg() Config { return Config{JWTSecret: testutil.TestJWTSecret} }

func roleClaim() Wiring { return Wiring{RoleClaim: "role"} }

// staticPolicies is a PolicySource fixed to p, whatever the tenant.
func staticPolicies(p *policy.Policy) PolicySource {
	return func(tenant.ID) *policy.Policy { return p }
}

// asTenant sets the request's resolved tenant, as api.TenantMW does.
func asTenant(id tenant.ID) func(*http.Request) {
	return func(r *http.Request) { *r = *r.WithContext(context.WithValue(r.Context(), testTenantKey{}, id)) }
}

// both applies every setup in order.
func both(setups ...func(*http.Request)) func(*http.Request) {
	return func(r *http.Request) {
		for _, setup := range setups {
			setup(r)
		}
	}
}

func TestMiddleware_NoToken_RolelessNoError(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), roleClaim(), nil)
	assert.Empty(t, c.role, "no token → empty role (resolved to default_role downstream)")
	assert.False(t, c.hasClaims)
	assert.NoError(t, c.authErr, "absent token is not an auth error")
}

func TestMiddleware_NonBearerHeader_TreatedAsNoToken(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), roleClaim(), func(r *http.Request) { r.Header.Set("Authorization", "Basic dXNlcjpwYXNz") })
	assert.Empty(t, c.role)
	assert.NoError(t, c.authErr)
}

func TestMiddleware_ValidToken_FlatRole(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), roleClaim(), bearer(testutil.MakeJWT(t, map[string]any{"role": "editor"})))
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
	c := run(t, cfg(), roleClaim(), bearer(testutil.MakeJWT(t, map[string]any{"role": "viewer", "tenant_id": int64(1234567890123456789)})))
	require.True(t, c.hasClaims)
	assert.Equal(t, json.Number("1234567890123456789"), c.claims["tenant_id"])

	eq := "{{ jwt.tenant_id }}"
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"tenant_id": {Eq: &eq}}}}},
	}}
	perms := policy.Evaluate(p, "viewer", "clicks", "select", c.claims)
	require.True(t, perms.Allowed)
	assert.Equal(t, "`tenant_id` = ?", perms.Select.WhereClause)
	assert.Equal(t, []any{"1234567890123456789"}, perms.Select.WhereParams)
}

// TestMiddleware_NumericClaimSpelling_BindsCanonically: json.Number keeps the
// token's literal spelling, so without normalization the bound filter value
// would depend on how the IdP spelled the number — and a numeric ClickHouse
// column rejects '1.0'/'1e3' as a TYPE_MISMATCH error on every query for that
// role. The claims ride a real signed token (a json.Number claim value
// marshals verbatim into the payload) so the exact parser configuration is
// what's under test, per the note on the large-integer test above.
func TestMiddleware_NumericClaimSpelling_BindsCanonically(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		literal string
		want    string
	}{
		{"integer spelling stays exact", "1234567890123456789", "1234567890123456789"},
		{"float spelling of an integer", "1.0", "1"},
		{"exponent spelling", "1e3", "1000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := run(t, cfg(), roleClaim(), bearer(testutil.MakeJWT(t, map[string]any{"role": "viewer", "tenant_id": json.Number(tt.literal)})))
			require.True(t, c.hasClaims)

			eq := "{{ jwt.tenant_id }}"
			p := &policy.Policy{Tables: map[string]policy.TablePolicy{
				"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"tenant_id": {Eq: &eq}}}}},
			}}
			perms := policy.Evaluate(p, "viewer", "clicks", "select", c.claims)
			require.True(t, perms.Allowed)
			assert.Equal(t, []any{tt.want}, perms.Select.WhereParams)
		})
	}
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
			c := run(t, cfg(), roleClaim(), func(r *http.Request) { r.Header.Set("Authorization", scheme+" "+tok) })
			assert.Equal(t, "editor", c.role, "the Bearer auth-scheme must be case-insensitive")
			assert.True(t, c.hasClaims)
			assert.NoError(t, c.authErr)
		})
	}
}

func TestMiddleware_ValidToken_NestedRoleClaim(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeJWT(t, map[string]any{"app_metadata": map[string]any{"role": "manager"}})
	c := run(t, cfg(), Wiring{RoleClaim: "app_metadata.role"}, bearer(tok))
	assert.Equal(t, "manager", c.role)
}

func TestMiddleware_ValidToken_NoRoleClaim_RolelessNoError(t *testing.T) {
	t.Parallel()
	// A valid token without the role claim authenticates but carries no role —
	// it resolves to default_role downstream, and is NOT an auth error.
	c := run(t, cfg(), roleClaim(), bearer(testutil.MakeJWT(t, map[string]any{"sub": "u1"})))
	assert.Empty(t, c.role)
	assert.True(t, c.hasClaims, "claims are still set for a valid token")
	assert.NoError(t, c.authErr)
}

func TestMiddleware_DefaultRoleClaim(t *testing.T) {
	t.Parallel()
	// Empty RoleClaim defaults to "role".
	c := run(t, cfg(), Wiring{}, bearer(testutil.MakeJWT(t, map[string]any{"role": "viewer"})))
	assert.Equal(t, "viewer", c.role)
}

func TestMiddleware_InvalidToken_FallsBackWithError(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), roleClaim(), bearer("not.a.jwt"))
	assert.Empty(t, c.role, "invalid token falls back to the default role")
	assert.False(t, c.hasClaims)
	require.Error(t, c.authErr)
	assert.True(t, errors.Is(c.authErr, errInvalidToken))
}

func TestMiddleware_WrongSecret_FallsBackWithError(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeJWT(t, map[string]any{"role": "viewer"}) // signed with testutil secret
	c := run(t, Config{JWTSecret: "a-different-secret-entirely!"}, roleClaim(), bearer(tok))
	assert.Empty(t, c.role)
	assert.True(t, errors.Is(c.authErr, errInvalidToken))
}

func TestMiddleware_ExpiredToken_FallsBackWithExpiredError(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeExpiredJWT(t, map[string]any{"role": "viewer"})
	c := run(t, cfg(), roleClaim(), bearer(tok))
	assert.Empty(t, c.role)
	require.Error(t, c.authErr)
	assert.True(t, errors.Is(c.authErr, errTokenExpired), "expired tokens get the distinct expired error")
	assert.Equal(t, "token expired", c.authErr.Error())
}

// TestMiddleware_ExpZeroClaim_TokenExpired pins the deliberate validation
// shift that rides along with jwt.WithJSONNumber: float64 decoding
// special-cased a literal exp of 0 as "no expiry claim" (the token never
// expired); as a json.Number it reads as the epoch, so the token is expired.
// A jwt/v5 upgrade or a dropped parser option that silently restored
// never-expiring exp:0 tokens must fail here.
func TestMiddleware_ExpZeroClaim_TokenExpired(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeJWT(t, map[string]any{"role": "viewer", "exp": 0})
	c := run(t, cfg(), roleClaim(), bearer(tok))
	assert.Empty(t, c.role)
	require.Error(t, c.authErr)
	assert.True(t, errors.Is(c.authErr, errTokenExpired), "exp: 0 must read as the epoch (expired), not as no-expiry")
}

func TestMiddleware_QueryParamToken_StrippedFromURL(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	c := run(t, cfg(), roleClaim(), func(r *http.Request) { r.URL.RawQuery = "token=" + tok })
	assert.Equal(t, "viewer", c.role)
	assert.Empty(t, c.tokenInURL, "the ?token param must be stripped so it stays out of our own logs")
}

func TestMiddleware_HeaderTakesPrecedenceOverQuery(t *testing.T) {
	t.Parallel()
	header := testutil.MakeJWT(t, map[string]any{"role": "admin"})
	query := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	c := run(t, cfg(), roleClaim(), func(r *http.Request) {
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
	c := run(t, cfg(), roleClaim(), func(r *http.Request) {
		r.URL.RawQuery = "table=clicks&token=" + query
		r.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	})
	assert.Equal(t, "viewer", c.role, "a non-Bearer header falls through to the query token")
	assert.Empty(t, c.tokenInURL)
	assert.Equal(t, "clicks", c.otherQueryParam, "unrelated query params must survive the strip")
}

// The strip must not repair a query that does not parse. Re-encoding what
// ParseQuery could read erases the pairs it could not, and a handler that
// parses strictly in order to refuse them (the ops ?tenant=) would then see a
// clean query: `?tenant=acme;x=1&token=…` read the default tenant. The token
// is still read and still removed; every other byte reaches the handler as
// it was sent.
func TestMiddleware_QueryToken_MalformedQuerySurvivesTheStrip(t *testing.T) {
	t.Parallel()
	tok := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	tests := []struct {
		name, query, want string
	}{
		{name: "semicolon pair before the token", query: "tenant=acme;x=1&token=" + tok, want: "tenant=acme;x=1"},
		{name: "semicolon pair after the token", query: "token=" + tok + "&tenant=acme;x=1", want: "tenant=acme;x=1"},
		{name: "bad escape", query: "tenant=%zz&token=" + tok, want: "tenant=%zz"},
		{name: "the rest is left in the order and spelling it was sent", query: "b=2&token=" + tok + "&a=%41&x=%zz", want: "b=2&a=%41&x=%zz"},
		{name: "an escaped key is the token too", query: "%74oken=" + tok + "&x=%zz", want: "x=%zz"},
		{name: "every token pair goes", query: "token=" + tok + "&x=%zz&token=second", want: "x=%zz"},
		{name: "a pair the parser could not read as a token stays", query: "token=" + tok + "&token=%zz", want: "token=%zz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := run(t, cfg(), roleClaim(), func(r *http.Request) { r.URL.RawQuery = tt.query })
			assert.Equal(t, "viewer", c.role, "a malformed pair elsewhere does not cost the request its token")
			assert.Equal(t, tt.want, c.rawQuery)
			assert.Empty(t, c.tokenInURL)
		})
	}
}

func TestMiddleware_InvalidQueryParamToken_FallsBackWithError(t *testing.T) {
	t.Parallel()
	c := run(t, cfg(), roleClaim(), func(r *http.Request) { r.URL.RawQuery = "token=not.a.jwt" })
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
	c := run(t, cfg(), roleClaim(), bearer(signed))
	assert.Empty(t, c.role, "alg:none token must not authenticate")
	assert.False(t, c.hasClaims)
	assert.True(t, errors.Is(c.authErr, errInvalidToken))
}

// jwksServer serves one Ed25519 verification key under kid, and returns the
// signer that pairs with it. Deliberately larger than a real JWK Set when
// pad is set: the response is padded past the fetch cap.
func jwksServer(t *testing.T, kid string, pad int) (*httptest.Server, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	body, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid,
		"x": base64.RawURLEncoding.EncodeToString(pub),
	}}, "pad": strings.Repeat("x", pad)})
	require.NoError(t, err)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, priv
}

// signEdDSA signs claims with priv under kid, the token a JWKS deployment's
// IdP issues.
func signEdDSA(t *testing.T, priv ed25519.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	claims["exp"] = jwt.NewNumericDate(time.Now().Add(time.Hour))
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(priv)
	require.NoError(t, err)
	return signed
}

// roleEventually reports the role serve observes for setup once it is want,
// for a token that validates only after a JWKS fetch has landed.
func roleEventually(t *testing.T, a *Authenticator, setup func(*http.Request), want string) {
	t.Helper()
	require.Eventually(t, func() bool { return serve(t, a, setup).role == want }, 5*time.Second, 10*time.Millisecond,
		"the token never authenticated as %q", want)
}

// A JWKS-verified token authenticates once the key set has been fetched.
// Construction itself does not wait for the fetch: the verifier is in place
// at once, fail-closed, and the fetched keys flip it.
func TestMiddleware_JWKS_TokenAuthenticatesOnceFetched(t *testing.T) {
	t.Parallel()
	srv, priv := jwksServer(t, "k1", 0)
	a := newAuth(t, Config{}, Wiring{JWKSURL: srv.URL, RoleClaim: "role"}, nil)
	roleEventually(t, a, bearer(signEdDSA(t, priv, "k1", jwt.MapClaims{"role": "editor"})), "editor")
	c := serve(t, a, bearer(signEdDSA(t, priv, "k1", jwt.MapClaims{"role": "editor"})))
	require.True(t, c.hasClaims)
	assert.NoError(t, c.authErr)
}

// An unreachable JWKS endpoint costs nothing at construction and fails
// closed: the verifier exists, no token validates — the HMAC secret is not
// a fallback for a JWKS tenant — and a token of the right family records
// ErrVerifierPending, the one the tenant routes answer 503 to, rather than
// invalid-token.
func TestMiddleware_JWKSUnreachable_FailsClosedWithoutWaiting(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	started := time.Now()
	a := newAuth(t, cfg(), Wiring{JWKSURL: srv.URL, RoleClaim: "role"}, nil)
	assert.Less(t, time.Since(started), time.Second, "construction must not wait on the endpoint")

	c := serve(t, a, bearer(testutil.MakeJWT(t, map[string]any{"role": "admin"})))
	assert.Empty(t, c.role, "an HMAC token must not validate under a JWKS verifier, fetched or not")
	assert.False(t, c.hasClaims)
	assert.True(t, errors.Is(c.authErr, errInvalidToken), "the wrong family is refused before any key is consulted")

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	c = serve(t, a, bearer(signEdDSA(t, priv, "k1", jwt.MapClaims{"role": "admin"})))
	assert.Empty(t, c.role)
	assert.True(t, errors.Is(c.authErr, ErrVerifierPending), "a token that could not be checked is pending, not invalid")
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

	// An empty set is a fetched set: the verifier is ready, and the token is
	// invalid rather than pending.
	a := newAuth(t, Config{}, Wiring{JWKSURL: srv.URL, RoleClaim: "role"}, nil)
	var c captured
	require.Eventually(t, func() bool {
		c = serve(t, a, bearer(signed))
		return !errors.Is(c.authErr, ErrVerifierPending)
	}, 5*time.Second, 10*time.Millisecond, "the empty set never loaded")
	assert.Empty(t, c.role, "empty JWKS → no key validates the token → roleless default")
	assert.False(t, c.hasClaims)
	assert.True(t, errors.Is(c.authErr, errInvalidToken), "present-but-unverifiable token records invalid-token")
}

// A JWK Set response past the cap is refused, named as such in the refresh
// warning, and the verifier stays fail-closed. Captures the default logger,
// so not parallel.
func TestMiddleware_JWKSResponseCap(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelWarn)
	srv, priv := jwksServer(t, "k1", jwksMaxBytes)
	a := newAuth(t, Config{}, Wiring{JWKSURL: srv.URL, RoleClaim: "role"}, nil)
	require.Eventually(t, func() bool { return strings.Contains(logs.String(), errJWKSTooLarge.Error()) },
		5*time.Second, 10*time.Millisecond, "the refusal names the cap: %s", logs.String())
	c := serve(t, a, bearer(signEdDSA(t, priv, "k1", jwt.MapClaims{"role": "editor"})))
	assert.Empty(t, c.role)
	assert.True(t, errors.Is(c.authErr, ErrVerifierPending))
}

// Each tenant verifies with its own wiring: a token an IdP issued for one
// tenant is rejected under another tenant's header, whatever key or secret
// the other uses. A tenant-exempt route (no resolved tenant) verifies as the
// default tenant, and a tenant with no verifier at all fails closed.
func TestAuthenticator_VerifierPerTenant(t *testing.T) {
	t.Parallel()
	acme, acmeKey := jwksServer(t, "acme-1", 0)
	globex, globexKey := jwksServer(t, "globex-1", 0)
	a := NewAuthenticator(cfg(), testTenantOf, nil)
	t.Cleanup(a.Close)
	a.Reconfigure(tenant.Default, roleClaim())
	a.Reconfigure("acme", Wiring{JWKSURL: acme.URL, RoleClaim: "role"})
	a.Reconfigure("globex", Wiring{JWKSURL: globex.URL, RoleClaim: "role"})

	acmeToken := signEdDSA(t, acmeKey, "acme-1", jwt.MapClaims{"role": "editor"})
	globexToken := signEdDSA(t, globexKey, "globex-1", jwt.MapClaims{"role": "editor"})
	hmacToken := testutil.MakeJWT(t, map[string]any{"role": "editor"})
	roleEventually(t, a, both(asTenant("acme"), bearer(acmeToken)), "editor")
	roleEventually(t, a, both(asTenant("globex"), bearer(globexToken)), "editor")

	tests := []struct {
		name  string
		setup func(*http.Request)
	}{
		{name: "acme's token under globex", setup: both(asTenant("globex"), bearer(acmeToken))},
		{name: "globex's token under acme", setup: both(asTenant("acme"), bearer(globexToken))},
		{name: "the HMAC token under a JWKS tenant", setup: both(asTenant("acme"), bearer(hmacToken))},
		{name: "a JWKS token under the HMAC default tenant", setup: both(asTenant(tenant.Default), bearer(acmeToken))},
		{name: "a JWKS token on a tenant-exempt route", setup: bearer(acmeToken)},
		{name: "a tenant with no verifier", setup: both(asTenant("initech"), bearer(hmacToken))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := serve(t, a, tt.setup)
			assert.Empty(t, c.role)
			assert.False(t, c.hasClaims)
			assert.True(t, errors.Is(c.authErr, errInvalidToken))
		})
	}
	t.Run("the HMAC token on a tenant-exempt route is the default tenant's", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "editor", serve(t, a, bearer(hmacToken)).role)
	})
	t.Run("no token under a tenant with no verifier is roleless, not an error", func(t *testing.T) {
		t.Parallel()
		c := serve(t, a, asTenant("initech"))
		assert.Empty(t, c.role)
		assert.NoError(t, c.authErr)
	})
}

// A tenant served does not vouch for loses its verifier, and Close drops
// them all: past either, a token for that tenant fails closed.
func TestAuthenticator_PruneAndClose(t *testing.T) {
	t.Parallel()
	a := NewAuthenticator(cfg(), testTenantOf, nil)
	a.Reconfigure(tenant.Default, roleClaim())
	a.Reconfigure("acme", roleClaim())
	a.Reconfigure("globex", roleClaim())
	tok := bearer(testutil.MakeJWT(t, map[string]any{"role": "editor"}))
	require.Equal(t, "editor", serve(t, a, both(asTenant("acme"), tok)).role)

	a.Prune(func(id tenant.ID) bool { return id == tenant.Default || id == "globex" })
	assert.Equal(t, "editor", serve(t, a, both(asTenant("globex"), tok)).role, "a served tenant keeps its verifier")
	assert.Equal(t, "editor", serve(t, a, tok).role)
	c := serve(t, a, both(asTenant("acme"), tok))
	assert.Empty(t, c.role, "a forgotten tenant's token fails closed")
	assert.True(t, errors.Is(c.authErr, errInvalidToken))

	a.Close()
	c = serve(t, a, tok)
	assert.Empty(t, c.role)
	assert.True(t, errors.Is(c.authErr, errInvalidToken))
}

func TestMiddleware_OperatorKey(t *testing.T) {
	t.Parallel()
	adminStore := func() PolicySource { return staticPolicies(&policy.Policy{AdminRole: "admin"}) }
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
		w          Wiring
		store      PolicySource
		setup      func(*http.Request)
		wantOp     bool
		wantRole   string
		wantClaims bool
	}{
		{
			name:     "match sets operator bit and stamps the live admin role",
			cfg:      Config{JWTSecret: testutil.TestJWTSecret, OperatorKey: testOperatorKey},
			w:        roleClaim(),
			store:    adminStore(),
			setup:    operatorHeader(testOperatorKey),
			wantOp:   true,
			wantRole: "admin",
		},
		{
			name:     "stamped role tracks a custom admin_role, read live",
			cfg:      Config{OperatorKey: testOperatorKey},
			store:    staticPolicies(&policy.Policy{AdminRole: "superuser"}),
			setup:    operatorHeader(testOperatorKey),
			wantOp:   true,
			wantRole: "superuser",
		},
		{
			// Break-glass: a nil/deleted policy makes the admin role resolve to "",
			// but the operator bit is still set so RequireAdmin can admit the request
			// to the admin surface (inspect policy, reload settings) while locked out.
			name:   "nil policy sets the operator bit but an empty role (break-glass)",
			cfg:    Config{OperatorKey: testOperatorKey},
			store:  staticPolicies(nil),
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
			cfg:        Config{JWTSecret: testutil.TestJWTSecret, OperatorKey: testOperatorKey},
			w:          roleClaim(),
			store:      adminStore(),
			setup:      withBoth("wrong-key"),
			wantRole:   "editor",
			wantClaims: true,
		},
		{
			// The operator key is checked before the Bearer token, so it wins even
			// when a valid JWT is also present (and never parses the JWT claims).
			name:     "operator key wins over a valid JWT",
			cfg:      Config{JWTSecret: testutil.TestJWTSecret, OperatorKey: testOperatorKey},
			w:        roleClaim(),
			store:    adminStore(),
			setup:    withBoth(testOperatorKey),
			wantOp:   true,
			wantRole: "admin",
		},
		{
			name:  "no operator key configured → header ignored, roleless",
			cfg:   cfg(),
			w:     roleClaim(),
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
			cfg:      Config{JWTSecret: testutil.TestJWTSecret, OperatorKey: testOperatorKey},
			w:        roleClaim(),
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
			cfg:        Config{JWTSecret: testutil.TestJWTSecret, OperatorKey: testOperatorKey},
			w:          roleClaim(),
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
			c := runOp(t, tt.cfg, tt.w, tt.store, tt.setup)
			assert.Equal(t, tt.wantOp, c.isOperator, "operator bit")
			assert.Equal(t, tt.wantRole, c.role, "role")
			assert.Equal(t, tt.wantClaims, c.hasClaims, "claims present")
			// Every operator-key outcome (match, or fall-through to a valid JWT, or
			// roleless) records no auth error.
			assert.NoError(t, c.authErr)
		})
	}
}

// The admin role the operator key stamps is the request tenant's, read from
// that tenant's policy: what the policy evaluator's admin bypass then grants
// is that tenant's data plane under that tenant's admin_role spelling. On a
// tenant-exempt route it is the default tenant's.
func TestMiddleware_OperatorKey_AdminRoleIsTheRequestTenants(t *testing.T) {
	t.Parallel()
	policies := func(id tenant.ID) *policy.Policy {
		switch id {
		case "acme":
			return &policy.Policy{AdminRole: "acme-admin"}
		case tenant.Default:
			return &policy.Policy{AdminRole: "admin"}
		}
		return nil
	}
	a := NewAuthenticator(Config{OperatorKey: testOperatorKey}, testTenantOf, policies)
	t.Cleanup(a.Close)
	a.Reconfigure(tenant.Default, roleClaim())
	a.Reconfigure("acme", roleClaim())
	a.Reconfigure("globex", roleClaim())

	tests := []struct {
		name  string
		setup func(*http.Request)
		want  string
	}{
		{name: "tenant route", setup: both(asTenant("acme"), operatorHeader(testOperatorKey)), want: "acme-admin"},
		{name: "tenant-exempt route reads the default tenant", setup: operatorHeader(testOperatorKey), want: "admin"},
		{name: "a tenant with no policy stamps no role, operator bit only", setup: both(asTenant("globex"), operatorHeader(testOperatorKey))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := serve(t, a, tt.setup)
			assert.True(t, c.isOperator)
			assert.Equal(t, tt.want, c.role)
		})
	}
}

// The operator key is the most privileged credential and returns before the
// Bearer path, so it is the easiest place for the ?token strip to be skipped —
// which it was until the bearerToken call was hoisted above the operator branch.
func TestMiddleware_OperatorKey_StripsQueryToken(t *testing.T) {
	t.Parallel()
	store := staticPolicies(&policy.Policy{AdminRole: "admin"})
	query := testutil.MakeJWT(t, map[string]any{"role": "viewer"})
	c := runOp(t, Config{OperatorKey: testOperatorKey}, Wiring{}, store, func(r *http.Request) {
		r.URL.RawQuery = "table=clicks&token=" + query
		r.Header.Set("X-Operator-Key", testOperatorKey)
	})
	assert.True(t, c.isOperator, "the operator key still wins")
	assert.Equal(t, "admin", c.role)
	assert.Empty(t, c.tokenInURL, "the unused ?token must be stripped on the operator path too")
	assert.Equal(t, "clicks", c.otherQueryParam)
}

// captureInfo routes the default logger's Info+ records to the returned
// buffer, so a test can assert both that the failed-operator-attempt WARN
// fires and that ordinary traffic does not emit it. The default logger is
// process-wide, so the tests that call it run serially (see logtest.Capture).
func captureInfo(t *testing.T) *logtest.Buffer {
	t.Helper()
	return logtest.Capture(t, slog.LevelInfo)
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
	cfg := Config{OperatorKey: testOperatorKey}
	store := staticPolicies(&policy.Policy{AdminRole: "admin"})

	t.Run("wrong key via X-Operator-Key logs WARN and falls through", func(t *testing.T) {
		buf := captureInfo(t)
		c := runOp(t, cfg, Wiring{}, store, operatorHeader("wrong-key"))
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
		buf := captureInfo(t)
		c := runOp(t, cfg, Wiring{}, store, func(r *http.Request) {
			r.Header.Set("Authorization", "Operator wrong-key")
		})
		assert.False(t, c.isOperator)
		assert.Contains(t, buf.String(), `"msg":"operator key authentication failed"`)
	})

	t.Run("absent operator credential does not emit the failed-attempt WARN", func(t *testing.T) {
		buf := captureInfo(t)
		c := runOp(t, cfg, Wiring{}, store, nil) // no operator header at all
		assert.False(t, c.isOperator)
		assert.NotContains(t, buf.String(), "operator key authentication failed",
			"an absent operator credential is an ordinary request, not a failed attempt")
	})

	t.Run("successful operator auth logs the INFO audit, not the failure WARN", func(t *testing.T) {
		buf := captureInfo(t)
		c := runOp(t, cfg, Wiring{}, store, operatorHeader(testOperatorKey))
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

// TestAuthenticator_ReconfigureSwapsRoleClaim pins the reload contract: the
// middleware reads the current verifier per request, so a Reconfigure that
// changes role_claim is visible to the next request with no rebuild — and one
// that changes nothing keeps the verifier it has, refresh and all.
func TestAuthenticator_ReconfigureSwapsRoleClaim(t *testing.T) {
	t.Parallel()
	a := newAuth(t, cfg(), roleClaim(), nil)
	var got string
	h := a.Middleware()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = RoleFromContext(r.Context()) }))

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"role": "old", "app_metadata": map[string]any{"role": "new"}, "exp": time.Now().Add(time.Hour).Unix()})
	signed, err := tok.SignedString([]byte(testutil.TestJWTSecret))
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+signed)

	h.ServeHTTP(httptest.NewRecorder(), req)
	assert.Equal(t, "old", got)

	before := (*a.verifiers.Load())[tenant.Default]
	a.Reconfigure(tenant.Default, roleClaim())
	assert.Same(t, before, (*a.verifiers.Load())[tenant.Default], "unchanged wiring keeps the verifier")

	a.Reconfigure(tenant.Default, Wiring{RoleClaim: "app_metadata.role"})
	h.ServeHTTP(httptest.NewRecorder(), req)
	assert.Equal(t, "new", got, "the next request sees the reconfigured claim path")
}

// TestAuthenticator_ReconfigureAppliesUnreachableJWKS pins "settings are the
// authority": a reload pointing at an unreachable JWKS swaps the verifier
// anyway, so the HMAC token stops validating (fail closed) instead of the
// previous verifier lingering.
func TestAuthenticator_ReconfigureAppliesUnreachableJWKS(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	a := newAuth(t, cfg(), Wiring{}, nil)

	var got string
	h := a.Middleware()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = RoleFromContext(r.Context()) }))
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"role": "analyst", "exp": time.Now().Add(time.Hour).Unix()})
	signed, err := tok.SignedString([]byte(testutil.TestJWTSecret))
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	h.ServeHTTP(httptest.NewRecorder(), req)
	require.Equal(t, "analyst", got)

	a.Reconfigure(tenant.Default, Wiring{JWKSURL: srv.URL})
	got = ""
	h.ServeHTTP(httptest.NewRecorder(), req)
	assert.Empty(t, got, "the unreachable JWKS is applied: the HMAC token no longer authenticates")

	// A reachable JWKS on the next reload swaps again (still asymmetric-only).
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"keys":[]}`)) }))
	defer ok.Close()
	a.Reconfigure(tenant.Default, Wiring{JWKSURL: ok.URL})
	got = ""
	h.ServeHTTP(httptest.NewRecorder(), req)
	assert.Empty(t, got)
}
