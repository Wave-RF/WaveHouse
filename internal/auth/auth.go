package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"
)

// Config is the boot-config half of authentication: the secrets, fixed for
// the process lifetime and shared by every tenant.
type Config struct {
	// JWTSecret is the HMAC secret, the verifier of a tenant whose Wiring
	// names no JWKS URL.
	JWTSecret string
	// OperatorKey is the optional non-JWT operator credential; a match on
	// the presented credential (Authorization: Operator <key>, or the
	// X-Operator-Key alias) authorizes a full-access platform operator (see
	// Middleware).
	OperatorKey string
}

// Wiring is one tenant's half: the verifier wiring its settings directory's
// auth block carries, adopted per tenant (Authenticator.Reconfigure).
type Wiring struct {
	JWKSURL   string
	RoleClaim string // dot-separated claim path, e.g. "role" or "app_metadata.role"
}

// roleClaim is RoleClaim with the default applied.
func (w Wiring) roleClaim() string {
	if w.RoleClaim == "" {
		return "role"
	}
	return w.RoleClaim
}

// TenantSource names the tenant a request resolved to — the store
// api.TenantMW put in the context, and its Tenant() — and false on a
// tenant-exempt route, where none was resolved.
type TenantSource func(context.Context) (tenant.ID, bool)

// PolicySource yields a tenant's access-control policy, read per request
// so a settings reload applies to the next one. nil for a tenant that is
// not being served — the ops tree over a nested directory with no tenant 0,
// where the gate reads no policy either.
type PolicySource func(tenant.ID) *policy.Policy

// jwksMaxBytes caps a JWK Set response. A set is a handful of keys of a few
// hundred bytes each, so the cap is far above any real one and only stops a
// misconfigured or hostile endpoint from having a fetch read without bound.
const jwksMaxBytes = 1 << 20

// ErrVerifierPending is the auth error stashed for a token that could not
// be checked because its tenant's JWKS has not been fetched yet — at boot,
// or after a reload moved the tenant to a new URL. Distinct from an invalid
// token on purpose: the token may well be good, so the tenant route answers
// 503 + Retry-After (api) rather than evaluate the request under the policy
// default_role, which could accept its data under a lesser role while
// another pod holding the keys would have served it properly.
var ErrVerifierPending = errors.New("token verifier not ready: the tenant's JWKS has not been fetched yet")

var (
	hmacMethods       = []string{"HS256", "HS384", "HS512"}
	asymmetricMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512", "EdDSA"}

	errJWKSTooLarge = fmt.Errorf("jwks response exceeds %d bytes", jwksMaxBytes)
)

// verifier is one tenant's immutable JWT verification setup: the key source,
// the signing-algorithm allowlist pinned to it, and the role claim path. The
// Authenticator swaps a whole verifier atomically, so a request never sees a
// JWKS key source paired with the HMAC allowlist.
type verifier struct {
	secret       string
	roleClaim    string
	url          string
	validMethods []string
	// jwks is the JWKS key source (fetch); nil until built.
	jwks atomic.Pointer[keyfunc.Keyfunc]
	// ready reports that a key set has been fetched at least once: the
	// library hands back an empty set when the first fetch fails, so the key
	// source existing is not the signal — a set being stored is
	// (observedKeys). Until then the verifier is pending.
	ready atomic.Bool
	// cancel stops the fetch and the library's refresh goroutine once the
	// verifier is replaced.
	cancel context.CancelFunc
}

func (v *verifier) keyFunc(t *jwt.Token) (any, error) {
	// JWKS-or-HMAC, JWKS first: when configured it is the sole verifier; the
	// HMAC shared secret is reached only when JWKS isn't in play. A key set
	// not fetched yet fails closed rather than falling to the secret.
	if v.url != "" {
		jwks := v.jwks.Load()
		if jwks == nil || !v.ready.Load() {
			return nil, ErrVerifierPending
		}
		return (*jwks).Keyfunc(t)
	}
	if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, jwt.ErrSignatureInvalid
	}
	return []byte(v.secret), nil
}

// newVerifier builds a verifier from the secrets and one tenant's wiring.
// Accepted signing algorithms are restricted to the verifier's family, so
// jwt.Parse rejects an unexpected alg (including "none") before keyFunc runs
// — defense-in-depth against alg-confusion and alg:none attacks. With a JWKS
// URL the key set is fetched off this goroutine: the verifier is in place at
// once and fails closed — no token validates, requests fall to the policy
// default_role — until the fetch succeeds, the same posture as an
// unreachable ClickHouse, so neither boot nor a reload waits on the URL.
func newVerifier(cfg Config, w Wiring) *verifier {
	v := &verifier{secret: cfg.JWTSecret, roleClaim: w.roleClaim(), url: w.JWKSURL}
	if v.url == "" {
		v.validMethods = hmacMethods
		return v
	}
	v.validMethods = asymmetricMethods
	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel
	go v.fetch(ctx)
	return v
}

// The library's refresh cadence, as keyfunc.NewDefault sets it: the hourly
// refresh, and the refetch an unknown key id triggers — one per five minutes
// per tenant, a request arriving while the limiter is closed waiting up to
// a minute on it. Kept library-managed by decision (#583 story 9).
const (
	jwksRefreshInterval  = time.Hour
	jwksUnknownKIDEvery  = 5 * time.Minute
	jwksRateLimitWaitMax = time.Minute
)

// fetch builds the JWKS key source: keyfunc.NewDefault's wiring, spelled
// out so the set is stored through observedKeys. The library fetches the
// set once and then keeps it fresh on its own — hourly, and on the first
// unknown key id, rate-limited — under ctx; a fetch that fails, the first
// included, is logged and retried, never returned: the verifier stays
// pending until one succeeds. The URL was validated as absolute http(s), so
// construction itself cannot fail; if it ever did, the verifier stays
// pending, which is the same fail-closed state.
func (v *verifier) fetch(ctx context.Context) {
	remote, err := jwkset.NewStorageFromHTTP(v.url, jwkset.HTTPClientStorageOptions{
		Client:                    jwksClient,
		Ctx:                       ctx,
		NoErrorReturnFirstHTTPReq: true,
		RefreshInterval:           jwksRefreshInterval,
		Storage:                   &observedKeys{Storage: jwkset.NewMemoryStorage(), ready: &v.ready},
		RefreshErrorHandler: func(_ context.Context, err error) {
			// A replaced verifier's cancelled fetch is not a failure. The
			// verifier's own context, not the handler's: the library hands
			// over the fetch's, already cancelled.
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "jwks refresh failed; no token validates until it succeeds", "url", v.url, "error", err)
			}
		},
	})
	if err == nil {
		var client jwkset.Storage
		client, err = jwkset.NewHTTPClient(jwkset.HTTPClientOptions{
			HTTPURLs:          map[string]jwkset.Storage{v.url: remote},
			RateLimitWaitMax:  jwksRateLimitWaitMax,
			RefreshUnknownKID: rate.NewLimiter(rate.Every(jwksUnknownKIDEvery), 1),
		})
		if err == nil {
			var jwks keyfunc.Keyfunc
			if jwks, err = keyfunc.New(keyfunc.Options{Ctx: ctx, Storage: client}); err == nil {
				v.jwks.Store(&jwks)
				return
			}
		}
	}
	slog.Error("jwks key source not created; no token validates for this verifier", "url", v.url, "error", err)
}

// observedKeys is the key set behind a verifier, marking it ready the first
// time the library stores a fetched set — the one success signal it gives,
// since a failed fetch is reported and an empty set is what it starts with.
type observedKeys struct {
	jwkset.Storage
	ready *atomic.Bool
}

func (k *observedKeys) KeyReplaceAll(ctx context.Context, given []jwkset.JWK) error {
	if err := k.Storage.KeyReplaceAll(ctx, given); err != nil {
		return err
	}
	k.ready.Store(true)
	return nil
}

// stop ends a replaced verifier's fetch and refresh.
func (v *verifier) stop() {
	if v.cancel != nil {
		v.cancel()
	}
}

// jwksClient is the fetch client every verifier shares: the default
// transport with each response body capped at jwksMaxBytes.
var jwksClient = &http.Client{Transport: cappedTransport{next: http.DefaultTransport, max: jwksMaxBytes}}

type cappedTransport struct {
	next http.RoundTripper
	max  int64
}

func (t cappedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &cappedBody{ReadCloser: resp.Body, remaining: t.max}
	return resp, nil
}

// cappedBody fails a read past its cap with errJWKSTooLarge — an error that
// names the cause, where io.LimitReader's EOF would leave a truncated set to
// fail as a JSON syntax error.
type cappedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		return 0, errJWKSTooLarge
	}
	return n, err
}

// Authenticator owns one verifier per tenant behind the middleware, so a
// settings reload can replace a tenant's without restarting: Reconfigure
// builds a new verifier from the tenant's adopted wiring and swaps it in
// unconditionally; Prune drops the verifiers of tenants a reload removed;
// Close stops every JWKS refresh. The secrets are boot config and never
// change.
type Authenticator struct {
	cfg      Config
	tenantOf TenantSource
	policies PolicySource
	mu       sync.Mutex // serializes Reconfigure, Prune, and Close
	// verifiers is replaced whole, never edited, so a request's lookup is
	// one lock-free load.
	verifiers atomic.Pointer[map[tenant.ID]*verifier]
}

// NewAuthenticator builds an Authenticator with no verifier yet: the caller
// adds one per tenant with Reconfigure. tenantOf names the request's tenant;
// nil reads every request as tenant.Default (a single-tenant test). policies
// backs the operator-key path (the admin role it stamps is the request
// tenant's) and may be nil when no operator key is configured.
func NewAuthenticator(cfg Config, tenantOf TenantSource, policies PolicySource) *Authenticator {
	a := &Authenticator{cfg: cfg, tenantOf: tenantOf, policies: policies}
	a.verifiers.Store(&map[tenant.ID]*verifier{})
	return a
}

// Reconfigure gives tenant id a verifier built from w, replacing the one it
// has when w changed — the adopted settings are the authority, so the swap
// does not depend on a JWKS fetch succeeding (see newVerifier) — and keeping
// it, refresh and all, when w did not. The replaced verifier's JWKS refresh
// is stopped.
func (a *Authenticator) Reconfigure(id tenant.ID, w Wiring) {
	a.mu.Lock()
	defer a.mu.Unlock()
	old := (*a.verifiers.Load())[id]
	if old != nil && old.roleClaim == w.roleClaim() && old.url == w.JWKSURL {
		return
	}
	next := maps.Clone(*a.verifiers.Load())
	next[id] = newVerifier(a.cfg, w)
	a.verifiers.Store(&next)
	if old != nil {
		old.stop()
	}
}

// Prune drops the verifier of every tenant served does not vouch for — the
// tenants a reload removed or rejected — and stops their JWKS refresh: a
// tenant that is not being served has no work running for it, and one that
// is adopted again is rebuilt, fetch and all, by Reconfigure.
func (a *Authenticator) Prune(served func(tenant.ID) bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := *a.verifiers.Load()
	next := make(map[tenant.ID]*verifier, len(cur))
	for id, v := range cur {
		if served(id) {
			next[id] = v
		} else {
			v.stop()
		}
	}
	if len(next) == len(cur) {
		return
	}
	a.verifiers.Store(&next)
}

// Close stops every verifier's JWKS refresh. The middleware then fails
// closed for every tenant; it is not meant to be served past this.
func (a *Authenticator) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, v := range *a.verifiers.Load() {
		v.stop()
	}
	a.verifiers.Store(&map[tenant.ID]*verifier{})
}

// verifierFor returns the request's tenant and its verifier: the tenant
// api.TenantMW resolved, tenant.Default on a tenant-exempt route (the ops
// tree, which resolves none). A nil verifier is a tenant that has none —
// fail closed, see middleware.
func (a *Authenticator) verifierFor(ctx context.Context) (tenant.ID, *verifier) {
	id := tenant.Default
	if a.tenantOf != nil {
		if resolved, ok := a.tenantOf(ctx); ok {
			id = resolved
		}
	}
	return id, (*a.verifiers.Load())[id]
}

// Middleware returns the http middleware bound to this Authenticator; it
// reads the request tenant's current verifier on every request.
func (a *Authenticator) Middleware() func(http.Handler) http.Handler {
	return middleware(a.verifierFor, a.cfg.OperatorKey, a.policies)
}

var (
	errInvalidToken = errors.New("invalid token")
	errTokenExpired = errors.New("token expired")
)

// operatorKeyFailures counts requests that presented an operator credential
// which did NOT match the configured key. A wrong operator key is never sent by
// accident — legitimate callers present a Bearer JWT or nothing — so a mismatch
// is a probing/brute-force signal against the most privileged credential in the
// system, meant to be alerted on. Paired with the WARN in Middleware; a
// package-level instrument mirroring wavehouse_ingest_dedupe_missing_id_total.
var operatorKeyFailures, _ = otel.Meter("wavehouse-auth").Int64Counter(
	"wavehouse_auth_operator_key_failures_total",
	metric.WithDescription("Requests presenting an operator key that did not match the configured value"),
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
// Verification is JWKS-or-HMAC, not both: when the tenant's JWKSURL is
// configured JWKS is the sole verifier and the HMAC secret is ignored;
// otherwise the HMAC secret (JWTSecret) is used. Accepted signing algorithms are
// restricted to the active verifier's family (asymmetric for JWKS, HMAC
// otherwise) so a token can't force an alg-confusion or alg:none bypass. With
// neither JWKSURL nor JWTSecret configured, no token can validate and every
// request falls back to the default role — i.e. a pure public deployment.
//
// The verifier is the request tenant's (current). A tenant that has no
// verifier at all fails closed — every token is invalid — and a JWKS URL
// whose key set has not been fetched yet records ErrVerifierPending instead,
// which the tenant routes turn into a 503 rather than a default_role
// evaluation; neither falls to the secret.
//
// When cfg.OperatorKey is set, a non-JWT operator path is checked before the
// Bearer token (see operatorKey below): a constant-time match on the presented
// credential authorizes a full-access platform operator independent of the JWT verifier.
// policies backs that path — the live admin role is read from the request
// tenant's policy per request — and operator authentications are logged at
// info (audit). A presented credential that does not match is logged at warn
// and counted by wavehouse_auth_operator_key_failures_total (a probing
// signal), then falls through like any unauthenticated request. policies may
// be nil when no operator key is configured.
func middleware(current func(context.Context) (tenant.ID, *verifier), operatorKeyCfg string, policies PolicySource) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// One verifier per request: the swap is atomic, so a reload lands
			// between requests, never inside one.
			id, v := current(r.Context())

			// Resolved before the operator branch purely for its side effect: it
			// strips ?token from r.URL, and an operator-key match returns without
			// ever reaching the Bearer path. Leaving it below would exempt the
			// most privileged credential from the strip — keep this first.
			tokenStr := bearerToken(r)

			// Operator key: a non-JWT break-glass/operator credential, checked
			// before any Bearer token. A constant-time match on the presented
			// credential (Authorization: Operator <key>, or the X-Operator-Key
			// alias — see operatorKey) authorizes a full-access platform operator,
			// independent of the JWT verifier. It stamps two things into the
			// context: the live admin role — so the policy evaluator's admin bypass
			// grants unrestricted data-plane access while a policy exists — and an
			// operator bit, which RequireAdmin honors even when the policy is
			// nil (policies.json empty), so the operator can still reach the
			// admin surface to trigger a settings reload.
			if operatorKeyCfg != "" {
				// Resolve the presented credential once. An empty credential
				// (no operator header at all) never matches and is not a failed
				// attempt — it's just an ordinary request that falls through to
				// the Bearer/default path. The constant-time compare runs only
				// on a non-empty credential, guarding the key bytes; whether a
				// header was sent is not secret.
				presented := operatorKey(r)
				match := presented != "" &&
					subtle.ConstantTimeCompare([]byte(presented), []byte(operatorKeyCfg)) == 1
				if match {
					// Audit at Info (not Debug): the operator key is the most
					// privileged credential in the system — full data-plane +
					// admin, honored even when the policy is wiped — so its use
					// must be visible in production logs (Info+), mirroring the
					// WARN emitted on an authz denial. Correlation fields
					// (request_id, and eventually the trusted-proxy client IP) are
					// deliberately NOT stamped per-call-site — they belong in the
					// global TraceHandler (internal/observability) so every log line
					// gets them uniformly; tracked in #333. When OTel is enabled this
					// line already carries trace_id/span_id from that handler.
					slog.LogAttrs(r.Context(), slog.LevelInfo, "operator key authenticated request",
						slog.String("path", r.URL.Path),
						slog.String("method", r.Method),
					)
					var p *policy.Policy
					if policies != nil {
						p = policies(id)
					}
					ctx := WithOperator(r.Context())
					ctx = WithRole(ctx, policy.AdminRole(p))
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
				if presented != "" {
					// Present but wrong: record a failed operator-key attempt and
					// fall through to the normal Bearer/default path — this
					// middleware never rejects (authentication is decoupled from
					// authorization), so the request resolves roleless like any
					// other unauthenticated caller. WARN + a counter because a
					// mismatch is a strong probing/brute-force signal: nobody
					// sends a wrong operator key by accident. Same correlation-field
					// deferral (request_id / client IP → #333) as the audit line above.
					operatorKeyFailures.Add(r.Context(), 1)
					slog.LogAttrs(r.Context(), slog.LevelWarn, "operator key authentication failed",
						slog.String("path", r.URL.Path),
						slog.String("method", r.Method),
					)
				}
			}

			if tokenStr == "" {
				// No token: roleless request (not an error), resolved to
				// default_role downstream.
				next.ServeHTTP(w, r)
				return
			}
			if v == nil {
				// A tenant with no verifier — resolved before its verifier was
				// built, or the ops tree over a nested directory serving no
				// tenant 0 — fails closed like a key set not fetched yet.
				r = r.WithContext(WithAuthError(r.Context(), errInvalidToken))
				next.ServeHTTP(w, r)
				return
			}

			// A presented token authenticates ONLY through the explicit success
			// path below; every other outcome falls through to the fail-safe
			// default at the end (roleless + recorded error). Ordering it this way
			// means a future missing return, or a mis-set !ok/!Valid condition,
			// can never accidentally promote an unverified token to a real role.
			// WithJSONNumber decodes numeric claims as json.Number instead of
			// float64, which is exact only to 2^53 — without it a large numeric
			// claim (a 19-digit tenant id) silently rounds, and two tenants whose
			// ids differ in the trailing digits can bind the same policy filter
			// value. exp/nbf/iat validation handles json.Number natively, with
			// one deliberate tightening: a literal exp of 0, which float64
			// decoding special-cased as "no expiry", now reads as the epoch and
			// is expired (see CHANGELOG).
			token, err := jwt.Parse(tokenStr, v.keyFunc, jwt.WithValidMethods(v.validMethods), jwt.WithJSONNumber())
			if err == nil && token.Valid {
				if claims, ok := token.Claims.(jwt.MapClaims); ok {
					ctx := WithClaims(r.Context(), claims)
					ctx = WithRole(ctx, extractClaim(claims, v.roleClaim))
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
				// Intentionally no else/return: jwt.Parse yields jwt.MapClaims
				// when no custom claims type is supplied (as here), so this !ok
				// branch is effectively unreachable. Were it ever true, falling
				// through to the fail-safe below is exactly right — a verified
				// token whose claims we can't read is treated as unverifiable
				// (roleless + invalid-token), never promoted to a role. Parse
				// errors and !Valid tokens converge on that same fail-safe: this
				// middleware records the reason rather than rejecting the request,
				// because authentication is decoupled from authorization.
			}

			// Fail-safe default: present-but-unverifiable token. Fall back to the
			// roleless default_role, but record why so a gate that denies can fail
			// loud (401) instead of as a bare 403.
			r = r.WithContext(WithAuthError(r.Context(), tokenError(err)))
			next.ServeHTTP(w, r)
		})
	}
}

// authScheme returns the credential following the given Authorization
// auth-scheme (e.g. "Bearer", "Operator") and whether the scheme matched. The
// scheme name is compared case-insensitively, as RFC 7235 requires. Shared by
// operatorKey and bearerToken so both schemes parse identically.
func authScheme(r *http.Request, scheme string) (string, bool) {
	name, cred, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	return cred, ok && strings.EqualFold(name, scheme)
}

// operatorKey extracts the presented operator credential from either the
// standard Authorization header with the "Operator" auth-scheme (preferred: the
// Authorization header is forwarded verbatim by proxies and doesn't collide with
// Bearer JWTs) or the X-Operator-Key header (a convenience alias). The
// Authorization header takes precedence. Returns "" when neither is present. The
// result is compared against the configured key in constant time by the caller.
// The key is never accepted via the URL (unlike the JWT ?token= fallback) so an
// admin secret can't leak into request lines or access logs.
func operatorKey(r *http.Request) string {
	if key, ok := authScheme(r, "Operator"); ok {
		return key
	}
	return r.Header.Get("X-Operator-Key")
}

// bearerToken extracts a JWT from the Authorization: Bearer header, or — for
// clients that can't set headers — the ?token query parameter. The Authorization
// header takes precedence when both are present; the Bearer auth-scheme is
// matched case-insensitively (RFC 7235). Returns "" if absent.
//
// A ?token is stripped from the URL whichever credential wins, so a credential
// this request never used can't ride along in r.URL into a later handler's log
// line. That only protects *our own* logs — it has already crossed every
// intermediary in the request URI, so proxies must redact query strings
// themselves.
//
// The strip must not repair the query on its way through. ParseQuery skips a
// pair it cannot read and keeps going, so re-encoding what it did read erases
// that pair — and a handler that parses the query strictly in order to refuse
// a malformed one (the ops ?tenant=) would then see a clean query and serve
// the default tenant. A query that does not parse therefore loses its token
// pairs and nothing else.
func bearerToken(r *http.Request) string {
	// Strip before either return: a header-authenticated request carrying a
	// stray ?token would otherwise keep the unused JWT in r.URL.
	var queryToken string
	// params holds every pair that did parse, error or not — what r.URL.Query
	// returns, so which token is read does not depend on the rest of the query.
	params, err := url.ParseQuery(r.URL.RawQuery)
	if tok := params.Get("token"); tok != "" {
		queryToken = tok
		if err == nil {
			params.Del("token")
			r.URL.RawQuery = params.Encode()
		} else {
			r.URL.RawQuery = withoutTokenPairs(r.URL.RawQuery)
		}
	}

	if tok, ok := authScheme(r, "Bearer"); ok {
		return tok
	}
	return queryToken
}

// withoutTokenPairs cuts the token pairs out of a raw query and leaves every
// other byte as it was sent. A pair counts only if ParseQuery would have read
// it as one: a malformed pair that merely looks like a token stays, for the
// same strict parse to refuse.
func withoutTokenPairs(raw string) string {
	pairs := strings.Split(raw, "&")
	kept := pairs[:0]
	for _, pair := range pairs {
		key, value, _ := strings.Cut(pair, "=")
		k, kerr := url.QueryUnescape(key)
		_, verr := url.QueryUnescape(value)
		if k == "token" && kerr == nil && verr == nil && !strings.Contains(pair, ";") {
			continue
		}
		kept = append(kept, pair)
	}
	return strings.Join(kept, "&")
}

// tokenError maps a jwt parse failure to a stable, caller-safe error for the
// fail-loud message: a verifier still fetching its keys and an expired token
// are distinguished, everything else (bad signature, malformed, nil)
// collapses to a generic invalid-token error so no library internals leak to
// clients.
func tokenError(err error) error {
	switch {
	case errors.Is(err, ErrVerifierPending):
		return ErrVerifierPending
	case errors.Is(err, jwt.ErrTokenExpired):
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
