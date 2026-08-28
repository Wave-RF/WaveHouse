package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Config holds configuration for the JWT authentication middleware. JWKSURL
// and RoleClaim are settings-directory keys (hot-reloadable via
// Authenticator.Reconfigure); JWTSecret and OperatorKey are boot-config
// secrets, fixed for the process lifetime.
type Config struct {
	JWTSecret   string
	JWKSURL     string
	RoleClaim   string // dot-separated claim path, e.g. "role" or "app_metadata.role"
	OperatorKey string // optional non-JWT operator credential; a match on the presented credential (Authorization: Operator <key>, or the X-Operator-Key alias) authorizes a full-access platform operator (see Middleware)
}

// verifier is one immutable JWT verification setup: the key source, the
// signing-algorithm allowlist pinned to it, and the role claim path. The
// Authenticator swaps a whole verifier atomically, so a request never sees
// a JWKS key source paired with the HMAC allowlist.
type verifier struct {
	jwks         keyfunc.Keyfunc
	secret       string
	roleClaim    string
	validMethods []string
	// cancel stops the JWKS background refresh once the verifier is replaced.
	cancel context.CancelFunc
	// url is the JWKS URL this verifier was built from (for the no-op check).
	url string
}

func (v *verifier) jwksURL() string { return v.url }

func (v *verifier) keyFunc(t *jwt.Token) (any, error) {
	// JWKS-or-HMAC, JWKS first: when configured it is the sole verifier; the
	// HMAC shared secret is reached only when JWKS isn't in play.
	if v.jwks != nil {
		return v.jwks.Keyfunc(t)
	}
	if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, jwt.ErrSignatureInvalid
	}
	return []byte(v.secret), nil
}

// newVerifier builds a verifier from cfg. When JWKSURL is set the JWK Set
// is fetched synchronously; with requireFetch (boot) an unreachable or
// misconfigured endpoint is an error so the process refuses to start rather
// than run where no token can validate. Without it (reload) the adopted
// URL is applied regardless: until the background refresh (or the refresh
// an unknown key id triggers) succeeds, no JWT validates and requests fall
// to the policy default_role — the same fail-closed posture as an
// unreachable ClickHouse, fixed by the next reload. Refresh failures are
// logged through logger when non-nil.
func newVerifier(cfg Config, requireFetch bool, logger *slog.Logger) (*verifier, error) {
	v := &verifier{secret: cfg.JWTSecret, roleClaim: cfg.RoleClaim, url: cfg.JWKSURL}
	if v.roleClaim == "" {
		v.roleClaim = "role"
	}
	if cfg.JWKSURL != "" {
		ctx, cancel := context.WithCancel(context.Background())
		noErrorReturnFirstHTTPReq := !requireFetch
		override := keyfunc.Override{NoErrorReturnFirstHTTPReq: &noErrorReturnFirstHTTPReq}
		if logger != nil {
			override.RefreshErrorHandlerFunc = func(u string) func(context.Context, error) {
				return func(_ context.Context, err error) {
					logger.Warn("jwks refresh failed; no token validates until it succeeds", "url", u, "error", err)
				}
			}
		}
		jwks, err := keyfunc.NewDefaultOverrideCtx(ctx, []string{cfg.JWKSURL}, override)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("initialize JWKS from %q: %w", cfg.JWKSURL, err)
		}
		v.jwks, v.cancel = jwks, cancel
	}
	// Restrict accepted signing algorithms to the active verifier's family, so
	// jwt.Parse rejects an unexpected alg (including "none") before keyFunc runs
	// — defense-in-depth against alg-confusion and alg:none attacks. Keyed off
	// the runtime verifier (jwks != nil), not just cfg.
	if v.jwks != nil {
		v.validMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512", "EdDSA"}
	} else {
		v.validMethods = []string{"HS256", "HS384", "HS512"}
	}
	return v, nil
}

// Authenticator owns the verifier behind the middleware so a settings
// reload can replace it without restarting: Reconfigure builds a new
// verifier from the adopted settings and swaps it in unconditionally. The
// operator key is boot config and never changes.
type Authenticator struct {
	operatorKey string
	store       *policy.Store
	logger      *slog.Logger
	mu          sync.Mutex // serializes Reconfigure
	cur         atomic.Pointer[verifier]
}

// NewAuthenticator builds the boot-time verifier from cfg. An unreachable
// JWKS endpoint is an error so boot fails loudly rather than degraded.
func NewAuthenticator(cfg Config, store *policy.Store, logger *slog.Logger) (*Authenticator, error) {
	v, err := newVerifier(cfg, true, logger)
	if err != nil {
		return nil, err
	}
	a := &Authenticator{operatorKey: cfg.OperatorKey, store: store, logger: logger}
	a.cur.Store(v)
	return a, nil
}

// Reconfigure replaces the verifier with one built from cfg's JWKSURL,
// RoleClaim, and JWTSecret when any of them changed. The adopted settings
// are the authority, so the swap does not wait on the JWKS fetch (see
// newVerifier). The replaced verifier's JWKS refresh is stopped.
func (a *Authenticator) Reconfigure(cfg Config) {
	a.mu.Lock()
	defer a.mu.Unlock()
	old := a.cur.Load()
	roleClaim := cfg.RoleClaim
	if roleClaim == "" {
		roleClaim = "role"
	}
	if old != nil && old.roleClaim == roleClaim && old.secret == cfg.JWTSecret && (old.jwks != nil) == (cfg.JWKSURL != "") && old.jwksURL() == cfg.JWKSURL {
		return
	}
	// The URL was validated as absolute http(s) and the first fetch is not
	// required, so construction cannot fail; a nil verifier would fail closed
	// anyway (every token rejected), matching the fetch-pending state.
	v, err := newVerifier(cfg, false, a.logger)
	if err != nil {
		if a.logger != nil {
			a.logger.Error("auth reconfigure: build verifier", "error", err)
		}
		return
	}
	a.cur.Store(v)
	if old != nil && old.cancel != nil {
		old.cancel()
	}
}

// Middleware returns the http middleware bound to this Authenticator; it
// reads the current verifier on every request.
func (a *Authenticator) Middleware() func(http.Handler) http.Handler {
	return middleware(a.cur.Load, a.operatorKey, a.store, a.logger)
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
// Verification is JWKS-or-HMAC, not both: when JWKSURL is configured (and
// initializes) JWKS is the sole verifier and the HMAC secret is ignored;
// otherwise the HMAC secret (JWTSecret) is used. Accepted signing algorithms are
// restricted to the active verifier's family (asymmetric for JWKS, HMAC
// otherwise) so a token can't force an alg-confusion or alg:none bypass. With
// neither JWKSURL nor JWTSecret configured, no token can validate and every
// request falls back to the default role — i.e. a pure public deployment.
//
// If JWKSURL is set but its JWK Set can't be fetched at startup,
// NewAuthenticator returns an error so the caller can fail fast instead of
// booting into a degraded state where no token can validate.
//
// When cfg.OperatorKey is set, a non-JWT operator path is checked before the
// Bearer token (see operatorKey below): a constant-time match on the presented
// credential authorizes a full-access platform operator independent of the JWT verifier.
// store and logger back that path — the live admin role is read from store per
// request, and operator authentications are logged at info (audit). A presented
// credential that does not match is logged at warn and counted by
// wavehouse_auth_operator_key_failures_total (a probing signal), then falls
// through like any unauthenticated request. Both store and logger may be nil
// when no operator key is configured.
func middleware(current func() *verifier, operatorKeyCfg string, store *policy.Store, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// One verifier per request: the swap is atomic, so a reload lands
			// between requests, never inside one.
			v := current()

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
			// nil/deleted, so the operator can still reach the admin surface to
			// restore a wiped policy.
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
					if logger != nil {
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
						logger.LogAttrs(r.Context(), slog.LevelInfo, "operator key authenticated request",
							slog.String("path", r.URL.Path),
							slog.String("method", r.Method),
						)
					}
					var p *policy.Policy
					if store != nil {
						p = store.Get()
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
					if logger != nil {
						logger.LogAttrs(r.Context(), slog.LevelWarn, "operator key authentication failed",
							slog.String("path", r.URL.Path),
							slog.String("method", r.Method),
						)
					}
				}
			}

			if tokenStr == "" {
				// No token: roleless request (not an error), resolved to
				// default_role downstream.
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
func bearerToken(r *http.Request) string {
	// Strip before either return: a header-authenticated request carrying a
	// stray ?token would otherwise keep the unused JWT in r.URL.
	var queryToken string
	if params := r.URL.Query(); params.Get("token") != "" {
		queryToken = params.Get("token")
		params.Del("token")
		r.URL.RawQuery = params.Encode()
	}

	if tok, ok := authScheme(r, "Bearer"); ok {
		return tok
	}
	return queryToken
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
