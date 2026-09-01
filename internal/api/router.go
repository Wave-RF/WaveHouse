package api

import (
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Dependencies holds all handler dependencies.
type Dependencies struct {
	Ingest          *IngestHandler
	Query           *QueryHandler
	SSE             *StreamHandler
	Health          *HealthHandler
	Version         *VersionHandler
	Schema          *SchemaHandler
	DLQ             *DLQHandler
	Pipes           *PipesHandler
	StructuredQuery *StructuredQueryHandler
	// Settings, if non-nil, mounts POST /v1/ops/settings/reload — the API
	// trigger for reloading the settings directory. Nil when no settings
	// directory is configured (nothing to reload).
	Settings *SettingsHandler
	AuthMW   func(http.Handler) http.Handler
	// PolicySource backs the RequireAdmin gate: the admin role (policy.AdminRole)
	// is read live from the adopted policy, so admin_role changes apply on reload.
	PolicySource policy.Source
	JS           jetstream.JetStream // for SSE gap-fill
	// CORSOrigins returns the allowed CORS origins, read per request so a
	// settings reload applies immediately (settings.Store.CORSOrigins in
	// production). An empty or nil list — including a nil func — denies every
	// browser origin; ["*"] is the only allow-all spelling.
	CORSOrigins func() []string
	Logger      *slog.Logger
	// MetricsHandler, if non-nil, is mounted at MetricsPath as an unauthenticated
	// endpoint (Prometheus convention). Wired by main.go from the OTel Prometheus
	// exporter when observability.metrics.prometheus.enabled is true AND port is 0.
	MetricsHandler http.Handler
	MetricsPath    string
}

// NewRouter creates the chi router with all routes.
func NewRouter(deps Dependencies) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	// No middleware.RealIP: it rewrites r.RemoteAddr from spoofable forwarded
	// headers on every request (chi deprecated it for the IP-spoofing GHSAs),
	// and nothing here reads RemoteAddr — WaveHouse does no per-IP logic (that's
	// the reverse proxy's job). Trusted-proxy-aware client-IP capture for
	// traces/logs is tracked in #333; don't re-add RealIP to get it.
	r.Use(jsonRecoverer)
	r.Use(corsMiddleware(deps.CORSOrigins))

	// Route the chi router's own 404/405 paths through writeJSONError so
	// hits to unknown URLs and unsupported methods carry the same JSON
	// error contract as handler-emitted errors. Without this chi falls
	// back to http.Error / empty bodies and the response is text/plain.
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONError(w, http.StatusNotFound, "not found")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	})

	metricsPath := deps.MetricsPath
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip span creation on infra/probe paths:
			//   /v1/stream    — long-lived streams (SSE); the standard
			//                   HTTP tracer would emit one span per stream
			//                   that lives until the client disconnects. // TODO: do we not want this behavior?
			//   prometheus    — scrape every ~15s would produce ~4 spans/min
			//                   of pure infra cardinality, and creates a
			//                   self-loop when the same backend stores both
			//                   traces and scraped metrics.
			//   /livez, /readyz — liveness/readiness probes (and the
			//                   deprecated /healthz, /health, /ready aliases),
			//                   plus the SDK's /v1/health ping, inflate span
			//                   counts and skew latency percentiles.
			p := r.URL.Path
			if strings.HasPrefix(p, "/v1/stream") ||
				p == "/livez" || p == "/readyz" ||
				p == "/healthz" || p == "/health" || p == "/ready" ||
				p == "/v1/health" ||
				(metricsPath != "" && p == metricsPath) {
				next.ServeHTTP(w, r)
				return
			}
			// Normal REST tracing for everything else
			otelhttp.NewMiddleware("wavehouse-api")(next).ServeHTTP(w, r)
		})
	})

	// Public endpoints. /livez and /readyz are the canonical probe names
	// (current Kubernetes convention — the kube-apiserver split that replaced
	// the older conflated /healthz). /healthz is kept as a permanent alias of
	// /livez (it's the most widely-recognized name); /health and /ready are
	// deprecated aliases, kept for v0.1.x and scheduled for removal in v0.2.0
	// (see CHANGELOG). The SDK-facing public liveness ping is /v1/health.
	r.Get("/livez", deps.Health.Liveness)
	r.Get("/readyz", deps.Health.Readiness)
	r.Get("/healthz", deps.Health.Liveness) // permanent alias of /livez
	r.Get("/health", deps.Health.Liveness)  // deprecated alias of /livez
	r.Get("/ready", deps.Health.Readiness)  // deprecated alias of /readyz
	r.Get("/version", deps.Version.Handle)

	// Prometheus scrape endpoint — wired only when prometheus.enabled is true
	// AND prometheus.port is 0 (mount on this router). When prometheus.port
	// is non-zero, main.go runs a dedicated listener instead and this is nil.
	if deps.MetricsHandler != nil && deps.MetricsPath != "" {
		r.Method(http.MethodGet, deps.MetricsPath, deps.MetricsHandler)
	}

	// API v1 endpoints. The JWT auth middleware always runs (no enable/disable switch).
	r.Route("/v1", func(r chi.Router) {
		r.Use(deps.AuthMW)

		// Public content-free liveness ping. Lives under /v1 deliberately:
		// it's documented API surface the SDK relies on to check "is this
		// server reachable" before sending data, so it must stay public even
		// in deployments that filter the bare /livez|/readyz|/healthz probe
		// paths at the reverse proxy. AuthMW runs but never rejects, so no
		// token is required and there's no authz gate. Mirrors /livez under
		// the hood (200 past boot, 503 while degraded), no body.
		r.Get("/health", deps.Health.Online)

		// Single admin gate for every admin-equivalent surface. The admin role
		// is policy.AdminRole (configurable via admin_role, "admin" by default),
		// read live from the policy store so changes apply without a restart.
		// Declaring it once keeps the gate consistent across the tree.
		requireAdmin := RequireAdmin(deps.PolicySource, deps.Logger)

		r.Post("/ingest", deps.Ingest.Handle)
		r.Get("/stream", deps.SSE.Handle)

		// Structured query endpoint.
		if deps.StructuredQuery != nil {
			r.Post("/query", deps.StructuredQuery.Handle)
		}

		// Named query pipes.
		if deps.Pipes != nil {
			r.Get("/pipes/{name}", deps.Pipes.Execute)
			r.Post("/pipes/{name}", deps.Pipes.Execute)
		}

		// Ops routes — every admin-gated surface lives under /v1/ops. The
		// requireAdmin gate covers the whole tree; every surface below —
		// including raw-SQL passthrough — shares the same admin principal
		// set (policy.AdminRole).
		r.Route("/ops", func(r chi.Router) {
			r.Use(requireAdmin)

			// Schema discovery.
			r.Get("/schema", deps.Schema.Get)
			r.Post("/schema/refresh", deps.Schema.Refresh)

			// DLQ stats.
			if deps.DLQ != nil {
				r.Get("/dlq/stats", deps.DLQ.Stats)
			}

			// Raw-SQL passthrough. The only sanctioned surface for
			// non-insert mutations (DELETE/UPDATE/TRUNCATE/DROP/ALTER/…)
			// and for ad-hoc SELECTs that don't fit the structured
			// query AST. Authorization is the /v1/ops/* gate above:
			// raw SQL has no per-statement scope check (we can't
			// authorize predicates without a full SQL parser), so the
			// role gate is the entire authorization story. Non-admin
			// callers use the structured ingest path, structured
			// queries (`/v1/query?table={table}`), or named pipes.
			r.Post("/query", deps.Query.Handle)

			if deps.Pipes != nil {
				r.Get("/pipes", deps.Pipes.List)
				r.Get("/pipes/{name}", deps.Pipes.Get)
			}
			if deps.Settings != nil {
				r.Post("/settings/reload", deps.Settings.Reload)
			}
		})
	})

	return r
}

// jsonRecoverer recovers from panics in downstream handlers and emits a
// JSON 500 via writeJSONError instead of chi/middleware.Recoverer's
// empty-bodied 500 (which leaves Content-Type at the stdlib default).
// Panics are logged with stack and request metadata. http.ErrAbortHandler
// is re-panicked per the chi convention so the server's serve loop still
// terminates the connection cleanly.
//
// If the handler had already written headers or body bytes before the
// panic, the response is not patched: the headers are committed to the
// wire and a JSON 500 appended after them would corrupt the partial
// response. In that case the panic is still logged, but the connection
// is left to terminate as-is.
func jsonRecoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		defer func() {
			rvr := recover()
			if rvr == nil {
				return
			}
			if err, ok := rvr.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(rvr)
			}
			// slog's structured key-value API and its built-in handlers
			// escape control characters in string values, so path/method/
			// err carry no log-injection risk despite the linter's taint
			// heuristic on slog calls inside an *http.Request scope.
			slog.LogAttrs(r.Context(), slog.LevelError, "panic recovered in handler",
				slog.Any("err", rvr),
				slog.String("stack", string(debug.Stack())),
				slog.String("path", r.URL.Path),
				slog.String("method", r.Method),
			)
			if ww.Status() == 0 && ww.BytesWritten() == 0 {
				writeJSONError(ww, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(ww, r)
	})
}

// RequireAdmin restricts a route to the policy admin role (policy.AdminRole —
// configurable via admin_role, "admin" by default). The role established by the
// auth middleware and the live policy are read per request, so an admin_role
// change applies on reload. A nil policy (policies.json empty) admits nobody
// via a role — IsAdmin(nil) is false — so no token can re-open a locked-out
// deployment through this gate. The exception is the operator key:
// auth.IsOperator passes this gate even under a nil policy, so an operator can
// still trigger a settings reload after fixing the files (break-glass).
//
// Authentication is decoupled from this gate: a missing/invalid/expired token
// resolves to an empty (non-admin) role and is denied here. Denials go through
// writeAuthzDenied, so a present-but-invalid token fails loud (401 + token
// reason) rather than as a bare 403.
func RequireAdmin(store policy.Source, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var p *policy.Policy
			if store != nil {
				p = store()
			}
			// The operator key (Authorization: Operator <key>, or the X-Operator-Key
			// alias) authorizes the admin surface independently of the policy, so it
			// passes even when p is nil — the break-glass path that keeps the admin
			// surface (settings reload, pipes/schema reads) reachable while locked out.
			role := policy.ResolveRole(p, auth.RoleFromContext(r.Context()))
			if auth.IsOperator(r.Context()) || policy.IsAdmin(p, role) {
				next.ServeHTTP(w, r)
				return
			}
			writeAuthzDenied(w, r, logger, role, nil, slog.String("gate", "admin"))
		})
	}
}

// corsMiddleware handles CORS preflight and response headers.
//
// Origin policy:
//   - allowedOrigins contains "*": any origin is accepted. The response
//     echoes "*" (no Vary), which is correct for our Bearer-auth API because
//     no credentials are required to read the response.
//   - Otherwise: only origins in the allowlist receive Access-Control-Allow-Origin
//     (echoed back, with Vary: Origin so caches key on it). An empty or nil
//     list — including a nil getter, i.e. no settings source wired — is an
//     empty allowlist: every browser origin is denied. "*" is the only
//     allow-all spelling, so [] can't hot-reload into allow-all by accident.
//
// Credentials: we deliberately do NOT emit Access-Control-Allow-Credentials.
// WaveHouse is a Bearer-token API (see internal/auth) — clients
// send Authorization: Bearer <jwt> as an explicit request header, not via
// browser-managed cookies, so credentials mode is unnecessary. Sending
// Allow-Credentials: true together with Allow-Origin: "*" is also a CORS
// spec violation that browsers reject. Keeping credentials off both fixes
// the spec violation and shrinks the CSRF surface (see issue #30).
//
// Non-CORS requests (no Origin header) are passed through unchanged — we
// don't decorate same-origin responses with CORS noise.
func corsMiddleware(origins func() []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Same-origin / non-browser request: skip CORS decoration entirely.
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Resolved per request (not captured at router build) so a settings
			// reload changes the allowlist without a restart. The lists are a
			// handful of origins, so a linear scan beats rebuilding a set.
			var allowedOrigins []string
			if origins != nil {
				allowedOrigins = origins()
			}
			allowAll := false
			originListed := false
			for _, o := range allowedOrigins {
				if o == "*" {
					allowAll = true
				}
				if o == origin {
					originListed = true
				}
			}

			allowed := false
			switch {
			case allowAll:
				w.Header().Set("Access-Control-Allow-Origin", "*")
				allowed = true
			default:
				// In allowlist mode the response is per-origin, so always set
				// Vary: Origin — even when the origin is rejected. Without
				// this, a shared cache could memoize the headerless reject
				// response under the URL alone and replay it to a later
				// allowed-origin request, stripping the CORS headers and
				// breaking the legitimate client.
				w.Header().Set("Vary", "Origin")
				if originListed {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					allowed = true
				}
			}

			if allowed {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				// Last-Event-ID lets a cross-origin SSE client resume a stream
				// (read by StreamHandler.Handle); without it the preflight fails.
				w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, Last-Event-ID, X-Request-ID")
				w.Header().Set("Access-Control-Expose-Headers", "X-Cache, X-Request-ID")
				w.Header().Set("Access-Control-Max-Age", "3600")
			}

			if r.Method == http.MethodOptions {
				// Always short-circuit OPTIONS — disallowed origins simply get a
				// 204 with no CORS headers, which the browser will treat as a
				// preflight failure.
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
