package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go/jetstream"
)

// Dependencies holds all handler dependencies.
type Dependencies struct {
	Ingest          *IngestHandler
	Query           *QueryHandler
	SSE             *SSEHandler
	WS              *WSHandler
	Health          *HealthHandler
	Schema          *SchemaHandler
	DLQ             *DLQHandler
	Policy          *PolicyHandler
	Pipes           *PipesHandler
	StructuredQuery *StructuredQueryHandler
	AuthMW          func(http.Handler) http.Handler
	JS              jetstream.JetStream // for SSE/WS gap-fill
}

// NewRouter creates the chi router with all routes.
func NewRouter(deps Dependencies) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)

	// Public endpoints.
	r.Get("/health", deps.Health.Liveness)
	r.Get("/ready", deps.Health.Readiness)

	// API v1 endpoints (auth middleware may be no-op if disabled).
	r.Route("/v1", func(r chi.Router) {
		r.Use(deps.AuthMW)
		r.Post("/ingest/{table}", deps.Ingest.Handle)
		r.Post("/query", deps.Query.Handle)
		r.Get("/stream/sse", deps.SSE.Handle)
		r.Get("/stream/ws", deps.WS.Handle)

		// Schema discovery.
		r.Get("/schema", deps.Schema.List)
		r.Get("/schema/{table}", deps.Schema.Get)
		r.Post("/schema/refresh", deps.Schema.Refresh)

		// Structured query endpoint.
		if deps.StructuredQuery != nil {
			r.Post("/tables/{table}/query", deps.StructuredQuery.Handle)
		}

		// Named query pipes.
		if deps.Pipes != nil {
			r.Get("/pipes/{name}", deps.Pipes.Execute)
			r.Post("/pipes/{name}", deps.Pipes.Execute)
		}

		// DLQ stats.
		if deps.DLQ != nil {
			r.Get("/dlq/stats", deps.DLQ.Stats)
		}

		// Admin routes (require admin or service role).
		r.Route("/admin", func(r chi.Router) {
			r.Use(RequireRole("admin", "service"))
			if deps.Policy != nil {
				r.Get("/policy", deps.Policy.Get)
				r.Put("/policy", deps.Policy.Put)
				r.Post("/policy/validate", deps.Policy.Validate)
			}
			if deps.Pipes != nil {
				r.Get("/pipes", deps.Pipes.List)
				r.Get("/pipes/{name}", deps.Pipes.Get)
				r.Put("/pipes/{name}", deps.Pipes.Put)
				r.Delete("/pipes/{name}", deps.Pipes.Delete)
			}
		})
	})

	return r
}

// RequireRole returns middleware that restricts access to the given roles.
// When auth is disabled (no role in context), access is allowed.
func RequireRole(roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			role := RoleFromContext(r.Context())
			// Allow if no role is set (auth disabled).
			if role == "" {
				next.ServeHTTP(w, r)
				return
			}
			for _, allowed := range roles {
				if role == allowed {
					next.ServeHTTP(w, r)
					return
				}
			}
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		})
	}
}

// corsMiddleware handles CORS preflight and response headers.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, X-Request-ID")
		w.Header().Set("Access-Control-Expose-Headers", "X-Cache, X-Request-ID")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Max-Age", "3600")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
