package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go/jetstream"
)

// Dependencies holds all handler dependencies.
type Dependencies struct {
	Ingest *IngestHandler
	Query  *QueryHandler
	SSE    *SSEHandler
	WS     *WSHandler
	Health *HealthHandler
	Schema *SchemaHandler
	DLQ    *DLQHandler
	AuthMW func(http.Handler) http.Handler
	JS     jetstream.JetStream // for SSE/WS gap-fill
}

// NewRouter creates the chi router with all routes.
func NewRouter(deps Dependencies) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

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

		// DLQ stats.
		if deps.DLQ != nil {
			r.Get("/dlq/stats", deps.DLQ.Stats)
		}
	})

	return r
}
