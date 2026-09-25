// The ops-only listener of a process without the api role, apart from
// wire.go for the same reason as wire_nats.go: the e2e stack runs every role.

package app

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
)

// wireOpsAuth is the authentication of a process without the api role: the
// operator key and nothing else. Token verifiers — and the JWKS fetches that
// keep them — are per API process, so no token validates here and the reload
// route admits the operator alone (api.NewOpsRouter).
func (a *App) wireOpsAuth() func(http.Handler) http.Handler {
	operatorKey := strings.TrimSpace(a.cfg.Auth.OperatorKey)
	if operatorKey == "" {
		slog.Warn("no auth.operator_key set: a process without the api role takes only the operator key on POST /v1/ops/settings/reload, so its settings can only be reloaded by SIGHUP or the directory watcher")
	}
	authn := auth.NewAuthenticator(auth.Config{OperatorKey: operatorKey}, nil, nil)
	return authn.Middleware()
}

// wireOpsHTTP serves the ops-only router of a process without the api role:
// the probes, /version, the metrics endpoint, and the settings reload.
// Readiness pings the ClickHouse pools when the process has them (the ingest
// role); a sweeper-only process is ready once booted.
func (a *App) wireOpsHTTP(authMW func(http.Handler) http.Handler) {
	health := api.NewHealthHandler(nil)
	if a.pools != nil {
		health.Ping = a.pools.Ping
	}
	deps := api.OpsDependencies{
		Health:   health,
		Version:  api.NewVersionHandler(a.build.Version, a.build.GitCommit, a.build.BuildTime),
		Settings: api.NewSettingsHandler(a.tenants),
		AuthMW:   authMW,
	}
	deps.MetricsHandler, deps.MetricsPath = a.inlineMetrics()
	a.handler = api.NewOpsRouter(deps)
	a.wireServers(nil)
}
