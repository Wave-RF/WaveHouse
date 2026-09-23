package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

const (
	serviceName       = "wavehouse"
	readHeaderTimeout = 10 * time.Second
)

// withoutContext adapts a local store's Close, which has no deadline to
// honor, to the component close signature.
func withoutContext(release func() error) func(context.Context) error {
	return func(context.Context) error { return release() }
}

// wireSettings adopts the settings directory — the hot-reloadable half of
// configuration (dedupe, dlq, query, schema, stream, cors — see
// settings.TenantConfig). Required: config.Validate already rejected an
// empty settings.dir, and an invalid directory refuses boot. The binary
// carries no compiled defaults; `wavehouse bootstrap` writes the seed. A
// *reload* of an invalid directory merely keeps the previous snapshot. A
// nested directory (one folder per tenant, #583) fails closed per tenant
// instead, at boot and on reload alike: see settings.Registry.
//
// The access-control policy and the named pipes (policies.json / pipes.json)
// are read per request off the adopted snapshot, so a reload applies to the
// next request with no hook.
func (a *App) wireSettings() error {
	tenants, _ := settings.Open(a.cfg.Settings.Dir)
	if tenants == nil {
		return fmt.Errorf("settings directory %s invalid, refusing to start — findings above; `wavehouse validate` reproduces them, `wavehouse bootstrap` writes a starter directory", a.cfg.Settings.Dir)
	}
	a.tenants = tenants
	// Registered first: hooks run in registration order, and every other one
	// reads tenant 0 through the store this one tracks.
	a.trackDefaultStore()
	a.onDefaultAdopt(a.trackDefaultStore)
	a.policies = func() *policy.Policy { return defaultSetting(a, (*settings.Store).Policy) }
	switch _, served := tenants.For(tenant.Default); {
	case !tenants.Nested():
		if a.policies() == nil {
			slog.Warn("no policy adopted — every token-based request is denied until policies.json defines one (fail closed)")
		}
	case !served:
		slog.Warn("nested settings directory with no tenant 0 being served: the ClickHouse connection, the MQ byte budget, the JWT verifier, and the async paths (ingest worker, sweeper, stream hub, schema refresh) are still configured from tenant 0's config.json, so they run unconfigured — no ClickHouse address, /livez degraded — until a 0 folder is adopted")
	}
	return nil
}

// trackDefaultStore remembers tenant 0's store as of its last adoption. The
// registry stops handing out a rejected tenant's store and forgets a removed
// one, but the store keeps its last adopted document either way — and that is
// what the process-wide resources go on following (defaultSetting).
func (a *App) trackDefaultStore() {
	if store, ok := a.tenants.For(tenant.Default); ok {
		a.defaultStore.Store(store)
	}
}

// defaultSetting reads one setting of the default tenant, which the
// process-wide resources (ClickHouse, MQ, auth) follow until #583 gives
// each tenant its own. It reads tenant 0's last adopted document, so a
// 0 folder a reload rejected or removed leaves every one of them as it was —
// the ones a hook reconciles and the one read per request (the operator
// key's admin role) alike. A nested directory that has never served a tenant
// 0 reads T's zero value, which wireSettings warned about at boot.
func defaultSetting[T any](a *App, get func(*settings.Store) T) T {
	store := a.defaultStore.Load()
	if store == nil {
		var zero T
		return zero
	}
	return get(store)
}

// onDefaultAdopt registers fn to run after each reload that adopts the
// default tenant, so a nested directory's other tenants never move the
// process-wide resources, and a rejected 0 folder leaves them as they were.
func (a *App) onDefaultAdopt(fn func()) {
	a.tenants.AfterAdopt(func(adopted []tenant.ID) {
		if slices.Contains(adopted, tenant.Default) {
			fn()
		}
	})
}

// shortestKeepalive is the shape of the one keepalive wheel every tenant's
// streams share: the stream.keepalive_* pair of the tenant with the shortest
// keepalive_interval among those being served. The interval is an upper bound
// on how long a quiet stream goes unwritten, so the shortest one keeps every
// tenant's — at the cost of one tenant setting the cadence for all, which is
// why honoring each tenant's own is tracked in #597. A flat directory's one
// tenant gets exactly its own pair; with no tenant served the zeros fall back
// to the wheel's defaults.
func shortestKeepalive(tenants *settings.Registry) (period time.Duration, buckets int) {
	for _, store := range tenants.All() {
		// Strictly shorter, so tenants tied on the interval resolve to the
		// first in id order rather than to map order.
		if p, b := store.Keepalive(); period == 0 || p < period {
			period, buckets = p, b
		}
	}
	return period, buckets
}

// perTenant adapts a store accessor to the tenant-keyed getter the async
// paths take: they hold a tenant id (tenant.Default today, the MQ subject's
// from #583 story 5), not a request's resolved store. A miss — a nested
// directory with no 0 folder, or with a rejected one — is logged and read as
// T's zero value; what a removed tenant means to each async path is story
// 3's to decide.
func perTenant[T any](tenants *settings.Registry, get func(*settings.Store) T) func(tenant.ID) T {
	return func(id tenant.ID) T {
		store, ok := tenants.For(id)
		if !ok {
			slog.Error("no settings store for tenant; reading the zero value", "tenant", id)
			var zero T
			return zero
		}
		return get(store)
	}
}

// dlqFor adapts the registry to the ingest worker's per-table DLQ switch. A
// miss reads as DLQ on, not as the zero value perTenant would give: off lets
// the worker drop a message it cannot read, and not knowing the tenant is no
// reason to destroy its row. Parked, it survives until the tenant resolves.
func dlqFor(tenants *settings.Registry) func(tenant.ID, string) bool {
	return func(id tenant.ID, table string) bool {
		store, ok := tenants.For(id)
		if !ok {
			slog.Error("no settings store for tenant; parking its failed rows on the DLQ", "tenant", id, "table", table)
			return true
		}
		return store.DLQFor(table)
	}
}

// sharedTables is the cache the ingest worker invalidates through until #583
// story 6 gives each tenant its own ClickHouse. Every tenant reads the same
// tables today, so an insert into one changes what every tenant would read:
// the worker names one tenant's namespaces (its own), and this bumps them
// under every tenant the registry knows, the named one included. Known, not
// served: a rejected tenant keeps its cache entries and comes back into
// service with them, so leaving it out would let a folder repaired inside a
// TTL serve pre-insert rows. A tenant removed and restored inside a TTL still
// can — the registry forgets a removed tenant, and what becomes of its cache
// is story 3's — and reads are untouched: a tenant's cached results stay its
// own. Goes away with story 6, when a table is one tenant's.
type sharedTables struct {
	cache.Cache
	tenants *settings.Registry
}

func (s sharedTables) Invalidate(ctx context.Context, namespaces []cache.Namespace) (uint64, error) {
	ids := map[tenant.ID]bool{}
	for _, ns := range namespaces {
		ids[ns.Tenant] = true
	}
	for id := range s.tenants.Known() {
		ids[id] = true
	}
	all := make([]cache.Namespace, 0, len(ids)*len(namespaces))
	for _, id := range slices.Sorted(maps.Keys(ids)) {
		for _, ns := range namespaces {
			ns.Tenant = id
			all = append(all, ns)
		}
	}
	return s.Cache.Invalidate(ctx, all)
}

// wireObservability initializes the OTel pipeline whenever either OTLP push
// or Prometheus exposition is wanted — Prometheus-only operation
// (Alloy/scrape, no collector) is a first-class mode, and the OTel SDK
// MeterProvider is the shared substrate. Endpoint, TLS, and auth headers
// come from the standard OTEL_EXPORTER_OTLP_* env vars, read by the SDK. A
// malformed header is logged and skipped by the SDK (fail-soft);
// InitProvider's own error is likewise non-fatal — logged, stdout-only from
// there on.
func (a *App) wireObservability(ctx context.Context) {
	cfg := a.cfg
	if !cfg.OTel.Enabled && !cfg.Prometheus.Enabled {
		return
	}
	shutdown, promHandler, err := observability.InitProvider(ctx, serviceName, observability.ProviderConfig{
		TracesEnabled:     cfg.OTel.Enabled && cfg.OTel.Traces.Enabled,
		TracesSampleRate:  cfg.OTel.Traces.SampleRate,
		MetricsEnabled:    cfg.OTel.Enabled && cfg.OTel.Metrics.Enabled,
		PrometheusEnabled: cfg.Prometheus.Enabled,
		LogsEnabled:       cfg.OTel.Enabled && cfg.OTel.Logs.Enabled,
	})
	if err != nil {
		slog.Error("failed to initialize observability, falling back to stdout", "error", err)
		return
	}
	a.promHandler = promHandler
	// Not a component: Close runs the flush after every component has
	// released, on its own budget (see App.Close). InitProvider's shutdown
	// returns at that deadline even when a flush is stuck in gRPC backoff
	// against an unreachable collector.
	a.flush = shutdown

	// Only swap to the OTLP-aware logger when OTLP logs are wired up;
	// Prometheus-only mode keeps the stdout-only handler main installed.
	if cfg.OTel.Enabled && cfg.OTel.Logs.Enabled {
		slog.SetDefault(observability.NewLogger(serviceName, a.logLevel, true, cfg.OTel.Logs.SampleRate).With(
			"version", a.build.Version,
			"build_time", a.build.BuildTime,
			"git_commit", a.build.GitCommit,
		))
	}
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "localhost:4317 (SDK default)"
	}
	logger := slog.Default()
	switch {
	case cfg.OTel.Enabled && cfg.Prometheus.Enabled:
		logger.Info("observability pipeline established", "otlp_endpoint", otlpEndpoint, "prometheus", true)
	case cfg.OTel.Enabled:
		logger.Info("observability pipeline established", "otlp_endpoint", otlpEndpoint)
	case cfg.Prometheus.Enabled:
		logger.Info("observability pipeline established", "prometheus", true)
	}
}

// wireClickHouse opens the one driver.Conn every consumer holds. The wiring
// is the settings directory's clickhouse block plus the boot-config
// password; a reload that changes it swaps the connection behind the
// manager unconditionally — the adopted settings are the authority, and
// reachability surfaces where it already does (schema discovery retries,
// /readyz, query errors). The HTTP-side consumers read Target/QueryTimeout
// per request.
func (a *App) wireClickHouse() error {
	params := func() chconn.Params {
		c := defaultSetting(a, (*settings.Store).ClickHouse)
		return chconn.Params{
			Addr: c.Addr, HTTPPort: c.HTTPPort, HTTPScheme: c.HTTPScheme,
			Database: c.Database, Username: c.Username, Password: a.cfg.ClickHouse.Password,
			QueryTimeout: c.QueryTimeout,
		}
	}
	ch, err := chconn.Open(params())
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}
	a.ch = ch
	a.add(component{name: "clickhouse", close: withoutContext(ch.Close)})
	a.onDefaultAdopt(func() {
		if err := ch.Reconfigure(params()); err != nil {
			slog.Error("clickhouse reconfigure", "error", err)
		}
	})
	return nil
}

// wireDiscovery runs the boot-time schema discovery — non-fatal. If the
// first Refresh fails (ClickHouse unreachable, database missing, etc.) the
// binary is marked degraded via bootState (which /livez surfaces as 503 +
// diagnostic) and Run retries in the background with exponential backoff.
// The process still binds its port so operators can `curl /livez` instead
// of grepping a restart-loop log. Once a Refresh succeeds, bootState flips
// to nil and /livez returns 200. The periodic auto-refresh starts only after
// the first successful Refresh (boot or retry) so it never races
// RetryRefresh on Refresh calls or on bootState writes.
func (a *App) wireDiscovery(ctx context.Context) {
	a.bootState = api.NewBootState(nil)
	// Both sources are read per refresh, so a settings reload retunes the
	// cadence and a ClickHouse reconfigure moves the database without a restart.
	registry := discovery.NewSchemaRegistry(a.ch, a.ch.Database, tenant.Default, perTenant(a.tenants, (*settings.Store).SchemaRefreshInterval))
	a.registry = registry
	bootErr := registry.Refresh(ctx)
	if bootErr != nil {
		slog.Warn("schema discovery failed on boot, retrying in background", "error", bootErr)
		a.bootState.Set(fmt.Errorf("schema discovery: %w", bootErr))
	}
	a.add(component{name: "schema discovery", run: func(ctx context.Context) error {
		if bootErr != nil {
			err := registry.RetryRefresh(ctx, 2*time.Second, 60*time.Second, func(attemptErr error) {
				slog.Warn("schema discovery retry failed", "error", attemptErr)
				a.bootState.Set(fmt.Errorf("schema discovery: %w", attemptErr))
			})
			if err != nil {
				// ctx cancelled before success — the process is shutting down.
				return nil
			}
			slog.Info("schema discovery succeeded after retry, /livez now 200")
			a.bootState.Set(nil)
		}
		registry.StartAutoRefresh(ctx)
		return nil
	}})
}

// legacyDedupeDir is where the one store lived before #583 story 7 gave each
// tenant its own: tenant 0's, implicitly. tenant.Parse reserves the name, as
// it does the queue's nats, in any letter case, so no tenant's directory is
// ever this one — not on a case-insensitive filesystem either.
const legacyDedupeDir = "pebble"

// wireDedupe builds the dedupe stores: one per tenant (#583 story 7), at
// data_dir/<tenant>/dedupe whatever the settings directory's shape — the
// four files are tenant 0 — each following its own tenant's hot-reloadable
// dedupe.enabled. Which store that is, is the factory's business alone; the
// embedded Pebble one is what a process chooses here, and an earlier
// layout's data_dir/pebble is moved to tenant 0's directory once
// (moveLegacyDedupeStore). One reconcile closure sets every store to what
// the registry says: open exactly when its tenant is served with the switch
// on, closed — its data left on disk — when the tenant is switched off,
// rejected, or removed. It is registered as the after-adopt hook BEFORE the
// boot apply (Apply is idempotent), so a reload landing between the two
// can't leave a tenant's settings saying "on" with its store still closed —
// either the hook sees it or the boot apply reads it. A failed open follows
// the registry's own rule for the shape: flat refuses boot, like every other
// store, and on reload logs and leaves the store closed — ingest then fails
// closed (500 "dedupe failed") rather than silently publishing un-deduped,
// since the files asked for dedupe; nested fails closed per tenant the same
// way at boot too, the next reload retrying, so one tenant's unopenable
// store never costs the others their process.
func (a *App) wireDedupe() error {
	nested := a.tenants.Nested()
	dir := func(id tenant.ID) string { return filepath.Join(a.cfg.DataDir, id.String(), "dedupe") }
	if err := moveLegacyDedupeStore(a.cfg.DataDir, dir(tenant.Default)); err != nil {
		return fmt.Errorf("dedupe store relocation: %w", err)
	}
	stores := dedupe.NewStores(func(id tenant.ID) *dedupe.Managed { return dedupe.NewManaged(dedupe.Embedded(dir(id))) })
	a.dedup = stores
	a.add(component{name: "dedupe", close: withoutContext(stores.Close)})
	reconcile := func() error {
		// The gone tenants' stores close first, so a tenant renamed only in
		// letter case — one directory to a case-insensitive filesystem —
		// never has both spellings open at once.
		served := func(id tenant.ID) bool {
			_, ok := a.tenants.For(id)
			return ok
		}
		if err := stores.Retain(served); err != nil {
			slog.Error("dedupe store close failed", "error", err)
		}
		var errs []error
		for id, store := range a.tenants.All() {
			m := stores.For(id)
			enabled := store.DedupeEnabled()
			wasOpen := m.Open()
			// Tenant 0's store is the one an earlier deployment had, so a
			// fresh directory there is a first run or a lost volume. Another
			// tenant's first enable always starts fresh, and a lost volume
			// shows in the NATS check.
			if enabled && !wasOpen && id == tenant.Default {
				config.WarnIfFreshDataDir("pebble", dir(id))
			}
			if err := m.Apply(enabled); err != nil {
				config.LogStorageInitError("dedupe", dir(id), err)
				errs = append(errs, fmt.Errorf("tenant %s: %w", id, err))
				continue
			}
			if m.Open() != wasOpen {
				slog.Info("dedupe store reconciled with settings", "tenant", id, "enabled", enabled)
			}
		}
		return errors.Join(errs...)
	}
	a.tenants.AfterAdopt(func([]tenant.ID) { _ = reconcile() })
	if err := reconcile(); err != nil && !nested {
		return fmt.Errorf("dedupe open: %w", err)
	}
	return nil
}

// moveLegacyDedupeStore moves an earlier layout's data_dir/pebble — tenant
// 0's store, implicitly — to dst, tenant 0's directory, once: a standalone
// deployment keeps its seen ids across the upgrade, and later across the
// move to a nested directory with an explicit 0 folder. Both present (an
// older binary ran in between and started a new store at the old path) is
// left alone and warned about: dst is the one in use, and deleting data is
// never boot's call. A failed move refuses boot rather than open an empty
// store and let duplicates through in silence.
func moveLegacyDedupeStore(dataDir, dst string) error {
	src := filepath.Join(dataDir, legacyDedupeDir)
	if _, err := os.Stat(src); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if _, err := os.Stat(dst); err == nil {
		slog.Warn("dedupe store already moved to tenant 0's directory; the old directory is unused and can be removed", "old", src, "path", dst)
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	slog.Info("dedupe store moved to tenant 0's directory", "old", src, "path", dst)
	return nil
}

// wireMQ starts the MQ — the embedded NATS under data_dir/nats, the one
// place the implementation is chosen; everything after it sees mq.Broker.
// mq.max_bytes_gb is hot-reloadable: after each adoption the new budget is
// handed to the MQ, which owns how it is split across its queues and keeps
// them consistent (see mq.Broker.SetMaxBytes).
func (a *App) wireMQ() error {
	dir := filepath.Join(a.cfg.DataDir, "nats")
	config.WarnIfFreshDataDir("nats", dir)
	var broker mq.Broker
	broker, err := mq.NewEmbedded(dir, defaultSetting(a, (*settings.Store).MQMaxBytes))
	if err != nil {
		config.LogStorageInitError("mq", dir, err)
		return fmt.Errorf("mq open: %w", err)
	}
	a.mq = broker
	a.add(component{name: "mq", close: withoutContext(broker.Close)})

	// Only register system metric gauges when a real MeterProvider is in
	// place — otherwise `otel.GetMeterProvider()` returns the no-op SDK
	// provider and RegisterCallback silently no-ops, making this look
	// authoritative when it's actually doing nothing.
	if a.cfg.OTel.Enabled || a.cfg.Prometheus.Enabled {
		if err := observability.RegisterSystemMetrics(broker.Stats, a.dedup.Stats); err != nil {
			slog.Error("failed to register system metrics", "error", err)
		}
	}

	// Rooted in the App's stop context, so a reload caught mid-hook by
	// SIGTERM gives up rather than holding the drain past
	// server.shutdown_timeout.
	a.onDefaultAdopt(func() {
		mb := defaultSetting(a, (*settings.Store).MQMaxBytes)
		if mb == broker.MaxBytes() {
			return
		}
		if err := broker.SetMaxBytes(a.stopCtx, mb); err != nil {
			slog.Error("mq stream resize failed; the next reload retries", "error", err)
			return
		}
		slog.Info("mq stream limits reconciled with settings", "max_bytes_gb", mb>>30)
	})
	return nil
}

// wireCache opens the L1 cache — the only tier in standalone mode.
func (a *App) wireCache() error {
	l1, err := cache.NewLocal(a.cfg.Cache.L1MaxCost)
	if err != nil {
		return fmt.Errorf("cache init: %w", err)
	}
	// TODO: eventually this is where we can switch between ristretto, redis, tiered (both), etc
	a.cache = l1
	a.add(component{name: "cache", close: withoutContext(l1.Close)})
	return nil
}

// wireSweeper adds the active sweeper — purges messages that are both
// written to ClickHouse and older than the SSE gap window
// (stream.gap_window_minutes, re-read every sweep). Runs every minute.
func (a *App) wireSweeper() {
	sweeper := ingest.NewSweeper(a.mq, tenant.Default, perTenant(a.tenants, (*settings.Store).GapWindow))
	a.add(component{name: "sweeper", run: func(ctx context.Context) error {
		sweeper.Start(ctx)
		return nil
	}})
}

// wireStreaming builds the SSE fan-out: one metric set shared by the Hub
// (drop counts) and the stream handler (write counts); the Hub that
// projects/serializes each event once per (topic, role) and pushes it to
// that role's subscribers; the MQ → Hub bridge; and the keepalive wheel.
func (a *App) wireStreaming() {
	a.sseMetrics = stream.NewMetrics()
	a.hub = stream.NewHub(tenant.Default, perTenant(a.tenants, (*settings.Store).Policy), a.registry, a.sseMetrics)

	// Hub bridge: MQ → broadcast to connected SSE clients. The Hub decodes and
	// projects each event itself (skipping malformed payloads), so the bridge
	// just forwards the raw bytes and acks.
	a.add(component{name: "hub bridge", run: func(ctx context.Context) error {
		err := a.mq.Subscribe(ctx, "hub-bridge", func(msg *mq.Message) error {
			a.hub.Broadcast(msg.TopicKey(), msg.Data)
			if err := msg.Ack(); err != nil {
				slog.Warn("failed to ack message from embedded hub bridge", "error", err)
			}
			return nil
		})
		if err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}})

	// Shared keepalive wheel: one goroutine nudges idle streams so proxies
	// don't idle-close them. Runs for the process lifetime; a reload that
	// changes stream.keepalive_* rebuilds the ring in place under the live
	// connections — any tenant's reload, since every tenant's streams ride
	// the one wheel (shortestKeepalive).
	heartbeater := stream.NewHeartbeater(shortestKeepalive(a.tenants))
	a.tenants.AfterAdopt(func([]tenant.ID) { heartbeater.Reconfigure(shortestKeepalive(a.tenants)) })
	a.heartbeater = heartbeater
	a.add(component{name: "keepalive", run: func(ctx context.Context) error {
		heartbeater.Run(ctx)
		return nil
	}})
}

// wireIngestWorker adds the batch consumer (JetStream → ClickHouse). Its
// consumer is created when Run starts it; at shutdown the in-flight batches
// drain within the shutdown timeout.
func (a *App) wireIngestWorker() {
	a.add(component{name: "ingest worker", run: func(ctx context.Context) error {
		stop, failed, err := ingest.StartIngestWorker(ctx, a.mq, sharedTables{Cache: a.cache, tenants: a.tenants}, a.ch.Target, tenant.Default, dlqFor(a.tenants))
		if err != nil {
			return err
		}
		// A worker that ends on its own (its consumer was deleted, the MQ
		// connection closed) cannot be revived from here and would otherwise
		// leave the API accepting events nothing writes. Returning the error
		// fails Run, which stops every other component; the supervisor's
		// restart recreates the consumer at boot.
		var workerErr error
		select {
		case <-ctx.Done():
		case workerErr = <-failed:
		}
		shutCtx, cancel := a.shutdownContext()
		defer cancel()
		if err := stop(shutCtx); err != nil {
			slog.Error("ingest worker cleanup error", "error", err)
		}
		return workerErr
	}})
}

// wireAuth builds the JWT middleware up front so a misconfigured or
// unreachable JWKS endpoint fails startup loudly rather than booting into a
// degraded state. jwks_url and role_claim are settings; the secrets are boot
// config. A reload rebuilds the verifier from the adopted settings
// unconditionally — an unreachable JWKS then fails closed (no token
// validates, requests fall to default_role) until it is reachable or the
// next reload.
//
// There is no on/off switch — the middleware always runs. With neither a
// secret (boot config) nor a JWKS URL (settings) no token can validate, so
// every request falls back to the policy default_role (a pure public
// deployment). That's a valid posture, so it warns rather than fails.
func (a *App) wireAuth() (func(http.Handler) http.Handler, error) {
	cfg := a.cfg
	switch {
	case cfg.Auth.JWTSecret == "" && defaultSetting(a, (*settings.Store).Auth).JWKSURL == "":
		slog.Warn("no auth.jwt_secret (boot config) or auth.jwks_url (settings) set: no token can be validated, so every request resolves to the policy default_role (public access)")
	case cfg.Auth.JWTSecret == "change-me-in-production":
		slog.Warn("WH_AUTH_JWT_SECRET is using the default insecure value")
	}

	operatorKey := strings.TrimSpace(cfg.Auth.OperatorKey)
	switch {
	case operatorKey == "" && a.tenants.Nested():
		// Not the recovery concern below: over a nested directory the key is
		// the ops tree's only credential, and there is no watcher either.
		slog.Warn("nested settings directory and no auth.operator_key set: the operator key is the only credential /v1/ops/* takes over a nested directory, so no caller can reach those routes — settings can only be reloaded by SIGHUP, which reloads every tenant")
	case operatorKey == "":
		slog.Warn("no auth.operator_key set: if you lose the JWT secret, lose control of the JWKS endpoint, or lose your HMAC secret — or policies.json is emptied — every token-based request is denied and the only recovery is editing the settings directory on the host")
	default:
		slog.Info("operator key is set: requests presenting it via 'Authorization: Operator <key>' (or the X-Operator-Key alias) are authorized as a full-access platform operator, and can trigger a settings reload over HTTP while the server is locked out")
	}

	authConfig := func() auth.Config {
		s := defaultSetting(a, (*settings.Store).Auth)
		return auth.Config{
			JWTSecret:   cfg.Auth.JWTSecret,
			JWKSURL:     s.JWKSURL,
			RoleClaim:   s.RoleClaim,
			OperatorKey: operatorKey,
		}
	}
	authn, err := auth.NewAuthenticator(authConfig(), a.policies)
	if err != nil {
		return nil, fmt.Errorf("auth middleware init: %w", err)
	}
	a.onDefaultAdopt(func() { authn.Reconfigure(authConfig()) })
	return authn.Middleware(), nil
}

// wireReloadTriggers adds SIGHUP and the directory watcher. All three
// triggers (these two and POST /v1/ops/settings/reload) funnel into the same
// serialized Registry.Reload, and a rejected reload keeps the previous good
// snapshot. They only start in Run, after New has registered every
// AfterAdopt hook (ClickHouse reconnect, dedupe stores, keepalive wheel, auth
// verifier): the watcher reloads once as soon as its watch exists, and that
// reload must already drive every hook — a hook registered after the first
// reload could miss it.
//
// A nested directory gets no watcher (#583): whoever writes a tenant's
// folder calls the reload route once the folder is complete, where a watcher
// would validate it half-written and, with no previous snapshot to fall back
// on, drop the tenant. SIGHUP reloads the whole tree in both shapes.
func (a *App) wireReloadTriggers() {
	// Registered here and released only at the end of Close, deliberately:
	// Notify takes SIGHUP off its default disposition (terminate), and a
	// Stop when the loop returns — the start of shutdown — would put it back
	// for the whole drain and release, where a hangup is easy to hit
	// (closing the terminal after Ctrl-C signals the process group). Once
	// ctx is done nothing reads the channel, so a late SIGHUP is discarded:
	// ignored, as a reload of a process on its way out should be.
	// The loop reads its own copy: Close nils the field after Run has
	// joined this goroutine, and the copy keeps that from being a data race
	// on any path that closes without joining.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	a.hup = hup
	a.add(component{name: "sighup", run: func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-hup:
				// Both cases ready at once is a coin flip; a reload must
				// not start once the stop has.
				if ctx.Err() != nil {
					return nil
				}
				a.tenants.Reload("sighup")
			}
		}
	}})
	if a.tenants.Nested() {
		slog.Info("nested settings directory: no directory watcher — reload via POST /v1/ops/settings/reload or SIGHUP")
		return
	}
	a.add(component{name: "settings watcher", run: func(ctx context.Context) error {
		// Watcher setup failure degrades, not fatal: SIGHUP and the ops
		// endpoint still reload.
		if err := a.tenants.Watch(ctx); err != nil {
			slog.Error("settings directory watcher failed; reload via SIGHUP or POST /v1/ops/settings/reload", "error", err)
		}
		return nil
	}})
}

// wireHTTP builds the handlers, the router, the API server, and — with
// prometheus.port set — the metrics sidecar. Same-port Prometheus mounts on
// the API router instead.
func (a *App) wireHTTP(authMW func(http.Handler) http.Handler) {
	ingestHandler := api.NewIngestHandler(a.registry, a.mq)
	ingestHandler.PolicySource = (*settings.Store).Policy
	ingestHandler.Dedup = func(s *settings.Store) dedupe.Deduplicator { return a.dedup.For(s.Tenant()) }
	ingestHandler.DedupeSettings = (*settings.Store).DedupeFor

	healthHandler := api.NewHealthHandler(a.ch)
	healthHandler.Boot = a.bootState

	streamHandler := api.NewStreamHandler(a.hub, a.mq)
	streamHandler.Metrics = a.sseMetrics
	streamHandler.Heartbeater = a.heartbeater
	// Closed when the API server begins shutting down, ending every open
	// stream at once (see serve).
	closing := make(chan struct{})
	streamHandler.Closing = closing

	pipesHandler := api.NewPipesHandler(func(s *settings.Store) pipes.Source { return s }, (*settings.Store).Policy, a.ch, a.cache, a.ch.QueryTimeout)
	pipesHandler.Tenants = a.tenants

	deps := api.Dependencies{
		Ingest: ingestHandler,
		// /v1/ops/query proxies straight to ClickHouse over HTTP — no native
		// driver involvement. Same HTTP target as the ingest worker, resolved
		// per request.
		Query:           api.NewQueryHandler(a.ch.Target, a.ch.QueryTimeout),
		SSE:             streamHandler,
		Health:          healthHandler,
		Version:         api.NewVersionHandler(a.build.Version, a.build.GitCommit, a.build.BuildTime),
		Schema:          api.NewSchemaHandler(a.registry),
		DLQ:             api.NewDLQHandler(a.mq),
		Pipes:           pipesHandler,
		StructuredQuery: api.NewStructuredQueryHandler(a.ch, a.cache, a.registry, (*settings.Store).Policy, (*settings.Store).TimestampBucketSeconds, a.ch.QueryTimeout, (*settings.Store).DefaultMaxRows),

		AuthMW:       authMW,
		Tenants:      a.tenants,
		PolicySource: a.policies,
		CORSOrigins:  (*settings.Store).CORSOrigins,
		Settings:     api.NewSettingsHandler(a.tenants),
	}

	prom := a.cfg.Prometheus
	if a.promHandler != nil && prom.Port == 0 {
		deps.MetricsHandler = a.promHandler
		deps.MetricsPath = prom.Path
	}
	a.handler = api.NewRouter(deps)

	// ReadHeaderTimeout only, deliberately: net/http leaves ReadTimeout's
	// deadline on the connection while the handler runs, so its background
	// read would time out and cancel the request context — ending every
	// /v1/stream connection at that timeout. WriteTimeout would cut the
	// same long-lived responses.
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", a.cfg.Server.Port),
		Handler:           a.handler,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	srv.RegisterOnShutdown(sync.OnceFunc(func() { close(closing) }))
	a.add(component{name: "http server", run: func(ctx context.Context) error {
		return a.serve(ctx, "server", srv, a.listener)
	}})

	if a.promHandler != nil && prom.Port != 0 {
		mux := http.NewServeMux()
		mux.Handle(prom.Path, a.promHandler)
		promSrv := &http.Server{
			Addr:              fmt.Sprintf(":%d", prom.Port),
			Handler:           mux,
			ReadHeaderTimeout: readHeaderTimeout,
			// Full Read/Write timeouts are safe here — unlike the main API
			// server (SSE), the Prometheus sidecar serves only single-shot
			// scrape requests, so an unbounded slow client has no legitimate
			// reason to hold a connection.
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		}
		a.add(component{name: "prometheus server", run: func(ctx context.Context) error {
			// Not fatal: the API keeps serving without its scrape endpoint.
			if err := a.serve(ctx, "prometheus metrics server", promSrv, nil); err != nil {
				slog.Error("prometheus server error", "error", err)
			}
			return nil
		}})
	}
}

// serve runs srv on ln — bound to srv.Addr when nil — until ctx is done,
// then drains it within the shutdown timeout and force-closes whatever is
// still open at the deadline. Shutdown stops accepting, waits for in-flight
// requests, and cancels nothing, so an SSE stream — a request that never
// finishes on its own — would ride the drain to the deadline; its
// OnShutdown hook ends every stream as the drain begins instead (the client
// reconnects and gap-fills via Last-Event-ID), leaving the budget to the
// request/response work it is for. A bind or serve failure is returned; a
// drain failure is logged, since the process is exiting anyway.
func (a *App) serve(ctx context.Context, name string, srv *http.Server, ln net.Listener) error {
	if ln == nil {
		var lc net.ListenConfig
		var err error
		if ln, err = lc.Listen(ctx, "tcp", srv.Addr); err != nil {
			return err
		}
	}
	slog.Info("starting "+name, "addr", ln.Addr().String())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err := <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	slog.Info("shutting down " + name)
	shutCtx, cancel := a.shutdownContext()
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error(name+" shutdown error; closing open connections", "error", err)
		_ = srv.Close()
	}
	<-served
	return nil
}

// shutdownContext caps how long a graceful stop waits (server.shutdown_timeout).
func (a *App) shutdownContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(a.cfg.Server.ShutdownTimeout)*time.Second)
}
