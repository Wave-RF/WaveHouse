package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
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

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/coord"
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
	a.policies = func() *policy.Policy { return defaultPolicy(tenants) }
	if !tenants.Nested() && a.policies() == nil {
		slog.Warn("no policy adopted — every token-based request is denied until policies.json defines one (fail closed)")
	}
	return nil
}

// defaultPolicy is the default tenant's access-control policy, which the ops
// gate of a flat directory reads its admin role from per request. There
// tenant 0 is the whole directory, always served: a reload that fails keeps
// the previous document. A nested directory's ops gate reads no policy at all
// (api.NewRouter).
func defaultPolicy(tenants *settings.Registry) *policy.Policy {
	store, ok := tenants.For(tenant.Default)
	if !ok {
		return nil
	}
	return store.Policy()
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

// gapWindows is the history the sweeper keeps for each tenant: its own
// stream.gap_window_minutes, since each tenant's events have a queue of their
// own — for a rejected tenant, the window its folder last had, because a
// rejection is the common reload failure (a typo, fixed minutes later) and
// its clients resume from Last-Event-ID once it is served again. A removed
// tenant is not named, so it keeps no history (mq.Purger.PurgeAcked).
func gapWindows(tenants *settings.Registry) map[tenant.ID]time.Duration {
	windows := map[tenant.ID]time.Duration{}
	for id, store := range tenants.Known() {
		if store == nil {
			windows[id] = keepEverything
			continue
		}
		windows[id] = store.GapWindow()
	}
	return windows
}

// keepEverything is the window of a tenant whose folder has been rejected
// since boot: this process has never read its stream.gap_window_minutes, so
// none of the history its queue holds is known to be past it. A rejected
// tenant is sent no new events, so what it keeps is what its queue held at
// boot.
const keepEverything = time.Duration(math.MaxInt64)

// served reports whether the registry is serving tenant id: what the
// per-tenant resources — verifiers, dedupe stores, open streams, the cache
// version index — are pruned by once a reload removes or rejects their
// tenant.
func (a *App) served(id tenant.ID) bool {
	_, ok := a.tenants.For(id)
	return ok
}

// perTenant adapts a store accessor to the tenant-keyed getter the async
// paths take: they hold a tenant id — the one each message's topic names
// for the stream hub and the ingest worker (#583 story 5) — not a request's
// resolved store. A miss — a tenant no longer served, or a 0 a nested
// directory does not hold — is logged and read as T's zero value. By then a
// tenant a reload removed or rejected has had its streams ended (Hub.Prune)
// and its schema loop stopped (discoveries), so a miss is an event still in
// flight; the ingest worker reads its DLQ switch through dlqFor instead.
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
// So a removed or rejected tenant's queued rows are parked in its own
// dead-letter queue rather than left unacked for its return: unacked, each
// would be redelivered every ack wait for as long as the tenant is away, and
// would hold the tenant's ack floor, so the sweeper could purge none of its
// queue past it.
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

// sharedTables is the cache the ingest worker invalidates through. A
// tenant's tables are the ones on its ClickHouse address and database, and
// the tenants naming the same address and database — whatever their user or
// tls block, so across pools — read the same tables: an insert into one
// changes what every one of them would read. The worker names one tenant's
// namespaces (the batch's), and this bumps them under every tenant sharing
// its tables (chconn.Pools.SharingTables), the named one included. Reads are
// untouched: a tenant's cached results stay its own. A tenant on no pool —
// rejected, removed, or one no pool could be opened for, such as by the
// connection ceiling — is out of the fan-out, and its cache is orphaned
// when it gets one (wireClickHouse, Cache.InvalidateTenant), so a folder
// repaired or restored inside a TTL never serves pre-insert rows; a pipe
// result names no table, so no insert invalidates it and between those it
// stays until its TTL expires (#343).
type sharedTables struct {
	cache.Cache
	sharing func(tenant.ID) []tenant.ID
}

func (s sharedTables) Invalidate(ctx context.Context, namespaces []cache.Namespace) (uint64, error) {
	ids := map[tenant.ID]bool{}
	for _, ns := range namespaces {
		ids[ns.Tenant] = true
		for _, id := range s.sharing(ns.Tenant) {
			ids[id] = true
		}
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

// wireClickHouse opens the ClickHouse pools: one per distinct address,
// database, user, password and tls tuple among the served tenants' clickhouse
// blocks (with the boot-config password), shared by the tenants naming it and
// sized to their largest ask (#583 story 6). Every reload reconciles them —
// a new tuple opens (no dial), a tenant whose tuple changed is repointed, a
// tuple no tenant names is released after its grace — under the boot
// config's clickhouse.max_total_conns, the ceiling on the open pools' sizes
// together: capacity is sized once, per process, so pools above it refuse
// boot like the rest of an impossible boot config (#530), and at a reload a
// resize above it is refused with the pool kept at its size, and a tuple
// that cannot be opened — the ceiling, a certificate file that cannot be
// read, or options the driver refuses — leaves its tenants on the pool they
// had, or on none when they had none; both logged, and retried by the next
// reload. Reachability surfaces
// where it already does (schema discovery retries, /readyz, query errors).
// Every consumer resolves its tenant's pool per call (chConn, chTargetFor).
func (a *App) wireClickHouse() error {
	members := func() []chconn.Member {
		var ms []chconn.Member
		for id, store := range a.tenants.All() {
			c := store.ClickHouse()
			ms = append(ms, chconn.Member{Tenant: id, Params: chconn.Params{
				Addr: c.Addr, HTTPPort: c.HTTPPort, HTTPScheme: c.HTTPScheme,
				Database: c.Database, Username: c.Username, Password: a.cfg.ClickHouse.Password,
				QueryTimeout: c.QueryTimeout,
				TLS:          chconn.TLS(c.TLS),
				Headers:      c.Headers,
				MaxOpenConns: c.MaxOpenConns, MaxIdleConns: c.MaxIdleConns,
			}})
		}
		return ms
	}
	pools, err := chconn.NewPools(a.cfg.ClickHouse.MaxTotalConns, members())
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}
	a.pools = pools
	a.add(component{name: "clickhouse", close: withoutContext(pools.Close)})
	a.tenants.AfterAdopt(func([]tenant.ID) {
		stale, err := pools.Reconcile(members())
		if err != nil {
			slog.Error("clickhouse pools reconciled in part; the next reload retries", "error", err)
		}
		// A tenant back on a pool after an absence was out of the cache
		// fan-out (sharedTables) while away, and one moved to another
		// address or database now reads other tables: either way what it
		// cached is stale, so all of it is orphaned at once.
		for _, id := range stale {
			if err := a.cache.InvalidateTenant(a.stopCtx, id); err != nil {
				slog.Error("cache invalidation of a stale tenant failed; it may serve stale rows until they expire", "tenant", id, "error", err)
			}
		}
	})
	return nil
}

// chConn is the connection of tenant id, or an untyped nil when the tenant
// is on no pool — never a nil *Manager inside a non-nil driver.Conn, which
// would pass a nil check and panic on use.
func (a *App) chConn(id tenant.ID) driver.Conn {
	m := a.pools.For(id)
	if m == nil {
		return nil
	}
	return m
}

// discoverySource is what tenant id's schema registry discovers from, read
// per refresh so a reload that repoints the tenant or moves its database
// applies to the next one: its pool's connection and the database that pool
// was opened for — never the adopted document's, which a refused move would
// pair with the pool the tenant kept, discovering a database its queries and
// inserts do not use.
func (a *App) discoverySource(id tenant.ID) discovery.Source {
	return func() (driver.Conn, string) {
		m := a.pools.For(id)
		if m == nil {
			return nil, ""
		}
		return m, m.Identity().Database
	}
}

// The store-keyed getters the handlers take: each resolves the request
// tenant's pool or registry per call, so a reload that repoints the tenant
// applies to the next request.

func (a *App) chConnFor(s *settings.Store) driver.Conn { return a.chConn(s.Tenant()) }

func (a *App) chTargetFor(s *settings.Store) chconn.Target { return a.pools.Target(s.Tenant()) }

func (a *App) registryFor(s *settings.Store) *discovery.SchemaRegistry {
	return a.discoveries.For(s.Tenant())
}

// queryTimeout is the tenant's read deadline, a per-call setting rather
// than a property of the pool it shares.
func queryTimeout(s *settings.Store) time.Duration { return s.ClickHouse().QueryTimeout }

// wireDiscovery builds one schema registry per served tenant, each with a
// refresh loop of its own (discoveries), and the boot state /livez reports:
// 503 with the latest discovery failure while no tenant has completed a
// first discovery, then 200 for the rest of the process lifetime — with one
// tenant, the rule there always was. A failure goes with its tenant: once the
// tenant it names is no longer served, the diagnostic is the no-tenant one
// again. Non-fatal either way. A flat
// directory's tenant 0 is refreshed synchronously here, as before, so the
// port binds with the state known; a failure marks the binary degraded and
// leaves the retry (jittered backoff 2s → 60s) to its loop. A nested directory's
// tenants refresh in their loops from the start, so boot never waits on a
// tenant's ClickHouse, and a nested directory serving no tenant stays
// degraded until a reload adopts one that loads. The process still binds its
// port so operators can `curl /livez` instead of grepping a restart-loop
// log; once a tenant has loaded, another tenant's outage is that tenant's
// log line and counter, never a probe failure.
func (a *App) wireDiscovery(ctx context.Context) {
	a.bootState = api.NewBootState(nil)
	nested := a.tenants.Nested()
	// loaded flips once, on the first tenant's first success. The check and
	// the BootState write happen under one lock, so a failure reported
	// while another tenant's success lands can never overwrite the cleared
	// state for good and pin /livez at 503.
	var (
		mu     sync.Mutex
		loaded bool
		// failing is the tenant the degraded diagnostic names.
		failing tenant.ID
	)
	noTenantLoaded := errors.New("schema discovery: no tenant has completed a first discovery yet")
	diagnostic := func(id tenant.ID, err error) error {
		if nested {
			return fmt.Errorf("schema discovery: tenant %s: %w", id, err)
		}
		return fmt.Errorf("schema discovery: %w", err)
	}
	d := newDiscoveries(a.stopCtx,
		func(id tenant.ID, _ *settings.Store) *discovery.SchemaRegistry {
			return discovery.NewSchemaRegistry(a.discoverySource(id), id, perTenant(a.tenants, (*settings.Store).SchemaRefreshInterval))
		},
		func(id tenant.ID, err error) {
			slog.Warn("schema discovery retry failed", "tenant", id, "error", err)
			mu.Lock()
			defer mu.Unlock()
			// A loop a reload stopped may report one last attempt after its
			// tenant has gone.
			if !loaded && a.served(id) {
				failing = id
				a.bootState.Set(diagnostic(id, err))
			}
		},
		func(id tenant.ID) {
			mu.Lock()
			defer mu.Unlock()
			if loaded {
				slog.Info("schema discovery succeeded after retry", "tenant", id)
				return
			}
			loaded = true
			slog.Info("schema discovery succeeded after retry, /livez now 200", "tenant", id)
			a.bootState.Set(nil)
		})
	a.discoveries = d
	if nested {
		a.bootState.Set(noTenantLoaded)
		d.reconcile(a.tenants)
	} else {
		// A flat registry always serves tenant 0: Open refused boot otherwise.
		store, _ := a.tenants.For(tenant.Default)
		reg := d.build(tenant.Default, store)
		if err := reg.Refresh(ctx); err != nil {
			slog.Warn("schema discovery failed on boot, retrying in background", "error", err)
			a.bootState.Set(diagnostic(tenant.Default, err))
		} else {
			loaded = true // no loop has started yet
		}
		d.adopt(tenant.Default, reg)
	}
	a.tenants.AfterAdopt(func([]tenant.ID) {
		d.reconcile(a.tenants)
		mu.Lock()
		defer mu.Unlock()
		if !loaded && failing != "" && !a.served(failing) {
			failing = ""
			a.bootState.Set(noTenantLoaded)
		}
	})
	a.add(component{name: "schema discovery", close: d.close})
}

// wireDedupe builds the dedupe stores — the one place the implementation is
// chosen.
func (a *App) wireDedupe() error {
	switch b := a.cfg.Dedupe.Backend; b {
	case config.DedupePebble:
		return a.wirePebbleDedupe()
	default:
		return unreachableBackend("dedupe.backend", b)
	}
}

// wirePebbleDedupe builds the dedupe stores: one per tenant (#583 story 7), each
// following its own tenant's hot-reloadable dedupe.enabled, over the
// embedded Pebble implementation, which is handed data_dir and decides the
// rest: every tenant's seen ids in one instance there, open while any
// tenant's store is (dedupe.Embedded). One reconcile closure sets every
// store to what the registry says: open exactly when its tenant is served
// with the switch on, closed — its seen ids kept — when the tenant is
// switched off, rejected, or removed. It is registered as the after-adopt
// hook BEFORE the boot apply (Apply is idempotent), so a reload landing
// between the two can't leave a tenant's settings saying "on" with its store
// still closed — either the hook sees it or the boot apply reads it. An
// instance that cannot open follows the registry's own rule for the shape:
// flat refuses boot, like every other store, and on reload logs and leaves
// the store closed — ingest then fails closed (500 "dedupe failed") rather
// than silently publishing un-deduped, since the files asked for dedupe;
// nested fails closed the same way at boot too, for every tenant with
// dedupe on, the next reload retrying, so it never costs the process.
func (a *App) wirePebbleDedupe() error {
	nested := a.tenants.Nested()
	embedded := dedupe.NewEmbedded(a.cfg.DataDir)
	stores := dedupe.NewStores(embedded.Tenant)
	a.dedup, a.dedupeStats = stores, embedded.Stats
	a.add(component{name: "dedupe", close: withoutContext(stores.Close)})
	reconcile := func() error {
		if err := stores.Retain(a.served); err != nil {
			slog.Error("dedupe store close failed", "error", err)
		}
		var errs []error
		for id, store := range a.tenants.All() {
			m := stores.For(id)
			enabled := store.DedupeEnabled()
			wasOpen := m.Open()
			// The instance opens with the first store switched on: a fresh
			// directory then is a first run or a lost volume.
			if enabled && !embedded.Open() && len(errs) == 0 {
				config.WarnIfFreshDataDir("pebble", embedded.Dir())
			}
			if err := m.Apply(enabled); err != nil {
				// The stores share the one instance, so a failure is every
				// store's: logged once, not once per tenant.
				if len(errs) == 0 {
					config.LogStorageInitError("dedupe", embedded.Dir(), err)
				}
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

// wireMQ starts the MQ — the one place the implementation is chosen;
// everything after it sees mq.Broker.
func (a *App) wireMQ(ctx context.Context) error {
	switch b := a.cfg.MQ.Backend; b {
	case config.MQEmbedded:
		return a.wireEmbeddedMQ(ctx)
	case config.MQNATS:
		return a.wireNATSMQ(ctx)
	default:
		return unreachableBackend("mq.backend", b)
	}
}

// wireNATSMQ connects to the operator's NATS (mq.backend: nats) and waits,
// up to mq.nats.topology_wait, for the streams and durables it needs; a
// topology still wrong then refuses boot with every finding. The operator
// owns every limit, so a tenant's mq.max_bytes_gb is not handed over
// (config.Warnings says so at boot).
func (a *App) wireNATSMQ(ctx context.Context) error {
	n := a.cfg.MQ.NATS
	broker, err := mq.NewNATS(ctx, mq.NATSConfig{
		URLs:         n.URLs,
		Name:         n.Name,
		CredsFile:    n.CredsFile,
		NKeySeedFile: n.NKeySeedFile,
		User:         n.User,
		PasswordFile: n.PasswordFile,
		TLS: mq.NATSTLS{
			CAFile: n.TLS.CAFile, CertFile: n.TLS.CertFile, KeyFile: n.TLS.KeyFile,
			ServerName: n.TLS.ServerName, HandshakeFirst: n.TLS.HandshakeFirst,
		},
		JSDomain: n.JSDomain,
		// AckWait, MaxAckPending and Prefetch are left to mq's defaults,
		// which are the ingest worker's own.
		Topology: mq.NATSTopology{
			Prefix:         n.SubjectPrefix,
			Partitions:     n.Partitions,
			IngestConsumer: n.IngestConsumer,
			HistoryStream:  n.HistoryStream,
			PublishTimeout: n.PublishTimeout,
		},
		ConnectTimeout: n.ConnectTimeout,
		TopologyWait:   n.TopologyWait,
	})
	if err != nil {
		return fmt.Errorf("mq open: %w", err)
	}
	a.adoptMQ(broker)
	return nil
}

// adoptMQ makes broker the process's MQ, closed with it.
func (a *App) adoptMQ(broker mq.Broker) {
	a.mq = broker
	a.add(component{name: "mq", close: withoutContext(broker.Close)})

	// Only register system metric gauges when a real MeterProvider is in
	// place — otherwise `otel.GetMeterProvider()` returns the no-op SDK
	// provider and RegisterCallback silently no-ops, making this look
	// authoritative when it's actually doing nothing.
	if a.cfg.OTel.Enabled || a.cfg.Prometheus.Enabled {
		if err := observability.RegisterSystemMetrics(broker.Stats, a.dedupeStats); err != nil {
			slog.Error("failed to register system metrics", "error", err)
		}
	}
}

// wireEmbeddedMQ starts the embedded NATS under data_dir/nats and hands it
// each served tenant's mq.max_bytes_gb, which opens that tenant's queue the
// first time. The budget is hot-reloadable: after every
// reload the registry applies, each served tenant's is handed over again,
// and the MQ owns how it is split across the tenant's queues and keeps them
// consistent (see mq.Broker.SetMaxBytes). A tenant no longer served keeps
// its queue at the budget it last had. A queue that cannot be opened or
// resized follows the registry's rule for the shape: a flat directory
// refuses boot, like every other store, and on a reload logs it, keeping the
// previous budget; a nested directory logs it at boot too, so it never costs
// the process — the tenant's ingest answers 503 until its queue opens, each
// reload trying again, and publishes too at the pace the MQ allows. The hook
// is registered before the boot apply, as the dedupe one is. The boot apply
// runs on ctx, New's, so a stop signaled during a boot that opens many queues
// is not held up by them.
func (a *App) wireEmbeddedMQ(ctx context.Context) error {
	dir := filepath.Join(a.cfg.DataDir, "nats")
	config.WarnIfFreshDataDir("nats", dir)
	var broker mq.Broker
	broker, err := mq.NewEmbedded(dir)
	if err != nil {
		config.LogStorageInitError("mq", dir, err)
		return fmt.Errorf("mq open: %w", err)
	}
	a.adoptMQ(broker)

	// The hook's apply is rooted in the App's stop context, so a reload
	// caught mid-hook by SIGTERM gives up rather than holding the drain past
	// server.shutdown_timeout; a done ctx ends the pass over the tenants.
	reconcile := func(ctx context.Context) error {
		var errs []error
		for id, store := range a.tenants.All() {
			if err := ctx.Err(); err != nil {
				errs = append(errs, err)
				break
			}
			mb := store.MQMaxBytes()
			if mb == broker.MaxBytes(id) {
				continue
			}
			if err := broker.SetMaxBytes(ctx, id, mb); err != nil {
				slog.Error("mq queue not reconciled with settings; the next reload retries", "tenant", id, "error", err)
				errs = append(errs, fmt.Errorf("tenant %s: %w", id, err))
				continue
			}
			slog.Info("mq queue reconciled with settings", "tenant", id, "max_bytes_gb", mb>>30)
		}
		return errors.Join(errs...)
	}
	a.tenants.AfterAdopt(func([]tenant.ID) { _ = reconcile(a.stopCtx) })
	if err := reconcile(ctx); err != nil && !a.tenants.Nested() {
		return fmt.Errorf("mq open: %w", err)
	}
	return nil
}

// pruner is a cache whose version index lives in the process and would
// otherwise keep a tenant that stopped being served (cache.LocalCache).
type pruner interface {
	Prune(served func(tenant.ID) bool)
}

// The hook below asserts pruner at run time; this keeps LocalCache from
// silently dropping out of it.
var _ pruner = (*cache.LocalCache)(nil)

// wireCache opens the query-result cache — the one place the implementation
// is chosen. After every reload a tenant no longer served, removed or
// rejected alike, has its in-process version index dropped (#262); its cache
// is orphaned with it, as it would be anyway when it came back
// (wireClickHouse). A shared backend keeps no such index and is skipped.
func (a *App) wireCache() error {
	var c cache.Cache
	switch b := a.cfg.Cache.Backend; b {
	case config.CacheLocal:
		l1, err := cache.NewLocal(a.cfg.Cache.L1MaxCost)
		if err != nil {
			return fmt.Errorf("cache init: %w", err)
		}
		c = l1
	case config.CacheRedis:
		rc, err := redisConfig(a.cfg.Cache.Redis)
		if err != nil {
			return fmt.Errorf("cache init: %w", err)
		}
		r, err := cache.NewRedis(rc)
		if err != nil {
			return fmt.Errorf("cache init: %w", err)
		}
		c = r
	default:
		return unreachableBackend("cache.backend", b)
	}
	a.cache = c
	a.add(component{name: "cache", close: withoutContext(c.Close)})
	a.tenants.AfterAdopt(func([]tenant.ID) {
		if p, ok := a.cache.(pruner); ok {
			p.Prune(a.served)
		}
	})
	return nil
}

// redisConfig maps the boot config's cache.redis block onto the backend's
// config. Load has applied every default and validated the block; the TLS
// files are read again here, so the connection uses what is on disk now.
func redisConfig(r config.CacheRedisConfig) (cache.RedisConfig, error) {
	t, err := r.TLS.Config()
	if err != nil {
		return cache.RedisConfig{}, err
	}
	return cache.RedisConfig{
		Addrs:            r.Addrs,
		Mode:             r.Mode,
		SentinelMaster:   r.SentinelMaster,
		Username:         r.Username,
		Password:         r.Password,
		DB:               r.DB,
		TLS:              t,
		KeyPrefix:        r.KeyPrefix,
		Timeout:          r.Timeout,
		DialTimeout:      r.DialTimeout,
		MaxValueBytes:    r.MaxValueBytes,
		CompressMinBytes: r.CompressMinBytes,
		VersionTTL:       r.VersionTTL,
	}, nil
}

// unreachableBackend is each layer switch's default case. config.Validate
// refuses a backend with no case, so reaching it means a Config built by hand
// without one (the zero value is not the default), or a case missing here.
func unreachableBackend[T ~string](key string, got T) error {
	return fmt.Errorf("%s %q has no wiring: a Config built without config.Load must name the backend of every layer it wires", key, got)
}

// wireCoord opens the lease coordinator the singleton loops campaign on.
func (a *App) wireCoord() error {
	switch b := a.cfg.Coord.Backend; b {
	case config.CoordLocal:
		c := coord.NewLocal()
		a.coord = c
		a.add(component{name: "coord", close: c.Close})
		return nil
	default:
		return unreachableBackend("coord.backend", b)
	}
}

// sweeperLease is the lease the sweeper runs under, one sweeper per queue.
const sweeperLease = "sweeper"

// elected runs fn only while this process holds lease, campaigning again
// whenever the term ends (coord.RunElected): the loop of a role that must
// run in one process at a time, however many processes run the role.
func (a *App) elected(lease string, fn func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return coord.RunElected(ctx, a.coord, lease, coord.RetryPeriod, func(ctx context.Context, _ coord.Term) error {
			return fn(ctx)
		})
	}
}

// wireSweeper adds the active sweeper — purges messages that are both
// written to ClickHouse and older than their tenant's SSE gap window (its own
// stream.gap_window_minutes, re-read every sweep — see gapWindows). Runs
// every minute, while this process holds the sweeper lease.
func (a *App) wireSweeper() {
	sweeper := ingest.NewSweeper(a.mq, func() map[tenant.ID]time.Duration { return gapWindows(a.tenants) })
	a.add(component{name: "sweeper", run: a.elected(sweeperLease, func(ctx context.Context) error {
		sweeper.Start(ctx)
		return nil
	})})
}

// wireStreaming builds the SSE fan-out: one metric set shared by the Hub
// (drop counts) and the stream handler (write counts); the Hub that
// projects/serializes each event once per (topic, role) and pushes it to
// that role's subscribers; the MQ → Hub bridge; and the keepalive wheel.
// After every reload the Hub ends the open streams of each tenant no longer
// served, removed or rejected alike (Hub.Prune); the client reconnects into
// that tenant's 404 or 503 and gap-fills once it is served again.
func (a *App) wireStreaming() {
	a.sseMetrics = stream.NewMetrics()
	a.hub = stream.NewHub(perTenant(a.tenants, (*settings.Store).Policy), a.discoveries.For, a.sseMetrics)
	a.tenants.AfterAdopt(func([]tenant.ID) { a.hub.Prune(a.served) })

	// Hub bridge: MQ → broadcast to connected SSE clients. The Hub decodes and
	// projects each event itself (skipping malformed payloads) under the
	// tenant the message's topic names, so the bridge just forwards the topic
	// and the raw bytes, and acks.
	a.add(component{name: "hub bridge", run: func(ctx context.Context) error {
		err := a.mq.Subscribe(ctx, "hub-bridge", func(msg *mq.Message) error {
			a.hub.Broadcast(msg.Topic(), msg.Data)
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
		stop, failed, err := ingest.StartIngestWorker(ctx, a.mq, sharedTables{Cache: a.cache, sharing: a.pools.SharingTables}, a.pools.Target, dlqFor(a.tenants))
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

// wireAuth builds the JWT middleware: one verifier per tenant being served,
// from that tenant's auth block (jwks_url, role_claim), with the secrets from
// boot config shared by all. A tenant's verifier is rebuilt after a reload
// that adopts it with changed wiring, kept when the wiring is unchanged, and
// dropped once the tenant stops being served, removed or rejected alike — no
// work runs for a tenant that is not served, and a folder adopted again is
// rebuilt from scratch. A JWKS key set is fetched off the
// boot and reload paths, so an unreachable endpoint never holds either: until
// a fetch succeeds — retried with backoff from a second, then kept fresh by
// the library hourly and on an unknown key id — that tenant's token-bearing
// requests are refused with a 503 (never evaluated under its default_role).
// The verifiers are released with the other components.
//
// There is no on/off switch — the middleware always runs. With neither a
// secret (boot config) nor a JWKS URL (that tenant's settings), no token can
// validate for that tenant, so its every request falls back to its policy
// default_role (a public tenant). That's a valid posture, so it warns per
// tenant rather than fails.
func (a *App) wireAuth() func(http.Handler) http.Handler {
	cfg := a.cfg
	switch cfg.Auth.JWTSecret {
	case "":
		// Per tenant: with no boot secret each tenant is as public as its own
		// jwks_url leaves it, and one tenant's provider says nothing about
		// another's.
		for id, store := range a.tenants.All() {
			if store.Auth().JWKSURL == "" {
				slog.Warn("no auth.jwt_secret (boot config) and no auth.jwks_url in this tenant's settings: no token can be validated for it, so its every request resolves to its policy default_role (public access)", "tenant", id)
			}
		}
	case "change-me-in-production":
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

	// The operator key's admin role is the request tenant's. Silent on a
	// miss, unlike perTenant: the one is the ops tree over a nested
	// directory serving no tenant 0, where the gate reads no policy either.
	policies := func(id tenant.ID) *policy.Policy {
		if store, ok := a.tenants.For(id); ok {
			return store.Policy()
		}
		return nil
	}
	// The request's tenant is its resolved store's — one read of the context,
	// the one api.TenantMW wrote.
	tenantOf := func(ctx context.Context) (tenant.ID, bool) {
		store, ok := api.StoreFromContext(ctx)
		if !ok {
			return "", false
		}
		return store.Tenant(), true
	}
	authn := auth.NewAuthenticator(auth.Config{JWTSecret: cfg.Auth.JWTSecret, OperatorKey: operatorKey}, tenantOf, policies)
	a.add(component{name: "auth", close: func(context.Context) error {
		authn.Close()
		return nil
	}})
	wiring := func(store *settings.Store) auth.Wiring {
		s := store.Auth()
		return auth.Wiring{JWKSURL: s.JWKSURL, RoleClaim: s.RoleClaim}
	}
	for id, store := range a.tenants.All() {
		authn.Reconfigure(id, wiring(store))
	}
	a.tenants.AfterAdopt(func(adopted []tenant.ID) {
		for _, id := range adopted {
			if store, ok := a.tenants.For(id); ok {
				authn.Reconfigure(id, wiring(store))
			}
		}
		authn.Prune(a.served)
	})
	return authn.Middleware()
}

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

// wireReloadTriggers adds SIGHUP and the directory watcher. All three
// triggers (these two and POST /v1/ops/settings/reload) funnel into the same
// serialized Registry.Reload, and a rejected reload keeps the previous good
// snapshot. They only start in Run, after New has registered every
// AfterAdopt hook (ClickHouse reconnect, dedupe stores, keepalive wheel, open
// streams, auth verifiers): the watcher reloads once as soon as its watch
// exists, and that reload must already drive every hook — a hook registered
// after the first reload could miss it.
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
	ingestHandler := api.NewIngestHandler(a.registryFor, a.mq)
	ingestHandler.PolicySource = (*settings.Store).Policy
	ingestHandler.Dedup = func(s *settings.Store) dedupe.Deduplicator { return a.dedup.For(s.Tenant()) }
	ingestHandler.DedupeSettings = (*settings.Store).DedupeFor

	// Readiness pings every open pool at once and is ready at the first
	// answer: one tenant's ClickHouse outage is not the process's.
	healthHandler := api.NewHealthHandler(a.pools.Ping)
	healthHandler.Boot = a.bootState

	streamHandler := api.NewStreamHandler(a.hub, a.mq)
	streamHandler.Metrics = a.sseMetrics
	streamHandler.Heartbeater = a.heartbeater
	streamHandler.Served = a.served
	// Closed when the API server begins shutting down, ending every open
	// stream at once (see serve).
	closing := make(chan struct{})
	streamHandler.Closing = closing

	pipesHandler := api.NewPipesHandler(func(s *settings.Store) pipes.Source { return s }, (*settings.Store).Policy, a.chConnFor, a.cache, queryTimeout)
	pipesHandler.Tenants = a.tenants

	schemaHandler := api.NewSchemaHandler(a.registryFor)
	schemaHandler.Tenants = a.tenants

	// /v1/ops/query proxies straight to ClickHouse over HTTP — no native
	// driver involvement. The HTTP target of the tenant ?tenant= names,
	// resolved per request like the ingest worker's.
	queryHandler := api.NewQueryHandler(a.chTargetFor, queryTimeout)
	queryHandler.Tenants = a.tenants

	deps := api.Dependencies{
		Ingest:          ingestHandler,
		Query:           queryHandler,
		SSE:             streamHandler,
		Health:          healthHandler,
		Version:         api.NewVersionHandler(a.build.Version, a.build.GitCommit, a.build.BuildTime),
		Schema:          schemaHandler,
		DLQ:             api.NewDLQHandler(a.mq),
		Pipes:           pipesHandler,
		StructuredQuery: api.NewStructuredQueryHandler(a.chConnFor, a.cache, a.registryFor, (*settings.Store).Policy, (*settings.Store).TimestampBucketSeconds, queryTimeout, (*settings.Store).DefaultMaxRows),

		AuthMW:       authMW,
		Tenants:      a.tenants,
		PolicySource: a.policies,
		CORSOrigins:  (*settings.Store).CORSOrigins,
		Settings:     api.NewSettingsHandler(a.tenants),
	}

	deps.MetricsHandler, deps.MetricsPath = a.inlineMetrics()
	a.handler = api.NewRouter(deps)
	a.wireServers(func() { close(closing) })
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

// inlineMetrics is the metrics endpoint to mount on the main router: with
// prometheus.port 0 only, since a non-zero port gets its own listener.
func (a *App) inlineMetrics() (http.Handler, string) {
	if a.promHandler == nil || a.cfg.Prometheus.Port != 0 {
		return nil, ""
	}
	return a.promHandler, a.cfg.Prometheus.Path
}

// wireServers adds the server of a.handler on server.port and, with
// prometheus.port set, the metrics sidecar. onShutdown, when set, runs as the
// main server begins its drain.
func (a *App) wireServers(onShutdown func()) {
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
	if onShutdown != nil {
		srv.RegisterOnShutdown(sync.OnceFunc(onShutdown))
	}
	a.add(component{name: "http server", run: func(ctx context.Context) error {
		return a.serve(ctx, "server", srv, a.listener)
	}})

	if prom := a.cfg.Prometheus; a.promHandler != nil && prom.Port != 0 {
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
