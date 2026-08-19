package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/stream"
)

// Pre-populated build info variables, set via ldflags by the Makefile and by
// GoReleaser at release time. The defaults here are the unstamped-build
// fallbacks; buildInfoFallback below fills them in for `go install` builds.
//
// All three MUST keep constant string initializers: `go build -ldflags -X`
// silently does nothing to a variable whose initializer isn't a constant.
// BuildTime was `time.Now().Format(time.RFC3339)`, so every `-X
// main.BuildTime=...` the Makefile and .goreleaser.yaml have ever passed was
// dropped on the floor and /version reported the process's start time as its
// build time.
var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func init() { buildInfoFallback() }

// buildInfoFallback recovers the build stamps from the metadata the Go
// toolchain embeds on its own, for binaries built without our ldflags.
//
// `go install github.com/Wave-RF/WaveHouse/cmd/wavehouse@v0.1.0` — the install
// path the README documents — passes no ldflags, so a perfectly good tagged
// build would otherwise report itself as "dev"/"unknown"/"unknown". The module
// version and VCS stamps the toolchain records cover exactly that case.
//
// ldflags always win, and each field is gated on ITS OWN default rather than on
// Version alone: a build that stamps only -X main.GitCommit would otherwise have
// that value overwritten here, since Version would still read "dev". The
// Makefile and GoReleaser both stamp all three today, but the contract should
// hold for a partial link too.
func buildInfoFallback() {
	if Version != "dev" && GitCommit != "unknown" && BuildTime != "unknown" {
		return
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	// "(devel)" is what a build with no VCS metadata reports (`-buildvcs=false`,
	// or building outside a repo) — no better than the "dev" we already have.
	// A normal `go build` in a checkout gets a real pseudo-version, and
	// `go install pkg@vX.Y.Z` gets the module version.
	if v := info.Main.Version; Version == "dev" && v != "" && v != "(devel)" {
		Version = strings.TrimPrefix(v, "v")
	}

	// Present only when the binary was built from a VCS checkout; `go install
	// module@version` builds from the module cache and carries neither.
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if GitCommit == "unknown" && s.Value != "" {
				GitCommit = s.Value
			}
		case "vcs.time":
			if BuildTime == "unknown" && s.Value != "" {
				BuildTime = s.Value
			}
		}
	}
}

func main() {
	// Subcommand dispatch. `health` self-probes /livez for the distroless
	// Dockerfile HEALTHCHECK; `validate` checks a settings directory without
	// starting the server. An unknown command is a usage error — it must
	// never fall through and silently start the server (`wavehouse validat`
	// booting a listener is not a typo anyone wants). Hand-rolled on purpose:
	// three commands and zero flags don't earn a CLI framework; the day a
	// subcommand grows flags, port this switch to cobra.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "health":
			os.Exit(runHealthCheck())
		case "validate":
			os.Exit(runValidate(os.Args[2:]))
		case "version", "--version", "-v":
			fmt.Printf("wavehouse %s (commit %s, built %s)\n", Version, GitCommit, BuildTime)
			os.Exit(0)
		case "help", "--help", "-h":
			printUsage(os.Stdout)
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "wavehouse: unknown command %q\n\n", os.Args[1])
			printUsage(os.Stderr)
			os.Exit(2)
		}
	}
	os.Exit(run())
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprintf(w, `usage:
  wavehouse                 start the server
  wavehouse validate [dir]  validate a settings directory (dir falls back to %s)
  wavehouse health          liveness self-probe against the local server (container HEALTHCHECK)
  wavehouse version         print version, commit, and build time
  wavehouse help            show this help
`, config.EnvSettingsDir)
}

// run executes the binary and returns a process exit code. Using a
// separate function (rather than os.Exit directly in main) ensures deferred
// cleanups — especially OTEL flush — still run before the process exits.
func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("starting WaveHouse", "version", Version, "build_time", BuildTime, "git_commit", GitCommit)

	cfgPath := "config.yaml"
	if p := os.Getenv("WH_CONFIG"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error("load config", "error", err)
		return 1
	}

	// Validate auth config. There is no on/off switch — the JWT middleware
	// always runs. With neither a secret nor a JWKS URL no token can validate,
	// so every request falls back to the policy default_role (a pure public
	// deployment). That's a valid posture, so warn rather than fail.
	switch {
	case cfg.Auth.JWTSecret == "" && cfg.Auth.JWKSURL == "":
		logger.Warn("no auth.jwt_secret or auth.jwks_url set: no token can be validated, so every request resolves to the policy default_role (public access)")
	case cfg.Auth.JWTSecret == "change-me-in-production":
		logger.Warn("WH_AUTH_JWT_SECRET is using the default insecure value")
	}

	cfg.Auth.OperatorKey = strings.TrimSpace(cfg.Auth.OperatorKey)
	if cfg.Auth.OperatorKey == "" {
		logger.Warn("no auth.operator_key set: if you lose the JWT secret, lose control of the JWKS endpoint, or lose your HMAC secret — or the policy is wiped — you will be locked out remotely and will need SSH access to restore the policy file and reboot")
	} else {
		logger.Info("operator key is set: requests presenting it via 'Authorization: Operator <key>' (or the X-Operator-Key alias) are authorized as a full-access platform operator, and can restore a wiped policy over HTTP")
	}

	ctx := context.Background()
	serviceName := "wavehouse"

	var level slog.Level
	switch strings.ToUpper(strings.TrimSpace(os.Getenv("WH_LOG_LEVEL"))) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logLevel := &slog.LevelVar{}
	logLevel.Set(level)

	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	var promHandler http.Handler
	// Provider init runs whenever either OTLP push or Prometheus exposition is
	// wanted — Prometheus-only operation (Alloy/scrape, no collector) is a
	// first-class mode. The OTel SDK MeterProvider is the shared substrate.
	if cfg.OTel.Enabled || cfg.Prometheus.Enabled {
		// Endpoint, TLS, and auth headers come from the standard
		// OTEL_EXPORTER_OTLP_* env vars, read by the SDK. A malformed header is
		// logged and skipped by the SDK (fail-soft); InitProvider's own error is
		// likewise non-fatal — we log it and fall back to stdout below.
		otelShutdown, ph, err := observability.InitProvider(ctx, serviceName, observability.ProviderConfig{
			TracesEnabled:     cfg.OTel.Enabled && cfg.OTel.Traces.Enabled,
			TracesSampleRate:  cfg.OTel.Traces.SampleRate,
			MetricsEnabled:    cfg.OTel.Enabled && cfg.OTel.Metrics.Enabled,
			PrometheusEnabled: cfg.Prometheus.Enabled,
			LogsEnabled:       cfg.OTel.Enabled && cfg.OTel.Logs.Enabled,
		})
		if err != nil {
			logger.Error("failed to initialize observability, falling back to stdout", "error", err)
		} else {
			promHandler = ph
			defer func() {
				// Bound shutdown so an unreachable collector can't hang exit:
				// otelShutdown honors this deadline internally (see
				// observability.InitProvider), returning even when a provider's
				// flush is stuck in gRPC backoff.
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = otelShutdown(ctx)
			}()

			// Only swap to the OTLP-aware logger when OTLP logs are wired up;
			// Prometheus-only mode keeps the stdout-only handler set earlier.
			if cfg.OTel.Enabled && cfg.OTel.Logs.Enabled {
				otelLogger := observability.NewLogger(serviceName, logLevel, true, cfg.OTel.Logs.SampleRate)
				logger = otelLogger.With(
					"version", Version,
					"build_time", BuildTime,
					"git_commit", GitCommit,
				)
				slog.SetDefault(logger)
			}
			otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
			if otlpEndpoint == "" {
				otlpEndpoint = "localhost:4317 (SDK default)"
			}
			switch {
			case cfg.OTel.Enabled && cfg.Prometheus.Enabled:
				logger.Info("observability pipeline established", "otlp_endpoint", otlpEndpoint, "prometheus", true)
			case cfg.OTel.Enabled:
				logger.Info("observability pipeline established", "otlp_endpoint", otlpEndpoint)
			case cfg.Prometheus.Enabled:
				logger.Info("observability pipeline established", "prometheus", true)
			}
		}
	}

	// ClickHouse connection.
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouse.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouse.Database,
			Username: cfg.ClickHouse.Username,
			Password: cfg.ClickHouse.Password,
		},
	})
	if err != nil {
		logger.Error("clickhouse open", "error", err)
		return 1
	}
	defer func() { _ = chConn.Close() }()

	// Process-lifetime context — cancelled by the SIGINT/SIGTERM handler
	// below. Created here (instead of further down) so the boot-time schema
	// discovery retry goroutine ties cleanly to shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Schema discovery — non-fatal on boot. If the first Refresh fails
	// (ClickHouse unreachable, database missing, etc.) we mark the binary
	// degraded via bootState (which /livez surfaces as 503 + diagnostic)
	// and retry in the background with exponential backoff. The process
	// still binds :8080 so operators can `curl /livez` instead of grepping
	// a restart-loop log. Once a Refresh succeeds, bootState flips to nil
	// and /livez returns 200. The periodic auto-refresh ticker is started
	// only after the first successful Refresh (sync or retry) so it never
	// races RetryRefresh on Refresh calls or on bootState writes.
	bootState := api.NewBootState(nil)
	refreshInterval := time.Duration(cfg.Schema.RefreshInterval) * time.Second
	registry := discovery.NewSchemaRegistry(chConn, cfg.ClickHouse.Database, refreshInterval, logger)
	if err := registry.Refresh(ctx); err != nil {
		logger.Warn("schema discovery failed on boot, retrying in background", "error", err)
		bootState.Set(fmt.Errorf("schema discovery: %w", err))
		go func() {
			retryErr := registry.RetryRefresh(ctx, 2*time.Second, 60*time.Second, func(attemptErr error) {
				logger.Warn("schema discovery retry failed", "error", attemptErr)
				bootState.Set(fmt.Errorf("schema discovery: %w", attemptErr))
			})
			if retryErr != nil {
				// ctx cancelled before success — process is shutting down.
				return
			}
			logger.Info("schema discovery succeeded after retry, /livez now 200")
			bootState.Set(nil)
			go registry.StartAutoRefresh(ctx)
		}()
	} else {
		go registry.StartAutoRefresh(ctx)
	}

	// State directories — one configurable root, fixed subdir convention.
	natsDir := filepath.Join(cfg.DataDir, "nats")
	pebbleDir := filepath.Join(cfg.DataDir, "pebble")

	// Optional embedded dedupe (Pebble).
	var dedup dedupe.Deduplicator
	if cfg.Dedupe.Enabled {
		config.WarnIfFreshDataDir(logger, "pebble", pebbleDir)
		dedup, err = dedupe.NewEmbedded(pebbleDir)
		if err != nil {
			config.LogStorageInitError(logger, "dedupe", pebbleDir, err)
			return 1
		}
		defer func() { _ = dedup.Close() }()
	}

	// Embedded MQ (NATS).
	config.WarnIfFreshDataDir(logger, "nats", natsDir)
	maxBytes := int64(cfg.MQ.MaxBytesGB) * 1024 * 1024 * 1024
	embeddedMQ, err := mq.NewEmbedded(natsDir, maxBytes)
	if err != nil {
		config.LogStorageInitError(logger, "mq", natsDir, err)
		return 1
	}
	defer func() { _ = embeddedMQ.Close() }()

	// Only register system metric gauges when a real MeterProvider is in
	// place — otherwise `otel.GetMeterProvider()` returns the no-op SDK
	// provider and RegisterCallback silently no-ops, making this look
	// authoritative when it's actually doing nothing.
	if cfg.OTel.Enabled || cfg.Prometheus.Enabled {
		if err := observability.RegisterSystemMetrics(embeddedMQ.GetServer(), dedup); err != nil {
			logger.Error("failed to register system metrics", "error", err)
		}
	}

	// DLQ stream.
	if cfg.DLQ.Enabled {
		if err := api.EnsureDLQStream(ctx, embeddedMQ.JetStream(), maxBytes/10); err != nil {
			logger.Error("dlq stream init", "error", err)
			return 1
		}
	}

	// L1 cache only in standalone mode.
	l1, err := cache.NewLocal(cfg.Cache.L1MaxCost)
	if err != nil {
		logger.Error("cache init", "error", err)
		return 1
	}
	// TODO: eventually this is where we can switch between ristretto, redis, tiered (both), etc
	cache := l1
	defer func() { _ = cache.Close() }()

	// Policy store (NATS KV + optional file bootstrap).
	policyStore, err := policy.NewStore(ctx, embeddedMQ.JetStream(), cfg.Policy.FilePath, logger)
	if err != nil {
		logger.Error("policy store init", "error", err)
		return 1
	}

	// Pipes store (NATS KV + optional SQL file directory).
	pipesStore, err := pipes.NewStore(ctx, embeddedMQ.JetStream(), cfg.Pipes.Dir, logger)
	if err != nil {
		logger.Error("pipes store init", "error", err)
		return 1
	}

	// Active sweeper — purges messages that are both written to CH and
	// older than the SSE gap window. Runs every minute.
	gapWindow := time.Duration(cfg.MQ.GapWindowMinutes) * time.Minute
	sweeper := ingest.NewSweeper(embeddedMQ.JetStream(), gapWindow, logger)

	// Streaming fan-out: one SSE metric set shared by the Hub (drop counts) and the
	// handler (write counts), and the Hub that projects/serializes each event once
	// per (topic, role) and pushes it to that role's subscribers.
	sseMetrics := stream.NewMetrics()
	streamHub := stream.NewHub(policyStore, registry, sseMetrics)

	// Start policy watch for cluster-wide updates.
	go policyStore.Watch(ctx)

	// Start batch consumer → ClickHouse.
	ingestCleanup, err := ingest.StartIngestWorker(
		ctx,
		embeddedMQ.NatsConn(),
		cache,
		cfg.ClickHouse.Addr,
		cfg.ClickHouse.HTTPPort, // Uses 8123 by default
		cfg.ClickHouse.HTTPScheme,
		cfg.ClickHouse.Username,
		cfg.ClickHouse.Password,
		cfg.ClickHouse.Database,
	)
	if err != nil {
		logger.Error("ingest worker init", "error", err)
		return 1
	}
	// Start active sweeper.
	go sweeper.Start(ctx)

	// Hub bridge: MQ → broadcast to connected SSE clients. The Hub decodes and
	// projects each event itself (skipping malformed payloads), so the bridge just
	// forwards the raw bytes and acks.
	if err := embeddedMQ.Subscribe(ctx, "ingest.>", "hub-bridge", func(msg *mq.Message) error {
		streamHub.Broadcast(msg.Subject, msg.Data)
		if err := msg.Ack(); err != nil {
			slog.Warn("failed to ack message from embedded hub bridge", "error", err)
		}
		return nil
	}); err != nil {
		logger.Error("hub bridge start", "error", err)
		return 1
	}

	// Build handlers.
	js := embeddedMQ.JetStream()
	ingestHandler := api.NewIngestHandler(registry, embeddedMQ, logger)
	ingestHandler.PolicyStore = policyStore
	if dedup != nil {
		ingestHandler.Dedup = dedup
		ingestHandler.IDField = cfg.Dedupe.IDField
		ingestHandler.RequireID = cfg.Dedupe.RequireID
	}

	var dlqHandler *api.DLQHandler
	if cfg.DLQ.Enabled {
		dlqHandler = api.NewDLQHandler(js, logger)
	}

	// TODO: is this really the best/right way to do this?
	// /v1/ops/query proxies straight to ClickHouse over HTTP — no native
	// driver involvement. Construct the base URL from the same fields the
	// ingest worker uses, defaulting the scheme to http if blank.
	queryHost, _, err := net.SplitHostPort(cfg.ClickHouse.Addr)
	if err != nil {
		queryHost = cfg.ClickHouse.Addr
	}
	queryScheme := cfg.ClickHouse.HTTPScheme
	if queryScheme == "" {
		queryScheme = "http"
	}
	queryEndpoint := fmt.Sprintf("%s://%s", queryScheme, net.JoinHostPort(queryHost, cfg.ClickHouse.HTTPPort))
	queryHandler := api.NewQueryHandler(queryEndpoint, cfg.ClickHouse.Username, cfg.ClickHouse.Password, cfg.ClickHouse.Database, cfg.ClickHouse.QueryTimeout)

	healthHandler := api.NewHealthHandler(chConn)
	healthHandler.Boot = bootState

	streamHandler := api.NewStreamHandler(streamHub, js)
	streamHandler.Metrics = sseMetrics

	// Shared keepalive wheel: one goroutine nudges idle streams so proxies don't
	// idle-close them. Runs for the process lifetime.
	heartbeater := stream.NewHeartbeater(cfg.Stream.KeepaliveInterval, cfg.Stream.KeepaliveBuckets)
	streamHandler.Heartbeater = heartbeater
	go heartbeater.Run(ctx)

	// Build the auth middleware up front so a misconfigured/unreachable JWKS
	// endpoint fails startup loudly rather than booting into a degraded state.
	authMW, err := auth.Middleware(auth.Config{
		JWTSecret:   cfg.Auth.JWTSecret,
		JWKSURL:     cfg.Auth.JWKSURL,
		RoleClaim:   cfg.Auth.RoleClaim,
		OperatorKey: cfg.Auth.OperatorKey,
	}, policyStore, logger)
	if err != nil {
		logger.Error("auth middleware init", "error", err)
		return 1
	}

	deps := api.Dependencies{
		Ingest:          ingestHandler,
		Query:           queryHandler,
		SSE:             streamHandler,
		Health:          healthHandler,
		Version:         api.NewVersionHandler(Version, GitCommit, BuildTime),
		Schema:          api.NewSchemaHandler(registry),
		DLQ:             dlqHandler,
		Policy:          api.NewPolicyHandler(policyStore),
		Pipes:           api.NewPipesHandler(pipesStore, policyStore, chConn, cache, cfg.ClickHouse.QueryTimeout, logger),
		StructuredQuery: api.NewStructuredQueryHandler(chConn, cache, registry, policyStore, cfg.Cache.TimestampBucketSeconds, cfg.ClickHouse.QueryTimeout, cfg.Query.DefaultMaxRows, logger),
		AuthMW:          authMW,
		PolicyStore:     policyStore,
		Logger:          logger,
		JS:              js,
		CORSOrigins:     cfg.Server.CORSAllowedOrigins,
	}

	// Prometheus /metrics routing: same-port → mount on API router,
	// dedicated port → spin a sidecar HTTP server below.
	promPath := cfg.Prometheus.Path
	promPort := cfg.Prometheus.Port
	var promSrv *http.Server
	if promHandler != nil {
		if promPort == 0 {
			deps.MetricsHandler = promHandler
			deps.MetricsPath = promPath
		} else {
			mux := http.NewServeMux()
			mux.Handle(promPath, promHandler)
			promSrv = &http.Server{
				Addr:              fmt.Sprintf(":%d", promPort),
				Handler:           mux,
				ReadHeaderTimeout: 10 * time.Second,
				// Full Read/Write timeouts are safe here — unlike the main API
				// server (SSE), the Prometheus sidecar serves only
				// single-shot scrape requests, so an unbounded slow client has
				// no legitimate reason to hold a connection.
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
			}
		}
	}

	router := api.NewRouter(deps)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Graceful shutdown.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down")
		shutCtx, shutCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeout)*time.Second)
		defer shutCancel()
		cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			logger.Error("server shutdown error", "error", err)
		}
		if promSrv != nil {
			if err := promSrv.Shutdown(shutCtx); err != nil {
				logger.Error("prometheus server shutdown error", "error", err)
			}
		}
		if err := ingestCleanup(shutCtx); err != nil {
			logger.Error("ingest worker cleanup error", "error", err)
		}
	}()

	if promSrv != nil {
		go func() {
			logger.Info("starting prometheus metrics server", "addr", promSrv.Addr, "path", promPath)
			if err := promSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("prometheus server error", "error", err)
			}
		}()
	}

	logger.Info("starting server", "port", cfg.Server.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", "error", err)
		return 1
	}
	return 0
}
