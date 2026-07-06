package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// runtimeStartOnce gates the `runtime.Start` call in InitProvider so that
// re-init (in tests, or any future hot-reload path) doesn't accumulate
// goroutines the upstream package exposes no way to stop. See the comment at
// the Do() call site for the trade-off.
var runtimeStartOnce sync.Once

// ProviderConfig wires the metrics/traces/logs pipeline. Each output is
// independently gated; SampleRate values must be in [0.0, 1.0] and only apply
// to head-based trace sampling. Log sampling is enforced inside NewLogger
// (per-level).
//
// MetricsEnabled drives the OTLP-push metric exporter; PrometheusEnabled
// drives the Prometheus exposition reader. Either, both, or neither may be
// set — the underlying OTel MeterProvider is the shared substrate. When
// PrometheusEnabled is true InitProvider returns a non-nil promHandler.
//
// The OTLP exporters take no endpoint/TLS/header options here: the
// OpenTelemetry SDK reads those from the standard OTEL_EXPORTER_OTLP_* env vars
// — endpoint (with `https://` selecting TLS via system root CAs), a custom CA
// via _CERTIFICATE, mutual TLS via _CLIENT_CERTIFICATE/_CLIENT_KEY, and auth
// _HEADERS. A malformed header is logged and skipped by the SDK (fail-soft),
// not fatal.
type ProviderConfig struct {
	TracesEnabled     bool
	TracesSampleRate  float64
	MetricsEnabled    bool
	PrometheusEnabled bool
	LogsEnabled       bool
}

// InitProvider sets up the OpenTelemetry pipeline, registering only the
// signals enabled in cfg. Always installs the W3C TraceContext + Baggage
// propagator (cheap, harmless when traces are off — in-process span
// extraction still works against a no-op tracer provider).
//
// Returns a shutdown function (always non-nil) and a Prometheus HTTP handler
// (non-nil iff PrometheusEnabled). The handler reads from a private
// prometheus.Registry — global registry pollution is avoided.
func InitProvider(ctx context.Context, serviceName string, cfg ProviderConfig) (func(context.Context) error, http.Handler, error) {
	// Snapshot the OTel globals on entry. On a partial init failure we want to
	// roll them back to whatever was installed before — otherwise the caller
	// (which continues on init error per main.go) keeps using shut-down
	// providers as the global state, strictly worse than the no-op defaults.
	prevProp := otel.GetTextMapPropagator()
	prevTP := otel.GetTracerProvider()
	prevMP := otel.GetMeterProvider()
	prevLP := global.GetLoggerProvider()

	var shutdownFuncs []func(context.Context) error

	shutdown := func(ctx context.Context) error {
		// Nothing registered (or a repeat call after the funcs were cleared)
		// is a genuine no-op — return before the deadline machinery so it
		// can't misreport an already-expired ctx as a shutdown failure.
		if len(shutdownFuncs) == 0 {
			return nil
		}

		errs := make([]error, len(shutdownFuncs))
		var wg sync.WaitGroup
		for i, fn := range shutdownFuncs {
			wg.Go(func() {
				errs[i] = fn(ctx)
			})
		}
		shutdownFuncs = nil

		// Fan the providers out concurrently AND bound the whole thing by
		// ctx: with an unreachable collector, traces and metrics honor the
		// deadline but the experimental logs SDK's BatchProcessor.Shutdown
		// (sdk/log v0.20.0) does not — while an export is mid-flight in gRPC
		// backoff it blocks for the exporter's full ~10s timeout, ignoring
		// ctx. Waiting on all of them (wg.Wait) would drag the whole shutdown
		// out to that ~10s. Every provider already got the same deadline, so
		// once it passes we return ctx.Err() instead of blocking process exit
		// on a flush that (collector down) can't succeed. A straggler
		// goroutine is reaped by the imminent os.Exit; reading errs on that
		// path would race it, so we only touch errs once done is closed (all
		// goroutines returned).
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			return errors.Join(errs...)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// On any setup error we run partial shutdown to release whatever was
	// already registered AND restore the prior globals so the caller falls back
	// to clean state. The setup error itself flows back via the return value;
	// the shutdown error is diagnostic-only — surface it on stdout so
	// partial-init failures are debuggable in production rather than silently
	// swallowed.
	handleErr := func(inErr error) {
		otel.SetTextMapPropagator(prevProp)
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
		global.SetLoggerProvider(prevLP)
		if shutErr := shutdown(ctx); shutErr != nil {
			slog.Warn("observability shutdown error during init cleanup",
				"shutdown_err", shutErr, "cause", inErr)
		}
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
	)
	if err != nil {
		handleErr(err)
		return shutdown, nil, err
	}

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	if cfg.TracesEnabled {
		traceExporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			handleErr(err)
			return shutdown, nil, err
		}

		tracerProvider := trace.NewTracerProvider(
			trace.WithBatcher(traceExporter, trace.WithBatchTimeout(time.Second*5)),
			trace.WithResource(res),
			trace.WithSampler(trace.TraceIDRatioBased(cfg.TracesSampleRate)),
		)
		shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
		otel.SetTracerProvider(tracerProvider)
	}

	var promHandler http.Handler
	if cfg.MetricsEnabled || cfg.PrometheusEnabled {
		readers := []metric.Reader{}

		if cfg.MetricsEnabled {
			metricExporter, err := otlpmetricgrpc.New(ctx)
			if err != nil {
				handleErr(err)
				return shutdown, nil, err
			}
			readers = append(readers, metric.NewPeriodicReader(metricExporter, metric.WithInterval(15*time.Second)))
		}

		if cfg.PrometheusEnabled {
			// Private Registry — keeps WaveHouse metrics out of the global
			// prometheus.DefaultRegisterer (which the prometheus client
			// library's process/Go collectors auto-register into). Tests
			// and embedded use cases would otherwise see those default
			// metrics leak into the /metrics output.
			reg := prometheus.NewRegistry()
			promExporter, err := otelprom.New(otelprom.WithRegisterer(reg))
			if err != nil {
				handleErr(err)
				return shutdown, nil, err
			}
			readers = append(readers, promExporter)
			promHandler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
		}

		mpOpts := []metric.Option{metric.WithResource(res)}
		for _, r := range readers {
			mpOpts = append(mpOpts, metric.WithReader(r))
		}
		meterProvider := metric.NewMeterProvider(mpOpts...)
		shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
		otel.SetMeterProvider(meterProvider)

		// `runtime.Start` spawns goroutines that the upstream package exposes
		// no way to stop. In production InitProvider is called exactly once,
		// but tests re-init the provider repeatedly — sync.Once caps the leak
		// at one goroutine for the whole process. Trade-off: the runtime
		// callbacks stay bound to the FIRST MeterProvider, so subsequent
		// re-inits get no runtime metrics on their new MeterProvider. No test
		// asserts on runtime metric presence and production never re-inits,
		// so this is acceptable until upstream adds a Stop().
		//
		// Errors here are intentionally non-fatal: a runtime-instrumentation
		// failure shouldn't tear down a fully-initialized OTel pipeline. Log
		// and continue with degraded host metrics rather than routing through
		// handleErr (which would roll back the globals).
		runtimeStartOnce.Do(func() {
			if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(15 * time.Second)); err != nil {
				slog.Warn("OTel runtime instrumentation failed to start; continuing with degraded host metrics",
					"err", err)
			}
		})
	}

	if cfg.LogsEnabled {
		// Endpoint, TLS, and headers come from the SDK's OTEL_EXPORTER_OTLP_*
		// env vars, same as traces/metrics. Known gap: the pinned otlploggrpc
		// (v0.19) ignores the env TLS-cert vars, so a custom/private CA and
		// mutual TLS do not apply to the logs signal (public-CA TLS and
		// plaintext still work). Upstream bug, not worked around here:
		// open-telemetry/opentelemetry-go#6661.
		logExporter, err := otlploggrpc.New(ctx)
		if err != nil {
			handleErr(err)
			return shutdown, nil, err
		}

		loggerProvider := log.NewLoggerProvider(
			log.WithProcessor(log.NewBatchProcessor(logExporter)),
			log.WithResource(res),
		)
		shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
		global.SetLoggerProvider(loggerProvider)
	}

	return shutdown, promHandler, nil
}
