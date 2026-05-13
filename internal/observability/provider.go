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
// Endpoint is the OTLP gRPC target. It is dialed only by the OTLP exporters
// (traces / metrics-OTLP / logs); Prometheus-only operation leaves it unused.
type ProviderConfig struct {
	Endpoint          string
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
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
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
		traceExporter, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
			otlptracegrpc.WithInsecure(),
		)
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
			metricExporter, err := otlpmetricgrpc.New(ctx,
				otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
				otlpmetricgrpc.WithInsecure(),
			)
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
		logExporter, err := otlploggrpc.New(ctx,
			otlploggrpc.WithEndpoint(cfg.Endpoint),
			otlploggrpc.WithInsecure(),
		)
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
