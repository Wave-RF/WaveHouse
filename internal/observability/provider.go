package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
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
	// already registered. The setup error itself flows back to the caller via
	// the return value; the shutdown error is diagnostic-only — surface it on
	// stdout so partial-init failures are debuggable in production rather than
	// silently swallowed.
	handleErr := func(inErr error) {
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

		if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(15 * time.Second)); err != nil {
			handleErr(err)
			return shutdown, nil, err
		}
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
