package observability

import (
	"context"
	"errors"
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

// ProviderConfig wires the OTel pipeline. Each signal is independently
// gated; SampleRate values must be in [0.0, 1.0] and only apply to head-based
// trace sampling. Log sampling is enforced inside NewLogger (per-level).
//
// MetricsPrometheusEnabled adds a Prometheus-format Reader to the
// MeterProvider alongside the OTLP push exporter. When true, InitProvider
// returns a non-nil promHandler that the caller mounts on an HTTP server.
type ProviderConfig struct {
	Endpoint                 string
	TracesEnabled            bool
	TracesSampleRate         float64
	MetricsEnabled           bool
	MetricsPrometheusEnabled bool
	LogsEnabled              bool
}

// InitProvider sets up the OpenTelemetry pipeline, registering only the
// signals enabled in cfg. Always installs the W3C TraceContext + Baggage
// propagator (cheap, harmless when traces are off — in-process span
// extraction still works against a no-op tracer provider).
//
// Returns a shutdown function (always non-nil) and a Prometheus HTTP handler
// (non-nil iff MetricsEnabled && MetricsPrometheusEnabled). The handler reads
// from a private prometheus.Registry — global registry pollution is avoided.
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

	handleErr := func(_ error) {
		_ = shutdown(ctx)
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
	if cfg.MetricsEnabled {
		readers := []metric.Reader{}

		metricExporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			handleErr(err)
			return shutdown, nil, err
		}
		readers = append(readers, metric.NewPeriodicReader(metricExporter, metric.WithInterval(15*time.Second)))

		if cfg.MetricsPrometheusEnabled {
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
