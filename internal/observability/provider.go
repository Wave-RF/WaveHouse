package observability

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
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
type ProviderConfig struct {
	Endpoint         string
	TracesEnabled    bool
	TracesSampleRate float64
	MetricsEnabled   bool
	LogsEnabled      bool
}

// InitProvider sets up the OpenTelemetry pipeline, registering only the
// signals enabled in cfg. Always installs the W3C TraceContext + Baggage
// propagator (cheap, harmless when traces are off — in-process span
// extraction still works against a no-op tracer provider).
func InitProvider(ctx context.Context, serviceName string, cfg ProviderConfig) (func(context.Context) error, error) {
	var shutdownFuncs []func(context.Context) error

	shutdown := func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	handleErr := func(inErr error) {
		_ = errors.Join(inErr, shutdown(ctx))
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
	)
	if err != nil {
		handleErr(err)
		return shutdown, err
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
			return shutdown, err
		}

		tracerProvider := trace.NewTracerProvider(
			trace.WithBatcher(traceExporter, trace.WithBatchTimeout(time.Second*5)),
			trace.WithResource(res),
			trace.WithSampler(trace.TraceIDRatioBased(cfg.TracesSampleRate)),
		)
		shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
		otel.SetTracerProvider(tracerProvider)
	}

	if cfg.MetricsEnabled {
		metricExporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			handleErr(err)
			return shutdown, err
		}

		meterProvider := metric.NewMeterProvider(
			metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(15*time.Second))),
			metric.WithResource(res),
		)
		shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
		otel.SetMeterProvider(meterProvider)

		if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(15 * time.Second)); err != nil {
			handleErr(err)
			return shutdown, err
		}
	}

	if cfg.LogsEnabled {
		logExporter, err := otlploggrpc.New(ctx,
			otlploggrpc.WithEndpoint(cfg.Endpoint),
			otlploggrpc.WithInsecure(),
		)
		if err != nil {
			handleErr(err)
			return shutdown, err
		}

		loggerProvider := log.NewLoggerProvider(
			log.WithProcessor(log.NewBatchProcessor(logExporter)),
			log.WithResource(res),
		)
		shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
		global.SetLoggerProvider(loggerProvider)
	}

	return shutdown, nil
}
