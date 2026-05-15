package observability

import (
	"context"
	"crypto/tls"
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
	"google.golang.org/grpc/credentials"
)

// runtimeStartOnce gates the `runtime.Start` call in InitProvider so that
// re-init (in tests, or any future hot-reload path) doesn't accumulate
// goroutines the upstream package exposes no way to stop. See the comment at
// the Do() call site for the trade-off.
var runtimeStartOnce sync.Once

// ProviderConfig wires the metrics/traces/logs pipeline. Endpoint is the
// default OTLP gRPC target; per-signal {Traces,Metrics,Logs}Endpoint values
// override it (empty inherits). MetricsEnabled and PrometheusEnabled are
// independent — either, both, or neither may be set.
type ProviderConfig struct {
	Endpoint          string
	Headers           map[string]string
	TracesEndpoint    string
	TracesEnabled     bool
	TracesSampleRate  float64
	MetricsEndpoint   string
	MetricsEnabled    bool
	PrometheusEnabled bool
	LogsEndpoint      string
	LogsEnabled       bool
	tlsConfig         *tls.Config
}

// SetTLSConfigForTesting injects a client *tls.Config for `https://`
// endpoints. Test-only — FakeOTLPTLS uses it to trust its self-signed cert.
// Production keeps tlsConfig nil (system roots).
func (c *ProviderConfig) SetTLSConfigForTesting(cfg *tls.Config) {
	c.tlsConfig = cfg
}

// pickEndpoint returns override if set, otherwise fallback.
func pickEndpoint(override, fallback string) string {
	if override != "" {
		return override
	}
	return fallback
}

// InitProvider sets up the OpenTelemetry pipeline, registering only the
// signals enabled in cfg. Always installs the W3C TraceContext + Baggage
// propagator. Returns a non-nil shutdown function and a Prometheus HTTP
// handler (non-nil iff PrometheusEnabled; reads from a private registry).
func InitProvider(ctx context.Context, serviceName string, cfg ProviderConfig) (func(context.Context) error, http.Handler, error) {
	// Snapshot globals so a partial-init failure can roll them back — main.go
	// continues on init error, and shut-down providers as global state are
	// worse than the no-op defaults.
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

	// On setup error, release whatever was already registered and restore
	// prior globals. Shutdown errors are diagnostic-only — log them rather
	// than swallowing.
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
		host, useTLS := ParseEndpoint(pickEndpoint(cfg.TracesEndpoint, cfg.Endpoint))
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(host)}
		if useTLS {
			opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsConfigOrDefault(cfg.tlsConfig))))
		} else {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
		}
		traceExporter, err := otlptracegrpc.New(ctx, opts...)
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
			host, useTLS := ParseEndpoint(pickEndpoint(cfg.MetricsEndpoint, cfg.Endpoint))
			opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(host)}
			if useTLS {
				opts = append(opts, otlpmetricgrpc.WithTLSCredentials(credentials.NewTLS(tlsConfigOrDefault(cfg.tlsConfig))))
			} else {
				opts = append(opts, otlpmetricgrpc.WithInsecure())
			}
			if len(cfg.Headers) > 0 {
				opts = append(opts, otlpmetricgrpc.WithHeaders(cfg.Headers))
			}
			metricExporter, err := otlpmetricgrpc.New(ctx, opts...)
			if err != nil {
				handleErr(err)
				return shutdown, nil, err
			}
			readers = append(readers, metric.NewPeriodicReader(metricExporter, metric.WithInterval(15*time.Second)))
		}

		if cfg.PrometheusEnabled {
			// Private Registry keeps WaveHouse metrics out of
			// prometheus.DefaultRegisterer (which the client library's
			// process/Go collectors auto-register into).
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

		// runtime.Start spawns goroutines with no upstream Stop(); sync.Once
		// caps the leak across test re-inits. Trade-off: runtime callbacks
		// stay bound to the FIRST MeterProvider, so subsequent re-inits get
		// no runtime metrics on their new provider. Errors here are
		// non-fatal — a runtime-instrumentation failure shouldn't tear down
		// a fully-initialized OTel pipeline.
		runtimeStartOnce.Do(func() {
			if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(15 * time.Second)); err != nil {
				slog.Warn("OTel runtime instrumentation failed to start; continuing with degraded host metrics",
					"err", err)
			}
		})
	}

	if cfg.LogsEnabled {
		host, useTLS := ParseEndpoint(pickEndpoint(cfg.LogsEndpoint, cfg.Endpoint))
		opts := []otlploggrpc.Option{otlploggrpc.WithEndpoint(host)}
		if useTLS {
			opts = append(opts, otlploggrpc.WithTLSCredentials(credentials.NewTLS(tlsConfigOrDefault(cfg.tlsConfig))))
		} else {
			opts = append(opts, otlploggrpc.WithInsecure())
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlploggrpc.WithHeaders(cfg.Headers))
		}
		logExporter, err := otlploggrpc.New(ctx, opts...)
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
