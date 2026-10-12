package observability

import (
	"context"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Identity names the process in its telemetry: the resource every span,
// metric series and OTLP log record carries, and the wavehouse_build_info
// series, so replicas of one role and builds in a mixed-version rollout can
// be told apart.
type Identity struct {
	// ServiceName is service.name unless OTEL_SERVICE_NAME overrides it.
	ServiceName string
	Version     string
	GitCommit   string
	BuildTime   string
	// InstanceID is service.instance.id: the boot config's instance_id, the
	// name a lease's holder carries too.
	InstanceID string
	Roles      []string
}

// The resource attributes with no OpenTelemetry semantic convention.
const (
	attrRoles     = attribute.Key("wavehouse.roles")
	attrGitCommit = attribute.Key("wavehouse.git_commit")
	attrBuildTime = attribute.Key("wavehouse.build_time")
)

// BuildInfoMetric is the gauge naming the build and roles each process runs.
const BuildInfoMetric = "wavehouse_build_info"

// roles is the role set as one value, "api,ingest,sweeper", so it reads the
// same as a resource attribute, a metric label and a log field.
func (id Identity) roles() string { return strings.Join(id.Roles, ",") }

// LogAttrs are the fields every log record carries to name the process and
// its build.
func (id Identity) LogAttrs() []any {
	return []any{
		"version", id.Version,
		"build_time", id.BuildTime,
		"git_commit", id.GitCommit,
		"instance_id", id.InstanceID,
	}
}

// newResource builds the resource from id, then lets OTEL_RESOURCE_ATTRIBUTES
// and OTEL_SERVICE_NAME override it key by key, the standard OpenTelemetry
// precedence. A malformed OTEL_RESOURCE_ATTRIBUTES is never fatal, like a
// malformed OTEL_EXPORTER_OTLP_HEADERS: the entries it could read apply, and
// the rest is logged.
func newResource(ctx context.Context, id Identity) *resource.Resource {
	attrs := []attribute.KeyValue{semconv.ServiceNameKey.String(id.ServiceName)}
	for _, kv := range []attribute.KeyValue{
		semconv.ServiceVersionKey.String(id.Version),
		semconv.ServiceInstanceIDKey.String(id.InstanceID),
		attrRoles.String(id.roles()),
		attrGitCommit.String(id.GitCommit),
		attrBuildTime.String(id.BuildTime),
	} {
		if kv.Value.AsString() != "" {
			attrs = append(attrs, kv)
		}
	}
	res, err := resource.New(ctx, resource.WithAttributes(attrs...), resource.WithFromEnv())
	if err != nil {
		slog.Warn("OTEL_RESOURCE_ATTRIBUTES is malformed; the entries that could be read apply", "error", err)
	}
	return res
}

// registerBuildInfo exports wavehouse_build_info, always 1, labeled with the
// build and the roles. The instance is no label: it is service.instance.id on
// the resource, which an OTLP backend maps to the series' instance and the
// Prometheus exporter puts on target_info.
func registerBuildInfo(mp metric.MeterProvider, id Identity) error {
	meter := mp.Meter("wavehouse-build")
	gauge, err := meter.Int64ObservableGauge(BuildInfoMetric,
		metric.WithDescription("Always 1; labeled with the version, commit, build time and roles this process runs"))
	if err != nil {
		return err
	}
	labels := metric.WithAttributes(
		attribute.String("version", id.Version),
		attribute.String("git_commit", id.GitCommit),
		attribute.String("build_time", id.BuildTime),
		attribute.String("roles", id.roles()),
	)
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(gauge, 1, labels)
		return nil
	}, gauge)
	return err
}
