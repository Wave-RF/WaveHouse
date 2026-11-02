package observability

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// TenantLimit is how many distinct tenants a process's metrics label (#891).
// Each labeled tenant is a series per label set on every tenant-tied metric,
// so past it the rest are reported as one, TenantOverflow, and the process
// logs once that the label can be turned off.
const TenantLimit = 100

// TenantOverflow is the tenant label value of every tenant past TenantLimit,
// as #908's D1 decided. tenant.Parse accepts it as an id, so a tenant named
// _other shares the bucket's series; no settings directory needs one.
const TenantOverflow = "_other"

// tenantLabels hands out tenant label values: a tenant's own id for the first
// limit distinct tenants it is asked about, TenantOverflow for any after. A
// tenant keeps the value it was first given for the life of the process, so
// its series never move between the two, and one that is removed keeps its
// place: its series simply stop growing.
type tenantLabels struct {
	limit  int
	mu     sync.RWMutex
	seen   map[string]struct{}
	warned bool
	// off is set while no provider exports the label: before InitProvider
	// builds one (and for good when metrics are off), and under
	// per_tenant_metrics: false. The value is then never exported, so
	// nothing is capped or warned about.
	off atomic.Bool
}

var processTenants = newProcessTenants()

// newProcessTenants is the process's labels, off until InitProvider builds a
// meter provider: the default config exports no metrics and never does.
func newProcessTenants() *tenantLabels {
	l := newTenantLabels(TenantLimit)
	l.off.Store(true)
	return l
}

func newTenantLabels(limit int) *tenantLabels {
	return &tenantLabels{limit: limit, seen: make(map[string]struct{})}
}

func (l *tenantLabels) value(id string) string {
	if l.off.Load() {
		return id
	}
	// Every tenant-labeled measurement asks, and once the cap fills and the
	// warning is out nothing changes, so the answer then takes the read lock.
	l.mu.RLock()
	_, ok := l.seen[id]
	settled := l.warned
	l.mu.RUnlock()
	if ok {
		return id
	}
	if settled {
		return TenantOverflow
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[id]; ok {
		return id
	}
	if len(l.seen) < l.limit {
		l.seen[id] = struct{}{}
		return id
	}
	if !l.warned {
		l.warned = true
		slog.Warn("metrics carry a tenant label for more distinct tenants than the limit; the rest are reported as tenant=\""+TenantOverflow+"\". Set prometheus.per_tenant_metrics: false (WH_METRICS_PER_TENANT=false) to report every tenant as one series",
			"limit", l.limit)
	}
	return TenantOverflow
}

// TenantLabel is the tenant attribute for a measurement about tenant id,
// capped at TenantLimit tenants per process. Each process fills its own cap,
// so a tenant can be labeled on one replica and TenantOverflow on another.
func TenantLabel(id string) attribute.KeyValue {
	return TenantAttr.String(processTenants.value(id))
}

type metricTenantKey struct{}

// WithMetricTenant returns ctx naming the tenant the measurements recorded
// under it are about, for work that is not a request (the ingest worker, a
// schema refresh). A request's is its served tenant: see SetRequestServed.
func WithMetricTenant(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, metricTenantKey{}, id)
}

// SetRequestServed marks the request's tenant (SetRequestTenant's) as one
// the process serves. Only then do the request's measurements carry it: a
// request for a tenant id the process does not hold is the client's to make
// up, and would fill the cap with ids nobody serves.
func SetRequestServed(ctx context.Context) {
	if s := scopeFrom(ctx); s != nil {
		s.served.Store(true)
	}
}

// metricTenant is the tenant measurements under ctx are about, if any:
// WithMetricTenant's, else the request's once it is served.
func metricTenant(ctx context.Context) (string, bool) {
	if id, ok := ctx.Value(metricTenantKey{}).(string); ok {
		return id, true
	}
	if s := scopeFrom(ctx); s != nil && s.served.Load() {
		if t := s.tenant.Load(); t != nil {
			return *t, true
		}
	}
	return "", false
}

// MetricAttrs is attrs plus, when ctx names a tenant, its TenantLabel: the
// measurement option every tenant-tied metric records with (#891). A
// measurement about no tenant (a probe, a reload of every tenant) carries
// none.
func MetricAttrs(ctx context.Context, attrs ...attribute.KeyValue) metric.MeasurementOption {
	if id, ok := metricTenant(ctx); ok {
		attrs = append(attrs, TenantLabel(id))
	}
	return metric.WithAttributes(attrs...)
}
