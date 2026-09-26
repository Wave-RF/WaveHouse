package cache

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Lookup outcomes for the "result" attribute of wavehouse_cache_lookups_total.
const (
	resultHit    = "hit"
	resultMiss   = "miss"   // nothing stored
	resultStale  = "stale"  // stored under versions since bumped
	resultBypass = "bypass" // server skipped: breaker open, not yet connected, or a bump this process owes would orphan the entry
	resultError  = "error"  // server failed or timed out
)

// metrics are a shared-cache backend's instruments. No tenant attribute:
// lookups are the hot path, and tenants are unbounded.
type metrics struct {
	backend      attribute.KeyValue
	lookups      metric.Int64Counter
	duration     metric.Float64Histogram
	invalidation metric.Int64Counter
	valueBytes   metric.Int64Histogram
	oversize     metric.Int64Counter
	setFailures  metric.Int64Counter
	registration metric.Registration
}

// newMetrics builds the instruments on the global meter provider, with
// gauges read from breakerOpen and pending on every collection. Call it
// after observability.InitProvider, like every other instrument.
func newMetrics(backend string, breakerOpen func() bool, pending func() int) (*metrics, error) {
	meter := otel.Meter("wavehouse-cache")
	m := &metrics{backend: attribute.String("backend", backend)}
	var errs [9]error
	m.lookups, errs[0] = meter.Int64Counter("wavehouse_cache_lookups_total",
		metric.WithDescription("Shared-cache lookups by result: hit, miss, stale (stored under since-bumped versions), bypass (server skipped, or held by an invalidation this process has yet to deliver), error"))
	m.duration, errs[1] = meter.Float64Histogram("wavehouse_cache_op_duration_seconds",
		metric.WithDescription("Shared-cache round-trip time by op: lookup, set, invalidate"), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25))
	m.invalidation, errs[2] = meter.Int64Counter("wavehouse_cache_invalidations_total",
		metric.WithDescription("Version-token bumps by result: ok, or deferred to the pending retry set"))
	m.valueBytes, errs[3] = meter.Int64Histogram("wavehouse_cache_value_bytes",
		metric.WithDescription("Size of each value written to the shared cache, after compression"), metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(256, 1<<10, 4<<10, 16<<10, 64<<10, 256<<10, 1<<20, 4<<20))
	m.oversize, errs[4] = meter.Int64Counter("wavehouse_cache_oversize_total",
		metric.WithDescription("Results not cached because they exceed the value size limit"))
	m.setFailures, errs[5] = meter.Int64Counter("wavehouse_cache_set_failures_total",
		metric.WithDescription("Shared-cache writes that failed, by reason: oom, timeout, other"))
	breakerGauge, err := meter.Int64ObservableGauge("wavehouse_cache_breaker_open",
		metric.WithDescription("1 while the shared cache is being bypassed (circuit breaker open, or never connected), else 0"))
	errs[6] = err
	pendingGauge, err := meter.Int64ObservableGauge("wavehouse_cache_invalidations_pending",
		metric.WithDescription("Version-token bumps not yet delivered to the shared cache; entries they would orphan may be served stale meanwhile"))
	errs[7] = err
	m.registration, errs[8] = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var open int64
		if breakerOpen() {
			open = 1
		}
		o.ObserveInt64(breakerGauge, open, metric.WithAttributes(m.backend))
		o.ObserveInt64(pendingGauge, int64(pending()), metric.WithAttributes(m.backend))
		return nil
	}, breakerGauge, pendingGauge)
	if err := errors.Join(errs[:]...); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *metrics) lookup(result string) {
	m.lookups.Add(context.Background(), 1, metric.WithAttributes(m.backend, attribute.String("result", result)))
}

func (m *metrics) op(op string, start time.Time) {
	m.duration.Record(context.Background(), time.Since(start).Seconds(),
		metric.WithAttributes(m.backend, attribute.String("op", op)))
}

func (m *metrics) invalidated(result string, n int) {
	if n > 0 {
		m.invalidation.Add(context.Background(), int64(n), metric.WithAttributes(m.backend, attribute.String("result", result)))
	}
}

func (m *metrics) stored(n int) {
	m.valueBytes.Record(context.Background(), int64(n), metric.WithAttributes(m.backend))
}

func (m *metrics) tooLarge() {
	m.oversize.Add(context.Background(), 1, metric.WithAttributes(m.backend))
}

func (m *metrics) setFailed(reason string) {
	m.setFailures.Add(context.Background(), 1, metric.WithAttributes(m.backend, attribute.String("reason", reason)))
}

func (m *metrics) close() {
	_ = m.registration.Unregister()
}
