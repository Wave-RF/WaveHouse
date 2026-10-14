package app

import (
	"context"
	"errors"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// wireSettingsMetrics exports how this process's settings reloads have gone
// (#776). A file of its own so wire.go only reports each hook's outcome
// (settings.Registry.Reconciled). Every series is per process and none
// carries a tenant (#538): which tenant was rejected is in the reload's log
// line.
func (a *App) wireSettingsMetrics() {
	if !a.cfg.OTel.Enabled && !a.cfg.Prometheus.Enabled {
		return
	}
	reg, err := registerSettingsMetrics(otel.Meter("wavehouse-settings"), a.tenants.Stats)
	if err != nil {
		slog.Error("failed to register settings metrics", "error", err)
		return
	}
	a.add(component{name: "settings metrics", close: withoutContext(reg.Unregister)})
}

// registerSettingsMetrics registers the settings gauges and counters on
// meter, read from stats on every collection.
func registerSettingsMetrics(meter metric.Meter, stats func() settings.ReloadStats) (metric.Registration, error) {
	var errs [7]error
	var (
		reloads, failures, removed       metric.Int64ObservableCounter
		tenants, successful, lastSuccess metric.Int64ObservableGauge
	)
	reloads, errs[0] = meter.Int64ObservableCounter("wavehouse_settings_reloads_total",
		metric.WithDescription("Settings reloads by trigger (boot, sighup, watch, api) and result: adopted (every tenant adopted and reconciled), partial (a tenant's folder rejected, or a component that failed to reconcile), rejected (nothing adopted)"))
	failures, errs[1] = meter.Int64ObservableCounter("wavehouse_settings_reconcile_failures_total",
		metric.WithDescription("Components that failed to reconcile with adopted settings, by component (clickhouse, dedupe, mq); the next reload retries"))
	removed, errs[2] = meter.Int64ObservableCounter("wavehouse_settings_tenants_removed_total",
		metric.WithDescription("Tenants a reload stopped serving because their folder was gone"))
	tenants, errs[3] = meter.Int64ObservableGauge("wavehouse_settings_tenants",
		metric.WithDescription("Tenants by state: served, or rejected (their folder failed its last validation; their requests answer 503)"))
	successful, errs[4] = meter.Int64ObservableGauge("wavehouse_settings_last_reload_successful",
		metric.WithDescription("1 when the last settings reload adopted every tenant and every component reconciled with it, else 0"))
	lastSuccess, errs[5] = meter.Int64ObservableGauge("wavehouse_settings_last_reload_success_timestamp_seconds",
		metric.WithDescription("Unix time of the last settings reload that adopted every tenant and every component reconciled with; 0 if none has"))
	var reg metric.Registration
	reg, errs[6] = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := stats()
		for k, n := range st.Reloads {
			o.ObserveInt64(reloads, n, metric.WithAttributes(attribute.String("trigger", k.Trigger), attribute.String("result", k.Result)))
		}
		for c, n := range st.ReconcileFailures {
			o.ObserveInt64(failures, n, metric.WithAttributes(attribute.String("component", c)))
		}
		o.ObserveInt64(removed, st.Removed)
		o.ObserveInt64(tenants, int64(st.Served), metric.WithAttributes(attribute.String("state", "served")))
		o.ObserveInt64(tenants, int64(st.Rejected), metric.WithAttributes(attribute.String("state", "rejected")))
		var ok, at int64
		if st.LastSuccessful {
			ok = 1
		}
		if !st.LastSuccess.IsZero() {
			at = st.LastSuccess.Unix()
		}
		o.ObserveInt64(successful, ok)
		o.ObserveInt64(lastSuccess, at)
		return nil
	}, reloads, failures, removed, tenants, successful, lastSuccess)
	if err := errors.Join(errs[:]...); err != nil {
		return nil, err
	}
	return reg, nil
}
