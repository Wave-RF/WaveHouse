package app

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/settings"
)

func TestRegisterSettingsMetrics(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	at := time.Unix(1_700_000_000, 0)
	st := settings.ReloadStats{
		Served: 3, Rejected: 1, Removed: 2,
		Reloads: map[settings.ReloadKey]int64{
			{Trigger: "boot", Result: settings.ResultAdopted}:   1,
			{Trigger: "api", Result: settings.ResultPartial}:    4,
			{Trigger: "watch", Result: settings.ResultRejected}: 5,
		},
		ReconcileFailures: map[string]int64{"clickhouse": 6},
		LastSuccess:       at,
	}
	reg, err := registerSettingsMetrics(mp.Meter("test"), func() settings.ReloadStats { return st })
	require.NoError(t, err)

	got := collectSettingsMetrics(t, reader)
	assert.Equal(t, map[string]int64{
		"result=adopted,trigger=boot":   1,
		"result=partial,trigger=api":    4,
		"result=rejected,trigger=watch": 5,
	}, got["wavehouse_settings_reloads_total"])
	assert.Equal(t, map[string]int64{"component=clickhouse": 6}, got["wavehouse_settings_reconcile_failures_total"])
	assert.Equal(t, map[string]int64{"": 2}, got["wavehouse_settings_tenants_removed_total"])
	assert.Equal(t, map[string]int64{"state=served": 3, "state=rejected": 1}, got["wavehouse_settings_tenants"])
	assert.Equal(t, map[string]int64{"": 0}, got["wavehouse_settings_last_reload_successful"])
	assert.Equal(t, map[string]int64{"": at.Unix()}, got["wavehouse_settings_last_reload_success_timestamp_seconds"])

	st = settings.ReloadStats{LastSuccessful: true}
	got = collectSettingsMetrics(t, reader)
	assert.Equal(t, map[string]int64{"": 1}, got["wavehouse_settings_last_reload_successful"])
	assert.Equal(t, map[string]int64{"": 0}, got["wavehouse_settings_last_reload_success_timestamp_seconds"], "no success yet reads 0")

	require.NoError(t, reg.Unregister())
	assert.Empty(t, collectSettingsMetrics(t, reader), "Close unregisters the callback")
}

// collectSettingsMetrics reads every int64 series reader holds: metric name → its
// attributes, rendered "k=v,…" in key order → value.
func collectSettingsMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[string]map[string]int64{}
	add := func(name string, attrs attribute.Set, v int64) {
		if out[name] == nil {
			out[name] = map[string]int64{}
		}
		out[name][attrs.Encoded(attribute.DefaultEncoder())] = v
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					add(m.Name, p.Attributes, p.Value)
				}
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					add(m.Name, p.Attributes, p.Value)
				}
			}
		}
	}
	return out
}
