package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// withParseTimeout is the settings patch of an ingest block that is the
// seed's with ingest.parse_timeout set to seconds (left out when 0).
func withParseTimeout(seconds int) map[string]any {
	block := map[string]any{"max_batch_bytes": 262144, "max_batch_wait_ms": 5000, "retry_backoff_max": 30}
	if seconds > 0 {
		block["parse_timeout"] = seconds
	}
	return map[string]any{"ingest": block}
}

// parseTimeoutOf is the limit the API applies to tenant id's requests.
func parseTimeoutOf(t *testing.T, a *App, id tenant.ID) time.Duration {
	t.Helper()
	store, ok := a.tenants.For(id)
	require.True(t, ok, "tenant %s is served", id)
	return a.parseTimeouts.For(store)
}

// A tenant's ingest.parse_timeout within boot config's ceiling is adopted,
// above the boot default as well as below it; with none, the boot default
// applies.
func TestNew_ParseTimeoutWithinTheCeilingIsAdopted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		seconds int
		want    time.Duration
	}{
		{"absent: the boot default", 0, 30 * time.Second},
		{"below the boot default", 10, 10 * time.Second},
		{"above the boot default", 60, time.Minute},
		{"at the ceiling", 300, 5 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := newApp(t, testConfig(t, writeSettings(t, withParseTimeout(tt.seconds))), Options{})
			assert.Equal(t, tt.want, parseTimeoutOf(t, a, tenant.Default))
			assert.True(t, a.tenants.Stats().LastSuccessful)
		})
	}
}

// pkg/settings cannot see boot config, so the API process checks the
// ceiling: over a flat directory a tenant above it refuses to start, naming
// both values; a Config built without config.Load, with no ingest block,
// refuses too.
func TestNew_ParseTimeoutOverTheCeilingRefusesBoot(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t, writeSettings(t, withParseTimeout(120)))
	cfg.Ingest.MaxParseTimeout = time.Minute
	_, err := newForTest(t.Context(), t, Options{Config: cfg})
	require.ErrorContains(t, err, "ingest parse timeout: tenant 0: ingest.parse_timeout 120 is over the boot config's ingest.max_parse_timeout 1m0s")

	cfg = testConfig(t, writeSettings(t, nil))
	cfg.Ingest = config.Ingest{}
	_, err = newForTest(t.Context(), t, Options{Config: cfg})
	require.ErrorContains(t, err, "ingest.parse_timeout or ingest.max_parse_timeout is zero")
}

// Over a nested directory a tenant above the ceiling at boot is served with
// the boot default, logged and counted, and every other tenant boots with its
// own; the reload that fixes the folder adopts its value.
func TestNew_NestedParseTimeoutOverTheCeilingServesTheDefault(t *testing.T) {
	t.Parallel()
	root := writeNestedSettings(t, map[string]map[string]any{
		"acme":   withParseTimeout(60),
		"globex": withParseTimeout(600),
	})
	a := newApp(t, testConfig(t, root), Options{})
	assert.Equal(t, time.Minute, parseTimeoutOf(t, a, "acme"), "the other tenant boots with its own")
	assert.Equal(t, 30*time.Second, parseTimeoutOf(t, a, "globex"), "refused at boot with none to keep: the default")
	stats := a.tenants.Stats()
	assert.Equal(t, int64(1), stats.ReconcileFailures["parse_timeout"])
	assert.False(t, stats.LastSuccessful, "the boot adoption is partial")

	addTenantFolder(t, root, "globex", withParseTimeout(120))
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Equal(t, 2*time.Minute, parseTimeoutOf(t, a, "globex"))
	assert.True(t, a.tenants.Stats().LastSuccessful)
}

// On a reload a tenant whose new value is over the ceiling keeps the limit it
// had, logged and counted (the reload is partial); the next reload with a
// value that fits applies it, and one that removes the key returns the tenant
// to the boot default.
func TestReload_ParseTimeoutOverTheCeilingKeepsThePrevious(t *testing.T) {
	t.Parallel()
	dir := writeSettings(t, withParseTimeout(60))
	a := newApp(t, testConfig(t, dir), Options{})
	require.Equal(t, time.Minute, parseTimeoutOf(t, a, tenant.Default))

	rewriteSettings(t, dir, withParseTimeout(600))
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted, "the folder itself validates")
	assert.Equal(t, time.Minute, parseTimeoutOf(t, a, tenant.Default), "the previous limit stays in force")
	stats := a.tenants.Stats()
	assert.Equal(t, int64(1), stats.ReconcileFailures["parse_timeout"])
	assert.False(t, stats.LastSuccessful, "the reload is partial")

	rewriteSettings(t, dir, withParseTimeout(120))
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Equal(t, 2*time.Minute, parseTimeoutOf(t, a, tenant.Default), "the next reload applies a value that fits")
	assert.True(t, a.tenants.Stats().LastSuccessful)

	rewriteSettings(t, dir, withParseTimeout(0))
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	assert.Equal(t, 30*time.Second, parseTimeoutOf(t, a, tenant.Default), "no key: the boot default")
}

// A tenant looked up before the hook has run — a reload adopted it and its
// AfterAdopt hooks have not run yet — gets what reconcile would give it with
// nothing to keep: its own value within the ceiling, else the default.
func TestParseTimeouts_ForBeforeTheHook(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		seconds int
		want    time.Duration
	}{
		{"its own", 60, time.Minute},
		{"none", 0, 30 * time.Second},
		{"over the ceiling", 600, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reg, findings := settings.Open(writeSettings(t, withParseTimeout(tt.seconds)))
			require.NotNil(t, reg, "findings: %v", findings)
			store, ok := reg.For(tenant.Default)
			require.True(t, ok)
			p := newParseTimeouts(reg, config.Ingest{ParseTimeout: 30 * time.Second, MaxParseTimeout: 5 * time.Minute})
			assert.Equal(t, tt.want, p.For(store))
		})
	}
}
