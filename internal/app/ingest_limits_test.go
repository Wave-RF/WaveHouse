package app

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// seedIngestLimits is what the seed's config.json adopts as.
var seedIngestLimits = ingest.Limits{MaxBatchBytes: 262144, MaxBatchWait: 5 * time.Second, InsertTimeout: 30 * time.Second, RetryBackoffMax: 30 * time.Second}

// A batch's wait plus its insert deadline must stay under mq.ack_wait, or
// the queue redelivers the rows of a batch still inserting. The settings
// directory cannot see boot config, so the ingest process checks it: at
// boot over a flat directory a violation refuses to start, naming both
// values; the boundary is strict.
func TestNew_IngestLimitsOverAckWaitRefuseBoot(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t, writeSettings(t, nil))
	cfg.MQ.AckWait = 35 * time.Second // the seed's 5s + 30s, not under
	_, err := New(t.Context(), Options{Config: cfg})
	require.ErrorContains(t, err, "ingest limits: tenant 0: ingest.max_batch_wait_ms 5000 plus clickhouse.insert_timeout 30 is not under mq.ack_wait 35s")

	cfg.MQ.AckWait = 36 * time.Second
	a := newApp(t, cfg, Options{})
	lim, ok := a.ingestLimits.For(tenant.Default)
	require.True(t, ok)
	assert.Equal(t, seedIngestLimits, lim)
}

// Over a nested directory a tenant whose values fail the rule at boot is
// served with none — the registry's rule for the shape, as the queue budget
// and the dedupe stores follow it — logged and counted, so the worker hands
// its rows back to the queue (its tests), and every other tenant boots with
// its limits; the reload that fixes the folder adopts them.
func TestNew_NestedIngestLimitsRefusedServeTheTenantWithNone(t *testing.T) {
	t.Parallel()
	root := writeNestedSettings(t, map[string]map[string]any{
		"acme":   nil,
		"globex": {"ingest": map[string]any{"max_batch_bytes": 1024, "max_batch_wait_ms": 30_000, "retry_backoff_max": 30}}, // 30s + 30s, not under 60s
	})
	a := newApp(t, testConfig(t, root), Options{})
	got, ok := a.ingestLimits.For("acme")
	require.True(t, ok, "the other tenant boots with its limits")
	assert.Equal(t, seedIngestLimits, got)
	_, ok = a.ingestLimits.For("globex")
	assert.False(t, ok, "refused at boot with none to keep")
	stats := a.tenants.Stats()
	assert.Equal(t, int64(1), stats.ReconcileFailures["ingest"])
	assert.False(t, stats.LastSuccessful, "the boot adoption is partial")

	addTenantFolder(t, root, "globex", nil)
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	got, ok = a.ingestLimits.For("globex")
	require.True(t, ok, "the reload that fixes the folder adopts its limits")
	assert.Equal(t, seedIngestLimits, got)
	assert.True(t, a.tenants.Stats().LastSuccessful)
}

// On a reload, a tenant whose new values fail the rule keeps the ones it
// had: the refusal is logged and counted (the reload is partial), and the
// next reload with values that fit applies them.
func TestReload_IngestLimitsRefusedKeepThePrevious(t *testing.T) {
	t.Parallel()
	dir := writeSettings(t, nil)
	a := newApp(t, testConfig(t, dir), Options{})
	lim, ok := a.ingestLimits.For(tenant.Default)
	require.True(t, ok)
	require.Equal(t, seedIngestLimits, lim)

	rewriteSettings(t, dir, map[string]any{"ingest": map[string]any{"max_batch_bytes": 1024, "max_batch_wait_ms": 30_000, "retry_backoff_max": 30}}) // 30s + 30s, not under 60s
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted, "the folder itself validates")
	got, ok := a.ingestLimits.For(tenant.Default)
	require.True(t, ok)
	assert.Equal(t, seedIngestLimits, got, "the previous values stay in force")
	stats := a.tenants.Stats()
	assert.Equal(t, int64(1), stats.ReconcileFailures["ingest"])
	assert.False(t, stats.LastSuccessful, "the reload is partial")

	rewriteSettings(t, dir, map[string]any{"ingest": map[string]any{"max_batch_bytes": 1024, "max_batch_wait_ms": 20_000, "retry_backoff_max": 30}})
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	got, ok = a.ingestLimits.For(tenant.Default)
	require.True(t, ok)
	assert.Equal(t, ingest.Limits{MaxBatchBytes: 1024, MaxBatchWait: 20 * time.Second, InsertTimeout: 30 * time.Second, RetryBackoffMax: 30 * time.Second}, got, "the next reload applies values that fit")
	assert.True(t, a.tenants.Stats().LastSuccessful)
}

// A tenant a reload adopts with values that fail the rule, and none to
// keep, has no limits (the worker hands its rows back), and a tenant no longer
// served has none either; the next reload that fixes the folder adopts it.
func TestReload_IngestLimitsNewTenantRefusedHasNone(t *testing.T) {
	t.Parallel()
	root := writeNestedSettings(t, map[string]map[string]any{"acme": nil})
	a := newApp(t, testConfig(t, root), Options{})
	_, ok := a.ingestLimits.For("acme")
	require.True(t, ok)
	_, ok = a.ingestLimits.For("globex")
	assert.False(t, ok, "not served")

	addTenantFolder(t, root, "globex", map[string]any{"ingest": map[string]any{"max_batch_bytes": 1024, "max_batch_wait_ms": 30_000, "retry_backoff_max": 30}})
	_, adopted := a.tenants.Reload("test")
	require.True(t, adopted)
	_, ok = a.ingestLimits.For("globex")
	assert.False(t, ok, "refused with none to keep")
	_, ok = a.ingestLimits.For("acme")
	assert.True(t, ok, "the other tenant is untouched")

	addTenantFolder(t, root, "globex", nil)
	_, adopted = a.tenants.Reload("test")
	require.True(t, adopted)
	got, ok := a.ingestLimits.For("globex")
	require.True(t, ok)
	assert.Equal(t, seedIngestLimits, got)
}

// A served tenant looked up before this collection's hook has run — the
// window between a reload adopting the tenant and its AfterAdopt hooks —
// reconciles on the spot: it is never read as a tenant with none, which
// would hand its first rows back to the queue, and one whose values fail the
// rule is still read as none.
func TestIngestLimits_ForReconcilesAServedTenantOnMiss(t *testing.T) {
	t.Parallel()
	reg, findings := settings.Open(writeSettings(t, nil))
	require.NotNil(t, reg, "findings: %v", findings)
	l := newIngestLimits(reg, time.Minute)
	got, ok := l.For(tenant.Default)
	require.True(t, ok, "reconciled on the miss")
	assert.Equal(t, seedIngestLimits, got)
	_, ok = l.For("globex")
	assert.False(t, ok, "not served")

	refused, findings := settings.Open(writeSettings(t, map[string]any{"ingest": map[string]any{"max_batch_bytes": 1024, "max_batch_wait_ms": 30_000, "retry_backoff_max": 30}}))
	require.NotNil(t, refused, "findings: %v", findings)
	_, ok = newIngestLimits(refused, time.Minute).For(tenant.Default)
	assert.False(t, ok, "values that fail the rule, with none to keep")
}

// The rule itself, at its boundary; against a negative wait or deadline,
// which only a Limits built outside the validator carries (a negative insert
// deadline would leave the INSERT unbounded); and against two values that
// each fit a duration but sum past one, as validated values can (a wrapped
// sum would read as under the ack wait): none may pass.
func TestCheckIngestLimits(t *testing.T) {
	t.Parallel()
	lim := ingest.Limits{MaxBatchWait: 5 * time.Second, InsertTimeout: 30 * time.Second}
	require.NoError(t, checkIngestLimits(lim, 36*time.Second))
	assert.ErrorContains(t, checkIngestLimits(lim, 35*time.Second), "is not under mq.ack_wait 35s")
	assert.ErrorContains(t, checkIngestLimits(lim, time.Minute/2), "not under mq.ack_wait 30s")

	huge := int64(30_000_000_000) // 30s written in nanoseconds, a count the validator refuses in a file: its conversion wraps
	wrapped := time.Duration(huge) * time.Second
	require.Negative(t, wrapped, "the case under test")
	assert.Error(t, checkIngestLimits(ingest.Limits{MaxBatchWait: 5 * time.Second, InsertTimeout: wrapped}, time.Minute), "a wrapped insert deadline")
	assert.Error(t, checkIngestLimits(ingest.Limits{MaxBatchWait: wrapped, InsertTimeout: 30 * time.Second}, time.Minute), "a wrapped wait")
	// Each fits a duration, as validated values do; their sum wraps.
	big := time.Duration(1<<62) + time.Second
	assert.Error(t, checkIngestLimits(ingest.Limits{MaxBatchWait: big, InsertTimeout: big}, time.Minute), "a wrapped sum")
	assert.Error(t, checkIngestLimits(ingest.Limits{MaxBatchWait: 0, InsertTimeout: 30 * time.Second}, time.Minute), "a zero wait")
}

// addTenantFolder writes (or replaces) one tenant folder of a nested root,
// as writeNestedSettings builds them: the seed with patch applied.
func addTenantFolder(t *testing.T, root, folder string, patch map[string]any) {
	t.Helper()
	dir := filepath.Join(root, folder)
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.Rename(writeSettings(t, patch), dir))
}
