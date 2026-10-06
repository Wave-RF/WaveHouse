package typelayer

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// unpublishedBase is a registry that publishes nothing: every tag is a 404,
// with no network involved.
func unpublishedBase(t *testing.T) []string {
	t.Helper()
	return []string{"file://" + t.TempDir()}
}

// TestNewEngine_AutofetchNeedsNoInstalledArtifact: with autofetch on, boot
// does not wait for an artifact; the line is fetched at its first bind, and a
// fetch that fails is that tenant's Unavailable, naming the fetch.
func TestNewEngine_AutofetchNeedsNoInstalledArtifact(t *testing.T) {
	t.Parallel()
	eng, err := NewEngine(Config{CacheDir: t.TempDir(), AutoFetch: true, Bases: unpublishedBase(t)})
	require.NoError(t, err, "nothing installed is not a boot failure")
	t.Cleanup(eng.Close)
	if installedCause(eng.reg, testLine) == "" {
		t.Skip("a system layout holds the test line, so this host has no cache without it")
	}

	eng.Bind("fresh", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, eng, "fresh")
	assert.Empty(t, u.Table, "the cause covers every table of the tenant")
	assert.Contains(t, u.Cause, "chtypes could not fetch the artifact for ClickHouse "+testLine)
	assert.Contains(t, u.Cause, string(chtypes.CodeArtifactUnpublished))
}

// TestBind_FailedFetchIsThatTenantsUnavailable: a line the registry cannot
// supply leaves the tenants on an installed line answering.
func TestBind_FailedFetchIsThatTenantsUnavailable(t *testing.T) {
	testEngine(t) // installs the test line
	eng, err := NewEngine(Config{AutoFetch: true, Bases: unpublishedBase(t)})
	require.NoError(t, err)
	t.Cleanup(eng.Close)

	eng.Bind("old", "1.2.3.4", "UTC", []*discovery.TableSchema{eventsTable()})
	eng.Bind("current", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, eng, "old")
	assert.Contains(t, u.Cause, "chtypes could not fetch the artifact for ClickHouse 1.2")
	assert.Contains(t, u.Cause, string(chtypes.CodeArtifactUnpublished))
	answers(t, eng, "current")
}

// TestBind_WrongLineCacheNeverServes: a cache holding only the test line never
// answers a tenant on another line, higher or lower, with autofetch on (the
// fetch fails here) or off; the tenants on the cached line still answer.
func TestBind_WrongLineCacheNeverServes(t *testing.T) {
	testEngine(t) // installs the test line
	for _, autofetch := range []bool{false, true} {
		eng, err := NewEngine(Config{AutoFetch: autofetch, Bases: unpublishedBase(t)})
		require.NoError(t, err)
		t.Cleanup(eng.Close)
		if installedCause(eng.reg, "26.3") == "" || installedCause(eng.reg, "99.1") == "" {
			t.Skip("a layout holds a line other than the test line")
		}

		eng.Bind("lower", "26.3.38.2", "UTC", []*discovery.TableSchema{eventsTable()})
		eng.Bind("higher", "99.1.1.1", "UTC", []*discovery.TableSchema{eventsTable()})
		for _, id := range []tenant.ID{"lower", "higher"} {
			u := unavailable(t, eng, id)
			assert.Empty(t, u.Table, "the cause covers every table of the tenant")
			assert.NotContains(t, u.Cause, testLine, "%s autofetch=%v", id, autofetch)
		}
		_, remembered := eng.libs.Load("26.3")
		assert.False(t, remembered)

		eng.Bind("current", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
		answers(t, eng, "current")
	}
}

// TestNewEngine_UnwritableCacheWarns: with autofetch on, a cache the process
// cannot write boots, since an installed line still binds, and says so.
func TestNewEngine_UnwritableCacheWarns(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes a mode-0500 directory")
	}
	dir := filepath.Join(t.TempDir(), "ro")
	require.NoError(t, os.Mkdir(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) }) //nolint:gosec // G302: restoring a test directory for its removal
	logs := logtest.Capture(t, slog.LevelWarn)

	eng, err := NewEngine(Config{CacheDir: dir, AutoFetch: true, Bases: unpublishedBase(t)})
	require.NoError(t, err)
	eng.Close()
	recs := withMsg(logRecords(t, logs), "chtypes cache is not writable: autofetch cannot install a missing line here; mount a writable directory at it")
	require.Len(t, recs, 1)
	assert.Equal(t, dir, recs[0]["cache"])
}

// TestNewEngine_UnreadableDefaultCacheRefuses: the cache $CHTYPES_CACHE names
// is checked as an explicit one is, once it exists: the SDK would skip a
// record it cannot read and serve, or fetch, around it.
func TestNewEngine_UnreadableDefaultCacheRefuses(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 directory")
	}
	dir := t.TempDir()
	entry := filepath.Join(dir, "unpacked", "sha256", "0123")
	require.NoError(t, os.MkdirAll(entry, 0o750))
	require.NoError(t, os.Chmod(entry, 0o000))
	t.Cleanup(func() { _ = os.Chmod(entry, 0o750) }) //nolint:gosec // G302: restoring a test directory for its removal
	t.Setenv("CHTYPES_CACHE", dir)

	_, err := NewEngine(Config{AutoFetch: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot read the artifact directory "+dir)
}
