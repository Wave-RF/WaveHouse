package typelayer

import (
	"context"
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

// answerWith makes e's opens answer every request with the library from's
// registry opens for line, whatever line was asked for: what chtypes 1.0.2
// does when the cache holds a higher line than the one requested
// (Wave-RF/chtypes#481).
func answerWith(e, from *Engine, line string) {
	e.open = func(ctx context.Context, _ string) (*chtypes.Library, error) {
		return from.reg.ForContext(ctx, line)
	}
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

// TestBind_WrongLineLibraryIsRefused: a library chtypes returns for another
// line than the one requested (Wave-RF/chtypes#481) is never served, and is
// not remembered for the line; the tenants on the line it was built for
// still answer.
func TestBind_WrongLineLibraryIsRefused(t *testing.T) {
	served := testEngine(t)
	lib := boundLib(served, tenant.Default)
	eng, err := NewEngine(Config{AutoFetch: true, Bases: unpublishedBase(t)})
	require.NoError(t, err)
	t.Cleanup(eng.Close)
	answerWith(eng, served, testLine)

	eng.Bind("lower", "26.3.38.2", "UTC", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, eng, "lower")
	assert.Empty(t, u.Table, "the cause covers every table of the tenant")
	assert.Contains(t, u.Cause, "a request for ClickHouse 26.3 with its "+lib.Version+" library (built for "+testLine+")")
	assert.Contains(t, u.Cause, "Wave-RF/chtypes#481")
	_, remembered := eng.libs.Load("26.3")
	assert.False(t, remembered, "a refused library is not remembered for the line")

	eng.Bind("current", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "current")
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
