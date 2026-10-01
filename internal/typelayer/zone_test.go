package typelayer

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// logRecords decodes what logtest.Capture collected.
func logRecords(t *testing.T, buf *logtest.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimRight(buf.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		out = append(out, rec)
	}
	return out
}

// withMsg is the records whose message is msg.
func withMsg(recs []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, r := range recs {
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

// TestBind_SDKZoneRefusalIsThatTenantsUnavailable: a second Engine (its own
// registry) asks for a line this process already opened in UTC, in another
// zone, before its registry has resolved the line. The SDK refuses the open
// itself, with an untyped error; the tenant gets the same zone cause the
// Engine's own check gives, with the SDK's words kept, and the first Engine's
// tenant keeps answering.
func TestBind_SDKZoneRefusalIsThatTenantsUnavailable(t *testing.T) {
	first := TestEngine(t, eventsTable()) // the line is open in UTC from here on

	second, err := NewEngine(Config{})
	require.NoError(t, err)
	t.Cleanup(second.Close)
	second.Bind("tokyo", TestServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})

	u := unavailable(t, second, "tokyo")
	assert.Empty(t, u.Table)
	assert.Contains(t, u.Cause, `"Asia/Tokyo"`)
	assert.Contains(t, u.Cause, `"UTC"`)
	assert.Contains(t, u.Cause, "one timezone per ClickHouse version line")
	assert.Contains(t, u.Cause, "already initialized", "the SDK's own refusal is kept beside ours")

	// The refusal is the tenant's, not the registry's: the same Engine serves a
	// tenant in the line's zone.
	second.Bind(tenant.Default, TestServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, second, tenant.Default)
	answers(t, first, tenant.Default)
}

// TestBind_ZoneRecordIsKeyedOnTheLibrary: the record names the library the
// SDK opened, by path, with the zone it was opened in.
func TestBind_ZoneRecordIsKeyedOnTheLibrary(t *testing.T) {
	eng := TestEngine(t, eventsTable())
	lib := boundLib(eng, tenant.Default)
	require.NotNil(t, lib)

	openedZones.mu.Lock()
	o, ok := openedZones.lib[lib.Path]
	openedZones.mu.Unlock()
	require.True(t, ok, "the bound library %s is recorded", lib.Path)
	assert.Equal(t, openedLib{line: testLine, zone: "UTC"}, o)
}

// TestBind_LogsTheLibraryOncePerTenant: which artifact answers for a tenant's
// server is logged when the tenant's library changes, not on every refresh,
// and a server on another patch of the line is a warning.
func TestBind_LogsTheLibraryOncePerTenant(t *testing.T) {
	eng := TestEngine(t, eventsTable())
	logs := logtest.Capture(t, slog.LevelInfo)

	eng.Bind("exact", TestServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	eng.Bind("exact", TestServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	otherPatch := testLine + ".1.1"
	eng.Bind("other", otherPatch, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "other")

	recs := logRecords(t, logs)
	exact := withMsg(recs, "chtypes library bound")
	require.Len(t, exact, 1, "one record for the tenant's first bind, none for the refresh")
	assert.Equal(t, "exact", exact[0]["tenant"])
	assert.Equal(t, TestServerVersion, exact[0]["server_version"])
	assert.Equal(t, TestServerVersion+"-lts", exact[0]["chtypes_version"])

	other := withMsg(recs, "chtypes library bound from another patch of the server's line; verdicts follow the artifact's patch")
	require.Len(t, other, 1)
	assert.Equal(t, "WARN", other[0]["level"])
	assert.Equal(t, otherPatch, other[0]["server_version"])
}

// sdkWarnedPatches makes each run's patch request a pair the SDK has not
// warned about yet: it warns once per (requested, loaded) pair per process.
var sdkWarnedPatches atomic.Int64

// TestNewEngine_SDKWarningsGoToTheLog: the SDK writes its warning that a
// requested patch is not installed through the registry's Progress writer,
// which NewEngine points at the process log, so nothing reaches stderr raw.
// The Engine itself asks for lines, which never warn; this asks the registry
// for an uninstalled patch directly to make the SDK speak.
func TestNewEngine_SDKWarningsGoToTheLog(t *testing.T) {
	eng := TestEngine(t)
	logs := logtest.Capture(t, slog.LevelInfo)

	patch := fmt.Sprintf("%s.0.%d", testLine, sdkWarnedPatches.Add(1))
	openedZones.mu.Lock()
	chtypes.SetDefaultTimezone("UTC") // the zone TestEngine opened the line in
	res, err := eng.reg.Resolve(chtypes.Version(patch))
	openedZones.mu.Unlock()
	require.NoError(t, err)
	require.False(t, res.Exact)

	warnings := withMsg(logRecords(t, logs), "chtypes SDK warning")
	require.Len(t, warnings, 1)
	assert.Equal(t, "WARN", warnings[0]["level"])
	assert.Contains(t, warnings[0]["message"], patch)
	assert.NotContains(t, warnings[0]["message"], "WARNING:", "the SDK's prefix is the record's level")
}

func TestSDKLog_OneRecordPerLine(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)

	in := []byte("chtypes: WARNING: first\n\n==> narrative\n")
	n, err := sdkLog{}.Write(in)
	require.NoError(t, err)
	assert.Equal(t, len(in), n)

	recs := logRecords(t, logs)
	require.Len(t, recs, 2)
	assert.Equal(t, "WARN", recs[0]["level"])
	assert.Equal(t, "first", recs[0]["message"])
	assert.Equal(t, "INFO", recs[1]["level"])
	assert.Equal(t, "==> narrative", recs[1]["message"])
}

func TestVersionLine(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"26.8.15.10":   "26.8",
		"26.8":         "26.8",
		"v26.8.15.10":  "26.8",
		"25.8.3.1-lts": "25.8",
		"1.2.3.4":      "1.2",
		"26":           "26",
		"":             "",
	} {
		assert.Equal(t, want, versionLine(in), "%q", in)
	}
}

func TestSamePatch(t *testing.T) {
	t.Parallel()
	assert.True(t, samePatch("26.8.15.10-lts", "26.8.15.10"))
	assert.True(t, samePatch("26.8.15.10", "26.8.15.10"))
	assert.False(t, samePatch("26.8.15.10-lts", "26.8.14.3"))
	assert.False(t, samePatch("26.8.15.10-lts", "26.8"))
}
