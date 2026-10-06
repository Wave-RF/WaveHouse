package typelayer

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
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

// zonedTable has a zone-less DateTime and no expression over it, so every
// tenant serves it whatever its zone.
func zonedTable() *discovery.TableSchema {
	return &discovery.TableSchema{Name: "zoned", Columns: []discovery.Column{
		{Name: "id", Type: "UInt8", Position: 1},
		{Name: "ts", Type: "DateTime", Position: 2},
	}}
}

// zonedTableOf is tenant id's zoned table, released when the test ends.
func zonedTableOf(t *testing.T, eng *Engine, id tenant.ID) *Table {
	t.Helper()
	tbl, err := eng.Table(id, "zoned")
	require.NoError(t, err, "tenant %s", id)
	t.Cleanup(tbl.Release)
	return tbl
}

// TestBind_CorruptArtifactIsThatTenantsUnavailable: a library that fails the
// loader's checks is the SDK's own CHTYPES_ARTIFACT_* refusal, for the tenant
// bound to it alone.
func TestBind_CorruptArtifactIsThatTenantsUnavailable(t *testing.T) {
	first := testEngine(t, eventsTable())
	dir := corruptLayout(t)

	second, err := NewEngine(Config{CacheDir: dir})
	require.NoError(t, err)
	t.Cleanup(second.Close)
	second.Bind("broken", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})

	u := unavailable(t, second, "broken")
	assert.Empty(t, u.Table, "the cause covers every table of the tenant")
	assert.Contains(t, u.Cause, "[CHTYPES_ARTIFACT_")
	answers(t, first, tenant.Default)
}

// TestBind_LogsTheLibraryOncePerTenant: which artifact answers for a tenant's
// server is logged when the tenant's library changes, not on every refresh,
// and a server on another patch of the line is a warning.
func TestBind_LogsTheLibraryOncePerTenant(t *testing.T) {
	eng := testEngine(t, eventsTable())
	// The line's newest installed build, which need not be the pinned patch.
	built := boundLib(eng, tenant.Default).Version
	logs := logtest.Capture(t, slog.LevelInfo)

	eng.Bind("exact", built, "UTC", []*discovery.TableSchema{eventsTable()})
	eng.Bind("exact", built, "UTC", []*discovery.TableSchema{eventsTable()})
	otherPatch := testLine + ".1.1"
	eng.Bind("other", otherPatch, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "other")

	recs := logRecords(t, logs)
	exact := withMsg(recs, "chtypes library bound")
	require.Len(t, exact, 1, "one record for the tenant's first bind, none for the refresh")
	assert.Equal(t, "exact", exact[0]["tenant"])
	assert.Equal(t, built, exact[0]["server_version"])
	assert.Equal(t, built, exact[0]["chtypes_version"])

	other := withMsg(recs, "chtypes library bound from another patch of the server's line; verdicts follow the artifact's patch")
	require.Len(t, other, 1)
	assert.Equal(t, "WARN", other[0]["level"])
	assert.Equal(t, otherPatch, other[0]["server_version"])
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

func TestSessionZone(t *testing.T) {
	t.Parallel()
	require.Equal(t, "UTC", imageZone(), "TestMain commits the image zone")
	assert.Empty(t, sessionZone(imageZone()), "the image zone needs no session zone")
	assert.Equal(t, "Asia/Tokyo", sessionZone("Asia/Tokyo"))
	assert.Equal(t, "Etc/UTC", sessionZone("Etc/UTC"), "zones compare by name, as chtypes does")
}

func TestZonelessDateTime(t *testing.T) {
	t.Parallel()
	for typ, want := range map[string]bool{
		"DateTime":                                    true,
		"DateTime64(3)":                               true,
		"Nullable(DateTime)":                          true,
		"Array(DateTime64(6))":                        true,
		"Map(String, DateTime)":                       true,
		"Tuple(a DateTime('UTC'), b DateTime)":        true,
		"LowCardinality(Nullable(DateTime))":          true,
		"DateTime('UTC')":                             false,
		"DateTime64(3, 'Asia/Tokyo')":                 false,
		"Array(Nullable(DateTime64(3, 'UTC')))":       false,
		"Tuple(a DateTime('UTC'), b DateTime('UTC'))": false,
		"Date":    false,
		"Date32":  false,
		"String":  false,
		"Time":    false,
		"Time64":  false,
		"UInt32":  false,
		"Dynamic": false,
	} {
		assert.Equal(t, want, zonelessDateTime(typ), typ)
	}
}

// TestZoneCause_RefusesOnlyAnExpressionBesideAZonelessDateTime pins the
// narrowed rule: with a session zone, a table is refused only when it pairs a
// zone-less DateTime with an expression the server computes in its own zone.
func TestZoneCause_RefusesOnlyAnExpressionBesideAZonelessDateTime(t *testing.T) {
	t.Parallel()
	ts := colDecl{Name: "ts", Type: "DateTime"}
	tsUTC := colDecl{Name: "ts", Type: "DateTime('UTC')"}
	day := colDecl{Name: "day", Type: "Date", DefaultKind: "DEFAULT", DefaultExpression: "toDate(ts)"}
	n := colDecl{Name: "n", Type: "UInt8"}
	for name, c := range map[string]struct {
		session string
		cols    []colDecl
		refused bool
	}{
		"image zone":                     {"", []colDecl{ts, day}, false},
		"datetime and an expression":     {"Asia/Tokyo", []colDecl{ts, day}, true},
		"datetime alone":                 {"Asia/Tokyo", []colDecl{ts, n}, false},
		"expression over a zoned column": {"Asia/Tokyo", []colDecl{tsUTC, day}, false},
		"materialized":                   {"Asia/Tokyo", []colDecl{ts, {Name: "m", Type: "UInt8", DefaultKind: "MATERIALIZED", DefaultExpression: "1"}}, true},
		"alias":                          {"Asia/Tokyo", []colDecl{ts, {Name: "a", Type: "UInt8", DefaultKind: "ALIAS", DefaultExpression: "n"}}, true},
		"ephemeral expression":           {"Asia/Tokyo", []colDecl{ts, {Name: "e", Type: "UInt8", DefaultKind: "EPHEMERAL", DefaultExpression: "1"}}, true},
		"type default of a denied column": {"Asia/Tokyo", []colDecl{{
			Name: "ts", Type: "DateTime", DefaultKind: "MATERIALIZED",
			DefaultExpression: "defaultValueOfTypeName('DateTime')", origin: exprTypeDefault,
		}}, false},
		"literal into the datetime": {"Asia/Tokyo", []colDecl{
			{Name: "ts", Type: "DateTime", DefaultKind: "DEFAULT", DefaultExpression: "'2024-01-01 00:00:00'", origin: exprLiteral},
		}, true},
		"literal into another column": {"Asia/Tokyo", []colDecl{
			ts, {Name: "s", Type: "String", DefaultKind: "DEFAULT", DefaultExpression: "'acme'", origin: exprLiteral},
		}, false},
	} {
		cause := zoneCause(c.session, c.cols)
		if !c.refused {
			assert.Empty(t, cause, name)
			continue
		}
		assert.Contains(t, cause, `"`+c.session+`"`, name)
		assert.Contains(t, cause, "Wave-RF/chtypes#419", name)
	}
}

// TestBind_TenantInAnotherZoneReadsInItsOwn: a tenant whose server is in
// another zone than the image's is served, its rows read in its own zone, as
// its server reads them; the image-zone tenant reads the same bytes in UTC.
func TestBind_TenantInAnotherZoneReadsInItsOwn(t *testing.T) {
	eng := testEngine(t, zonedTable())
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{zonedTable()})
	record := []byte(`{"id":1,"ts":"2024-01-01 09:00:00"}` + "\n")

	for id, want := range map[tenant.ID]string{
		tenant.Default: `[1, "2024-01-01T09:00:00Z"]`,
		"tokyo":        `[1, "2024-01-01T00:00:00Z"]`,
	} {
		tbl := zonedTableOf(t, eng, id)
		batch, err := tbl.Ingest(FormatJSONEachRow, record)
		require.NoError(t, err)
		require.Len(t, batch.Rows, 1)
		require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
		assert.Equal(t, want, string(batch.Rows[0].Line), "tenant %s", id)
	}
}

// TestFilter_TenantZoneAtCreateAndEval: a tenant in another zone gets definite
// answers from a DateTime predicate on the batch path (insert checks) and the
// block path (row filters), each the server's own: the claim is read in the
// tenant's zone like the row.
func TestFilter_TenantZoneAtCreateAndEval(t *testing.T) {
	eng := testEngine(t, zonedTable())
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{zonedTable()})
	tbl := zonedTableOf(t, eng, "tokyo")
	require.NotEmpty(t, tbl.session)

	at := func(v string) Predicate { return Predicate{Column: "ts", Op: "=", Values: []string{v}} }
	body := []byte(`{"id":1,"ts":"2024-01-01 09:00:00"}` + "\n" + `{"id":2,"ts":"2024-01-01 10:00:00"}` + "\n")
	batch, err := tbl.Ingest(FormatJSONEachRow, body, at("2024-01-01 09:00:00"))
	require.NoError(t, err)
	assert.Equal(t, []string{"", ReasonFilter}, checkReasons(t, batch), "the batch path answers, never declines")

	row, err := tbl.ParseRow(tbl.WireColumns, batch.Rows[0].Line)
	require.NoError(t, err)
	defer row.Close()
	for claim, want := range map[string]bool{
		"2024-01-01 09:00:00": true,  // the tenant's wall clock
		"2024-01-01 00:00:00": false, // UTC's
	} {
		visible, reason := row.VisibleWithReason([]Predicate{at(claim)})
		assert.Equal(t, want, visible, "claim %q: %s", claim, reason)
		if !want {
			assert.Equal(t, ReasonFilter, reason, "claim %q is answered, not declined", claim)
		}
	}
}

// TestTable_FilterAndParseShareOneZone pins the invariant behind zoneOpts:
// chtypes declines a batch whose filter was created in another zone, so the
// filter create and the parse must take their zone from the same place. The
// control proves the pin can fail: a filter compiled without the table's zone
// fails the very same batch.
func TestTable_FilterAndParseShareOneZone(t *testing.T) {
	eng := testEngine(t, zonedTable())
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{zonedTable()})
	tbl := zonedTableOf(t, eng, "tokyo")
	settings, _ := parseSettings(FormatJSONEachRow, IngestOptions{})
	body := []byte(`{"id":3,"ts":"2024-01-01 09:00:00"}` + "\n")
	expr, params, ok := tbl.render([]Predicate{{Column: "id", Op: ">", Values: []string{"2"}}})
	require.True(t, ok)

	ours := tbl.filterFor(expr, params)
	require.NotNil(t, ours)
	res, err := tbl.export(FormatJSONEachRow, body, settings, nil, ours)
	require.NoError(t, err, "a filter from zoneOpts runs on a parse from zoneOpts")
	require.Equal(t, chtypes.Accepted, res.Outcome)

	other, err := tbl.c.schema.CompileFilter(expr, chtypes.WithFilterParams(params))
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })
	_, err = tbl.export(FormatJSONEachRow, body, settings, nil, other)
	var ue *chtypes.UnsupportedError
	require.ErrorAs(t, err, &ue, "a filter in another zone than the batch's is declined whole")
}

// TestBind_ZoneChangeBumpsGenerationAndFlushesFilters: a server that changes
// zone is a new generation, so no filter compiled in the old zone answers a
// row parsed in the new one.
func TestBind_ZoneChangeBumpsGenerationAndFlushesFilters(t *testing.T) {
	eng := testEngine(t, zonedTable())
	eng.Bind("moving", testServerVersion, "UTC", []*discovery.TableSchema{zonedTable()})
	pred := []Predicate{{Column: "ts", Op: "=", Values: []string{"2024-01-01 09:00:00"}}}
	line := []byte(`[1, "2024-01-01T00:00:00Z"]`)

	visible := func() (bool, uint64) {
		tbl, err := eng.Table("moving", "zoned")
		require.NoError(t, err)
		defer tbl.Release()
		row, err := tbl.ParseRow(tbl.WireColumns, line)
		require.NoError(t, err)
		defer row.Close()
		return row.Visible(pred), tbl.Generation
	}
	before, gen := visible()
	assert.False(t, before, "09:00 UTC is not midnight UTC")

	eng.Bind("moving", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{zonedTable()})
	after, next := visible()
	assert.Greater(t, next, gen, "a zone change is a new generation")
	assert.True(t, after, "09:00 in Tokyo is midnight UTC: the filter was compiled afresh in the new zone")
}

// TestParseSettings_NeverCarriesSessionTimezone: the zone rides only on
// zoneOpts. A session_timezone key beside the option is a usage error in
// chtypes, and InsertSettings is pinned on the real INSERT, where a zone would
// move every DateTime the server stores.
func TestParseSettings_NeverCarriesSessionTimezone(t *testing.T) {
	t.Parallel()
	assert.NotContains(t, InsertSettings(), "session_timezone")
	assert.NotContains(t, compileSettings, "session_timezone")
	for _, f := range []Format{FormatJSONEachRow, FormatCSV, FormatTSV, FormatCSVWithNames, FormatTSVWithNames} {
		for _, strict := range []bool{false, true} {
			s, ok := parseSettings(f, IngestOptions{StrictPositional: strict})
			require.True(t, ok)
			assert.NotContains(t, s, "session_timezone", "format %d", f)
		}
	}
}

// TestRoleTable_ZoneRuleCoversTheProjection: a role's injected literal is read
// in the image zone, so on a tenant in another zone a literal into a zone-less
// DateTime is refused, while a literal into any other column, and a denied
// DateTime (its type's default, an instant), still compile.
func TestRoleTable_ZoneRuleCoversTheProjection(t *testing.T) {
	schema := &discovery.TableSchema{Name: "zoned", Columns: []discovery.Column{
		{Name: "id", Type: "UInt8", Position: 1},
		{Name: "ts", Type: "DateTime", Position: 2},
		{Name: "tenant", Type: "String", Position: 3},
	}}
	eng := testEngine(t, schema)
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{schema})

	_, err := eng.RoleTable("tokyo", "zoned", RoleShape{Defaults: map[string]string{"ts": "2024-01-01 00:00:00"}})
	var refused *RoleRefused
	require.ErrorAs(t, err, &refused)
	assert.Contains(t, refused.Cause, "Wave-RF/chtypes#419")

	for _, shape := range []RoleShape{
		{Defaults: map[string]string{"tenant": "acme"}},
		{Columns: []string{"id", "tenant"}},
	} {
		rt, err := eng.RoleTable("tokyo", "zoned", shape)
		require.NoError(t, err, "%+v", shape)
		assert.Equal(t, "Asia/Tokyo", rt.session)
		rt.Release()
	}
	// The same literal on the image-zone tenant is read in its server's zone.
	rt, err := eng.RoleTable(tenant.Default, "zoned", RoleShape{Defaults: map[string]string{"ts": "2024-01-01 00:00:00"}})
	require.NoError(t, err)
	rt.Release()
}

// TestBind_UnknownZoneIsThatTenantsUnavailable: a server zone name WaveHouse
// does not recognise could not serve any call of that tenant, so the tenant is
// refused at the bind, alone.
func TestBind_UnknownZoneIsThatTenantsUnavailable(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind("nowhere", testServerVersion, "Mars/Olympus_Mons", []*discovery.TableSchema{eventsTable()})

	u := unavailable(t, eng, "nowhere")
	assert.Empty(t, u.Table, "the cause covers every table of the tenant")
	assert.Contains(t, u.Cause, `"Mars/Olympus_Mons"`)
	answers(t, eng, tenant.Default)
}

// TestBind_LaterTenantInAZoneChtypesCannotLoadIsRefusedPerParse: chtypes is
// asked about a zone only at the first open, so once the image zone is
// committed, a tenant whose zone Go knows and chtypes cannot load is bound.
// Its zone then rides every call as session_timezone, and chtypes refuses each
// one with ClickHouse code 36: no record is accepted (a declined verdict, a
// 422 at the API) and no row parses (an error that is not Unavailable, so a
// stream withholds it as "error"). An artifact that ignored an unknown
// session_timezone would read the rows in the image zone and fail this test.
func TestBind_LaterTenantInAZoneChtypesCannotLoadIsRefusedPerParse(t *testing.T) {
	eng := testEngine(t, zonedTable())
	require.True(t, knownZone(goOnlyZone), "TestMain's ZONEINFO names the zone")
	require.Equal(t, "UTC", imageZone())
	eng.Bind("goonly", testServerVersion, goOnlyZone, []*discovery.TableSchema{zonedTable()})
	assert.Empty(t, eng.TenantCause("goonly"), "no bind after the first open asks chtypes about the zone")

	record := []byte(`{"id":1,"ts":"2024-01-01 09:00:00"}` + "\n")
	control, err := zonedTableOf(t, eng, tenant.Default).Ingest(FormatJSONEachRow, record)
	require.NoError(t, err)
	require.Len(t, control.Rows, 1)
	require.True(t, control.Rows[0].Accepted, "the image-zone tenant accepts the same record")

	tbl := zonedTableOf(t, eng, "goonly")
	require.Equal(t, goOnlyZone, tbl.session)
	batch, err := tbl.Ingest(FormatJSONEachRow, record)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Rows)
	for i, r := range batch.Rows {
		assert.False(t, r.Accepted, "record %d", i)
		assert.True(t, r.Declined, "record %d: %s", i, r.Message)
	}

	_, err = tbl.ParseRow(tbl.WireColumns, []byte(`[1, "2024-01-01T00:00:00Z"]`))
	require.Error(t, err)
	assert.False(t, IsUnavailable(err), "%v", err)
	ce, ok := chtypes.AsCallError(err)
	require.True(t, ok, "%v", err)
	assert.EqualValues(t, 36, ce.ChCode, "%v", err)
}

// imageZoneEnv=imageUnset starts this test binary with no image zone
// committed, which only a fresh process has.
const (
	imageZoneEnv = "WAVEHOUSE_TEST_IMAGE_ZONE"
	imageUnset   = "unset"
)

// goOnlyZone is a zone Go loads, from the ZONEINFO directory TestMain gives
// every process of this test binary, and chtypes, with its own zone data, does
// not.
const goOnlyZone = "Etc/Go_Only"

// withGoOnlyZone points ZONEINFO at a new directory holding goOnlyZone, and
// returns the function that removes it. Go reads ZONEINFO once, at a process's
// first zone lookup, so TestMain calls this before any test runs. Every other
// name still resolves from Go's own zone data.
func withGoOnlyZone() (cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "zoneinfo")
	if err != nil {
		return nil, err
	}
	cleanup = func() { _ = os.RemoveAll(dir) }
	zone := filepath.Join(dir, goOnlyZone)
	if err = os.MkdirAll(filepath.Dir(zone), 0o750); err == nil {
		err = os.WriteFile(zone, utcTZif(), 0o600)
	}
	if err == nil {
		err = os.Setenv("ZONEINFO", dir)
	}
	if err != nil {
		cleanup()
		return nil, err
	}
	return cleanup, nil
}

// utcTZif is a minimal TZif file (RFC 8536, version 1): one type, UTC.
func utcTZif() []byte {
	b := append([]byte("TZif"), make([]byte, 16)...)
	for _, n := range []uint32{0, 0, 0, 0, 1, 4} { // isut, isstd, leap, time, type, char counts
		b = binary.BigEndian.AppendUint32(b, n)
	}
	return append(b, 0, 0, 0, 0, 0, 0, 'U', 'T', 'C', 0)
}

// TestImage_DerivedFromTheFirstTenantServed runs each image-zone case in a
// process of its own with no image zone committed, so the zone is derived
// through Bind as production derives it, never hand-set.
func TestImage_DerivedFromTheFirstTenantServed(t *testing.T) {
	if os.Getenv(imageZoneEnv) != "" {
		t.Skip("already the subprocess")
	}
	testEngine(t) // skips without the artifact
	for _, test := range []string{
		"TestImageUnset_FirstTenantSetsTheZone",
		"TestImageUnset_MissingArtifactCommitsNothing",
		"TestImageUnset_UnknownZoneCommitsNothing",
		"TestImageUnset_RefusedZoneCommitsNothing",
		"TestImageUnset_FailedArtifactOpenCommitsNothing",
		"TestImageUnset_FailedFetchHoldsItsZone",
		"TestImageUnset_WrongLineCommitsNothing",
		"TestImageUnset_ConcurrentFirstBindsCommitOnce",
	} {
		t.Run(test, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+test+"$", "-test.v") //nolint:gosec // G204: this test binary, a test name
			cmd.Env = append(os.Environ(), imageZoneEnv+"="+imageUnset)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				t.Fatalf("subprocess failed:\n%s", out)
			}
			require.NoError(t, err)
			require.Contains(t, string(out), "--- PASS: "+test, string(out))
		})
	}
}

// unsetImage skips t outside the subprocess, and there returns an engine with
// no tenant bound and no image zone committed.
func unsetImage(t *testing.T) *Engine {
	t.Helper()
	if os.Getenv(imageZoneEnv) != imageUnset {
		t.Skip("runs in a subprocess TestImage_DerivedFromTheFirstTenantServed starts")
	}
	require.Empty(t, imageZone())
	eng, err := NewEngine(Config{})
	if err != nil {
		skipWithoutArtifact(t, err.Error())
	}
	t.Cleanup(eng.Close)
	return eng
}

// TestImageUnset_FirstTenantSetsTheZone: the first tenant served sets the image
// zone, so a Tokyo image serves a Tokyo tenant exactly, expressions included,
// and a UTC tenant is now the one in another zone.
func TestImageUnset_FirstTenantSetsTheZone(t *testing.T) {
	eng := unsetImage(t)
	tables := []*discovery.TableSchema{zonedTable(), eventsTable()}
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", tables)
	require.Equal(t, "Asia/Tokyo", imageZone())
	answers(t, eng, "tokyo")

	eng.Bind("utc", testServerVersion, "UTC", tables)
	u := unavailable(t, eng, "utc")
	assert.Equal(t, "events", u.Table)
	assert.Contains(t, u.Cause, `"UTC"`)
	assert.Contains(t, u.Cause, `"Asia/Tokyo"`)

	record := []byte(`{"id":1,"ts":"2024-01-01 09:00:00"}` + "\n")
	for id, want := range map[tenant.ID]string{
		"tokyo": `[1, "2024-01-01T00:00:00Z"]`,
		"utc":   `[1, "2024-01-01T09:00:00Z"]`,
	} {
		batch, err := zonedTableOf(t, eng, id).Ingest(FormatJSONEachRow, record)
		require.NoError(t, err)
		require.Len(t, batch.Rows, 1)
		require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
		assert.Equal(t, want, string(batch.Rows[0].Line), "tenant %s", id)
	}
}

// TestImageUnset_MissingArtifactCommitsNothing: a first tenant whose line has
// no installed artifact keeps its artifact cause and commits nothing, so the
// next tenant served sets the zone, and its DateTime DEFAULT now() table
// answers.
func TestImageUnset_MissingArtifactCommitsNothing(t *testing.T) {
	eng := unsetImage(t)
	eng.Bind("old", "1.2.3.4", "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, eng, "old")
	assert.Contains(t, u.Cause, "no installed artifact for ClickHouse 1.2")
	assert.Contains(t, u.Cause, string(chtypes.CodeArtifactMissing))
	require.Empty(t, imageZone())

	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{eventsTable(), zonedTable()})
	require.Equal(t, "UTC", imageZone())
	answers(t, eng, tenant.Default)
	zonedTableOf(t, eng, tenant.Default)
}

// TestImageUnset_UnknownZoneCommitsNothing: a first tenant whose zone name
// WaveHouse does not recognise is refused alone and commits nothing, so the
// next tenant served sets its own zone.
func TestImageUnset_UnknownZoneCommitsNothing(t *testing.T) {
	eng := unsetImage(t)
	eng.Bind("nowhere", testServerVersion, "Mars/Olympus_Mons", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, eng, "nowhere")
	assert.Contains(t, u.Cause, `"Mars/Olympus_Mons"`)
	require.Empty(t, imageZone())

	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	require.Equal(t, "Asia/Tokyo", imageZone())
	answers(t, eng, "tokyo")
}

// TestImageUnset_RefusedZoneCommitsNothing: a first tenant in a zone Go knows
// and chtypes cannot load fails its open and commits nothing. chtypes clears
// that setup (Wave-RF/chtypes#458), so a tenant in another zone is served next
// and sets the image zone. A tenant in that zone bound after the commit is
// TestBind_LaterTenantInAZoneChtypesCannotLoadIsRefusedPerParse.
func TestImageUnset_RefusedZoneCommitsNothing(t *testing.T) {
	eng := unsetImage(t)
	require.True(t, knownZone(goOnlyZone), "TestMain's ZONEINFO names the zone")
	eng.Bind("refused", testServerVersion, goOnlyZone, []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, eng, "refused")
	assert.Empty(t, u.Table, "the cause covers every table of the tenant")
	assert.Contains(t, u.Cause, `"`+goOnlyZone+`", and chtypes could not open a library in it`)
	require.Empty(t, imageZone())

	eng.Bind("utc", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable(), zonedTable()})
	require.Equal(t, "UTC", imageZone())
	answers(t, eng, "utc")
	zonedTableOf(t, eng, "utc")
}

// TestImageUnset_FailedArtifactOpenCommitsNothing: a first open whose artifact
// does not load keeps the artifact's own cause and commits nothing. Unlike a
// zone chtypes cannot load, chtypes keeps this open's setup, so a tenant in
// another zone gets Setup's refusal, which names the held zone, and one in
// that zone sets it. This flips when Wave-RF/chtypes#468 ships: chtypes then
// clears the setup after a failed first open, so the UTC tenant is served and
// sets the image zone. Update the test with the chtypes bump that brings it.
func TestImageUnset_FailedArtifactOpenCommitsNothing(t *testing.T) {
	eng := unsetImage(t)
	broken, err := NewEngine(Config{CacheDir: corruptCopy(t)})
	require.NoError(t, err)
	t.Cleanup(broken.Close)
	broken.Bind("broken", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, broken, "broken")
	assert.Contains(t, u.Cause, "[CHTYPES_ARTIFACT_")
	assert.NotContains(t, u.Cause, "server timezone", "an artifact failure is not reported as a zone problem")
	require.Empty(t, imageZone())

	eng.Bind("utc", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	u = unavailable(t, eng, "utc")
	assert.Empty(t, u.Table, "the cause covers every table of the tenant")
	assert.Contains(t, u.Cause, `"UTC", which chtypes refused as this process's image zone`)
	assert.Contains(t, u.Cause, `"Asia/Tokyo"`)
	require.Empty(t, imageZone())

	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	require.Equal(t, "Asia/Tokyo", imageZone())
	answers(t, eng, "tokyo")
}

// TestImageUnset_FailedFetchHoldsItsZone: a first open that has to fetch does
// so after Setup, since chtypes cannot install without opening, so a fetch that
// fails keeps that open's setup as a failed load does: a tenant in another zone
// gets Setup's refusal, and one in the held zone sets it. This flips with
// Wave-RF/chtypes#468 if its fix covers an open that fails before loading.
func TestImageUnset_FailedFetchHoldsItsZone(t *testing.T) {
	eng := unsetImage(t)
	unpublished, err := NewEngine(Config{CacheDir: t.TempDir(), AutoFetch: true, Bases: []string{"file://" + t.TempDir()}})
	require.NoError(t, err)
	t.Cleanup(unpublished.Close)
	if installedCause(unpublished.reg, testLine) == "" {
		t.Skip("a system layout holds the test line, so this host has no cache without it")
	}
	unpublished.Bind("fresh", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, unpublished, "fresh")
	assert.Contains(t, u.Cause, "chtypes could not fetch the artifact for ClickHouse "+testLine)
	assert.Contains(t, u.Cause, string(chtypes.CodeArtifactUnpublished))
	require.Empty(t, imageZone())

	eng.Bind("utc", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	u = unavailable(t, eng, "utc")
	assert.Contains(t, u.Cause, `"UTC", which chtypes refused as this process's image zone`)
	assert.Contains(t, u.Cause, `"Asia/Tokyo"`)

	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	require.Equal(t, "Asia/Tokyo", imageZone())
	answers(t, eng, "tokyo")
}

// TestImageUnset_WrongLineCommitsNothing: a first open that chtypes answers
// with another line's library (Wave-RF/chtypes#481) is refused and commits no
// image zone, though that library's load latched chtypes' setup, so only a
// tenant in its zone is served next.
func TestImageUnset_WrongLineCommitsNothing(t *testing.T) {
	eng := unsetImage(t)
	wrong, err := NewEngine(Config{CacheDir: t.TempDir(), AutoFetch: true, Bases: []string{"file://" + t.TempDir()}})
	require.NoError(t, err)
	t.Cleanup(wrong.Close)
	answerWith(wrong, eng, testLine)
	wrong.Bind("lower", "26.3.38.2", "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	u := unavailable(t, wrong, "lower")
	assert.Contains(t, u.Cause, "a request for ClickHouse 26.3 with its "+testLine+".")
	assert.Contains(t, u.Cause, "Wave-RF/chtypes#481")
	require.Empty(t, imageZone())

	eng.Bind("utc", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	u = unavailable(t, eng, "utc")
	assert.Contains(t, u.Cause, `"UTC", which chtypes refused as this process's image zone`)

	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	require.Equal(t, "Asia/Tokyo", imageZone())
	answers(t, eng, "tokyo")
}

// TestImageUnset_ConcurrentFirstBindsCommitOnce: first binds racing commit the
// image zone exactly once, to a servable tenant's zone, and no servable tenant
// meets a refused setup.
func TestImageUnset_ConcurrentFirstBindsCommitOnce(t *testing.T) {
	eng := unsetImage(t)
	logs := logtest.Capture(t, slog.LevelInfo)
	type bind struct {
		id          tenant.ID
		version, tz string
		servable    bool
	}
	var binds []bind
	for i := range 4 {
		binds = append(binds,
			bind{tenant.ID(fmt.Sprintf("tokyo%d", i)), testServerVersion, "Asia/Tokyo", true},
			bind{tenant.ID(fmt.Sprintf("utc%d", i)), testServerVersion, "UTC", true},
			bind{tenant.ID(fmt.Sprintf("old%d", i)), "1.2.3.4", "Europe/Berlin", false},
			bind{tenant.ID(fmt.Sprintf("nowhere%d", i)), testServerVersion, "Mars/Olympus_Mons", false},
		)
	}
	var wg sync.WaitGroup
	for _, b := range binds {
		wg.Go(func() { eng.Bind(b.id, b.version, b.tz, []*discovery.TableSchema{zonedTable()}) })
	}
	wg.Wait()

	assert.Contains(t, []string{"Asia/Tokyo", "UTC"}, imageZone())
	assert.Len(t, withMsg(logRecords(t, logs), "chtypes image zone committed"), 1)
	for _, b := range binds {
		if b.servable {
			assert.Empty(t, eng.TenantCause(b.id), "tenant %s", b.id)
			zonedTableOf(t, eng, b.id)
		} else {
			assert.NotEmpty(t, eng.TenantCause(b.id), "tenant %s", b.id)
		}
	}
}
