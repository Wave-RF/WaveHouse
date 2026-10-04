package typelayer

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strings"
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
	logs := logtest.Capture(t, slog.LevelInfo)

	eng.Bind("exact", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	eng.Bind("exact", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	otherPatch := testLine + ".1.1"
	eng.Bind("other", otherPatch, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "other")

	recs := logRecords(t, logs)
	exact := withMsg(recs, "chtypes library bound")
	require.Len(t, exact, 1, "one record for the tenant's first bind, none for the refresh")
	assert.Equal(t, "exact", exact[0]["tenant"])
	assert.Equal(t, testServerVersion, exact[0]["server_version"])
	assert.Equal(t, testServerVersion, exact[0]["chtypes_version"])

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
	require.Equal(t, testImageZone(), imageZone(), "TestMain commits the image zone")
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

// TestBind_UnknownZoneIsThatTenantsUnavailable: a server zone this host's
// zoneinfo does not know fails every call of that tenant, so the tenant is
// refused at the bind, alone.
func TestBind_UnknownZoneIsThatTenantsUnavailable(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind("nowhere", testServerVersion, "Mars/Olympus_Mons", []*discovery.TableSchema{eventsTable()})

	u := unavailable(t, eng, "nowhere")
	assert.Empty(t, u.Table, "the cause covers every table of the tenant")
	assert.Contains(t, u.Cause, `"Mars/Olympus_Mons"`)
	answers(t, eng, tenant.Default)
}

// imageZoneEnv names the image zone TestMain commits, or "unset" for none; a
// subprocess test sets it, since the image zone is fixed once per process.
const imageZoneEnv = "WAVEHOUSE_TEST_IMAGE_ZONE"

func testImageZone() string {
	if z := os.Getenv(imageZoneEnv); z != "" {
		return z
	}
	return "UTC"
}

// inSubprocess runs test alone in a fresh process of this test binary, with
// imageZoneEnv set to zone, and fails t if it does not pass.
func inSubprocess(t *testing.T, test, zone string) {
	t.Helper()
	if os.Getenv(imageZoneEnv) != "" {
		t.Skip("already the subprocess")
	}
	testEngine(t) // skips without the artifact

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+test+"$", "-test.v") //nolint:gosec // G204: this test binary, a test name
	cmd.Env = append(os.Environ(), imageZoneEnv+"="+zone)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		t.Fatalf("subprocess failed:\n%s", out)
	}
	require.NoError(t, err)
	require.Contains(t, string(out), "--- PASS: "+test, string(out))
}

// TestImage_FirstTenantInAnotherZone re-runs TestImage_ZoneOfTheFirstTenant in
// a process whose image zone is Asia/Tokyo: the first bound tenant's zone, as
// production commits it.
func TestImage_FirstTenantInAnotherZone(t *testing.T) {
	inSubprocess(t, "TestImage_ZoneOfTheFirstTenant", "Asia/Tokyo")
}

// TestImage_UnknownFirstZone re-runs TestImage_UnknownZoneFallsBackToUTC in a
// process that has committed no image zone yet.
func TestImage_UnknownFirstZone(t *testing.T) {
	inSubprocess(t, "TestImage_UnknownZoneFallsBackToUTC", "unset")
}

// TestImage_UnknownZoneFallsBackToUTC runs only in the subprocess: a first
// tenant whose zone this host does not know must not become the image zone,
// which would make chtypes refuse every open for every tenant. The image is
// UTC, that tenant alone is refused, and the next tenant answers.
func TestImage_UnknownZoneFallsBackToUTC(t *testing.T) {
	if os.Getenv(imageZoneEnv) != "unset" {
		t.Skip("runs in the subprocess TestImage_UnknownFirstZone starts")
	}
	require.Empty(t, imageZone())
	eng, err := NewEngine(Config{})
	if err != nil {
		skipWithoutArtifact(t, err.Error())
	}
	t.Cleanup(eng.Close)

	eng.Bind("nowhere", testServerVersion, "Mars/Olympus_Mons", []*discovery.TableSchema{eventsTable()})
	assert.Equal(t, "UTC", imageZone())
	u := unavailable(t, eng, "nowhere")
	assert.Contains(t, u.Cause, `"Mars/Olympus_Mons"`)

	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, tenant.Default)
}

// TestImage_ZoneOfTheFirstTenant runs only in the subprocess: a Tokyo image
// serves a Tokyo tenant exactly, expressions included, and a UTC tenant is now
// the one in another zone.
func TestImage_ZoneOfTheFirstTenant(t *testing.T) {
	if os.Getenv(imageZoneEnv) == "" {
		t.Skip("runs in the subprocess TestImage_FirstTenantInAnotherZone starts")
	}
	require.Equal(t, "Asia/Tokyo", imageZone())
	eng := testEngine(t) // binds tenant.Default in UTC, with no tables
	tables := []*discovery.TableSchema{zonedTable(), eventsTable()}
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", tables)
	eng.Bind("utc", testServerVersion, "UTC", tables)

	answers(t, eng, "tokyo")
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
