package typelayer

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/v2/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
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

// zonedTable has a zone-less DateTime and no expression over it.
func zonedTable() *discovery.TableSchema {
	return &discovery.TableSchema{Name: "zoned", Columns: []discovery.Column{
		{Name: "id", Type: "UInt8", Position: 1},
		{Name: "ts", Type: "DateTime", Position: 2},
	}}
}

// computedTable has a zone-less DateTime and DEFAULT and MATERIALIZED
// expressions that read it in the server's zone, plus timezone() itself.
func computedTable() *discovery.TableSchema {
	return &discovery.TableSchema{Name: "computed", Columns: []discovery.Column{
		{Name: "id", Type: "UInt8", Position: 1},
		{Name: "ts", Type: "DateTime", Position: 2},
		{Name: "day", Type: "Date", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "toDate(ts)", Position: 3},
		{Name: "hour", Type: "UInt8", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "toHour(ts)", Position: 4},
		{Name: "tz", Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "timezone()", Position: 5},
		{Name: "minute", Type: "UInt8", HasDefault: true, DefaultKind: "MATERIALIZED", DefaultExpression: "toMinute(ts)", Position: 6},
	}}
}

// tableOf is tenant id's table name, released when the test ends.
func tableOf(t *testing.T, eng *Engine, id tenant.ID, name string) *Table {
	t.Helper()
	tbl, err := eng.Table(id, name)
	require.NoError(t, err, "tenant %s table %s", id, name)
	t.Cleanup(tbl.Release)
	return tbl
}

// roleTableOf is tenant id's role view of table name for shape, released
// when the test ends.
func roleTableOf(t *testing.T, eng *Engine, id tenant.ID, name string, shape RoleShape) *Table {
	t.Helper()
	rt, err := eng.RoleTable(id, name, shape)
	require.NoError(t, err, "tenant %s table %s shape %+v", id, name, shape)
	t.Cleanup(rt.Release)
	return rt
}

// generation is tenant id's table name's generation, released at once.
func generation(t *testing.T, eng *Engine, id tenant.ID, name string) uint64 {
	t.Helper()
	tbl, err := eng.Table(id, name)
	require.NoError(t, err, "tenant %s table %s", id, name)
	defer tbl.Release()
	return tbl.Generation
}

// serverOf is the server tenant id's tables compiled on.
func serverOf(eng *Engine, id tenant.ID) *chtypes.Server {
	eng.mu.RLock()
	set := eng.tenants[id]
	eng.mu.RUnlock()
	set.bindMu.Lock()
	defer set.bindMu.Unlock()
	return set.srv
}

// describedZone is the zone chtypes says tbl's schema binds, from the server
// it compiled on.
func describedZone(t *testing.T, tbl *Table) string {
	t.Helper()
	d, err := tbl.c.schema.Describe()
	require.NoError(t, err)
	require.NotNil(t, d.Server, "the schema compiled on a server")
	return d.Server.Timezone
}

// exported ingests body into tbl and returns each record's exported line.
func exported(t *testing.T, tbl *Table, body string) []string {
	t.Helper()
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(body))
	require.NoError(t, err)
	require.Empty(t, batch.Declined)
	require.Nil(t, batch.Refused)
	out := make([]string, len(batch.Rows))
	for i, r := range batch.Rows {
		require.True(t, r.Accepted, "record %d: %s", i, r.Message)
		out[i] = string(r.Line)
	}
	return out
}

// requireServerClosed asserts srv is closed: chtypes refuses a compile on it.
func requireServerClosed(t *testing.T, lib *chtypes.Library, srv *chtypes.Server) {
	t.Helper()
	require.NotNil(t, srv)
	_, err := lib.CompileTable("CREATE TABLE t (id UInt8) ENGINE = "+neutralEngine, chtypes.OnServer(srv))
	var ue *chtypes.UsageError
	require.ErrorAs(t, err, &ue, "the server is closed")
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

// computedBody is two records into computedTable: a zone-less time, read in
// the server's zone, and an instant, whose day and hour the server's zone
// decides.
const computedBody = `{"id":1,"ts":"2024-01-01 05:00:00"}` + "\n" + `{"id":2,"ts":"2024-01-01T20:00:00Z"}` + "\n"

// computedWant is what each server zone stores for computedBody. The Tokyo
// rows are what TestIntegration_ServerZoneAgreesWithClickHouse measures a
// server in that zone storing; Etc/UTC stores UTC's rows in its own spelling.
var computedWant = map[string][]string{
	"UTC": {
		`[1, "2024-01-01T05:00:00Z", "2024-01-01", 5, "UTC"]`,
		`[2, "2024-01-01T20:00:00Z", "2024-01-01", 20, "UTC"]`,
	},
	"Etc/UTC": {
		`[1, "2024-01-01T05:00:00Z", "2024-01-01", 5, "Etc/UTC"]`,
		`[2, "2024-01-01T20:00:00Z", "2024-01-01", 20, "Etc/UTC"]`,
	},
	"Asia/Tokyo": {
		`[1, "2023-12-31T20:00:00Z", "2024-01-01", 5, "Asia/Tokyo"]`,
		`[2, "2024-01-01T20:00:00Z", "2024-01-02", 5, "Asia/Tokyo"]`,
	},
}

// imageZoneEnv names an image zone for this test binary's process to set up
// before any library loads; TestServer_ImageZoneIsNeverRead starts a process
// with it.
const imageZoneEnv = "WAVEHOUSE_TEST_IMAGE_ZONE"

// TestBind_TenantsInDifferentZonesAreBothServed: tenants on one line in
// different zones share its library and each compiles on a server in its own
// zone, so every table is served for both, a zone-less DateTime beside DEFAULT
// expressions included, and each stores what its own server would. The tenant
// bound first decides nothing for the other, and two spellings of one zone are
// each served as spelled.
func TestBind_TenantsInDifferentZonesAreBothServed(t *testing.T) {
	tables := []*discovery.TableSchema{eventsTable(), zonedTable(), computedTable()}
	eng := testEngine(t)
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", tables)
	eng.Bind("utc", testServerVersion, "UTC", tables)
	eng.Bind("unnamed", testServerVersion, "", tables) // "" is chtypes' own default, UTC
	eng.Bind("etc", testServerVersion, "Etc/UTC", tables)

	for id, zone := range map[tenant.ID]string{"tokyo": "Asia/Tokyo", "utc": "UTC", "unnamed": "UTC", "etc": "Etc/UTC"} {
		assert.Empty(t, eng.TenantCause(id), "tenant %s", id)
		answers(t, eng, id)
		for _, ts := range tables {
			assert.Equal(t, zone, describedZone(t, tableOf(t, eng, id, ts.Name)), "tenant %s table %s", id, ts.Name)
		}
		assert.Equal(t, computedWant[zone], exported(t, tableOf(t, eng, id, "computed"), computedBody), "tenant %s", id)
	}
	assert.Same(t, boundLib(eng, "tokyo"), boundLib(eng, "utc"), "one library per line")
	assert.NotSame(t, serverOf(eng, "tokyo"), serverOf(eng, "utc"), "one server per tenant")

	if zone := os.Getenv(imageZoneEnv); zone != "" {
		// The control: this process's image zone is not UTC, so a schema on
		// no server reads a zone-less time in it.
		s, err := boundLib(eng, "utc").CompileTable("CREATE TABLE t (ts DateTime) ENGINE = "+neutralEngine,
			chtypes.WithSettings(compileSettings))
		require.NoError(t, err)
		defer func() { _ = s.Close() }()
		settings, _ := parseSettings(FormatJSONEachRow, IngestOptions{})
		res, err := s.Rows(FormatJSONEachRow, []byte(`{"ts":"2024-01-01 00:00:00"}`+"\n"),
			chtypes.WithSettings(settings), chtypes.WithExport(chtypes.JSONCompactEachRow))
		require.NoError(t, err)
		loc, err := time.LoadLocation(zone)
		require.NoError(t, err)
		want := time.Date(2024, 1, 1, 0, 0, 0, 0, loc).UTC().Format(`["2006-01-02T15:04:05Z"]`)
		require.Equal(t, want+"\n", string(res.Payload), "the image zone is %s", zone)
	}
}

// TestServer_ImageZoneIsNeverRead runs the two-zone test in a process whose
// image zone is neither tenant's, so a schema that read the image zone
// anywhere would store another value there.
func TestServer_ImageZoneIsNeverRead(t *testing.T) {
	if os.Getenv(imageZoneEnv) != "" {
		t.Skip("already the subprocess")
	}
	testEngine(t) // skips without the artifact
	const test = "TestBind_TenantsInDifferentZonesAreBothServed"
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+test+"$", "-test.v") //nolint:gosec // G204: this test binary, a test name
	cmd.Env = append(os.Environ(), imageZoneEnv+"=America/New_York")
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		t.Fatalf("subprocess failed:\n%s", out)
	}
	require.NoError(t, err)
	require.Contains(t, string(out), "--- PASS: "+test, string(out))
}

// TestBind_DefaultTimezoneIsTheTenantsZone: a DEFAULT timezone() is filled
// with the tenant's server zone, on the table and for a role that may not
// write it, with an insert check attached or not; a record that supplies the
// column keeps its value.
func TestBind_DefaultTimezoneIsTheTenantsZone(t *testing.T) {
	schema := &discovery.TableSchema{Name: "stamped", Columns: []discovery.Column{
		{Name: "id", Type: "UInt8", Position: 1},
		{Name: "tz", Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "timezone()", Position: 2},
	}}
	eng := testEngine(t, schema)
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{schema})
	body := `{"id":1}` + "\n" + `{"id":2,"tz":"mine"}` + "\n"

	for id, zone := range map[tenant.ID]string{tenant.Default: "UTC", "tokyo": "Asia/Tokyo"} {
		func() {
			tbl, err := eng.Table(id, "stamped")
			require.NoError(t, err)
			defer tbl.Release()
			assert.Equal(t, []string{`[1, "` + zone + `"]`, `[2, "mine"]`}, exported(t, tbl, body), "tenant %s", id)
			batch, err := tbl.Ingest(FormatJSONEachRow, []byte(body), Predicate{Column: "tz", Op: "=", Values: []string{zone}})
			require.NoError(t, err)
			assert.Equal(t, []string{"", ReasonFilter}, checkReasons(t, batch), "tenant %s: the check sees the filled zone", id)
		}()
		rt := roleTableOf(t, eng, id, "stamped", RoleShape{Columns: []string{"id"}})
		assert.Equal(t, []string{`[3, "` + zone + `"]`}, exported(t, rt, `{"id":3}`+"\n"), "tenant %s", id)
	}
}

// TestFilter_ServerZoneAnswersRowFiltersAndRanges: a Tokyo tenant's row filter
// over a zone-less DateTime reads its literals in Tokyo, on the stream's block
// path and on the insert check's batch path alike, for an equality and for a
// range (the stream's form of a time_range); the UTC tenant reads the same
// stored instant in UTC.
func TestFilter_ServerZoneAnswersRowFiltersAndRanges(t *testing.T) {
	eng := testEngine(t, zonedTable())
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{zonedTable()})
	tokyo := tableOf(t, eng, "tokyo", "zoned")
	utc := tableOf(t, eng, tenant.Default, "zoned")
	stored := exported(t, tokyo, `{"id":1,"ts":"2024-01-01 09:00:00"}`+"\n")
	require.Equal(t, []string{`[1, "2024-01-01T00:00:00Z"]`}, stored, "a zone-less time reads in Tokyo")
	instant := []byte(`{"id":1,"ts":"2024-01-01T00:00:00Z"}` + "\n")

	at := func(v string) []Predicate { return []Predicate{{Column: "ts", Op: "=", Values: []string{v}}} }
	between := func(from, to string) []Predicate {
		return []Predicate{{Column: "ts", Op: ">", Values: []string{from}}, {Column: "ts", Op: "<", Values: []string{to}}}
	}
	tokyoClock := between("2024-01-01 08:30:00", "2024-01-01 09:30:00")
	utcClock := between("2023-12-31 23:30:00", "2024-01-01 00:30:00")
	for _, c := range []struct {
		name  string
		tbl   *Table
		preds []Predicate
		want  bool
	}{
		{"tokyo, its wall clock", tokyo, at("2024-01-01 09:00:00"), true},
		{"tokyo, UTC's wall clock", tokyo, at("2024-01-01 00:00:00"), false},
		{"tokyo, a range on its wall clock", tokyo, tokyoClock, true},
		{"tokyo, a range on UTC's wall clock", tokyo, utcClock, false},
		{"utc, a range on its wall clock", utc, utcClock, true},
		{"utc, a range on Tokyo's wall clock", utc, tokyoClock, false},
	} {
		row, err := c.tbl.ParseRow(c.tbl.WireColumns, []byte(stored[0]))
		require.NoError(t, err, c.name)
		visible, reason := row.VisibleWithReason(c.preds)
		row.Close()
		assert.Equal(t, c.want, visible, "%s (block): %s", c.name, reason)
		if !c.want {
			assert.Equal(t, ReasonFilter, reason, "%s: answered, not declined", c.name)
		}

		batch, err := c.tbl.Ingest(FormatJSONEachRow, instant, c.preds...)
		require.NoError(t, err)
		want := ReasonFilter
		if c.want {
			want = ""
		}
		assert.Equal(t, []string{want}, checkReasons(t, batch), "%s (batch)", c.name)
	}
}

// TestBind_ZoneChangeBumpsGenerationAndFlushesFilters: a server that changes
// zone is a new server and a new generation, so no filter compiled in the old
// zone answers a row parsed in the new one, a role's view reads in the new
// zone, and the old server is closed.
func TestBind_ZoneChangeBumpsGenerationAndFlushesFilters(t *testing.T) {
	eng := testEngine(t, zonedTable())
	eng.Bind("moving", testServerVersion, "UTC", []*discovery.TableSchema{zonedTable()})
	before := serverOf(eng, "moving")
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
	was, gen := visible()
	assert.False(t, was, "09:00 UTC is not midnight UTC")

	eng.Bind("moving", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{zonedTable()})
	now, next := visible()
	assert.Greater(t, next, gen, "a zone change is a new generation")
	assert.True(t, now, "09:00 in Tokyo is midnight UTC: the filter was compiled afresh on the new server")
	requireServerClosed(t, boundLib(eng, "moving"), before)

	assert.Equal(t, "Asia/Tokyo", describedZone(t, roleTableOf(t, eng, "moving", "zoned", RoleShape{Columns: []string{"id"}})))
}

// TestParseSettings_NeverCarriesSessionTimezone: no call names a zone; the
// server a schema compiled on gives it one. InsertSettings is pinned on the
// real INSERT too, where a zone would move every DateTime the server stores.
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

// TestRoleTable_ReadsInTheTenantsZone: a role's view parses on its tenant's
// schema, so a value injected into a zone-less DateTime is read in the
// tenant's zone, as a record's own field would be, and a denied DateTime
// takes its default there.
func TestRoleTable_ReadsInTheTenantsZone(t *testing.T) {
	schema := &discovery.TableSchema{Name: "zoned", Columns: []discovery.Column{
		{Name: "id", Type: "UInt8", Position: 1},
		{Name: "ts", Type: "DateTime", Position: 2},
		{Name: "tenant", Type: "String", Position: 3},
	}}
	eng := testEngine(t, schema)
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{schema})

	for id, want := range map[tenant.ID]string{
		tenant.Default: `[1, "2024-01-01T09:00:00Z", ""]`,
		"tokyo":        `[1, "2024-01-01T00:00:00Z", ""]`,
	} {
		rt := roleTableOf(t, eng, id, "zoned", RoleShape{Defaults: map[string]string{"ts": "2024-01-01 09:00:00"}})
		assert.Equal(t, []string{want}, exported(t, rt, `{"id":1}`+"\n"), "tenant %s", id)
	}
	rt := roleTableOf(t, eng, "tokyo", "zoned", RoleShape{Columns: []string{"id", "tenant"}})
	assert.Equal(t, "Asia/Tokyo", describedZone(t, rt))
	assert.Equal(t, []string{`[1, "1970-01-01T00:00:00Z", "x"]`}, exported(t, rt, `{"id":1,"tenant":"x"}`+"\n"))
}

// TestBind_UnknownZoneIsThatTenantsUnavailable: a server zone chtypes cannot
// load is refused when the tenant's server is described, for that tenant
// alone; the tenant's earlier tables and server are kept, so a rebind in its
// old zone answers on the same handles, and one in a new zone serves it.
func TestBind_UnknownZoneIsThatTenantsUnavailable(t *testing.T) {
	eng := testEngine(t, eventsTable())
	for _, zone := range []string{"Mars/Olympus_Mons", "Local"} {
		eng.Bind("nowhere", testServerVersion, zone, []*discovery.TableSchema{eventsTable()})
		u := unavailable(t, eng, "nowhere")
		assert.Empty(t, u.Table, "the cause covers every table of the tenant")
		assert.Contains(t, u.Cause, `ClickHouse reports server timezone "`+zone+`", which chtypes cannot serve`)
		assert.Contains(t, u.Cause, "Cannot load time zone "+zone, "chtypes' own refusal")
		answers(t, eng, tenant.Default)
	}
	eng.Bind("nowhere", testServerVersion, "UTC", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "nowhere")

	eng.Bind("mover", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	srv, gen := serverOf(eng, "mover"), generation(t, eng, "mover", "events")
	eng.Bind("mover", testServerVersion, "Mars/Olympus_Mons", []*discovery.TableSchema{eventsTable()})
	assert.Contains(t, unavailable(t, eng, "mover").Cause, `"Mars/Olympus_Mons"`)
	eng.Bind("mover", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "mover")
	assert.Same(t, srv, serverOf(eng, "mover"), "the server was kept")
	assert.Equal(t, gen, generation(t, eng, "mover", "events"), "and the handles")
}

// waitWriterPending polls until a writer is queued on tbl's lock: TryRLock
// fails only then, because the caller holds a read lock.
func waitWriterPending(t *testing.T, tbl *Table) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !tbl.mu.TryRLock() {
			return
		}
		tbl.mu.RUnlock()
		time.Sleep(time.Millisecond)
	}
	t.Error("no writer queued on the held table")
}

// TestForget_ClosesTheTenantsServerAfterItsTables: a forgotten tenant's server
// stays open while a request still holds one of its tables (checked once the
// teardown is queued behind that table), and closes once the teardown has
// closed them all; a later bind compiles on a fresh one.
func TestForget_ClosesTheTenantsServerAfterItsTables(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind("gone", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	srv, lib := serverOf(eng, "gone"), boundLib(eng, "gone")
	held, err := eng.Table("gone", "events")
	require.NoError(t, err)
	released := false
	defer func() {
		if !released {
			held.Release() // a failure below must not leave Close waiting on it
		}
	}()

	eng.Forget("gone")
	_, err = eng.RoleTable("gone", "events", RoleShape{Columns: []string{"id"}})
	require.True(t, IsUnavailable(err), "a forgotten tenant compiles nothing: %v", err)
	waitWriterPending(t, held)
	s, err := lib.CompileTable("CREATE TABLE t (id UInt8) ENGINE = "+neutralEngine, chtypes.OnServer(srv))
	require.NoError(t, err, "the server is open while a table of it is held")
	_ = s.Close()
	batch, err := held.Ingest(FormatJSONEachRow, []byte(eventsRecord))
	require.NoError(t, err)
	assert.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)

	held.Release()
	released = true
	eng.retiring.Wait()
	requireServerClosed(t, lib, srv)

	eng.Bind("gone", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	answers(t, eng, "gone")
	assert.NotSame(t, srv, serverOf(eng, "gone"))
}

// TestClose_ClosesEveryServer: shutdown closes each tenant's server.
func TestClose_ClosesEveryServer(t *testing.T) {
	eng := testEngine(t, eventsTable())
	eng.Bind("tokyo", testServerVersion, "Asia/Tokyo", []*discovery.TableSchema{eventsTable()})
	lib := boundLib(eng, "tokyo")
	servers := []*chtypes.Server{serverOf(eng, tenant.Default), serverOf(eng, "tokyo")}
	eng.Close()
	for _, srv := range servers {
		requireServerClosed(t, lib, srv)
	}
}

// TestServer_SchemaOutlivesItsServerHandle pins the SDK property the teardown
// order does not depend on but would fail safe on: a schema holds its own
// reference to its server, so closing the server handle first leaves the
// schema answering in the server's zone.
func TestServer_SchemaOutlivesItsServerHandle(t *testing.T) {
	eng := testEngine(t)
	lib := boundLib(eng, tenant.Default)
	srv, cause := newServer(lib, "Asia/Tokyo")
	require.Empty(t, cause)
	c, cause := compileColumns(lib, srv, declsOf(computedTable().Columns))
	require.Empty(t, cause)
	defer c.close()
	require.NoError(t, srv.Close())

	tbl := &Table{Name: "computed", c: c, WireColumns: wireColumns(c.desc)}
	assert.Equal(t, computedWant["Asia/Tokyo"], exported(t, tbl, computedBody))
}
