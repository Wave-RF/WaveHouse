package typelayer

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

func ingestTable(t *testing.T) *Table {
	t.Helper()
	eng := testEngine(t, eventsTable())
	tbl, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)
	return tbl
}

// TestIngest_PerRecordVerdicts pins the measured behaviour of the whole
// compile profile at once: a bad value is one record's rejection rather than
// the batch's, the codes are ClickHouse's own, and the accepted records come
// back as the bytes the server itself would store.
func TestIngest_PerRecordVerdicts(t *testing.T) {
	tbl := ingestTable(t)

	body := strings.Join([]string{
		`{"id":1,"name":"a","ts":"2026-01-15 10:30:00.000","tags":["x"],"score":null}`,
		`{"id":"bad","name":"b","ts":"2026-01-15 10:30:00.000","tags":[],"score":1}`,
		`{"id":3,"name":"c","ts":"2026-01-15 10:30:00.000","tags":[],"score":null,"extra":1}`,
		`{"id":4,"name":"d","ts":"2026-01-15 10:30:00.000","tags":[],"score":null}`,
	}, "\n") + "\n"

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(body))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 4, "one verdict per input record, index-aligned")

	assert.True(t, batch.Rows[0].Accepted)
	assert.Equal(t, 0, batch.Rows[0].Code)

	// An unparseable value: ClickHouse's own CANNOT_PARSE_TEXT.
	assert.False(t, batch.Rows[1].Accepted)
	assert.False(t, batch.Rows[1].Declined)
	assert.Equal(t, 27, batch.Rows[1].Code)
	assert.NotEmpty(t, batch.Rows[1].Message)

	// input_format_skip_unknown_fields=0 turns an unknown field into a real
	// per-row rejection instead of silent data loss.
	assert.False(t, batch.Rows[2].Accepted)
	assert.Equal(t, 117, batch.Rows[2].Code)
	assert.Contains(t, batch.Rows[2].Message, "extra")

	assert.True(t, batch.Rows[3].Accepted)
}

// TestIngest_AcceptedLineIsOneWireRow: the published line must be exactly one
// JSONCompactEachRow row with no trailing newline, with the MATERIALIZED column
// absent and the volatile DEFAULT already baked in.
func TestIngest_AcceptedLineIsOneWireRow(t *testing.T) {
	tbl := ingestTable(t)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":7,"name":"a","ts":"2026-01-15 10:30:00.000","tags":["x"],"score":null}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)

	line := string(batch.Rows[0].Line)
	assert.NotContains(t, line, "\n")
	assert.True(t, strings.HasPrefix(line, "["), line)
	assert.True(t, strings.HasSuffix(line, "]"), line)
	assert.Equal(t, len(tbl.WireColumns), strings.Count(line, ",")+1, "one cell per wire column: %s", line)
	// now() was substituted at parse time, so the worker inserts the bytes
	// verbatim and the server never re-evaluates the clock.
	assert.NotContains(t, line, "now()")
}

// TestIngest_OverflowIsStoredTruth, not the producer's spelling: 256 into a
// UInt8 wraps to 0, which is what the table will hold and therefore what the
// stream must show (#372's payload-vs-stored asymmetry, closed by construction).
func TestIngest_OverflowIsStoredTruth(t *testing.T) {
	tbl := ingestTable(t)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":256,"name":"a","ts":"2026-01-15 10:30:00.000","tags":[],"score":null}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.True(t, strings.HasPrefix(string(batch.Rows[0].Line), "[0,"), string(batch.Rows[0].Line))
}

// TestIngest_ComputedColumnsAreRejectedPerRecord: a record naming a
// MATERIALIZED or ALIAS column gets ClickHouse's own 117 and the rest of the
// batch still gets verdicts — no WaveHouse-side guard needed. An EPHEMERAL
// column is the one non-stored kind a record may name: its value feeds the
// DEFAULT over it and is never exported.
func TestIngest_ComputedColumnsAreRejectedPerRecord(t *testing.T) {
	schema := &discovery.TableSchema{
		Name: "computed",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "e", Type: "UInt8", DefaultKind: "EPHEMERAL", HasDefault: true, Position: 2},
			{Name: "d", Type: "UInt8", DefaultKind: "DEFAULT", DefaultExpression: "e + 1", HasDefault: true, Position: 3},
			{Name: "a", Type: "UInt8", DefaultKind: "ALIAS", DefaultExpression: "id + 2", HasDefault: true, Position: 4},
			{Name: "m", Type: "UInt32", DefaultKind: "MATERIALIZED", DefaultExpression: "id * 2", HasDefault: true, Position: 5},
		},
	}
	eng := testEngine(t, schema)
	tbl, err := eng.Table(tenant.Default, "computed")
	require.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, []string{"id", "d"}, tbl.WireColumns)

	body := strings.Join([]string{
		`{"id":1}`,
		`{"id":2,"e":5}`,
		`{"id":3,"a":9}`,
		`{"id":4,"d":7}`,
		`{"id":5,"m":1}`,
	}, "\n") + "\n"
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(body))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 5)

	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, "[1, 1]", string(batch.Rows[0].Line), "an absent EPHEMERAL takes its own default")
	require.True(t, batch.Rows[1].Accepted, batch.Rows[1].Message)
	assert.Equal(t, "[2, 6]", string(batch.Rows[1].Line), "the EPHEMERAL value feeds the DEFAULT and is not exported")
	assert.Equal(t, 117, batch.Rows[2].Code)
	assert.Contains(t, batch.Rows[2].Message, "a")
	require.True(t, batch.Rows[3].Accepted, batch.Rows[3].Message)
	// ClickHouse's writer separates cells with ", " — the worker inserts these
	// bytes verbatim, so nothing may re-render them.
	assert.Equal(t, "[4, 7]", string(batch.Rows[3].Line))
	assert.Equal(t, 117, batch.Rows[4].Code)
	assert.Contains(t, batch.Rows[4].Message, "m")
}

// TestIngest_EphemeralInputFollowsTheFormat: the formats that name their
// columns accept an EPHEMERAL one; positional CSV/TSV map their fields to the
// wire columns, so an ephemeral slot is not one of them.
func TestIngest_EphemeralInputFollowsTheFormat(t *testing.T) {
	schema := &discovery.TableSchema{
		Name: "eph",
		Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "ip", Type: "String", DefaultKind: "EPHEMERAL", DefaultExpression: "''", HasDefault: true, Position: 2},
			{Name: "ip_len", Type: "UInt64", DefaultKind: "DEFAULT", DefaultExpression: "length(ip)", HasDefault: true, Position: 3},
		},
	}
	eng := testEngine(t, schema)
	tbl, err := eng.Table(tenant.Default, "eph")
	require.NoError(t, err)
	defer tbl.Release()
	require.Equal(t, []string{"page", "ip_len"}, tbl.WireColumns)

	for _, tc := range []struct {
		name   string
		format Format
		body   string
		want   string
	}{
		{"JSONEachRow", FormatJSONEachRow, `{"page":"/a","ip":"1.2.3.4"}` + "\n", `["/a", 7]`},
		{"CSVWithNames", FormatCSVWithNames, "page,ip\n/a,1.2.3.4\n", `["/a", 7]`},
		{"TSVWithNames", FormatTSVWithNames, "ip\tpage\n1.2.3.4\t/a\n", `["/a", 7]`},
		{"CSV is the wire columns", FormatCSV, "/a,3\n", `["/a", 3]`},
		{"TSV is the wire columns", FormatTSV, "/a\t3\n", `["/a", 3]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := tbl.Ingest(tc.format, []byte(tc.body))
			require.NoError(t, err)
			require.Nil(t, batch.Refused)
			require.Len(t, batch.Rows, 1)
			require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
			assert.Equal(t, tc.want, string(batch.Rows[0].Line))
		})
	}

	// A positional body has no slot for the ephemeral column: a third field is
	// an error, not ip.
	batch, err := tbl.IngestWith(FormatCSV, IngestOptions{StrictPositional: true}, []byte("/a,1.2.3.4,7\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	assert.False(t, batch.Rows[0].Accepted)
}

// TestIngest_EphemeralAServerExpressionReadsIsRefused: the server computes a
// MATERIALIZED column itself from the inserted row, which never carries an
// ephemeral value, so an EPHEMERAL column one reads would be silently ignored
// there — even though a DEFAULT reads it too. It is refused instead (117),
// while another EPHEMERAL column of the same table that only a DEFAULT reads
// is still accepted.
func TestIngest_EphemeralAServerExpressionReadsIsRefused(t *testing.T) {
	schema := &discovery.TableSchema{
		Name: "eph",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "e", Type: "UInt8", DefaultKind: "EPHEMERAL", HasDefault: true, Position: 2},
			{Name: "m", Type: "UInt16", DefaultKind: "MATERIALIZED", DefaultExpression: "e * 2", HasDefault: true, Position: 3},
			{Name: "de", Type: "UInt16", DefaultKind: "DEFAULT", DefaultExpression: "e + 1", HasDefault: true, Position: 4},
			{Name: "f", Type: "UInt8", DefaultKind: "EPHEMERAL", HasDefault: true, Position: 5},
			{Name: "d", Type: "UInt16", DefaultKind: "DEFAULT", DefaultExpression: "f + 1", HasDefault: true, Position: 6},
			{Name: "md", Type: "UInt16", DefaultKind: "MATERIALIZED", DefaultExpression: "d * 2", HasDefault: true, Position: 7},
		},
	}
	eng := testEngine(t, schema)
	tbl, err := eng.Table(tenant.Default, "eph")
	require.NoError(t, err)
	defer tbl.Release()

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"e":5}`+"\n"+`{"id":2,"f":5}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 2)
	assert.Equal(t, 117, batch.Rows[0].Code, "e feeds a MATERIALIZED column the server computes without it")
	assert.Contains(t, batch.Rows[0].Message, "e")
	// md reads d, which travels on the wire already computed from f, so the
	// server's md agrees with this one: f is accepted.
	require.True(t, batch.Rows[1].Accepted, batch.Rows[1].Message)
	assert.Equal(t, "[2, 1, 6]", string(batch.Rows[1].Line))
}

// TestInsertSettings_AreAllSupported: every setting typelayer passes must be one
// chtypes understands. An unsupported one turns a real verdict into a decline,
// which is a silent availability regression on the ingest path.
func TestInsertSettings_AreAllSupported(t *testing.T) {
	tbl := ingestTable(t)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"name":"a","ts":"2026-01-15 10:30:00.000","tags":[],"score":null}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	require.False(t, batch.Rows[0].Declined, "InsertSettings must not contain a setting chtypes rejects: %s", batch.Rows[0].Message)
	assert.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
}

// TestInsertSettings_NullAsDefaultApplies: the worker pins it on the real
// INSERT, so chtypes has to see the same rule — an explicit null in a
// non-nullable column becomes the type's default rather than a rejection.
func TestInsertSettings_NullAsDefaultApplies(t *testing.T) {
	tbl := ingestTable(t)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":null,"name":"a","ts":"2026-01-15 10:30:00.000","tags":[],"score":null}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	assert.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.True(t, strings.HasPrefix(string(batch.Rows[0].Line), "[0,"), string(batch.Rows[0].Line))
}

// TestInsertSettings_BestEffortDateTime: without the worker's own
// date_time_input_format pin, chtypes would reject an RFC3339 value the real
// server accepts — a manufactured over-reject.
func TestInsertSettings_BestEffortDateTime(t *testing.T) {
	tbl := ingestTable(t)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"id":1,"name":"a","ts":"2026-01-15T10:30:00Z","tags":[],"score":null}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	assert.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
}

// TestIngest_DateTimeExportsAsRFC3339UTC: the exported row — what is published
// to the stream and inserted by the worker — spells every DateTime as RFC 3339
// in UTC at the column's scale, whatever the input spelling or the column's
// zone, as the query paths render it; Date and Date32 keep their own form.
func TestIngest_DateTimeExportsAsRFC3339UTC(t *testing.T) {
	eng := testEngine(t, &discovery.TableSchema{Name: "times", Columns: []discovery.Column{
		{Name: "dt", Type: "DateTime", Position: 1},
		{Name: "dt3", Type: "DateTime64(3)", Position: 2},
		{Name: "dt6", Type: "DateTime64(6)", Position: 3},
		{Name: "dtz", Type: "DateTime('Asia/Tokyo')", Position: 4},
		{Name: "dt3z", Type: "DateTime64(3, 'America/New_York')", Position: 5},
		{Name: "d", Type: "Date", Position: 6},
		{Name: "d32", Type: "Date32", Position: 7},
	}})
	tbl, err := eng.Table(tenant.Default, "times")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(`{"dt":"2026-03-24 12:00:00","dt3":"2026-03-24T14:00:00+02:00",`+
		`"dt6":"2026-03-24 12:00:00.123456","dtz":"2026-03-24 21:00:00","dt3z":"2026-03-24 08:00:00.12",`+
		`"d":"2026-03-24","d32":"1960-01-02"}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, `["2026-03-24T12:00:00Z", "2026-03-24T12:00:00.000Z", "2026-03-24T12:00:00.123456Z", `+
		`"2026-03-24T12:00:00Z", "2026-03-24T12:00:00.120Z", "2026-03-24", "1960-01-02"]`, string(batch.Rows[0].Line))
}

// TestIngest_ForwardSlashExportsUnescaped: the exported row leaves `/` as is
// wherever the writer puts a string — a value, an array element, a map key, a
// named tuple's field name — where the writer's default is `\/`, as the query
// paths render it. An insert check on the same parse changes nothing.
func TestIngest_ForwardSlashExportsUnescaped(t *testing.T) {
	eng := testEngine(t, &discovery.TableSchema{Name: "paths", Columns: []discovery.Column{
		{Name: "s", Type: "String", Position: 1},
		{Name: "lc", Type: "LowCardinality(String)", Position: 2},
		{Name: "arr", Type: "Array(String)", Position: 3},
		{Name: "m", Type: "Map(String, String)", Position: 4},
		{Name: "t", Type: "Tuple(`n/m` String)", Position: 5},
	}})
	tbl, err := eng.Table(tenant.Default, "paths")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)

	body := []byte(`{"s":"/home","lc":"a/b","arr":["c/d"],"m":{"k/1":"v/2"},"t":{"n/m":"x/y"}}` + "\n")
	const want = `["/home", "a/b", ["c/d"], {"k/1":"v/2"}, {"n/m":"x/y"}]`
	for name, checks := range map[string][]Predicate{
		"no checks":  nil,
		"with check": {{Column: "s", Op: "=", Values: []string{"/home"}}},
	} {
		batch, err := tbl.Ingest(FormatJSONEachRow, body, checks...)
		require.NoError(t, err, name)
		require.Len(t, batch.Rows, 1, name)
		require.True(t, batch.Rows[0].Accepted, "%s: %s", name, batch.Rows[0].Message)
		require.Empty(t, batch.Rows[0].CheckReason, name)
		assert.Equal(t, want, string(batch.Rows[0].Line), name)
	}
}

// TestIngest_NaNAndInfinityExportAsStrings: the export spells a Float NaN or
// infinity as a JSON string, the spelling the worker's INSERT stores as that
// value (a null would store the column's default), and /v1/query renders the
// same strings. The export reads output_format_json_quote_denormals, whose
// default renders null, so parseSettings pins it on.
func TestIngest_NaNAndInfinityExportAsStrings(t *testing.T) {
	eng := testEngine(t, &discovery.TableSchema{Name: "floats", Columns: []discovery.Column{
		{Name: "f", Type: "Float64", Position: 1},
		{Name: "g", Type: "Nullable(Float32)", Position: 2},
		{Name: "h", Type: "Float32", Position: 3},
		{Name: "i", Type: "Nullable(Float64)", Position: 4},
	}})
	tbl, err := eng.Table(tenant.Default, "floats")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(
		`{"f":"nan","g":"inf","h":"-inf","i":"nan"}`+"\n"+
			`{"f":"-inf","g":null,"h":"nan","i":"inf"}`+"\n"+
			`{"f":"inf","g":"-inf","h":"inf","i":"-inf"}`+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 3)
	for _, r := range batch.Rows {
		require.True(t, r.Accepted, r.Message)
	}
	assert.Equal(t, `["nan", "inf", "-inf", "nan"]`, string(batch.Rows[0].Line))
	assert.Equal(t, `["-inf", null, "nan", "inf"]`, string(batch.Rows[1].Line))
	assert.Equal(t, `["inf", "-inf", "inf", "-inf"]`, string(batch.Rows[2].Line))
}

func TestInsertSettings_ReturnsAFreshMap(t *testing.T) {
	t.Parallel()
	a := InsertSettings()
	a["async_insert"] = "0"
	assert.NotContains(t, InsertSettings(), "async_insert")
	assert.Equal(t, map[string]string{
		"date_time_input_format":       "best_effort",
		"input_format_null_as_default": "1",
	}, InsertSettings())
}

// TestIngest_CountIsChtypesOwn: blank lines and pretty-printed framing are
// not records. chtypes' per-record answer is the record count; nothing is
// padded on top of it.
func TestIngest_CountIsChtypesOwn(t *testing.T) {
	tbl := ingestTable(t)

	ndjson := "{\"id\":1,\"name\":\"a\",\"ts\":\"2026-01-15 10:30:00.000\",\"tags\":[],\"score\":null}\n\n" +
		"{\"id\":2,\"name\":\"b\",\"ts\":\"2026-01-15 10:30:00.000\",\"tags\":[],\"score\":null}\n"
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(ndjson))
	require.NoError(t, err)
	assert.True(t, batch.Answered)
	require.Len(t, batch.Rows, 2, "a blank line is not a record")
	assert.True(t, batch.Rows[0].Accepted)
	assert.True(t, batch.Rows[1].Accepted)

	pretty := "{\n  \"id\": 3,\n  \"name\": \"c\",\n  \"ts\": \"2026-01-15 10:30:00.000\",\n  \"tags\": [],\n  \"score\": null\n}\n"
	batch, err = tbl.Ingest(FormatJSONEachRow, []byte(pretty))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1, "a pretty-printed object is one record, not one per line")
	assert.True(t, batch.Rows[0].Accepted)
}

func TestCountRecords(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, countRecords(nil, 0))
	assert.Equal(t, 1, countRecords([]byte(`{"a":1}`), 0))
	assert.Equal(t, 2, countRecords([]byte("{}\n{}\n"), 0))
	assert.Equal(t, 2, countRecords([]byte("{}\n{}"), 0))
	assert.Equal(t, 5, countRecords([]byte("{}\n{}"), 5), "chtypes' own count wins when it has one")
}

// gatedTable has one column of each type ClickHouse refuses to create unless a
// type gate is set. Such a table exists on a server only because its CREATE
// passed the gate there.
func gatedTable() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "gated",
		Columns: []discovery.Column{
			{Name: "c", Type: "LowCardinality(UInt64)", Position: 1},
			{Name: "fs", Type: "FixedString(300)", Position: 2},
			{Name: "v", Type: "Variant(UInt32, Int64)", Position: 3},
		},
	}
}

// TestIngest_TypeGatedColumnsInsertAndFilter: the compile profile carries the
// type gates, so a table with a LowCardinality(UInt64), a FixedString wider
// than 256 or a Variant of similar types accepts its records and answers its
// filters. The control compiles the same declarations without the gates:
// every record is refused (455 and 44), which is what this table got on every
// call before. The v1 26.8 artifact accepts FixedString(300) ungated; its gate
// stays, harmless.
func TestIngest_TypeGatedColumnsInsertAndFilter(t *testing.T) {
	eng := testEngine(t, gatedTable())
	tbl, err := eng.Table(tenant.Default, "gated")
	require.NoError(t, err)
	defer tbl.Release()

	body := []byte(`{"c":5,"fs":"a","v":1}` + "\n" + `{"c":7,"fs":"b","v":2}` + "\n")
	batch, err := tbl.Ingest(FormatJSONEachRow, body)
	require.NoError(t, err)
	require.Len(t, batch.Rows, 2)
	for i, r := range batch.Rows {
		assert.True(t, r.Accepted, "record %d: %d %s", i, r.Code, r.Message)
	}

	isFive := Predicate{Column: "c", Op: "=", Values: []string{"5"}}
	batch, err = tbl.Ingest(FormatJSONEachRow, body, isFive)
	require.NoError(t, err)
	assert.Equal(t, []string{"", ReasonFilter}, checkReasons(t, batch), "the insert check answers t, f")

	stored, err := tbl.Ingest(FormatJSONEachRow, body)
	require.NoError(t, err)
	for i, want := range []bool{true, false} {
		row, err := tbl.ParseRow(tbl.WireColumns, stored.Rows[i].Line)
		require.NoError(t, err)
		visible, reason := row.VisibleWithReason([]Predicate{isFive})
		row.Close()
		assert.Equal(t, want, visible, "stored row %d: %s", i, reason)
	}

	for _, ts := range gatedTable().Columns {
		if ts.Name == "fs" {
			continue
		}
		stmt, err := createTable(tbl.lib, []colDecl{{Name: ts.Name, Type: ts.Type}})
		require.NoError(t, err)
		ungated, err := tbl.lib.CompileTable(stmt, chtypes.WithSettings(map[string]string{
			"input_format_allow_errors_ratio":  "1",
			"input_format_skip_unknown_fields": "0",
		}))
		var se *chtypes.SchemaError
		if errors.As(err, &se) {
			// Refused at the compile rather than per record: still the gate.
			assert.Contains(t, []int32{44, 455}, se.ChCode, "%s without the gates: %s", ts.Type, se.Message)
			continue
		}
		require.NoError(t, err, ts.Type)
		res, err := ungated.Rows(FormatJSONEachRow, []byte(`{"`+ts.Name+`":5}`+"\n"), chtypes.WithSettings(InsertSettings()))
		_ = ungated.Close()
		require.NoError(t, err, ts.Type)
		assert.NotEqual(t, chtypes.Accepted, res.Outcome, "%s without the gates", ts.Type)
		code := res.ErrCode
		if len(res.Rows) > 0 && code == 0 {
			code = res.Rows[0].ErrCode
		}
		assert.Contains(t, []int32{44, 455}, code, "%s without the gates: %s", ts.Type, res.ErrMsg)
	}
}

// TestIngest_EphemeralNoDefaultReadsStaysUnlisted: an EPHEMERAL column is
// listed only when a DEFAULT reads it. Listing one nothing reads makes the
// artifact reject, with no code, every record that omits it in a table that
// computes any column, so it stays refused (117) — its value would change
// nothing — and records omitting the listed ones still get verdicts.
func TestIngest_EphemeralNoDefaultReadsStaysUnlisted(t *testing.T) {
	schema := &discovery.TableSchema{
		Name: "eph",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "e1", Type: "UInt8", DefaultKind: "EPHEMERAL", HasDefault: true, Position: 2},
			{Name: "e2", Type: "String", DefaultKind: "EPHEMERAL", DefaultExpression: "'zz'", HasDefault: true, Position: 3},
			{Name: "unread", Type: "String", DefaultKind: "EPHEMERAL", DefaultExpression: "''", HasDefault: true, Position: 4},
			{Name: "d1", Type: "UInt16", DefaultKind: "DEFAULT", DefaultExpression: "e1 + 1", HasDefault: true, Position: 5},
			{Name: "d2", Type: "UInt64", DefaultKind: "DEFAULT", DefaultExpression: "length(e2)", HasDefault: true, Position: 6},
			{Name: "m", Type: "UInt64", DefaultKind: "MATERIALIZED", DefaultExpression: "id * 2", HasDefault: true, Position: 7},
		},
	}
	eng := testEngine(t, schema)
	tbl, err := eng.Table(tenant.Default, "eph")
	require.NoError(t, err)
	defer tbl.Release()
	require.Equal(t, []string{"id", "d1", "d2"}, tbl.WireColumns)

	body := strings.Join([]string{
		`{"id":1}`,
		`{"id":2,"e1":5}`,
		`{"id":3,"e2":"abc"}`,
		`{"id":4,"e1":5,"e2":"abc"}`,
		`{"id":5,"unread":"x"}`,
	}, "\n") + "\n"
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(body))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 5)
	for i, want := range []string{"[1, 1, 2]", "[2, 6, 2]", "[3, 1, 3]", "[4, 6, 3]"} {
		require.True(t, batch.Rows[i].Accepted, "record %d: %s", i+1, batch.Rows[i].Message)
		assert.Equal(t, want, string(batch.Rows[i].Line), "record %d", i+1)
	}
	assert.Equal(t, 117, batch.Rows[4].Code)
	assert.Contains(t, batch.Rows[4].Message, "unread")

	// A header that leaves a listed EPHEMERAL column out is the same omission.
	batch, err = tbl.Ingest(FormatCSVWithNames, []byte("id,e1\n6,5\n"))
	require.NoError(t, err)
	require.Nil(t, batch.Refused)
	require.True(t, batch.Rows[0].Accepted, batch.Rows[0].Message)
	assert.Equal(t, "[6, 6, 2]", string(batch.Rows[0].Line))
}
