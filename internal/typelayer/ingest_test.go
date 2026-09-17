package typelayer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

func ingestTable(t *testing.T) *Table {
	t.Helper()
	eng := TestEngine(t, eventsTable())
	tbl, err := eng.Table("events")
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
// MATERIALIZED, ALIAS or EPHEMERAL column gets ClickHouse's own 117 and the
// rest of the batch still gets verdicts — no WaveHouse-side guard needed.
func TestIngest_ComputedColumnsAreRejectedPerRecord(t *testing.T) {
	schema := &discovery.TableSchema{
		Name: "computed",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "e", Type: "UInt8", DefaultKind: "EPHEMERAL", HasDefault: true, Position: 2},
			{Name: "d", Type: "UInt8", DefaultKind: "DEFAULT", DefaultExpression: "e + 1", HasDefault: true, Position: 3},
			{Name: "a", Type: "UInt8", DefaultKind: "ALIAS", DefaultExpression: "id + 2", HasDefault: true, Position: 4},
		},
	}
	eng := TestEngine(t, schema)
	tbl, err := eng.Table("computed")
	require.NoError(t, err)
	defer tbl.Release()

	assert.Equal(t, []string{"id", "d"}, tbl.WireColumns)

	body := strings.Join([]string{
		`{"id":1}`,
		`{"id":2,"e":5}`,
		`{"id":3,"a":9}`,
		`{"id":4,"d":7}`,
	}, "\n") + "\n"
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(body))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 4)

	assert.True(t, batch.Rows[0].Accepted)
	assert.Equal(t, 117, batch.Rows[1].Code)
	assert.Contains(t, batch.Rows[1].Message, "e")
	assert.Equal(t, 117, batch.Rows[2].Code)
	assert.Contains(t, batch.Rows[2].Message, "a")
	assert.True(t, batch.Rows[3].Accepted)
	// ClickHouse's writer separates cells with ", " — the worker inserts these
	// bytes verbatim, so nothing may re-render them.
	assert.Equal(t, "[4, 7]", string(batch.Rows[3].Line))
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
