package typelayer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

const (
	recUUID = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"
	bom     = "\xEF\xBB\xBF"
)

// uuidTables are a UUID-first table for the positional formats, a
// name-addressed one whose omitted id takes a constant DEFAULT, and two more
// first-column types a leading byte order mark is measured against.
func uuidTables() []*discovery.TableSchema {
	return []*discovery.TableSchema{
		{Name: "stamps", Columns: []discovery.Column{
			{Name: "ts", Type: "DateTime", Position: 1},
			{Name: "page", Type: "String", Position: 2},
		}},
		{Name: "batches", Columns: []discovery.Column{
			{Name: "ns", Type: "Array(UInt8)", Position: 1},
			{Name: "page", Type: "String", Position: 2},
		}},
		{Name: "pings", Columns: []discovery.Column{
			{Name: "id", Type: "UUID", Position: 1},
			{Name: "page", Type: "String", Position: 2},
			{Name: "n", Type: "UInt8", Position: 3},
		}},
		{Name: "visits", Columns: []discovery.Column{
			{Name: "page", Type: "String", Position: 1},
			{Name: "id", Type: "UUID", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "toUUID('00000000-0000-0000-0000-000000000000')", Position: 2},
		}},
	}
}

// TestIngest_ShortAnswerIsDeclinedWhole pins the measured loss at its source:
// a UUID shorter than 36 characters makes ClickHouse's reader consume a fixed
// 36-byte window past it, and the records that window reaches get no verdict
// while the batch outcome stays Accepted. IngestWith must answer every record
// the body holds, declined, rather than return the short batch.
func TestIngest_ShortAnswerIsDeclinedWhole(t *testing.T) {
	eng := testEngine(t, uuidTables()...)
	table := func(name string) *Table {
		tbl, err := eng.Table(tenant.Default, name)
		require.NoError(t, err)
		t.Cleanup(tbl.Release)
		return tbl
	}
	visits, pings := table("visits"), table("pings")

	ndjson6 := `{"page":"/a","id":"x"}` + "\n" + strings.Repeat(`{"page":"/b"}`+"\n", 5)
	for _, tt := range []struct {
		name   string
		tbl    *Table
		format Format
		opts   IngestOptions
		body   string
		want   int
	}{
		// chtypes answered 3 of these six: the bad record and the two the
		// window did not reach.
		{"six NDJSON records", visits, FormatJSONEachRow, IngestOptions{}, ndjson6, 6},
		{"three NDJSON records", visits, FormatJSONEachRow, IngestOptions{}, "{\"page\":\"/a\",\"id\":\"x\"}\n{\"page\":\"/b\"}\n{\"page\":\"/c\"}", 3},
		{"a caller's exact count", visits, FormatJSONEachRow, IngestOptions{Records: 3}, " {\"page\":\"/a\",\"id\":\"x\"}\n{\"page\":\"/b\"}\n{\"page\":\"/c\"} ", 3},
		{"bare CSV", pings, FormatCSV, IngestOptions{}, "zzz,/a,1\n" + recUUID + ",/b,2\n" + recUUID + ",/c,3\n", 3},
		// A header line under header=absent is a record too, and its column
		// name is a short UUID.
		{"a header line read as a record", pings, FormatCSV, IngestOptions{StrictPositional: true}, "id,page,n\n" + recUUID + ",/b,2\n", 2},
		{"a blank CSV line", pings, FormatCSV, IngestOptions{StrictPositional: true}, recUUID + ",/a,1\n\n" + recUUID + ",/b,2\n", 3},
		{"a CSVWithNames types line", pings, FormatCSVWithNames, IngestOptions{}, "id,page,n\nUUID,String,UInt8\n" + recUUID + ",/b,2\n", 2},
		{"TSV", pings, FormatTSV, IngestOptions{}, "zzz\t/a\t1\n" + recUUID + "\t/b\t2\n" + recUUID + "\t/c\t3\n", 3},
		{"a byte order mark before bare CSV", pings, FormatCSV, IngestOptions{}, bom + "zzz,/a,1\n" + recUUID + ",/b,2\n" + recUUID + ",/c,3\n", 3},
		// A String first column keeps the mark as its value, so this names
		// line is no header to ClickHouse: it is a record whose id is `id`.
		{"a marked header a String-first table reads as a record", visits, FormatCSV, IngestOptions{}, bom + "page,id\n/a," + recUUID + "\n/b," + recUUID + "\n", 3},
		{"a marked TSV header a String-first table reads as a record", visits, FormatTSV, IngestOptions{}, bom + "page\tid\n/a\t" + recUUID + "\n/b\t" + recUUID + "\n", 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := tt.tbl.IngestWith(tt.format, tt.opts, []byte(tt.body))
			require.NoError(t, err)
			require.NotNil(t, batch.Miscount, "chtypes answered short; the batch must say so")
			assert.Equal(t, tt.want, batch.Miscount.Counted)
			assert.Less(t, batch.Miscount.Verdicts, tt.want)
			assert.Equal(t, tt.opts.Records > 0, batch.Miscount.Exact)
			assert.False(t, batch.Answered)
			require.Len(t, batch.Rows, tt.want, "one verdict per record the body holds")
			for _, r := range batch.Rows {
				assert.True(t, r.Declined, "declined, never accepted or refused: %+v", r)
				assert.Nil(t, r.Line)
				assert.Contains(t, r.Message, "chtypes answered")
			}
		})
	}
}

// TestIngest_WholeAnswersAreNotMiscounted: bodies chtypes reads whole keep
// their per-record answers — the count is a floor and must never trip on
// them.
func TestIngest_WholeAnswersAreNotMiscounted(t *testing.T) {
	eng := testEngine(t, uuidTables()...)
	table := func(name string) *Table {
		tbl, err := eng.Table(tenant.Default, name)
		require.NoError(t, err)
		t.Cleanup(tbl.Release)
		return tbl
	}
	visits, pings, stamps, batches := table("visits"), table("pings"), table("stamps"), table("batches")
	r := recUUID + ",/r,1\n"
	ts := "2024-01-01 00:00:00"
	for _, tt := range []struct {
		name   string
		tbl    *Table
		format Format
		opts   IngestOptions
		body   string
		rows   int
	}{
		{"a bad UUID in the last record", visits, FormatJSONEachRow, IngestOptions{}, "{\"page\":\"/b\"}\n{\"page\":\"/a\",\"id\":\"x\"}\n", 2},
		{"a 36-character bad UUID", visits, FormatJSONEachRow, IngestOptions{}, "{\"page\":\"/a\",\"id\":\"" + recUUID[:35] + "z\"}\n{\"page\":\"/b\"}\n", 2},
		{"concatenated, comma-separated and bracketed objects", visits, FormatJSONEachRow, IngestOptions{}, `{"page":"/a"}{"page":"/b"},{"page":"/c"}`, 3},
		{"an outer array", visits, FormatJSONEachRow, IngestOptions{}, `[{"page":"/a"},{"page":"/b"}]`, 2},
		{"blank and whitespace lines", visits, FormatJSONEachRow, IngestOptions{}, "{\"page\":\"/a\"}\n  \n\t\n{\"page\":\"/b\"}\n", 2},
		{"a raw newline inside a string", visits, FormatJSONEachRow, IngestOptions{}, "{\"page\":\"a\n{\"}\n{\"page\":\"/c\"}\n", 2},
		{"a scalar line", visits, FormatJSONEachRow, IngestOptions{}, "1\n{\"page\":\"/b\"}\n", 2},
		{"a quoted CSV newline", pings, FormatCSV, IngestOptions{StrictPositional: true}, recUUID + ",\"/a\nx\",1\n" + r, 2},
		{"a quoted CSV newline after whitespace", pings, FormatCSV, IngestOptions{StrictPositional: true}, recUUID + ", \"/a\nx\",1\n" + r, 2},
		{"a mid-field quote", pings, FormatCSV, IngestOptions{StrictPositional: true}, recUUID + ",a\"b,1\n" + r, 2},
		{"CRLF", pings, FormatCSV, IngestOptions{StrictPositional: true}, recUUID + ",/a,1\r\n" + recUUID + ",/b,2\r\n", 2},
		{"an escaped TSV newline", pings, FormatTSV, IngestOptions{StrictPositional: true}, recUUID + "\t/a\\\nx\t1\n" + recUUID + "\t/b\t2\n", 2},
		{"a detected names header", pings, FormatCSV, IngestOptions{}, "id,page,n\n" + r, 1},
		{"a detected quoted, padded header", pings, FormatCSV, IngestOptions{}, " \"id\", page , n\n" + r, 1},
		{"a detected subset header", pings, FormatCSV, IngestOptions{}, "page,id\n/r," + recUUID + "\n", 1},
		{"a detected names and types header", pings, FormatCSV, IngestOptions{}, "page,id,n\nString,UUID,UInt8\n/r," + recUUID + ",1\n", 1},
		{"a detected TSV header with an escape", pings, FormatTSV, IngestOptions{}, "i\\x64\tpage\tn\n" + recUUID + "\t/r\t1\n", 1},
		{"a CSVWithNames header", pings, FormatCSVWithNames, IngestOptions{}, "page,id\n/a," + recUUID + "\n/b," + recUUID + "\n", 2},
		{"a header with no records", pings, FormatCSV, IngestOptions{}, "id,page,n\n", 0},
		// ClickHouse skips a leading byte order mark unless the first column
		// is a string type, so the names line after it is a detected header.
		{"a byte order mark before a detected header", pings, FormatCSV, IngestOptions{}, bom + "id,page,n\n" + r + r, 2},
		{"a byte order mark before a DateTime-first header", stamps, FormatCSV, IngestOptions{}, bom + "ts,page\n" + ts + ",/a\n" + ts + ",/b\n", 2},
		{"a byte order mark before a detected TSV header", pings, FormatTSV, IngestOptions{}, bom + "id\tpage\tn\n" + recUUID + "\t/a\t1\n" + recUUID + "\t/b\t2\n", 2},
		{"a byte order mark before a CSVWithNames header", visits, FormatCSVWithNames, IngestOptions{}, bom + "\"page\",id\n\"/a\nx\"," + recUUID + "\n", 1},
		{"a byte order mark before a quoted newline", batches, FormatCSV, IngestOptions{StrictPositional: true}, bom + "\"[1,\n2]\",/a\n[3],/b\n", 2},
		// A String first column keeps it: the quote after it is a literal, so
		// its newline ends a record — three records, as ClickHouse reads them.
		{"a byte order mark a String first column keeps", visits, FormatCSV, IngestOptions{StrictPositional: true}, bom + "\"/a\nx\"," + recUUID + "\n/b," + recUUID + "\n", 3},
		{"LF CR line ends", pings, FormatCSV, IngestOptions{StrictPositional: true}, recUUID + ",/a,1\n\r" + recUUID + ",/b,2\n\r", 2},
		{"LF CR after a detected header", visits, FormatCSV, IngestOptions{}, "page,id\n\r/a," + recUUID + "\n\r/b," + recUUID + "\n\r", 2},
		{"LF CR after a CSVWithNames header", visits, FormatCSVWithNames, IngestOptions{}, "page,id\n\r/a," + recUUID + "\n\r/b," + recUUID + "\n\r", 2},
		// TSV has no LF CR line end: the CR opens the next record.
		{"a TSV CR after LF", visits, FormatTSV, IngestOptions{StrictPositional: true}, "/a\t" + recUUID + "\n\r/b\t" + recUUID + "\n", 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := tt.tbl.IngestWith(tt.format, tt.opts, []byte(tt.body))
			require.NoError(t, err)
			assert.Nil(t, batch.Miscount)
			assert.True(t, batch.Answered)
			assert.Len(t, batch.Rows, tt.rows)
		})
	}
}

func TestJSONObjects(t *testing.T) {
	t.Parallel()
	for body, want := range map[string]int{
		``:                                     0,
		`{}`:                                   1,
		"{\"a\":1}\n{\"a\":2}\n":               2,
		`{"a":{"b":{}}}{"c":[{},{}]}`:          2,
		`[{"a":1},{"a":2}]`:                    2,
		`{"a":"}{"}`:                           1,
		`{"a":"\"}{"}`:                         1,
		"{\n  \"a\": 1\n}\n\n{\n  \"a\": 2\n}": 2,
		`{"a":1}}{"b":2}`:                      2, // a stray closer does not hide what follows
		"{\"a\":1\n{\"b\":2}\n":                1, // a truncated object hides what follows: a floor, not a count
		"1\n\"x\"\nnull\n":                     0,
	} {
		assert.Equal(t, want, jsonObjects([]byte(body)), "%q", body)
	}
}

func TestCSVRecords(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		body          string
		n             int
		first, second string
	}{
		{"", 0, "", ""},
		{"a", 1, "a", ""},
		{"a\n", 1, "a", ""},
		{"a\nb", 2, "a", "b"},
		{"\n", 1, "", ""},
		{"a\n\nb\n", 3, "a", ""},
		{"a\n  ", 2, "a", "  "},
		{"\"x\ny\",1\nb\n", 2, "\"x\ny\",1", "b"},
		{" \"x\ny\",1\nb\n", 2, " \"x\ny\",1", "b"},
		{"\t\"x\ny\"\nb\n", 2, "\t\"x\ny\"", "b"},
		{"\"a\"\"\nb\"\nc\n", 2, "\"a\"\"\nb\"", "c"},
		{"\"a\\\"\nb\"\nc\n", 3, "\"a\\\"", "b\""},
		{"a\"b\nc\",1\n", 2, "a\"b", "c\",1"},
		{"'a\nb',1\n", 2, "'a", "b',1"},
		{"a\r\nb\r\n", 2, "a\r", "b\r"},
		{"a\n\rb\n\r", 2, "a", "b"},
		{"a\n\r\nb\n", 3, "a", ""},
		{"a\n\r\rb\n", 2, "a", "\rb"},
		{"\"x\n\ry\"\n\rb", 2, "\"x\n\ry\"", "b"},
		{"\"never closed\nstill quoted", 1, "\"never closed\nstill quoted", ""},
	} {
		n, first, second := csvRecords([]byte(tt.body))
		assert.Equal(t, tt.n, n, "%q", tt.body)
		assert.Equal(t, tt.first, string(first), "%q first", tt.body)
		assert.Equal(t, tt.second, string(second), "%q second", tt.body)
	}
}

func TestTSVRecords(t *testing.T) {
	t.Parallel()
	for body, want := range map[string]int{
		"":                0,
		"a\tb\n":          1,
		"a\\\nb\nc\n":     2,
		"a\\\\\nb\nc\n":   3,
		"a\\\\\\\nb\nc\n": 2,
		"a\n\nb":          3,
		"\"a\nb\"\n":      2,
		"a\n\rb\n\r":      3,
	} {
		n, _, _ := tsvRecords([]byte(body))
		assert.Equal(t, want, n, "%q", body)
	}
}

// TestDetectedHeaderRows pins the header auto-detection rule measured on
// 26.8.15.10, and the direction it errs in where a value cannot be decoded.
func TestDetectedHeaderRows(t *testing.T) {
	t.Parallel()
	wire := []string{"id", "page", "n"}
	types := map[string]string{"id": "UUID", "page": "String", "n": "UInt8"}
	for _, tt := range []struct {
		name          string
		csv           bool
		first, second string
		n, want       int
	}{
		{"names", true, "id,page,n", recUUID + ",/a,1", 2, 1},
		{"names, quoted and padded", true, ` "id" , page,n `, "x", 2, 1},
		{"a subset of the names", true, "page", "x", 2, 1},
		{"a superset of the names", true, "id,page,n,extra", "x", 2, 1},
		{"case differs", true, "ID,Page,N", "x", 2, 0},
		{"types alone", true, "UUID,String,UInt8", "x", 2, 0},
		{"data", true, "zzz,/a,1", "x", 2, 0},
		{"names then types", true, "id,page,n", "UUID,String,UInt8", 3, 2},
		{"names then types in the header's order", true, "page,id", "String,UUID", 3, 2},
		{"names then other identifiers", true, "id,page,n", "Foo,Bar,Baz", 3, 1},
		{"names then a type alias", true, "id,page,n", "UUID,TEXT,UInt8", 3, 1},
		{"names alone", true, "id,page,n", "", 1, 1},
		{"a CRLF names line", true, "id,page,n\r", "x", 2, 1},
		{"an unterminated quote may be a name", true, `id,page,"n`, "x", 2, 1},
		{"TSV names", false, "id\tpage\tn", "x", 2, 1},
		{"TSV names are not trimmed", false, " id \tpage", "x", 2, 0},
		{"TSV names unescape", false, "i\\x64\tpage\\\\\tn", "x", 2, 0},
		{"TSV hex escape", false, "i\\x64\tpage\tn", "x", 2, 1},
		{"an escape not decoded may be a name", false, "\\N\tpage\tn", "x", 2, 1},
		{"a TSV CR may be anything", false, "id\tpage\tn\r", "x", 2, 1},
	} {
		got := detectedHeaderRows(tt.csv, []byte(tt.first), []byte(tt.second), tt.n, wire, types)
		assert.Equal(t, tt.want, got, tt.name)
	}
}

func TestRecordFloor(t *testing.T) {
	t.Parallel()
	wire := []string{"id", "page", "n"}
	types := map[string]string{"id": "UUID", "page": "String", "n": "UInt8"}
	body := []byte("id,page,n\na\nb\n")
	assert.Equal(t, 2, recordFloor(FormatCSV, IngestOptions{}, body, wire, types), "detected header")
	assert.Equal(t, 3, recordFloor(FormatCSV, IngestOptions{StrictPositional: true}, body, wire, types), "header=absent")
	assert.Equal(t, 2, recordFloor(FormatCSVWithNames, IngestOptions{}, body, wire, types), "header=present")
	assert.Equal(t, 0, recordFloor(FormatCSVWithNames, IngestOptions{}, nil, wire, types))
	assert.Equal(t, 2, recordFloor(FormatJSONEachRow, IngestOptions{}, []byte("{}\n{}\n"), nil, nil))
	assert.Equal(t, 0, recordFloor(chtypes.JSONCompactEachRow, IngestOptions{}, []byte("[1]\n"), nil, nil), "a format Ingest does not parse")
}

// TestRecordFloor_ByteOrderMark: a leading mark is counted the way ClickHouse
// reads it — skipped under a header, kept as a String first column's value —
// and as the lower of the two readings before any other first column.
func TestRecordFloor_ByteOrderMark(t *testing.T) {
	t.Parallel()
	wire := []string{"id", "page", "n"}
	typed := func(first string) map[string]string {
		return map[string]string{"id": first, "page": "String", "n": "UInt8"}
	}
	header := []byte(bom + "id,page,n\na\nb\n")
	for _, tt := range []struct {
		first string
		want  int
	}{
		{"UUID", 2},
		{"Nullable(UUID)", 2},
		{"String", 3},
		{"FixedString(36)", 3},
		{"Nullable(String)", 3},
		{"LowCardinality(String)", 3},
		{"LowCardinality(Nullable(String))", 3},
		{"Array(String)", 2}, // keeps the mark too; the lower reading
		{"", 2},
	} {
		assert.Equal(t, tt.want, recordFloor(FormatCSV, IngestOptions{}, header, wire, typed(tt.first)), tt.first)
		assert.Equal(t, tt.want, recordFloor(FormatTSV, IngestOptions{}, []byte(strings.ReplaceAll(string(header), ",", "\t")), wire, typed(tt.first)), "TSV "+tt.first)
	}
	assert.Equal(t, 2, recordFloor(FormatCSVWithNames, IngestOptions{}, header, wire, typed("String")), "a header always skips it")

	// A quote right after the mark opens a field only where the mark is
	// skipped; the lower reading holds where that is not known.
	quoted := []byte(bom + "\"a\nb\",x\nc\n")
	assert.Equal(t, 2, recordFloor(FormatCSV, IngestOptions{StrictPositional: true}, quoted, wire, typed("UUID")))
	assert.Equal(t, 3, recordFloor(FormatCSV, IngestOptions{StrictPositional: true}, quoted, wire, typed("String")))
	// Here the skipped reading is the higher one (`"""` opens a field only
	// without the mark), and a non-string first column takes the lower.
	raised := []byte(bom + "\"\"\",\"\n\"")
	assert.Equal(t, 1, recordFloor(FormatCSV, IngestOptions{StrictPositional: true}, raised, wire, typed("UUID")))
	assert.Equal(t, 1, recordFloor(FormatCSV, IngestOptions{StrictPositional: true}, raised, wire, typed("String")))
	assert.Equal(t, 0, recordFloor(FormatCSV, IngestOptions{StrictPositional: true}, []byte(bom), wire, typed("UUID")))
}

func TestMiscount(t *testing.T) {
	t.Parallel()
	assert.Nil(t, miscount(0, 3, 3))
	assert.Nil(t, miscount(0, 4, 3), "a floor is no ceiling")
	assert.Equal(t, &Miscount{Counted: 3, Verdicts: 1}, miscount(0, 1, 3))
	assert.Nil(t, miscount(3, 3, 0))
	assert.Equal(t, &Miscount{Counted: 3, Verdicts: 4, Exact: true}, miscount(3, 4, 0), "an exact count holds both ways")
	assert.Equal(t, &Miscount{Counted: 3, Verdicts: 1, Exact: true}, miscount(3, 1, 0))
}
