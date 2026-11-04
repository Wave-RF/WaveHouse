package typelayer

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/v2/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// windowTable has a now64(9) DEFAULT, so a clock that differs between windows
// shows in the exported rows, and a Map column, the kind of table a call over
// 10 MiB is declined on.
func windowTable() *discovery.TableSchema {
	return &discovery.TableSchema{Name: "win", Columns: []discovery.Column{
		{Name: "id", Type: "UInt32", Position: 1},
		{Name: "s", Type: "String", Position: 2},
		{Name: "t", Type: "DateTime64(9)", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "now64(9)", Position: 3},
	}}
}

func pairTable() *discovery.TableSchema {
	return &discovery.TableSchema{Name: "pair", Columns: []discovery.Column{
		{Name: "a", Type: "String", Position: 1},
		{Name: "b", Type: "String", Position: 2},
	}}
}

func mapTable() *discovery.TableSchema {
	return &discovery.TableSchema{Name: "maps", Columns: []discovery.Column{
		{Name: "n", Type: "UInt64", Position: 1},
		{Name: "m", Type: "Map(String, UInt64)", Position: 2},
	}}
}

// windowSizes are the windows each body is read in: from a few records to
// fewer than two, so every cut, close and growth path runs.
var windowSizes = []Limits{
	{Bytes: 1 << 30, Records: 1 << 30}, // one call: the reference
	{Bytes: 1 << 30, Records: 2},
	{Bytes: 1 << 30, Records: 3},
	{Bytes: 1 << 30, Records: 7},
	{Bytes: 16, Records: 1 << 30},
	{Bytes: 40, Records: 1 << 30},
	{Bytes: 64, Records: 3},
	{Bytes: 101, Records: 1 << 30},
	{Bytes: 257, Records: 5},
	{Bytes: 1 << 30, Records: 1000},
}

// answer is what a test compares between window sizes: every record's
// verdict and exported line, or the body's refusal or decline.
func answer(b Batch) string {
	var sb strings.Builder
	switch {
	case b.Refused != nil:
		fmt.Fprintf(&sb, "refused %d at record %d: %s", b.Refused.Code, b.Refused.Record, b.Refused.Message)
	case b.Declined != "":
		sb.WriteString("declined: " + b.Declined)
	default:
		fmt.Fprintf(&sb, "columns %q\n", b.Columns)
		for i, r := range b.Rows {
			fmt.Fprintf(&sb, "%d accepted=%t declined=%t check=%q %s\n", i+1, r.Accepted, r.Declined, r.CheckReason, r.Line)
		}
	}
	return sb.String()
}

func windowBodies() []struct {
	name   string
	format Format
	strict bool
	body   string
} {
	var nd, arr, csv, tsv strings.Builder
	arr.WriteString("\xEF\xBB\xBF [\n")
	csv.WriteString("id,s,t\n") // auto-detected header
	for i := 1; i <= 23; i++ {
		s := strings.Repeat("x", i%5) + `{"[\"` // braces and quotes inside a string
		switch {
		case i%7 == 0:
			fmt.Fprintf(&nd, "{\n  \"id\": %d,\n  \"s\": %q\n}\n", i, s) // one record over several lines
		case i%5 == 0:
			fmt.Fprintf(&nd, `{"id":%d,"s":%q} `, i, s) // two records on one line
		default:
			fmt.Fprintf(&nd, `{"id":%d,"s":%q}`+"\n", i, s)
		}
		if i > 1 {
			arr.WriteString(",\n ")
		}
		fmt.Fprintf(&arr, `{"id":%d,"s":%q}`, i, s)
		fmt.Fprintf(&csv, "%d,\"line %d\nwith \"\"quotes\"\" and, a comma\",2024-01-01 00:00:00\n", i, i)
		fmt.Fprintf(&tsv, "%d\tv%d\\twith\\nescapes\t2024-01-01 00:00:00\n", i, i)
	}
	arr.WriteString("\n]\n")
	ndBody := nd.String()
	var long strings.Builder // a refusal far from any cut, in the second of 1000-record windows
	for i := 1; i <= 2000; i++ {
		if i == 1500 {
			long.WriteString(`{"id":"bad","s":"far from the cut"}` + "\n")
			continue
		}
		fmt.Fprintf(&long, `{"id":%d,"s":"padding"}`+"\n", i)
	}
	return []struct {
		name   string
		format Format
		strict bool
		body   string
	}{
		{"ndjson", FormatJSONEachRow, false, ndBody},
		{"array", FormatJSONEachRow, false, arr.String()},
		{"csv", FormatCSV, false, csv.String()},
		{"csv strict", FormatCSV, true, strings.TrimPrefix(csv.String(), "id,s,t\n")},
		{"csv names", FormatCSVWithNames, false, "s,id\n" + "a,1\n\"b\nb\",2\nc,3\nd,4\ne,5\nf,6\n"},
		{"tsv", FormatTSV, false, tsv.String()},
		{"tsv names", FormatTSVWithNames, false, "s\tid\n" + "a\t1\nb\t2\nc\t3\nd\t4\ne\t5\n"},
		{"refused late", FormatJSONEachRow, false, ndBody + `{"id":"bad","s":"x"}` + "\n" + `{"id":99,"s":"y"}` + "\n"},
		{"unknown field late", FormatJSONEachRow, false, ndBody + `{"id":1,"s":"x","nope":1}` + "\n"},
		{"truncated tail", FormatJSONEachRow, false, ndBody + `{"id":1,"s":"x`},
		{"array junk after", FormatJSONEachRow, false, arr.String() + "x"},
		{"csv refused late", FormatCSV, true, "1,a,2024-01-01 00:00:00\n2,b,2024-01-01 00:00:00\n3,c,2024-01-01 00:00:00\nx,d,2024-01-01 00:00:00\n"},
		{"refused far from a cut", FormatJSONEachRow, false, long.String()},
		{"tsv refused late", FormatTSV, false, tsv.String() + "x\ty\t2024-01-01 00:00:00\n"},
		{"tsv names refused late", FormatTSVWithNames, false, "s\tid\n" + "a\t1\nb\t2\nc\t3\nd\tx\n"},
	}
}

// TestIngest_WindowsAnswerAsOneCall: whatever the window size, a body gets
// the answer one call over it gives: the same verdicts and exported lines in
// the same order, or the same refusal numbered from the body's first record.
func TestIngest_WindowsAnswerAsOneCall(t *testing.T) {
	eng := testEngine(t, windowTable())
	tbl, err := eng.Table(tenant.Default, "win")
	require.NoError(t, err)
	defer tbl.Release()
	now := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	for _, c := range windowBodies() {
		t.Run(c.name, func(t *testing.T) {
			var want string
			for i, lim := range windowSizes {
				b, err := tbl.IngestWith(c.format, IngestOptions{StrictPositional: c.strict, Now: now, Window: lim}, []byte(c.body))
				require.NoError(t, err)
				if i == 0 {
					want = answer(b)
					continue
				}
				assert.Equal(t, want, answer(b), "window %+v", lim)
			}
			t.Log(want)
		})
	}
}

// TestIngest_WindowsShareOneClock: with Now set, every window's now64(9)
// DEFAULT is that instant; without it each call reads its own clock.
func TestIngest_WindowsShareOneClock(t *testing.T) {
	eng := testEngine(t, windowTable())
	tbl, err := eng.Table(tenant.Default, "win")
	require.NoError(t, err)
	defer tbl.Release()
	var body strings.Builder
	for i := range 40 {
		fmt.Fprintf(&body, `{"id":%d,"s":"x"}`+"\n", i)
	}
	lim := Limits{Bytes: 1 << 20, Records: 3}
	stamps := func(opts IngestOptions) map[string]int {
		b, err := tbl.IngestWith(FormatJSONEachRow, opts, []byte(body.String()))
		require.NoError(t, err)
		require.Len(t, b.Rows, 40)
		seen := map[string]int{}
		for _, r := range b.Rows {
			require.True(t, r.Accepted, r.Message)
			cells := strings.Split(strings.Trim(string(r.Line), "[]"), ", ")
			seen[cells[2]]++
		}
		return seen
	}
	now := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	assert.Equal(t, map[string]int{`"2026-03-04T05:06:07.123456789Z"`: 40}, stamps(IngestOptions{Now: now, Window: lim}))
	unpinned := stamps(IngestOptions{Window: lim})
	t.Logf("unpinned: %d distinct instants over 20 windows", len(unpinned))
	assert.Greater(t, len(unpinned), 1, "the control: unpinned windows read their own clocks")
}

// TestIngest_LaterCSVWindowsDetectNoHeader: a bare CSV or TSV body's later
// windows read with header detection off. The control shows why: by default
// the reader consumes a first line naming the columns, so a later window
// starting at the data row "a,b" would lose it.
func TestIngest_LaterCSVWindowsDetectNoHeader(t *testing.T) {
	eng := testEngine(t, pairTable())
	tbl, err := eng.Table(tenant.Default, "pair")
	require.NoError(t, err)
	defer tbl.Release()

	rows := func(body string, settings map[string]string) int {
		res, err := tbl.c.schema.Rows(chtypes.CSV, []byte(body), chtypes.WithSettings(settings))
		require.NoError(t, err)
		require.Equal(t, chtypes.Accepted, res.Outcome, res.ErrMsg)
		return len(res.Rows)
	}
	settings, _ := parseSettings(FormatCSV, IngestOptions{})
	assert.Equal(t, 1, rows("a,b\nx,y\n", settings), "the control: a first line naming the columns is a header")
	settings["input_format_csv_detect_header"] = "0"
	assert.Equal(t, 2, rows("a,b\nx,y\n", settings))

	for _, format := range []Format{FormatCSV, FormatTSV} {
		sep := ","
		if format == FormatTSV {
			sep = "\t"
		}
		var body strings.Builder
		for i := range 9 {
			if i == 4 {
				body.WriteString("a" + sep + "b\n") // a data row that names the columns
				continue
			}
			fmt.Fprintf(&body, "x%d%sy%d\n", i, sep, i)
		}
		for _, lim := range windowSizes {
			b, err := tbl.IngestWith(format, IngestOptions{Window: lim}, []byte(body.String()))
			require.NoError(t, err)
			assert.Len(t, b.Rows, 9, "format %v window %+v", format, lim)
		}
	}
}

// TestIngest_WindowsKeepBigMapBodiesUnderTheCut: a body over 10 MiB on a Map
// table, which one call declines (parallel-segment-cut), is accepted record
// for record in default windows.
func TestIngest_WindowsKeepBigMapBodiesUnderTheCut(t *testing.T) {
	if testing.Short() {
		t.Skip("parses a 15 MiB body")
	}
	eng := testEngine(t, mapTable())
	tbl, err := eng.Table(tenant.Default, "maps")
	require.NoError(t, err)
	defer tbl.Release()
	line := `{"n":1,"m":{"k":` + strconv.Itoa(1) + `,"padding_key_to_make_records_longer":12345}}` + "\n"
	n := (15 << 20) / len(line)
	body := []byte(strings.Repeat(line, n))

	one, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{Window: Limits{Bytes: 1 << 30, Records: 1 << 30}}, body)
	require.NoError(t, err)
	assert.Contains(t, one.Declined, "parallel", "the control: one call over the body is declined")

	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, body)
	require.NoError(t, err)
	require.Empty(t, b.Declined)
	require.Nil(t, b.Refused)
	require.Len(t, b.Rows, n)
	for i, r := range b.Rows {
		if !r.Accepted {
			t.Fatalf("record %d: %s", i+1, r.Message)
		}
	}
}
