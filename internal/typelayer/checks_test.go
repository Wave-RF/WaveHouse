package typelayer

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

func checksTable() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "checks",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "tenant", Type: "String", Position: 2},
			{Name: "kind", Type: "String", Position: 3},
		},
	}
}

// checksBody is four records whose verdicts under the cases below are the
// measured table (AUDIT §A.2).
const checksBody = `{"id":1,"tenant":"acme","kind":"a"}` + "\n" +
	`{"id":2,"tenant":"acme","kind":"a"}` + "\n" +
	`{"id":3,"tenant":"evil","kind":"a"}` + "\n" +
	`{"id":4,"tenant":"acme","kind":"z"}` + "\n"

func checksHandle(t *testing.T) *Table {
	t.Helper()
	eng := TestEngine(t, checksTable())
	tbl, err := eng.Table("checks")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)
	return tbl
}

// checkReasons is each record's CheckReason, after asserting every record
// parsed and that exactly the admitted ones carry exported bytes.
func checkReasons(t *testing.T, b Batch) []string {
	t.Helper()
	out := make([]string, len(b.Rows))
	for i, r := range b.Rows {
		require.True(t, r.Accepted, "record %d: %s", i, r.Message)
		assert.Equal(t, r.CheckReason == "", r.Line != nil,
			"record %d: bytes are exported for exactly the admitted records", i)
		out[i] = r.CheckReason
	}
	return out
}

// TestIngestChecks_MatchesTheMeasuredTable: one AND-joined filter attached to
// the one parse, a verdict per record, and bytes only for the records it
// admits.
func TestIngestChecks_MatchesTheMeasuredTable(t *testing.T) {
	tbl := checksHandle(t)

	const f = ReasonFilter
	cases := []struct {
		name  string
		preds []Predicate
		want  []string
	}{
		{
			"tenant equals",
			[]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}},
			[]string{"", "", f, ""},
		},
		{
			"kind in",
			[]Predicate{{Column: "kind", Op: "in", Values: []string{"a", "b"}}},
			[]string{"", "", "", f},
		},
		{
			"both, AND-joined",
			[]Predicate{
				{Column: "tenant", Op: "=", Values: []string{"acme"}},
				{Column: "kind", Op: "in", Values: []string{"a"}},
			},
			[]string{"", "", f, f},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := tbl.Ingest(FormatJSONEachRow, []byte(checksBody), tc.preds...)
			require.NoError(t, err)
			assert.Equal(t, tc.want, checkReasons(t, batch))
		})
	}

	// A record the filter cuts from the middle does not stop the reader: the
	// admitted record after it still carries its own bytes.
	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(checksBody),
		Predicate{Column: "tenant", Op: "=", Values: []string{"acme"}})
	require.NoError(t, err)
	assert.Equal(t, `[4, "acme", "z"]`, string(batch.Rows[3].Line))
}

// TestIngestChecks_NoPredicatesPassesEveryRow: a role with no check clauses
// pays nothing and hides nothing.
func TestIngestChecks_NoPredicatesPassesEveryRow(t *testing.T) {
	tbl := checksHandle(t)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(checksBody))
	require.NoError(t, err)
	assert.Equal(t, []string{"", "", "", ""}, checkReasons(t, batch))
	assert.Equal(t, 0, tbl.slots[0].filters.len(), "no filter is compiled without checks")
}

func TestIngestChecks_EmptyBody(t *testing.T) {
	tbl := checksHandle(t)

	batch, err := tbl.Ingest(FormatJSONEachRow, nil, Predicate{Column: "tenant", Op: "=", Values: []string{"acme"}})
	require.NoError(t, err)
	assert.Empty(t, batch.Rows)
}

// TestIngestChecks_FailsClosed: everything that is not a definite true
// withholds, and an unresolvable claim never reaches the compiler.
func TestIngestChecks_FailsClosed(t *testing.T) {
	tbl := checksHandle(t)
	compiled := func() int { return tbl.slots[0].filters.len() }
	const f, e = ReasonFilter, ReasonError

	t.Run("empty values", func(t *testing.T) {
		before := compiled()
		batch, err := tbl.Ingest(FormatJSONEachRow, []byte(checksBody),
			Predicate{Column: "tenant", Op: "=", Values: []string{"acme"}},
			Predicate{Column: "kind", Op: "in", Values: nil})
		require.NoError(t, err)
		assert.Equal(t, []string{f, f, f, f}, checkReasons(t, batch))
		assert.Equal(t, before, compiled(), "an unresolvable claim must not reach the compiler")
	})

	t.Run("unknown column", func(t *testing.T) {
		batch, err := tbl.Ingest(FormatJSONEachRow, []byte(checksBody),
			Predicate{Column: "nosuch", Op: "=", Values: []string{"x"}})
		require.NoError(t, err)
		assert.Equal(t, []string{f, f, f, f}, checkReasons(t, batch))
	})

	t.Run("value the column cannot read", func(t *testing.T) {
		batch, err := tbl.Ingest(FormatJSONEachRow, []byte(checksBody),
			Predicate{Column: "id", Op: "=", Values: []string{"abc"}})
		require.NoError(t, err)
		assert.Equal(t, []string{e, e, e, e}, checkReasons(t, batch),
			"a thrown predicate is 'we could not tell' (422), not 'the data says no' (403)")
		assert.Contains(t, batch.Rows[0].Message, "abc", "the predicate's own error rides along for the log")
	})

	t.Run("a filter closed between lookup and use", func(t *testing.T) {
		preds := []Predicate{{Column: "tenant", Op: "=", Values: []string{"closed"}}}
		expr, params, ok := tbl.render(preds)
		require.True(t, ok)
		for _, s := range tbl.slots { // whichever slot the request lands on
			f := tbl.filterOn(s, expr, params)
			require.NotNil(t, f)
			f.Close() // what an eviction racing this request does
		}

		batch, err := tbl.Ingest(FormatJSONEachRow, []byte(checksBody), preds...)
		require.NoError(t, err, "a closed filter fails the checks, not the request")
		assert.Equal(t, []string{ReasonDecline, ReasonDecline, ReasonDecline, ReasonDecline}, checkReasons(t, batch))
	})
}

// TestIngestChecks_ParseOutcomeDecidesFirst pins a measured trap: under the
// compile profile's allow_errors_ratio a record that does not parse is
// skipped, and chtypes answers it 'd' beside that outcome — with the verdict's
// own code and message EMPTY on 26.6. It must report its parse error (a 400
// with code 27), never a check decline (a 422), and never shift a neighbour
// onto its answer.
func TestIngestChecks_ParseOutcomeDecidesFirst(t *testing.T) {
	tbl := checksHandle(t)
	body := []byte(strings.Join([]string{
		`{"id":1,"tenant":"acme","kind":"a"}`,
		`{"id":"not-a-number","tenant":"acme","kind":"a"}`,
		`{"id":3,"tenant":"evil","kind":"a"}`,
		`{"id":4,"tenant":"acme","kind":"a"}`,
	}, "\n") + "\n")
	preds := []Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}}

	// The trap itself, at the SDK: a skipped row's verdict is 'd' with no
	// verdict code, and its error is only in ErrCode/ErrMsg.
	s := tbl.slots[0]
	expr, params, ok := tbl.render(preds)
	require.True(t, ok)
	res, err := s.schema.RowsExportWith(FormatJSONEachRow, body, InsertSettings(), chtypes.JSONCompactEachRow,
		chtypes.WithRowFilter(tbl.filterOn(s, expr, params)))
	require.NoError(t, err)
	require.Len(t, res.Rows, 4)
	require.Equal(t, chtypes.Skipped, res.Rows[1].Outcome)
	require.NotNil(t, res.Rows[1].Verdict)
	assert.Equal(t, chtypes.VerdictDecline, *res.Rows[1].Verdict)
	assert.Equal(t, 27, res.Rows[1].ErrCode)
	assert.Equal(t, 2, res.RowsPassed)
	assert.Equal(t, 1, res.RowsCut, "a skipped row is 'd' but not counted as cut")

	batch, err := tbl.Ingest(FormatJSONEachRow, body, preds...)
	require.NoError(t, err)
	require.Len(t, batch.Rows, 4)
	assert.True(t, batch.Rows[0].Accepted)
	assert.Empty(t, batch.Rows[0].CheckReason)
	assert.Equal(t, `[1, "acme", "a"]`, string(batch.Rows[0].Line))

	assert.False(t, batch.Rows[1].Accepted)
	assert.False(t, batch.Rows[1].Declined, "a parse refusal is a verdict about the data")
	assert.Equal(t, 27, batch.Rows[1].Code)
	assert.Empty(t, batch.Rows[1].CheckReason)

	assert.Equal(t, ReasonFilter, batch.Rows[2].CheckReason)
	assert.Nil(t, batch.Rows[2].Line)
	assert.Equal(t, `[4, "acme", "a"]`, string(batch.Rows[3].Line))
}

// TestIngestChecks_RejectedBatchExportsNothing pins the other measured trap:
// RowsPassed counts the admitted rows of a batch whose own outcome is
// rejected, and such a batch exports no bytes. It is reachable here — an
// NDJSON-declared single-line array is not reframed (only the JSON family's
// arrays are), and one bad element rejects it whole. Every record must be
// declined; none may be published on the strength of RowsPassed.
func TestIngestChecks_RejectedBatchExportsNothing(t *testing.T) {
	tbl := checksHandle(t)
	body := []byte(`[{"id":1,"tenant":"acme","kind":"a"},{"id":"x","tenant":"acme","kind":"a"},{"id":3,"tenant":"acme","kind":"a"}]`)
	preds := []Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}}

	s := tbl.slots[0]
	expr, params, ok := tbl.render(preds)
	require.True(t, ok)
	res, err := s.schema.RowsExportWith(FormatJSONEachRow, body, InsertSettings(), chtypes.JSONCompactEachRow,
		chtypes.WithRowFilter(tbl.filterOn(s, expr, params)))
	require.NoError(t, err)
	require.Equal(t, chtypes.Rejected, res.Outcome)
	require.Positive(t, res.RowsPassed, "the trap: an admitted row is counted in a rejected batch")
	require.Empty(t, res.Payload)

	batch, err := tbl.Ingest(FormatJSONEachRow, body, preds...)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Rows)
	for i, r := range batch.Rows {
		assert.False(t, r.Accepted, "record %d", i)
		assert.True(t, r.Declined, "record %d", i)
		assert.Nil(t, r.Line, "record %d", i)
	}
}

// TestIngestChecks_OnTheWithNamesFormats: the header is not a record, so the
// verdicts line up with the data lines whatever order the header names the
// columns in.
func TestIngestChecks_OnTheWithNamesFormats(t *testing.T) {
	tbl := checksHandle(t)

	batch, err := tbl.Ingest(FormatCSVWithNames, []byte("tenant,id,kind\nacme,1,a\nevil,2,a\nacme,x,a\nacme,4,a\n"),
		Predicate{Column: "tenant", Op: "=", Values: []string{"acme"}})
	require.NoError(t, err)
	require.Len(t, batch.Rows, 4)
	assert.Equal(t, `[1, "acme", "a"]`, string(batch.Rows[0].Line))
	assert.Equal(t, ReasonFilter, batch.Rows[1].CheckReason)
	assert.Equal(t, 27, batch.Rows[2].Code)
	assert.Equal(t, `[4, "acme", "a"]`, string(batch.Rows[3].Line))
}

func TestIngest_UnavailableTable(t *testing.T) {
	eng := TestEngine(t, checksTable())
	eng.Bind(TestServerVersion, "UTC", nil)

	_, err := eng.Table("checks")
	require.Error(t, err)
	assert.True(t, IsUnavailable(err))
}

func TestIngest_RefusesAFormatItCannotParse(t *testing.T) {
	tbl := checksHandle(t)

	for _, f := range []Format{chtypes.JSONCompactEachRow, chtypes.RowBinary, chtypes.Native, Format(99)} {
		_, err := tbl.Ingest(f, []byte(`{"id":1}`+"\n"))
		require.Error(t, err, "format %d", int(f))
		assert.False(t, IsUnavailable(err), "a bad format is a programming error, not an outage")
		assert.Contains(t, err.Error(), "FormatJSONEachRow")
	}
}

// TestIngest_PositionalFormats: CSV and TSV are declaration-order positional
// with no header line, which is why the format has to be a parameter rather
// than a constant.
func TestIngest_PositionalFormats(t *testing.T) {
	tbl := checksHandle(t)

	csv, err := tbl.Ingest(FormatCSV, []byte("1,acme,a\n2,evil,z\n"))
	require.NoError(t, err)
	require.Len(t, csv.Rows, 2)
	require.True(t, csv.Rows[0].Accepted, csv.Rows[0].Message)
	assert.Equal(t, `[1, "acme", "a"]`, string(csv.Rows[0].Line))

	tsv, err := tbl.Ingest(FormatTSV, []byte("1\tacme\ta\n"))
	require.NoError(t, err)
	require.Len(t, tsv.Rows, 1)
	require.True(t, tsv.Rows[0].Accepted, tsv.Rows[0].Message)
	assert.Equal(t, `[1, "acme", "a"]`, string(tsv.Rows[0].Line))

	// A header line is one failed record with ClickHouse's own code 27, even
	// one that names every column. The server would consume that line as a
	// header (input_format_*_detect_header is on by default); parseSettings
	// switches detection off, because a guessed header eats a data row that
	// happens to spell the column names. The WithNames formats declare one.
	for name, body := range map[string][]byte{
		"csv": []byte("id,tenant,kind\n1,acme,a\n"),
		"tsv": []byte("id\ttenant\tkind\n1\tacme\ta\n"),
	} {
		format := FormatCSV
		if name == "tsv" {
			format = FormatTSV
		}
		header, err := tbl.Ingest(format, body)
		require.NoError(t, err)
		require.Len(t, header.Rows, 2, name)
		assert.False(t, header.Rows[0].Accepted, name)
		assert.Equal(t, 27, header.Rows[0].Code, name)
		assert.True(t, header.Rows[1].Accepted, name)
	}
}

// TestIngest_WithNamesFormats pins the header formats as measured on the 26.6
// artifact: the header line is not a record (Rows index the data lines), it
// names the columns in any order, a column it omits takes its DEFAULT, and a
// name the schema lacks or a repeated name refuses the body whole with
// ClickHouse's own 117.
func TestIngest_WithNamesFormats(t *testing.T) {
	tbl := checksHandle(t)

	for _, tc := range []struct {
		format Format
		sep    string
	}{{FormatCSVWithNames, ","}, {FormatTSVWithNames, "\t"}} {
		body := func(lines ...string) []byte {
			for i, l := range lines {
				lines[i] = strings.ReplaceAll(l, ",", tc.sep)
			}
			return []byte(strings.Join(lines, "\n") + "\n")
		}
		name := fmt.Sprintf("format %d", int(tc.format))

		b, err := tbl.Ingest(tc.format, body("kind,id,tenant", "a,1,acme", "z,x,acme", "b,3,evil"))
		require.NoError(t, err, name)
		require.Len(t, b.Rows, 3, "%s: the header is not a record", name)
		assert.Equal(t, `[1, "acme", "a"]`, string(b.Rows[0].Line), name)
		assert.Equal(t, 27, b.Rows[1].Code, name)
		assert.Equal(t, `[3, "evil", "b"]`, string(b.Rows[2].Line), name)

		b, err = tbl.Ingest(tc.format, body("id,tenant", "1,acme"))
		require.NoError(t, err, name)
		require.Len(t, b.Rows, 1, name)
		assert.Equal(t, `[1, "acme", ""]`, string(b.Rows[0].Line), "%s: an omitted column takes its DEFAULT", name)

		b, err = tbl.Ingest(tc.format, body("id,tenant,kind"))
		require.NoError(t, err, name)
		assert.Empty(t, b.Rows, "%s: a header alone is zero records", name)
		assert.Nil(t, b.Refused, name)

		for _, header := range []string{"id,tenant,kind,extra", "id,id,kind"} {
			b, err = tbl.Ingest(tc.format, body(header, "1,acme,a,z"))
			require.NoError(t, err, name)
			require.NotNil(t, b.Refused, "%s: %s", name, header)
			assert.Equal(t, 117, b.Refused.Code, "%s: %s", name, header)
			assert.NotEmpty(t, b.Refused.Message)
			assert.Empty(t, b.Rows)
		}
	}

	// Header names match case-insensitively from 26.5, as the server does.
	b, err := tbl.Ingest(FormatCSVWithNames, []byte("ID,Tenant,KIND\n1,acme,a\n"))
	require.NoError(t, err)
	require.Len(t, b.Rows, 1)
	assert.True(t, b.Rows[0].Accepted, b.Rows[0].Message)
}

// BenchmarkIngest_HandlePool is the standing evidence behind maxPoolSize (re-run it on deployment hardware).
// Three arms, identical work, only the concurrency and the handle differ:
//
//   - serial: one goroutine, one handle — the cost of a call with no contention
//   - parallel-shared: GOMAXPROCS goroutines, ONE handle
//   - parallel-pooled: GOMAXPROCS goroutines, the whole pool
//
// Read it this way: if parallel-shared's ns/op is not meaningfully better than
// serial's, RowsExport is not parallelizing at all and a pool of handles
// cannot help — which is what darwin shows, while Linux scales (see maxPoolSize).
// Only when parallel-shared is ~GOMAXPROCS× worse than serial does a pool have
// anything to win, and parallel-pooled is then the size of the win.
func BenchmarkIngest_HandlePool(b *testing.B) {
	eng := TestEngine(b, checksTable())
	tbl, err := eng.Table("checks")
	require.NoError(b, err)
	defer tbl.Release()

	var body strings.Builder
	for i := range 500 {
		body.WriteString(`{"id":` + strconv.Itoa(i) + `,"tenant":"acme","kind":"a"}` + "\n")
	}
	raw := []byte(body.String())

	// Identical work on both arms — only the handle choice differs.
	export := func(b *testing.B, pick func() *schemaSlot) {
		b.Helper()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := pick().schema.RowsExport(
					FormatJSONEachRow, raw, InsertSettings(), chtypes.JSONCompactEachRow); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	b.Run("serial", func(b *testing.B) {
		s := tbl.slots[0]
		b.ResetTimer()
		for range b.N {
			if _, err := s.schema.RowsExport(
				FormatJSONEachRow, raw, InsertSettings(), chtypes.JSONCompactEachRow); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel-shared", func(b *testing.B) {
		export(b, func() *schemaSlot { return tbl.slots[0] })
	})
	b.Run("parallel-pooled", func(b *testing.B) {
		export(b, tbl.slot)
	})
}
