package typelayer

import (
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

const checksPayload = `[1, "acme", "a"]` + "\n" +
	`[2, "acme", "a"]` + "\n" +
	`[3, "evil", "a"]` + "\n" +
	`[4, "acme", "z"]` + "\n"

func checksHandle(t *testing.T) *Table {
	t.Helper()
	eng := TestEngine(t, checksTable())
	tbl, err := eng.Table("checks")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)
	return tbl
}

// TestCheckVerdicts_MatchesTheMeasuredTable reproduces AUDIT §A.2's measured
// answers: one AND-joined filter over one parse, verdicts index-aligned with
// the exported rows.
func TestCheckVerdicts_MatchesTheMeasuredTable(t *testing.T) {
	tbl := checksHandle(t)

	cases := []struct {
		name  string
		preds []Predicate
		want  []bool
	}{
		{
			"tenant equals",
			[]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}},
			[]bool{true, true, false, true},
		},
		{
			"kind in",
			[]Predicate{{Column: "kind", Op: "in", Values: []string{"a", "b"}}},
			[]bool{true, true, true, false},
		},
		{
			"both, AND-joined",
			[]Predicate{
				{Column: "tenant", Op: "=", Values: []string{"acme"}},
				{Column: "kind", Op: "in", Values: []string{"a"}},
			},
			[]bool{true, true, false, false},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verdicts, reasons, err := tbl.CheckVerdicts([]byte(checksPayload), tc.preds)
			require.NoError(t, err)
			assert.Equal(t, tc.want, verdicts)
			require.Len(t, reasons, len(tc.want))
			for i, ok := range tc.want {
				if ok {
					assert.Empty(t, reasons[i], "row %d", i)
				} else {
					assert.Equal(t, ReasonFilter, reasons[i], "row %d", i)
				}
			}
		})
	}
}

// TestCheckVerdicts_NoPredicatesPassesEveryRow: a role with no check clauses
// pays nothing and hides nothing.
func TestCheckVerdicts_NoPredicatesPassesEveryRow(t *testing.T) {
	tbl := checksHandle(t)

	verdicts, reasons, err := tbl.CheckVerdicts([]byte(checksPayload), nil)
	require.NoError(t, err)
	assert.Equal(t, []bool{true, true, true, true}, verdicts)
	assert.Equal(t, []string{"", "", "", ""}, reasons)
}

func TestCheckVerdicts_EmptyPayload(t *testing.T) {
	tbl := checksHandle(t)

	verdicts, reasons, err := tbl.CheckVerdicts(nil, []Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}})
	require.NoError(t, err)
	assert.Empty(t, verdicts)
	assert.Empty(t, reasons)
}

// TestCheckVerdicts_FailsClosed: everything that is not a definite true
// withholds, and an unresolvable claim never reaches the compiler.
func TestCheckVerdicts_FailsClosed(t *testing.T) {
	tbl := checksHandle(t)
	slotsBefore := func() int {
		n := 0
		for _, s := range tbl.slots {
			n += s.filters.len()
		}
		return n
	}

	t.Run("empty values", func(t *testing.T) {
		before := slotsBefore()
		verdicts, reasons, err := tbl.CheckVerdicts([]byte(checksPayload),
			[]Predicate{
				{Column: "tenant", Op: "=", Values: []string{"acme"}},
				{Column: "kind", Op: "in", Values: nil},
			})
		require.NoError(t, err)
		assert.Equal(t, []bool{false, false, false, false}, verdicts)
		assert.Equal(t, []string{ReasonFilter, ReasonFilter, ReasonFilter, ReasonFilter}, reasons)
		assert.Equal(t, before, slotsBefore(), "an unresolvable claim must not reach the compiler")
	})

	t.Run("unknown column", func(t *testing.T) {
		verdicts, _, err := tbl.CheckVerdicts([]byte(checksPayload),
			[]Predicate{{Column: "nosuch", Op: "=", Values: []string{"x"}}})
		require.NoError(t, err)
		assert.Equal(t, []bool{false, false, false, false}, verdicts)
	})

	t.Run("value the column cannot read", func(t *testing.T) {
		verdicts, reasons, err := tbl.CheckVerdicts([]byte(checksPayload),
			[]Predicate{{Column: "id", Op: "=", Values: []string{"abc"}}})
		require.NoError(t, err)
		assert.Equal(t, []bool{false, false, false, false}, verdicts)
		assert.Equal(t, []string{ReasonError, ReasonError, ReasonError, ReasonError}, reasons,
			"a thrown predicate is 'we could not tell' (422), not 'the data says no' (403)")
	})
}

// TestCheckVerdicts_AlignsWithTheACCEPTEDRows is the index-alignment contract
// the ingest path depends on: the payload holds only the records that parsed,
// so verdict i belongs to the i-th ACCEPTED record, not the i-th input record.
func TestCheckVerdicts_AlignsWithTheACCEPTEDRows(t *testing.T) {
	tbl := checksHandle(t)

	batch, err := tbl.Ingest(FormatJSONEachRow, []byte(strings.Join([]string{
		`{"id":1,"tenant":"acme","kind":"a"}`,
		`{"id":"not-a-number","tenant":"acme","kind":"a"}`,
		`{"id":3,"tenant":"evil","kind":"a"}`,
	}, "\n")+"\n"))
	require.NoError(t, err)
	require.Len(t, batch.Rows, 3)
	require.True(t, batch.Rows[0].Accepted)
	require.False(t, batch.Rows[1].Accepted, "the middle record must not parse")
	require.True(t, batch.Rows[2].Accepted)
	require.NotEmpty(t, batch.Payload)

	verdicts, _, err := tbl.CheckVerdicts(batch.Payload,
		[]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}})
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false}, verdicts, "two accepted rows, in accepted order")
}

func TestCheckVerdicts_UnavailableTable(t *testing.T) {
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

	// A header line is one skipped record with ClickHouse's own code 27 — the
	// reason chtypes FR I‑3 asks for CSVWithNames.
	header, err := tbl.Ingest(FormatCSV, []byte("id,tenant,kind\n1,acme,a\n"))
	require.NoError(t, err)
	require.Len(t, header.Rows, 2)
	assert.False(t, header.Rows[0].Accepted)
	assert.Equal(t, 27, header.Rows[0].Code)
	assert.True(t, header.Rows[1].Accepted)
}

// BenchmarkIngest_HandlePool is the standing evidence behind maxPoolSize.
// Three arms, identical work, only the concurrency and the handle differ:
//
//   - serial: one goroutine, one handle — the cost of a call with no contention
//   - parallel-shared: GOMAXPROCS goroutines, ONE handle
//   - parallel-pooled: GOMAXPROCS goroutines, the whole pool
//
// Read it this way: if parallel-shared's ns/op is not meaningfully better than
// serial's, RowsExport is not parallelizing at all and a pool of handles
// cannot help — which is what was measured on 2026-09-16 (see maxPoolSize).
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
