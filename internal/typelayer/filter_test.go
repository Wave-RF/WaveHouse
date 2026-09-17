package typelayer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// rowsTable carries one column of each family the row filter has to compare,
// including the three (UInt8, Int64, Float32) where a typed parameter used to
// disagree with the server and a String parameter does not.
func rowsTable() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "rows",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt8", Position: 1},
			{Name: "tenant", Type: "String", Position: 2},
			{Name: "ts", Type: "DateTime", Position: 3},
			{Name: "amt", Type: "Decimal(10,2)", Position: 4},
			{Name: "big", Type: "Int64", Position: 5},
			{Name: "ratio", Type: "Float32", Position: 6},
			{Name: "tags", Type: "Array(String)", Position: 7},
		},
	}
}

const sampleRow = `[7, "acme", "2026-01-15 10:30:00", "12.50", -5, 0.1, ["a"]]`

func parsedRow(t *testing.T) (*Table, *Row) {
	t.Helper()
	eng := TestEngine(t, rowsTable())
	tbl, err := eng.Table("rows")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)

	row, err := tbl.ParseRow(tbl.WireColumns, []byte(sampleRow))
	require.NoError(t, err)
	t.Cleanup(row.Close)
	return tbl, row
}

// TestVisible_StringBindingAcrossColumnFamilies is the pin for the whole
// §C.1 decision: every value binds as {pN:String} and the answer still matches
// the SQL path on every operator and every column family. The row is
// id=7, tenant="acme", ts=2026-01-15 10:30:00, amt=12.50, big=-5, ratio=0.1.
func TestVisible_StringBindingAcrossColumnFamilies(t *testing.T) {
	_, row := parsedRow(t)

	cases := []struct {
		name string
		pred Predicate
		want bool
	}{
		{"string equal", Predicate{Column: "tenant", Op: "=", Values: []string{"acme"}}, true},
		{"string equal miss", Predicate{Column: "tenant", Op: "=", Values: []string{"beta"}}, false},
		{"string not equal", Predicate{Column: "tenant", Op: "!=", Values: []string{"beta"}}, true},
		{"string not equal self", Predicate{Column: "tenant", Op: "!=", Values: []string{"acme"}}, false},
		{"string greater", Predicate{Column: "tenant", Op: ">", Values: []string{"aaa"}}, true},
		{"string less", Predicate{Column: "tenant", Op: "<", Values: []string{"aaa"}}, false},
		{"string in", Predicate{Column: "tenant", Op: "in", Values: []string{"acme", "beta"}}, true},
		{"string in miss", Predicate{Column: "tenant", Op: "in", Values: []string{"beta", "gamma"}}, false},

		{"uint equal", Predicate{Column: "id", Op: "=", Values: []string{"7"}}, true},
		{"uint not equal", Predicate{Column: "id", Op: "!=", Values: []string{"8"}}, true},
		{"uint greater", Predicate{Column: "id", Op: ">", Values: []string{"3"}}, true},
		{"uint less", Predicate{Column: "id", Op: "<", Values: []string{"3"}}, false},
		{"uint in", Predicate{Column: "id", Op: "in", Values: []string{"1", "7"}}, true},

		{"int64 equal", Predicate{Column: "big", Op: "=", Values: []string{"-5"}}, true},
		{"int64 not equal", Predicate{Column: "big", Op: "!=", Values: []string{"-5"}}, false},
		{"int64 greater", Predicate{Column: "big", Op: ">", Values: []string{"-6"}}, true},
		{"int64 less", Predicate{Column: "big", Op: "<", Values: []string{"-6"}}, false},
		{"int64 in", Predicate{Column: "big", Op: "in", Values: []string{"-5", "0"}}, true},

		// The #381 storage-narrowing case: a Float32 column's 0.1 is
		// 0.100000001490116…, so a constant widened to Float64 would NOT equal
		// it and the stream would then admit `!= '0.1'` on a row /v1/query
		// hides. A String parameter is read in the column's own domain.
		{"float32 equal", Predicate{Column: "ratio", Op: "=", Values: []string{"0.1"}}, true},
		{"float32 not equal", Predicate{Column: "ratio", Op: "!=", Values: []string{"0.1"}}, false},
		{"float32 greater", Predicate{Column: "ratio", Op: ">", Values: []string{"0.05"}}, true},
		{"float32 less", Predicate{Column: "ratio", Op: "<", Values: []string{"0.05"}}, false},
		{"float32 in", Predicate{Column: "ratio", Op: "in", Values: []string{"0.1", "2"}}, true},

		{"decimal equal", Predicate{Column: "amt", Op: "=", Values: []string{"12.50"}}, true},
		{"decimal equal shorter spelling", Predicate{Column: "amt", Op: "=", Values: []string{"12.5"}}, true},
		{"decimal not equal", Predicate{Column: "amt", Op: "!=", Values: []string{"12.49"}}, true},
		{"decimal greater", Predicate{Column: "amt", Op: ">", Values: []string{"12.49"}}, true},
		{"decimal less", Predicate{Column: "amt", Op: "<", Values: []string{"12.49"}}, false},
		{"decimal in", Predicate{Column: "amt", Op: "in", Values: []string{"12.50", "1"}}, true},

		{"datetime equal", Predicate{Column: "ts", Op: "=", Values: []string{"2026-01-15 10:30:00"}}, true},
		{"datetime not equal", Predicate{Column: "ts", Op: "!=", Values: []string{"2026-01-15 10:30:00"}}, false},
		{"datetime greater", Predicate{Column: "ts", Op: ">", Values: []string{"2026-01-01 00:00:00"}}, true},
		{"datetime less", Predicate{Column: "ts", Op: "<", Values: []string{"2026-01-01 00:00:00"}}, false},
		{"datetime in", Predicate{Column: "ts", Op: "in", Values: []string{"2026-01-15 10:30:00"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, row.Visible([]Predicate{tc.pred}))
		})
	}
}

// TestVisible_HostileSpellingsMatchTheServer pins the spellings the old typed
// binding got wrong, measured against a live 26.3 server in AUDIT §C.1. A
// value outside a sub-64-bit column's domain must NOT admit on any operator
// (binding through UInt64 made `7 < '256'` true, which let the stream show a
// row /v1/query hides), and a spelling the column cannot read at all is the
// server's own code 53 at evaluation time, which withholds.
func TestVisible_HostileSpellingsMatchTheServer(t *testing.T) {
	_, row := parsedRow(t)

	answered := []struct {
		pred Predicate
		want bool
	}{
		{Predicate{Column: "id", Op: "=", Values: []string{"256"}}, false},
		{Predicate{Column: "id", Op: "!=", Values: []string{"256"}}, true},
		{Predicate{Column: "id", Op: "<", Values: []string{"256"}}, false},
		{Predicate{Column: "id", Op: "<", Values: []string{"300"}}, false},
		{Predicate{Column: "id", Op: ">", Values: []string{"300"}}, false},
	}
	for _, tc := range answered {
		got, reason := row.VisibleWithReason([]Predicate{tc.pred})
		assert.Equal(t, tc.want, got, "%s %s %v", tc.pred.Column, tc.pred.Op, tc.pred.Values)
		assert.NotEqual(t, ReasonError, reason, "an out-of-domain constant is answered, not thrown")
	}

	// Spellings the column's reader refuses: ClickHouse code 53, reported as a
	// per-row error rather than a false, so the metric can tell them apart.
	for _, v := range []string{"-1", "1.5", "007", "abc", ""} {
		got, reason := row.VisibleWithReason([]Predicate{{Column: "id", Op: "=", Values: []string{v}}})
		assert.False(t, got, "id = %q", v)
		assert.Equal(t, ReasonError, reason, "id = %q", v)
	}
}

// storedRow parses one row whose `tenant` column holds the given value, with
// every other column at sampleRow's value.
func storedRow(t *testing.T, tbl *Table, tenant string) *Row {
	t.Helper()
	line, err := json.Marshal([]any{7, tenant, "2026-01-15 10:30:00", "12.50", -5, 0.1, []string{"a"}})
	require.NoError(t, err)
	row, err := tbl.ParseRow(tbl.WireColumns, line)
	require.NoError(t, err)
	t.Cleanup(row.Close)
	return row
}

// TestVisible_EscapedStringParamsMatchTheStoredValue pins the shared
// {p:String} encoding (chsql.EscapeStringParam) on the artifact.
//
// Measured on the 26.6 artifact BEFORE the encoding was applied: a stored
// `a\b` compared FALSE against the raw claim value (the artifact's parameter
// reader is ClickHouse's escaped-text reader, so `\b` arrived as a backspace),
// and a stored tab, newline or trailing backslash made the filter fail to
// compile at all — a decline, every row withheld. The SQL path answered true
// for all of them, so the two read surfaces disagreed. With the encoding they
// agree.
func TestVisible_EscapedStringParamsMatchTheStoredValue(t *testing.T) {
	eng := TestEngine(t, rowsTable())
	tbl, err := eng.Table("rows")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)

	cases := []struct {
		name   string
		stored string
	}{
		{"embedded backslash", `a\b`},
		{"tab", "a\tb"},
		{"newline", "a\nb"},
		{"carriage return", "a\rb"},
		{"single quote", "O'Brien"},
		{"trailing backslash", `trail\`},
		{"backslash then quote", `a\'b`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := storedRow(t, tbl, tc.stored)

			got, reason := row.VisibleWithReason([]Predicate{{Column: "tenant", Op: "=", Values: []string{tc.stored}}})
			assert.True(t, got, "tenant = %q withheld (reason %q)", tc.stored, reason)

			got, reason = row.VisibleWithReason([]Predicate{{Column: "tenant", Op: "in", Values: []string{"other", tc.stored}}})
			assert.True(t, got, "tenant in (…, %q) withheld (reason %q)", tc.stored, reason)

			got, _ = row.VisibleWithReason([]Predicate{{Column: "tenant", Op: "=", Values: []string{tc.stored + "x"}}})
			assert.False(t, got, "tenant = %q must not match", tc.stored+"x")
		})
	}

	// The encoding has to DISTINGUISH, not merely admit: the three characters
	// `a\tb` and the three bytes "a<TAB>b" are different stored values, and each
	// must match only its own filter. Unencoded, both filters read as the tab.
	t.Run("a literal backslash-t is not a tab", func(t *testing.T) {
		literal := storedRow(t, tbl, `a\tb`)
		assert.True(t, literal.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{`a\tb`}}}))
		assert.False(t, literal.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"a\tb"}}}))

		tab := storedRow(t, tbl, "a\tb")
		assert.True(t, tab.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"a\tb"}}}))
		assert.False(t, tab.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{`a\tb`}}}))
	})
}

// TestVisible_PredicatesAreANDed matches the SQL path's AND-joined WHERE.
func TestVisible_PredicatesAreANDed(t *testing.T) {
	_, row := parsedRow(t)

	assert.True(t, row.Visible([]Predicate{
		{Column: "tenant", Op: "=", Values: []string{"acme"}},
		{Column: "id", Op: "=", Values: []string{"7"}},
	}))
	assert.False(t, row.Visible([]Predicate{
		{Column: "tenant", Op: "=", Values: []string{"acme"}},
		{Column: "id", Op: "=", Values: []string{"8"}},
	}))
}

func TestVisible_NoPredicatesIsVisible(t *testing.T) {
	_, row := parsedRow(t)
	assert.True(t, row.Visible(nil))
}

// TestVisible_FailsClosed: every shape typelayer cannot answer for must hide
// the row. A row filter that widens on a bad input is the leak class this
// package exists to remove.
func TestVisible_FailsClosed(t *testing.T) {
	_, row := parsedRow(t)

	t.Run("unknown column", func(t *testing.T) {
		before := row.slot.filters.len()
		assert.False(t, row.Visible([]Predicate{{Column: "nosuch", Op: "=", Values: []string{"x"}}}))
		assert.Equal(t, before, row.slot.filters.len(), "an unknown column must not reach the compiler")
	})

	t.Run("empty values", func(t *testing.T) {
		before := row.slot.filters.len()
		assert.False(t, row.Visible([]Predicate{{Column: "tenant", Op: "in", Values: nil}}))
		assert.Equal(t, before, row.slot.filters.len(), "an unresolvable claim must not reach the compiler")
	})

	t.Run("unsupported operator", func(t *testing.T) {
		assert.False(t, row.Visible([]Predicate{{Column: "tenant", Op: "LIKE", Values: []string{"ac%"}}}))
	})

	t.Run("value the column cannot read", func(t *testing.T) {
		// "abc" is not a UInt8. A String parameter compiles, so the refusal is
		// the server's own per-row error rather than a compile failure.
		assert.False(t, row.Visible([]Predicate{{Column: "id", Op: "=", Values: []string{"abc"}}}))
	})

	t.Run("type mismatch against the column", func(t *testing.T) {
		// A String parameter compared with an Array column has no supertype.
		assert.False(t, row.Visible([]Predicate{{Column: "tags", Op: "=", Values: []string{"a"}}}))
	})
}

func TestVisibleWithReason_LabelsTheCause(t *testing.T) {
	tbl, row := parsedRow(t)

	ok, reason := row.VisibleWithReason([]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}})
	assert.True(t, ok)
	assert.Empty(t, reason)

	ok, reason = row.VisibleWithReason([]Predicate{{Column: "tenant", Op: "=", Values: []string{"beta"}}})
	assert.False(t, ok)
	assert.Equal(t, ReasonFilter, reason)

	ok, reason = row.VisibleWithReason([]Predicate{{Column: "id", Op: "=", Values: []string{"abc"}}})
	assert.False(t, ok)
	assert.Equal(t, ReasonError, reason, "a value the column's reader refuses throws on the row")

	ok, reason = row.VisibleWithReason([]Predicate{{Column: "nosuch", Op: "=", Values: []string{"x"}}})
	assert.False(t, ok)
	assert.Equal(t, ReasonFilter, reason, "an unexpressible predicate matches nothing, like the SQL path's 1 = 0")

	// A filter that will not compile is the decline case; render never emits
	// one, so it is reached through the compiler directly.
	assert.Nil(t, tbl.filterOn(row.slot, "notAFunction(`tenant`) = {p0:String}", map[string]string{"p0": "x"}))
}

// TestParseRow_ColumnsDriftIsAnError: a positional row is only interpretable
// against the generation that produced it.
func TestParseRow_ColumnsDriftIsAnError(t *testing.T) {
	eng := TestEngine(t, rowsTable())
	tbl, err := eng.Table("rows")
	require.NoError(t, err)
	defer tbl.Release()

	_, err = tbl.ParseRow([]string{"id", "tenant"}, []byte(sampleRow))
	require.ErrorIs(t, err, ErrColumnsDrift)

	reordered := append([]string(nil), tbl.WireColumns...)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	_, err = tbl.ParseRow(reordered, []byte(sampleRow))
	require.ErrorIs(t, err, ErrColumnsDrift, "same names in a different order is still drift")
}

func TestParseRow_AcceptsALineWithOrWithoutNewline(t *testing.T) {
	eng := TestEngine(t, rowsTable())
	tbl, err := eng.Table("rows")
	require.NoError(t, err)
	defer tbl.Release()

	for _, body := range []string{sampleRow, sampleRow + "\n"} {
		row, err := tbl.ParseRow(tbl.WireColumns, []byte(body))
		require.NoError(t, err)
		assert.True(t, row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}}))
		row.Close()
	}
}

// TestFilterCache_CompilesOncePerPredicateSet: the cache is what keeps a
// per-subscriber fan-out from recompiling on every event. One Row stays on one
// handle, so the count is that slot's.
func TestFilterCache_CompilesOncePerPredicateSet(t *testing.T) {
	_, row := parsedRow(t)
	require.Equal(t, 0, row.slot.filters.len())

	pred := []Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}}
	for range 5 {
		assert.True(t, row.Visible(pred))
	}
	assert.Equal(t, 1, row.slot.filters.len())

	// A different bound value is a different compiled handle: values are baked
	// in at compile time.
	assert.False(t, row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"beta"}}}))
	assert.Equal(t, 2, row.slot.filters.len())
}

// TestFilterCache_NegativeEntryStopsRecompiling: a broken expression must cost
// one compile and one log line, not one per event.
func TestFilterCache_NegativeEntryStopsRecompiling(t *testing.T) {
	tbl, row := parsedRow(t)
	for range 3 {
		assert.Nil(t, tbl.filterOn(row.slot, "notAFunction(`tenant`) = {p0:String}", map[string]string{"p0": "x"}))
	}
	assert.Equal(t, 1, row.slot.filters.len())
}

// TestFilterCache_BoundedUnderTenantValueChurn: the bound values come from
// tenant claims, so an unbounded cache is a denial of service. Eviction closes
// the handle it drops, against the real library.
func TestFilterCache_BoundedUnderTenantValueChurn(t *testing.T) {
	_, row := parsedRow(t)
	row.slot.filters = newFilterCache(8)

	for i := range 40 {
		pred := []Predicate{{Column: "tenant", Op: "=", Values: []string{strconv.Itoa(i)}}}
		assert.False(t, row.Visible(pred))
	}
	assert.Equal(t, 8, row.slot.filters.len())

	// The surviving handles still answer after their neighbours were closed.
	assert.True(t, row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}}))
}

// TestFilterCache_BudgetIsSplitAcrossTheHandlePool: the 4096 bound is what
// makes the cache not a DoS, and it must be a bound on the TABLE — a pool of
// handles must not multiply it.
func TestFilterCache_BudgetIsSplitAcrossTheHandlePool(t *testing.T) {
	eng := TestEngine(t, rowsTable())
	tbl, err := eng.Table("rows")
	require.NoError(t, err)
	defer tbl.Release()

	total := 0
	for _, s := range tbl.slots {
		total += s.filters.cap
	}
	assert.LessOrEqual(t, total, filterCacheSize)
	assert.Equal(t, poolSize(), len(tbl.slots))
}

// TestFilterCache_GenerationInvalidatesEntries: a filter only answers for the
// schema handle it was compiled against, so a rebind must not reuse one.
func TestFilterCache_GenerationInvalidatesEntries(t *testing.T) {
	eng := TestEngine(t, rowsTable())

	visible := func() bool {
		tbl, err := eng.Table("rows")
		require.NoError(t, err)
		defer tbl.Release()
		row, err := tbl.ParseRow(tbl.WireColumns, []byte(sampleRow))
		require.NoError(t, err)
		defer row.Close()
		return row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}})
	}
	assert.True(t, visible())

	// Widen a column: the signature changes, so the handle and every filter
	// over it are rebuilt, while the wire arity stays the same.
	changed := rowsTable()
	changed.Columns[0].Type = "UInt16"
	eng.Bind(TestServerVersion, "UTC", []*discovery.TableSchema{changed})
	assert.True(t, visible(), "a rebind recompiles rather than reusing a freed handle")
}

func (c *filterCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// TestVisible_ConcurrentSubscribers: the hub evaluates K filters against one
// parsed block; one LoadedSchema serializes them internally, so this must be
// safe rather than merely fast.
func TestVisible_ConcurrentSubscribers(t *testing.T) {
	_, row := parsedRow(t)

	done := make(chan bool, 16)
	for i := range 16 {
		go func() {
			done <- row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{fmt.Sprintf("t%d", i%4)}}})
		}()
	}
	for range 16 {
		assert.False(t, <-done)
	}
}

// TestTable_ConcurrentAcrossThePool exercises every entry point on one table
// from many goroutines at once. Under -race it is the pin that the pool's
// round-robin, the per-slot filter caches and the shared block parsing are
// safe; a slot chosen per call rather than per Row would show up here as a
// filter and a block on different handles.
func TestTable_ConcurrentAcrossThePool(t *testing.T) {
	eng := TestEngine(t, rowsTable())

	const goroutines = 16
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20 {
				tbl, err := eng.Table("rows")
				if !assert.NoError(t, err) {
					return
				}

				row, err := tbl.ParseRow(tbl.WireColumns, []byte(sampleRow))
				if assert.NoError(t, err) {
					assert.True(t, row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}}))
					assert.False(t, row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{fmt.Sprintf("t%d", (g+i)%4)}}}))
					row.Close()
				}

				batch, err := tbl.Ingest(FormatJSONEachRow,
					[]byte(`{"id":7,"tenant":"acme","ts":"2026-01-15 10:30:00","amt":"12.50","big":-5,"ratio":0.1,"tags":["a"]}`+"\n"))
				assert.NoError(t, err)
				assert.Len(t, batch.Rows, 1)

				verdicts, _, err := tbl.CheckVerdicts([]byte(sampleRow+"\n"),
					[]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}})
				assert.NoError(t, err)
				assert.Equal(t, []bool{true}, verdicts)

				tbl.Release()
			}
		}()
	}
	wg.Wait()
}
