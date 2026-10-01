package typelayer

import (
	"encoding/json"
	"fmt"
	"math/big"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
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
	eng := testEngine(t, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)

	row, err := tbl.ParseRow(tbl.WireColumns, []byte(sampleRow))
	require.NoError(t, err)
	t.Cleanup(row.Close)
	return tbl, row
}

// TestVisible_StringBindingAcrossColumnFamilies is the pin for the String
// binding: every value binds as {pN:String} and the answer still matches
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

// TestVisible_HostileSpellingsMatchNothing: on an integer column a claim is
// compared through the strict cast, so a value outside the column's domain, or
// a spelling that is not its canonical form, matches nothing on EVERY operator
// — `!=` included — and is answered false rather than thrown. On a
// non-integer column the String binding is unchanged: a spelling the column's
// reader refuses is still the server's own error (code 72 on this Float32
// column), which withholds as an error.
func TestVisible_HostileSpellingsMatchNothing(t *testing.T) {
	_, row := parsedRow(t)

	for _, v := range []string{"256", "300", "-1", "1.5", "007", "+7", "7.0", "1e3", "abc", "", " 7", "18446744073709551623"} {
		for _, op := range []string{"=", "!=", "<", ">", "in"} {
			got, reason := row.VisibleWithReason([]Predicate{{Column: "id", Op: op, Values: []string{v}}})
			assert.False(t, got, "id %s %q", op, v)
			assert.Equal(t, ReasonFilter, reason, "id %s %q: answered, not thrown", op, v)
		}
	}

	for _, v := range []string{"abc", "1.5.5"} {
		got, reason := row.VisibleWithReason([]Predicate{{Column: "ratio", Op: "=", Values: []string{v}}})
		assert.False(t, got, "ratio = %q", v)
		assert.Equal(t, ReasonError, reason, "ratio = %q", v)
	}
}

// intsTable holds one column per integer width the strict cast has to cover.
func intsTable() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "ints",
		Columns: []discovery.Column{
			{Name: "u8", Type: "UInt8", Position: 1},
			{Name: "u32", Type: "UInt32", Position: 2},
			{Name: "u64", Type: "UInt64", Position: 3},
			{Name: "i64", Type: "Int64", Position: 4},
			{Name: "u128", Type: "UInt128", Position: 5},
			{Name: "i128", Type: "Int128", Position: 6},
			{Name: "u256", Type: "UInt256", Position: 7},
			{Name: "i256", Type: "Int256", Position: 8},
			{Name: "nu64", Type: "Nullable(UInt64)", IsNullable: true, Position: 9},
		},
	}
}

// intDomain is a column's [min, max].
func intDomain(typ string) (*big.Int, *big.Int) {
	bits := map[string]uint{"8": 8, "32": 32, "64": 64, "128": 128, "256": 256}
	pow := func(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }
	signed := strings.HasPrefix(typ, "Int")
	n := bits[strings.TrimPrefix(strings.TrimPrefix(typ, "U"), "Int")]
	if signed {
		return new(big.Int).Neg(pow(n - 1)), new(big.Int).Sub(pow(n-1), big.NewInt(1))
	}
	return big.NewInt(0), new(big.Int).Sub(pow(n), big.NewInt(1))
}

// TestVisible_IntegerClaimsMatchExactlyWhatFits drives every integer width
// with the boundary claims that used to wrap (2^63, 2^64, 2^64+5, 2^127,
// 2^128, 2^255, 2^256, 2^256+5, their negatives) and the non-canonical
// spellings, on every operator, against rows holding 0, 5, the column's MIN
// and MAX (and NULL). The answer must be the mathematical one when the claim
// is the canonical spelling of a value the column can hold, and false
// otherwise — never an over-admit, and never a thrown row.
func TestVisible_IntegerClaimsMatchExactlyWhatFits(t *testing.T) {
	eng := testEngine(t, intsTable())
	tbl, err := eng.Table(tenant.Default, "ints")
	require.NoError(t, err)
	t.Cleanup(tbl.Release)

	pow := func(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }
	add := func(a *big.Int, d int64) *big.Int { return new(big.Int).Add(a, big.NewInt(d)) }
	neg := func(a *big.Int) *big.Int { return new(big.Int).Neg(a) }
	claims := []string{
		"0", "5", "-1", "255", "256", "4294967295", "4294967296",
		"007", "+5", "1.5", "5.0", "1e3", "abc", "", "-0",
	}
	for _, n := range []*big.Int{
		pow(63), add(neg(pow(63)), -1), neg(pow(63)), add(pow(63), -1),
		pow(64), add(pow(64), -1), add(pow(64), 5), pow(127), add(neg(pow(127)), -1), add(pow(127), -1),
		pow(128), add(pow(128), -1), pow(255), add(pow(255), -1), neg(pow(255)), pow(256), add(pow(256), -1), add(pow(256), 5),
	} {
		claims = append(claims, n.String())
	}

	cols := intsTable().Columns
	type stored struct {
		vals map[string]*big.Int // nil value: NULL
		row  *Row
	}
	var rows []stored
	for _, label := range []string{"0", "5", "MIN", "MAX"} {
		vals := map[string]*big.Int{}
		line := make([]any, len(cols))
		for i, c := range cols {
			base := strings.TrimSuffix(strings.TrimPrefix(c.Type, "Nullable("), ")")
			lo, hi := intDomain(base)
			var v *big.Int
			switch label {
			case "0":
				v = big.NewInt(0)
			case "5":
				v = big.NewInt(5)
			case "MIN":
				v = lo
			case "MAX":
				v = hi
			}
			if c.IsNullable && label == "MIN" {
				line[i], vals[c.Name] = nil, nil
				continue
			}
			line[i], vals[c.Name] = v.String(), v
		}
		b, err := json.Marshal(line)
		require.NoError(t, err)
		row, err := tbl.ParseRow(tbl.WireColumns, b)
		require.NoError(t, err)
		t.Cleanup(row.Close)
		rows = append(rows, stored{vals: vals, row: row})
	}

	cells := 0
	for _, c := range cols {
		base := strings.TrimSuffix(strings.TrimPrefix(c.Type, "Nullable("), ")")
		lo, hi := intDomain(base)
		for _, claim := range claims {
			v, isInt := new(big.Int).SetString(claim, 10)
			fits := isInt && v.String() == claim && v.Cmp(lo) >= 0 && v.Cmp(hi) <= 0
			for _, op := range []string{"=", "!=", "<", ">", "in"} {
				for _, r := range rows {
					cells++
					stored := r.vals[c.Name]
					want := false
					if fits && stored != nil {
						cmp := stored.Cmp(v)
						want = map[string]bool{"=": cmp == 0, "in": cmp == 0, "!=": cmp != 0, "<": cmp < 0, ">": cmp > 0}[op]
					}
					got, reason := r.row.VisibleWithReason([]Predicate{{Column: c.Name, Op: op, Values: []string{claim}}})
					if got != want || (!got && reason != ReasonFilter) {
						t.Errorf("%s(%v) %s %q: got %v (%s), want %v", c.Type, stored, op, claim, got, reason, want)
					}
				}
			}
		}
	}

	// A multi-element _in list keeps the elements that fit and drops the rest,
	// element by element: 2^64+5 must not wrap onto the row holding 5.
	for _, c := range cols {
		for _, r := range rows {
			stored := r.vals[c.Name]
			want := stored != nil && stored.Sign() == 0
			got := r.row.Visible([]Predicate{{
				Column: c.Name, Op: "in",
				Values: []string{add(pow(64), 5).String(), "007", "0", add(pow(256), 5).String(), "abc"},
			}})
			assert.Equal(t, want, got, "%s(%v) IN (2^64+5, 007, 0, 2^256+5, abc)", c.Type, stored)
		}
	}
	t.Logf("%d cells", cells)
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
	eng := testEngine(t, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
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
		// "abc" is not a UInt8 (the strict cast makes it NULL) nor a Float32
		// (the server's own per-row error). Neither is a compile failure.
		assert.False(t, row.Visible([]Predicate{{Column: "id", Op: "=", Values: []string{"abc"}}}))
		assert.False(t, row.Visible([]Predicate{{Column: "ratio", Op: "=", Values: []string{"abc"}}}))
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

	ok, reason = row.VisibleWithReason([]Predicate{{Column: "ratio", Op: "=", Values: []string{"abc"}}})
	assert.False(t, ok)
	assert.Equal(t, ReasonError, reason, "a value the column's reader refuses throws on the row")

	ok, reason = row.VisibleWithReason([]Predicate{{Column: "id", Op: "=", Values: []string{"abc"}}})
	assert.False(t, ok)
	assert.Equal(t, ReasonFilter, reason, "an integer claim that does not fit is answered false")

	ok, reason = row.VisibleWithReason([]Predicate{{Column: "nosuch", Op: "=", Values: []string{"x"}}})
	assert.False(t, ok)
	assert.Equal(t, ReasonFilter, reason, "an unexpressible predicate matches nothing, like the SQL path's 1 = 0")

	// A filter that will not compile is the decline case; render never emits
	// one, so it is reached through the compiler directly.
	assert.Nil(t, tbl.filterOn(row.slot, "notAFunction(`tenant`) = {p0:String}", map[string]string{"p0": "x"}))
}

// TestParseRow_ColumnsDriftIsAnError: a column list no INSERT into this
// generation could name is drift, not a parse attempt. A row whose list is
// fine but whose values do not fit it is not drift: the block holds the row's
// refusal and every predicate over it withholds (measured on the 26.6
// artifact: ParseBlock reports no call-level error for a malformed row).
func TestParseRow_ColumnsDriftIsAnError(t *testing.T) {
	eng := testEngine(t, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
	require.NoError(t, err)
	defer tbl.Release()

	for name, cols := range map[string][]string{
		"unknown column":       {"id", "dropped"},
		"case differs":         {"id", "Tenant"},
		"repeated column":      {"id", "id"},
		"empty list":           {},
		"nil list":             nil,
		"full list, one extra": append(append([]string(nil), tbl.WireColumns...), "extra"),
	} {
		_, err = tbl.ParseRow(cols, []byte(`[7, "x"]`))
		require.ErrorIs(t, err, ErrColumnsDrift, name)
	}

	row, err := tbl.ParseRow([]string{"id", "tenant"}, []byte(sampleRow))
	require.NoError(t, err, "a valid list with a bad row is not drift")
	defer row.Close()
	ok, reason := row.VisibleWithReason([]Predicate{{Column: "id", Op: "=", Values: []string{"7"}}})
	assert.False(t, ok, "seven fields under a two-column list do not parse, so nothing matches")
	assert.Equal(t, ReasonDecline, reason)
}

// defaultsTable has the column kinds an INSERT column list interacts with: a
// literal DEFAULT, a Nullable column with a DEFAULT, and a DEFAULT computed
// from another column.
func defaultsTable() *discovery.TableSchema {
	return &discovery.TableSchema{
		Name: "events",
		Columns: []discovery.Column{
			{Name: "id", Type: "UInt32", Position: 1},
			{Name: "tenant", Type: "String", Position: 2},
			{Name: "region", Type: "String", Position: 3, DefaultKind: "DEFAULT", DefaultExpression: "'eu'"},
			{Name: "note", Type: "Nullable(String)", Position: 4, DefaultKind: "DEFAULT", DefaultExpression: "'n/a'"},
			{Name: "next", Type: "UInt32", Position: 5, DefaultKind: "DEFAULT", DefaultExpression: "id + 1"},
		},
	}
}

// TestParseRow_ColumnSubsetTakesTheServersDefaults: a narrower list, in any
// order, parses as the INSERT naming those columns would store the row. Every
// unlisted column holds its DEFAULT — a Nullable one too, which a null-padded
// full-width row would have stored as NULL — and a DEFAULT over a listed
// column is computed from the listed value.
func TestParseRow_ColumnSubsetTakesTheServersDefaults(t *testing.T) {
	eng := testEngine(t, defaultsTable())
	tbl, err := eng.Table(tenant.Default, "events")
	require.NoError(t, err)
	defer tbl.Release()

	for name, tc := range map[string]struct {
		cols []string
		line string
	}{
		"declaration order": {[]string{"id", "tenant"}, `[5, "acme"]`},
		"permuted":          {[]string{"tenant", "id"}, `["acme", 5]`},
	} {
		row, err := tbl.ParseRow(tc.cols, []byte(tc.line))
		require.NoError(t, err, name)
		for _, p := range []Predicate{
			{Column: "id", Op: "=", Values: []string{"5"}},
			{Column: "tenant", Op: "=", Values: []string{"acme"}},
			{Column: "region", Op: "=", Values: []string{"eu"}},
			{Column: "note", Op: "=", Values: []string{"n/a"}},
			{Column: "next", Op: "=", Values: []string{"6"}},
		} {
			ok, reason := row.VisibleWithReason([]Predicate{p})
			assert.True(t, ok, "%s: %s %s %v: %s", name, p.Column, p.Op, p.Values, reason)
		}
		row.Close()
	}

	// The full list still takes the no-list path and reads every field.
	row, err := tbl.ParseRow(tbl.WireColumns, []byte(`[5, "acme", "us", null, 9]`))
	require.NoError(t, err)
	defer row.Close()
	assert.True(t, row.Visible([]Predicate{{Column: "region", Op: "=", Values: []string{"us"}}}))
	assert.True(t, row.Visible([]Predicate{{Column: "next", Op: "=", Values: []string{"9"}}}))
}

func TestParseRow_AcceptsALineWithOrWithoutNewline(t *testing.T) {
	eng := testEngine(t, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
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

// TestFilterCache_EverySlotHoldsTheWholeBudget: a filter answers only on its
// own handle, so the slot an evaluation lands on must hold the whole working
// set. Every slot of a grown pool is bounded by filterCacheSize itself, not
// by a share of it, and one slot holds more distinct filters than an
// eight-way split gave it (512) without recompiling any of them.
func TestFilterCache_EverySlotHoldsTheWholeBudget(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(maxPoolSize))
	eng := testEngine(t, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
	require.NoError(t, err)
	defer tbl.Release()

	p := tbl.pool
	assert.Len(t, p.list(), 1, "one handle after Bind; the rest are compiled on contention")
	held := make([]*schemaSlot, 0, maxPoolSize)
	for range maxPoolSize {
		held = append(held, p.acquire())
	}
	for _, s := range held {
		p.release(s)
	}
	require.Len(t, p.list(), maxPoolSize)
	for _, s := range p.list() {
		assert.Equal(t, filterCacheSize, s.filters.cap)
	}

	s := p.first()
	filter := func(i int) *chtypes.LoadedFilter {
		expr, params, ok := tbl.render([]Predicate{{Column: "tenant", Op: "=", Values: []string{"user-" + strconv.Itoa(i)}}})
		require.True(t, ok)
		return tbl.filterOn(s, expr, params)
	}
	n := filterCacheSize/maxPoolSize + 1
	compiled := make([]*chtypes.LoadedFilter, n)
	for i := range compiled {
		compiled[i] = filter(i)
		require.NotNil(t, compiled[i])
	}
	assert.Equal(t, n, s.filters.len())
	for i, f := range compiled {
		assert.Same(t, f, filter(i), "filter %d is still cached, not recompiled", i)
	}
}

// TestFilterCache_GenerationInvalidatesEntries: a filter only answers for the
// schema handle it was compiled against, so a rebind must not reuse one.
func TestFilterCache_GenerationInvalidatesEntries(t *testing.T) {
	eng := testEngine(t, rowsTable())

	visible := func() bool {
		tbl, err := eng.Table(tenant.Default, "rows")
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
	eng.Bind(tenant.Default, testServerVersion, "UTC", []*discovery.TableSchema{changed})
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
// slot choice, the per-slot filter caches and the shared block parsing are
// safe; a slot chosen per call rather than per Row would show up here as a
// filter and a block on different handles.
func TestTable_ConcurrentAcrossThePool(t *testing.T) {
	eng := testEngine(t, rowsTable())

	const goroutines = 16
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20 {
				tbl, err := eng.Table(tenant.Default, "rows")
				if !assert.NoError(t, err) {
					return
				}

				row, err := tbl.ParseRow(tbl.WireColumns, []byte(sampleRow))
				if assert.NoError(t, err) {
					assert.True(t, row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{"acme"}}}))
					assert.False(t, row.Visible([]Predicate{{Column: "tenant", Op: "=", Values: []string{fmt.Sprintf("t%d", (g+i)%4)}}}))
					row.Close()
				}

				record := []byte(`{"id":7,"tenant":"acme","ts":"2026-01-15 10:30:00","amt":"12.50","big":-5,"ratio":0.1,"tags":["a"]}` + "\n")
				batch, err := tbl.Ingest(FormatJSONEachRow, record)
				assert.NoError(t, err)
				assert.Len(t, batch.Rows, 1)

				checked, err := tbl.Ingest(FormatJSONEachRow, record,
					Predicate{Column: "tenant", Op: "=", Values: []string{"acme"}})
				if assert.NoError(t, err) && assert.Len(t, checked.Rows, 1) {
					assert.Empty(t, checked.Rows[0].CheckReason)
					assert.NotNil(t, checked.Rows[0].Line)
				}

				tbl.Release()
			}
		}()
	}
	wg.Wait()
}

// BenchmarkVisible_ClaimChurn is the stream's fan-out: one parsed event, then
// one evaluation per subscriber, each subscriber's claim a distinct filter,
// on a pool grown to its limit. The event's handle must hold the whole set:
// subscribers=600 is past the 512 a slot got when the table's budget was
// split across a pool of 8, and inside filterCacheSize.
func BenchmarkVisible_ClaimChurn(b *testing.B) {
	eng := testEngine(b, rowsTable())
	tbl, err := eng.Table(tenant.Default, "rows")
	require.NoError(b, err)
	defer tbl.Release()

	// Grow the pool to its limit, as a busy table's would be.
	p := tbl.pool
	held := make([]*schemaSlot, 0, poolSize())
	for range poolSize() {
		held = append(held, p.acquire())
	}
	for _, s := range held {
		p.release(s)
	}

	for _, n := range []int{64, 600} {
		preds := make([][]Predicate, n)
		for i := range preds {
			preds[i] = []Predicate{{Column: "tenant", Op: "=", Values: []string{"user-" + strconv.Itoa(i)}}}
		}
		event := func(b *testing.B) {
			row, err := tbl.ParseRow(tbl.WireColumns, []byte(sampleRow))
			if err != nil {
				b.Fatal(err)
			}
			for _, pred := range preds {
				row.Visible(pred)
			}
			row.Close()
		}
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			event(b) // compiles the set on the slot a serial event lands on
			b.ResetTimer()
			for range b.N {
				event(b)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/eval")
		})
	}
}
