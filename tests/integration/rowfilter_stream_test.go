//go:build integration

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
)

// TestRowFilterStream_DifferentialAgainstClickHouse pins the stream/query
// row-visibility agreement with ClickHouse itself as the oracle: for every
// column shape × insertable payload × filter constant × operator, the
// stream's verdict must equal what the production /v1/query returns over the
// SAME stored row, for a role whose row filter is that one predicate. A query
// ClickHouse rejects means the role reads no rows on the query path, so the
// stream must withhold too.
//
// Both surfaces run as production runs them. The policy is adopted from the
// settings directory, one role per (constant, operator) cell, and each query
// goes through the wired server with a token for its role. The stream side is
// the hub's own evaluator over the app's own type layer, bound by the app's
// refresh hook to what discovery found (server version, zone, tables), and it
// parses the positional JSONCompactEachRow line the ingest path publishes for
// the row, read back from the server rather than reconstructed here.
//
// On an integer column both surfaces compare a claim through the strict cast
// (chsql.StrictInt), so there is a second oracle as well: the admitted set
// must be the mathematically correct one — the comparison itself when the
// constant is the canonical spelling of a value the column can hold, and
// nothing at all otherwise. Parity alone could not catch an over-admit both
// surfaces share, and the plain String binding had one: a constant at or past
// 2^64 wrapped on every integer width.
//
// Parity is STRICT on every constant and every operator: there is no "the
// stream may be stricter" bucket and no excluded case. A filter value bound in
// a TYPED parameter used to disagree with the server — a sub-64-bit column's
// out-of-domain constant ("256" against UInt8) admitted where the server hides
// it, and a constant past a Decimal's precision could not be bound at all.
// Binding every value as {p:String} closed both, so a divergence appearing
// here is a regression, not a known gap.
func TestRowFilterStream_DifferentialAgainstClickHouse(t *testing.T) {
	shapes := []diffShape{
		{
			name:    "uint64",
			ddl:     "UInt64",
			intType: "UInt64",
			payloads: []any{
				json.Number("16777217"),
				json.Number("9007199254740993"), // 2^53+1: float64 would collapse it onto its neighbor
				"12345678901234567890",          // string-encoded (the JS-precision-loss escape hatch), > 2^63
				json.Number("0"),
			},
			constants: []string{
				"16777216", "16777217",
				"9007199254740992", "9007199254740993",
				"12345678901234567890",
				"0",
				// Spellings and magnitudes the old Go comparator refused outright
				// (it could not reproduce ClickHouse's own inconsistency): an
				// exponent form, a fractional constant, a negative against an
				// unsigned column, and two values past the column's width. Both
				// sides now go through ClickHouse, so these are strict-parity
				// cases rather than a "never admit where SQL hides" subset.
				"1e3", "1.5", "-5",
				"18446744073709551616", "99999999999999999999999",
			},
		},
		{
			name:      "int64",
			ddl:       "Int64",
			intType:   "Int64",
			payloads:  []any{json.Number("-5"), json.Number("9007199254740993")},
			constants: []string{"-4", "-5", "9007199254740992", "9223372036854775808"},
		},
		{
			name:     "uint8",
			ddl:      "UInt8",
			intType:  "UInt8",
			payloads: []any{json.Number("0"), json.Number("5"), json.Number("255")},
			// "256"/"300" were excluded while the stream bound integers through
			// the widest integer type and answered `5 < '256'` true where the
			// server answers false. Binding every value as {p:String} closed
			// that, so they are strict-parity constants — this is the pin for it.
			constants: []string{"5", "255", "0", "-1", "256", "300"},
		},
		// The integer widths, with every boundary that used to wrap.
		intShape("uint8_bounds", "UInt8", "UInt8"),
		intShape("uint32_bounds", "UInt32", "UInt32"),
		intShape("uint64_bounds", "UInt64", "UInt64"),
		intShape("int64_bounds", "Int64", "Int64"),
		intShape("uint128_bounds", "UInt128", "UInt128"),
		intShape("int128_bounds", "Int128", "Int128"),
		intShape("uint256_bounds", "UInt256", "UInt256"),
		intShape("int256_bounds", "Int256", "Int256"),
		intShape("nullable_uint64_bounds", "Nullable(UInt64)", "UInt64"),
		{
			name: "float32",
			ddl:  "Float32",
			payloads: []any{
				json.Number("16777217"), // stores as 16777216 — the review repro
				json.Number("16777218"),
				json.Number("0.1"),
				json.Number("1.5"),
			},
			constants: []string{"16777216", "16777217", "0.1", "1.5", "2", "1e3"},
		},
		{
			name: "float64",
			ddl:  "Float64",
			payloads: []any{
				json.Number("9007199254740992"),
				json.Number("9007199254740993"), // stores rounded: the storage domain collapses it
				json.Number("0.1"),
			},
			constants: []string{"9007199254740992", "9007199254740993", "0.1"},
		},
		{
			name: "decimal_10_2",
			ddl:  "Decimal(10, 2)",
			payloads: []any{
				json.Number("1.005"), // stores as 1.00 (truncation, not rounding)
				json.Number("1.02"),
				json.Number("-1.005"),
			},
			// "999999999" is past Precision−Scale. It used to be a
			// "never admit where SQL hides" case because the constant could not
			// be bound as that Decimal at all; as a String parameter it is read
			// in the column's domain and agrees outright.
			constants: []string{"1.005", "1.004", "1", "1.5", "-1", "1.50", "1e3", "999999999"},
		},
		{
			name: "string",
			ddl:  "String",
			// Byte ordering, not collation: "9" sorts after "100", which is
			// exactly the leak a text fallback would cause on a numeric column
			// and exactly the right answer on this one.
			//
			// The last five are the {p:String} escaping cases. A ClickHouse
			// query parameter is read by an escaped-text reader on BOTH
			// surfaces — the server's HTTP interface and the chtypes artifact's
			// filter params — so an unencoded backslash arrives as an escape
			// sequence and an unencoded tab or newline does not parse at all.
			// Measured before chsql.EscapeStringParam was applied in
			// typelayer.render(): the backslash rows answered false and the
			// tab/newline/trailing-backslash rows declined, while this oracle
			// (a natively-bound literal) answered true — the stream hid rows
			// the query path returns. Strict parity here is the pin for that.
			payloads:  []any{"acme", "9", "100", "", `a\b`, "a\tb", "a\nb", `trail\`, "O'Brien"},
			constants: []string{"acme", "beta", "9", "100", "", `a\b`, "a\tb", "a\nb", `trail\`, "O'Brien"},
		},
		{
			name: "datetime",
			ddl:  "DateTime",
			// The policy author's zone-less spelling against the stored instant.
			payloads:  []any{"2026-06-21 04:00:00", "2026-06-21T04:00:01Z"},
			constants: []string{"2026-06-21 04:00:00", "2026-06-21 04:00:01"},
		},
	}

	// One row under test per (shape, payload), and one policy for the whole
	// corpus: on each shape's table, a role per (constant, operator) cell
	// filtering v by that one predicate, and on an integer table one more whose
	// _in reads a claim array.
	var rows []diffRow
	tables := map[string]policy.TablePolicy{}
	for _, sh := range shapes {
		table := createTable(t, "id UInt32, v "+sh.ddl, "ORDER BY id")
		grants := policy.TablePolicy{}
		for i, constant := range sh.constants {
			for _, op := range diffOps {
				grants[cellRole(i, op)] = rowFilterGrant(t, op, constant)
			}
		}
		if sh.intType != "" {
			claimIn := "{{ jwt.ids }}"
			grants[claimInRole] = policy.RolePermissions{Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"v": {In: &claimIn}}}}
		}
		tables[table] = grants

		anyStored := false
		for i, payload := range sh.payloads {
			if err := rowFilterInsert(t, table, map[string]any{"id": uint32(i), "v": payload}); err != nil {
				// Un-storable payloads are the documented transient (DLQ) class,
				// out of the parity claim — skip, on the record.
				t.Logf("payload %v not insertable into %s (%v); skipping", payload, sh.ddl, err)
				continue
			}
			anyStored = true
			line := rowFilterStoredLine(t, table, uint32(i))
			rows = append(rows, diffRow{
				shape: sh.name, intType: sh.intType, table: table, id: uint32(i), payload: payload,
				columns: []string{"id", "v"}, line: line, stored: rowFilterStoredInt(t, sh.intType, line),
			})
		}
		require.True(t, anyStored, "corpus for %s must contain insertable payloads", sh.name)
	}
	p := policy.Policy{Tables: tables}
	withPolicy(t, p)

	// Every cell, row by row: each of the shape's (constant, operator) roles, and
	// on an integer column each claim array under the _in role.
	byShape := map[string][]string{}
	for _, sh := range shapes {
		byShape[sh.name] = sh.constants
	}
	var cells []diffCell
	for ri, r := range rows {
		for i, constant := range byShape[r.shape] {
			for _, op := range diffOps {
				cells = append(cells, diffCell{row: ri, role: cellRole(i, op), op: op.sql, constant: constant})
			}
		}
		if r.intType != "" {
			for _, set := range intInSets {
				cells = append(cells, diffCell{row: ri, role: claimInRole, claims: map[string]any{"ids": toAnys(set)}, set: set})
			}
		}
	}

	// The hub's evaluator over the app's type layer, which createTable's
	// refreshes already bound.
	require.NotNil(t, env(t).types, "the api role wires a type layer")
	got := rowFilterStreamVerdicts(t, stream.NewRowEvaluator(env(t).types), &p, rows, cells)
	want, sqlErrs := rowFilterQueryVerdicts(t, rows, cells)

	mathCells, admitted := 0, 0
	for i, c := range cells {
		r := rows[c.row]
		if c.set != nil {
			// A multi-element _in from a claim array: each element is cast on its
			// own, so the elements that fit decide and the rest drop out — 2^64+5
			// must not wrap onto a row holding 5.
			mathCells++
			exact := false
			for _, v := range c.set {
				if eq, ok := intExpected(r.intType, r.stored, "=", v); ok && eq {
					exact = true
				}
			}
			if got[i] != want[i] || want[i] != exact {
				t.Errorf("%s: stored %v IN %v — stream says %v, /v1/query says %v (query err: %v), correct is %v",
					r.shape, r.payload, c.set, got[i], want[i], sqlErrs[i], exact)
			}
			continue
		}
		if want[i] {
			admitted++
		}
		if got[i] != want[i] {
			t.Errorf("%s: stored %v %s %q — stream says %v, /v1/query says %v (query err: %v)",
				r.shape, r.payload, c.op, c.constant, got[i], want[i], sqlErrs[i])
		}
		if exact, ok := intExpected(r.intType, r.stored, c.op, c.constant); ok {
			mathCells++
			if want[i] != exact || got[i] != exact {
				t.Errorf("%s: stored %v %s %q — admitted stream=%v query=%v, the mathematically correct answer is %v",
					r.shape, r.payload, c.op, c.constant, got[i], want[i], exact)
			}
		}
	}
	// A harness that answered "no row" everywhere would agree with itself.
	require.Positive(t, admitted, "some cell must admit its row on the query path")
	t.Logf("%d cells compared stream against /v1/query (%d admitted), %d of them also against the exact answer", len(cells), admitted, mathCells)
}

// diffRow is one row under test: the table it lives in, its id, the
// positional line the ingest path would publish for it, and on an integer
// column the value stored (nil for NULL).
type diffRow struct {
	shape   string
	intType string
	table   string
	id      uint32
	payload any
	columns []string
	line    []byte
	stored  *big.Int
}

// diffCell is one comparison: a stored row read as one role. A constant cell
// names its operator and constant; a claim-array cell runs as claimInRole with
// set as the ids claim.
type diffCell struct {
	row      int // index into the rows
	role     string
	claims   map[string]any
	op       string
	constant string
	set      []string
}

// diffOp is one operator under the differential: its SQL spelling, for the
// oracle and the messages, and the role-name spelling.
type diffOp struct{ sql, name string }

var diffOps = []diffOp{{"=", "eq"}, {"!=", "neq"}, {">", "gt"}, {"<", "lt"}, {"in", "in"}}

// claimInRole is the role whose _in reads the ids claim.
const claimInRole = "claim_in"

// cellRole names the role of one (constant, operator) cell. Role names repeat
// across tables: each table's grant is its own.
func cellRole(constant int, op diffOp) string {
	return fmt.Sprintf("c%d_%s", constant, op.name)
}

// rowFilterGrant reads a table, filtered by one operator on v with a literal
// constant.
func rowFilterGrant(t *testing.T, op diffOp, constant string) policy.RolePermissions {
	t.Helper()
	f := policy.Filter{}
	switch op.name {
	case "eq":
		f.Eq = &constant
	case "neq":
		f.Neq = &constant
	case "gt":
		f.Gt = &constant
	case "lt":
		f.Lt = &constant
	case "in":
		// A placeholder-free _in template resolves to a one-element set, so this
		// is the IN (…) renderer on both surfaces with one bound value.
		f.In = &constant
	default:
		t.Fatalf("unknown op %q", op.name)
	}
	return policy.RolePermissions{Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"v": f}}}
}

// rowFilterStreamVerdicts is the stream's verdict on each cell, reached the
// way the hub reaches it for an event: the row is prepared ONCE — the parse is
// per event — and each cell's grant, resolved through the full production path
// (Evaluate → Visible), is asked of that one view, as each subscriber's is.
// cells come row by row.
func rowFilterStreamVerdicts(t *testing.T, eval stream.RowEvaluator, p *policy.Policy, rows []diffRow, cells []diffCell) []bool {
	t.Helper()
	got := make([]bool, len(cells))
	for start := 0; start < len(cells); {
		r := rows[cells[start].row]
		end := start
		for end < len(cells) && cells[end].row == cells[start].row {
			end++
		}
		view, err := eval.Prepare(tenant.Default, r.table, r.columns, r.line)
		for i := start; i < end; i++ {
			perms := policy.Evaluate(p, cells[i].role, r.table, "select", cells[i].claims)
			require.True(t, perms.Allowed)
			if err == nil { // withheld otherwise: no view, no row
				got[i], _ = view.Visible(perms)
			}
		}
		if err == nil {
			view.Close()
		}
		start = end
	}
	return got
}

// diffQueryWorkers is how many /v1/query requests the differential keeps in
// flight: below the suite tenant's max_open_conns (10), so none of them waits
// on the server's own ClickHouse pool. One at a time, the round trips were
// most of a minute of the suite.
const diffQueryWorkers = 8

// rowFilterQueryVerdicts asks the production /v1/query, for every cell, whether
// it returns the stored row to the cell's role with the cell's claims, and
// returns each answer with ClickHouse's rejection when there was one. A query
// ClickHouse rejects means the role reads no rows on that path; any other
// failure is the harness's and fails the test, so a broken token or policy
// cannot pass as "withheld on both". The cells are independent reads, so they
// go diffQueryWorkers at a time over one pool of keep-alive connections.
func rowFilterQueryVerdicts(t *testing.T, rows []diffRow, cells []diffCell) ([]bool, []error) {
	t.Helper()
	// Tokens are minted here, on the test's goroutine, one per role and claim set.
	auth := make([]string, len(cells))
	minted := map[string]string{}
	for i, c := range cells {
		key := c.role + "\x00" + strings.Join(c.set, "\x00")
		if _, ok := minted[key]; !ok {
			minted[key] = bearer(t, c.role, c.claims)
		}
		auth[i] = minted[key]
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost = diffQueryWorkers, diffQueryWorkers
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}

	want := make([]bool, len(cells))
	sqlErrs := make([]error, len(cells))
	harnessErrs := make([]error, len(cells))
	var next atomic.Int64
	var broken atomic.Bool
	var wg sync.WaitGroup
	for range diffQueryWorkers {
		wg.Go(func() {
			for !broken.Load() {
				i := int(next.Add(1) - 1)
				if i >= len(cells) {
					return
				}
				want[i], sqlErrs[i], harnessErrs[i] = rowFilterQueryVerdict(client, rows[cells[i].row], auth[i])
				if harnessErrs[i] != nil {
					broken.Store(true)
				}
			}
		})
	}
	wg.Wait()
	for i, err := range harnessErrs {
		if err != nil {
			t.Fatalf("role %s on %s: %v", cells[i].role, rows[cells[i].row].table, err)
		}
	}
	return want, sqlErrs
}

// rowFilterQueryVerdict is one cell's /v1/query: whether r comes back, the
// rejection when ClickHouse refused the query, or the harness failure.
func rowFilterQueryVerdict(client *http.Client, r diffRow, authorization string) (visible bool, rejected, harness error) {
	body, err := json.Marshal(map[string]any{
		"columns": []string{"id"},
		"filters": []any{map[string]any{"column": "id", "op": "eq", "value": r.id}},
	})
	if err != nil {
		return false, nil, err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		sharedEnv.baseURL+"/v1/query?table="+url.QueryEscape(r.table), bytes.NewReader(body))
	if err != nil {
		return false, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authorization)
	resp, err := client.Do(req)
	if err != nil {
		return false, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, nil, err
	}
	if resp.StatusCode == http.StatusBadRequest {
		var refusal queryError
		if json.Unmarshal(raw, &refusal) == nil && refusal.Code == "clickhouse.rejected" {
			return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw), nil
		}
	}
	if resp.StatusCode != http.StatusOK {
		return false, nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
	}
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return false, nil, fmt.Errorf("decode %s: %w", raw, err)
	}
	if len(out) > 1 {
		return false, nil, fmt.Errorf("id is unique per table, yet %d rows came back: %s", len(out), raw)
	}
	return len(out) == 1, nil, nil
}

// Boundary constants for the integer shapes: each width's own edges, the
// values that wrapped under the plain String binding, and spellings that are
// not canonical.
var intBoundaryConstants = func() []string {
	p := func(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }
	add := func(a *big.Int, d int64) string { return new(big.Int).Add(a, big.NewInt(d)).String() }
	neg := func(a *big.Int) *big.Int { return new(big.Int).Neg(a) }
	return []string{
		"0", "1", "5", "-1", "-5", "255", "256", "4294967295", "4294967296",
		add(p(63), 0), add(p(63), -1), add(neg(p(63)), -1), add(neg(p(63)), 0),
		add(p(64), 0), add(p(64), -1), add(p(64), 5),
		add(p(127), 0), add(p(127), -1), add(neg(p(127)), -1), add(neg(p(127)), 0),
		add(p(128), 0), add(p(128), -1),
		add(p(255), 0), add(p(255), -1), add(neg(p(255)), -1), add(neg(p(255)), 0),
		add(p(256), 0), add(p(256), -1), add(p(256), 5),
		"007", "+5", "5.0", "1e3", "abc", "",
	}
}()

// intInSets are claim arrays for the multi-element _in cases.
var intInSets = [][]string{
	{"18446744073709551621", "0"},                          // 2^64+5 must not wrap onto 5
	{"5", "007", "abc"},                                    // the junk elements drop out, 5 still decides
	{"-1", "340282366920938463463374607431768211456", "1"}, // 2^128
	{"115792089237316195423570985008687907853269984665640564039457584007913129639941", "+5", "5.0"}, // 2^256+5
}

// diffShape is one column type under the differential. intType names the
// bare integer type the column holds, "" for a non-integer column; it is
// declared here rather than derived with chsql.IntegerType, so a regression in
// that derivation shows up as a wrong answer instead of a skipped oracle.
type diffShape struct {
	name      string
	ddl       string
	intType   string
	payloads  []any
	constants []string
}

// intShape is a differential shape for one integer type: a row at each edge of
// its domain (and a NULL for a Nullable column), filtered by every boundary
// constant.
func intShape(name, ddl, intType string) diffShape {
	lo, hi := intDomain(intType)
	payloads := []any{"0", "1", "5", hi.String()}
	if lo.Sign() < 0 {
		payloads = append(payloads, lo.String(), "-5")
	}
	if strings.HasPrefix(ddl, "Nullable(") {
		payloads = append(payloads, nil)
	}
	return diffShape{name: name, ddl: ddl, intType: intType, payloads: payloads, constants: intBoundaryConstants}
}

// intDomain is an integer type's [min, max].
func intDomain(name string) (*big.Int, *big.Int) {
	bits, err := strconv.Atoi(strings.TrimPrefix(strings.TrimPrefix(name, "U"), "Int"))
	if err != nil {
		panic(name)
	}
	one := big.NewInt(1)
	if strings.HasPrefix(name, "U") {
		return big.NewInt(0), new(big.Int).Sub(new(big.Int).Lsh(one, uint(bits)), one)
	}
	half := new(big.Int).Lsh(one, uint(bits-1))
	return new(big.Int).Neg(half), new(big.Int).Sub(half, one)
}

// rowFilterStoredInt reads the stored value back off the published line for an
// integer column; nil for NULL or a non-integer column.
func rowFilterStoredInt(t *testing.T, intType string, line []byte) *big.Int {
	t.Helper()
	if intType == "" {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var cols []any
	require.NoError(t, dec.Decode(&cols))
	require.Len(t, cols, 2)
	var text string
	switch v := cols[1].(type) {
	case nil:
		return nil
	case json.Number:
		text = v.String()
	case string:
		text = v
	default:
		t.Fatalf("stored integer came back as %T", v)
	}
	n, ok := new(big.Int).SetString(text, 10)
	require.True(t, ok, "stored integer %q", text)
	return n
}

// intExpected is the mathematically correct verdict for an integer column: a
// constant that is not the canonical spelling of a value the column can hold
// admits nothing, and a NULL row is never admitted. ok is false for a
// non-integer column, which has no such oracle here.
func intExpected(intType string, stored *big.Int, op, constant string) (bool, bool) {
	if intType == "" {
		return false, false
	}
	lo, hi := intDomain(intType)
	v, ok := new(big.Int).SetString(constant, 10)
	if !ok || v.String() != constant || v.Cmp(lo) < 0 || v.Cmp(hi) > 0 || stored == nil {
		return false, true
	}
	cmp := stored.Cmp(v)
	switch op {
	case "=", "in":
		return cmp == 0, true
	case "!=":
		return cmp != 0, true
	case "<":
		return cmp < 0, true
	case ">":
		return cmp > 0, true
	}
	panic(op)
}

func toAnys(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// rowFilterStoredLine reads one stored row back as the positional
// JSONCompactEachRow line the ingest path publishes for it — the exact bytes the
// stream evaluates, produced by the server rather than reconstructed here.
func rowFilterStoredLine(t *testing.T, table string, id uint32) []byte {
	t.Helper()
	q := url.Values{}
	q.Set("database", testCHDatabase)
	q.Set("param_target_table", table)
	q.Set("param_id", fmt.Sprint(id))
	q.Set("query", "SELECT * FROM {target_table:Identifier} WHERE id = {id:UInt32} FORMAT JSONCompactEachRow")
	// The DateTime spelling a published row carries (typelayer's export).
	q.Set("date_time_output_format", "iso")
	body := rowFilterCH(t, http.MethodGet, q, nil)
	line := strings.TrimRight(string(body), "\n")
	require.NotEmpty(t, line, "stored row must come back")
	require.NotContains(t, line, "\n", "exactly one row per id")
	return []byte(line)
}

// rowFilterInsert inserts one JSONEachRow row over ClickHouse HTTP with the same
// parsing settings the ingest worker pins, and returns ClickHouse's verdict as
// an error (nil on 2xx).
func rowFilterInsert(t *testing.T, table string, row map[string]any) error {
	t.Helper()
	body, err := json.Marshal(row)
	require.NoError(t, err)

	q := url.Values{}
	q.Set("database", testCHDatabase)
	q.Set("param_target_table", table)
	q.Set("query", "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow")
	for k, v := range typelayer.InsertSettings() {
		q.Set(k, v)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		env(t).chHTTPURL+"?"+q.Encode(), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", testCHUser)
	req.Header.Set("X-ClickHouse-Key", testCHPassword)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// rowFilterCH runs one ClickHouse HTTP request and returns the body, failing the
// test on anything but 2xx.
func rowFilterCH(t *testing.T, method string, q url.Values, body io.Reader) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, env(t).chHTTPURL+"?"+q.Encode(), body)
	require.NoError(t, err)
	req.Header.Set("X-ClickHouse-User", testCHUser)
	req.Header.Set("X-ClickHouse-Key", testCHPassword)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 300, "clickhouse: %s", out)
	return out
}
