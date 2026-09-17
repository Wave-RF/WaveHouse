//go:build integration

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
)

// TestRowFilterStream_DifferentialAgainstClickHouse pins the stream/query
// row-visibility agreement with ClickHouse itself as the oracle (the #381
// review's storage-narrowing fail-open): for every column shape × insertable
// payload × filter constant × operator, the stream's verdict must equal what a
// structured query returns over the SAME stored row — `WHERE v <op> ?` with the
// constant bound exactly as predicatesToSQL binds it. A constant ClickHouse
// rejects with a type error means the role reads no rows on the query path, so
// the stream must withhold too.
//
// Both sides now read the STORED row: the payload goes in over the worker's own
// HTTP surface, comes back out as the positional JSONCompactEachRow line the
// ingest path publishes, and the stream's evaluator parses that with the same
// ClickHouse build the server is running. So this no longer checks a Go
// re-derivation of storage narrowing against ClickHouse — it checks the WIRING,
// which is the part that can still be wrong.
//
// Parity is STRICT on every constant and every operator: there is no "the
// stream may be stricter" bucket and no excluded case. There used to be both,
// because a filter value bound in a TYPED parameter disagreed with the server —
// a sub-64-bit column's out-of-domain constant ("256" against UInt8) admitted
// where the server hides it, and a constant past a Decimal's precision could
// not be bound at all. Binding every value as {p:String} (AUDIT §C.1) closed
// both, so a divergence appearing here is a regression, not a known gap.
func TestRowFilterStream_DifferentialAgainstClickHouse(t *testing.T) {
	shapes := []struct {
		name      string
		ddl       string
		payloads  []any
		constants []string
	}{
		{
			name: "uint64",
			ddl:  "UInt64",
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
			payloads:  []any{json.Number("-5"), json.Number("9007199254740993")},
			constants: []string{"-4", "-5", "9007199254740992", "9223372036854775808"},
		},
		{
			name:     "uint8",
			ddl:      "UInt8",
			payloads: []any{json.Number("0"), json.Number("5"), json.Number("255")},
			// "256"/"300" were excluded while the stream bound integers through
			// the widest integer type and answered `5 < '256'` true where the
			// server answers false. String binding closed that (AUDIT §C.1), so
			// they are strict-parity constants — this is the pin for it.
			constants: []string{"5", "255", "0", "-1", "256", "300"},
		},
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

	ops := []string{"=", "!=", ">", "<", "in"}

	// One row under test per (shape, payload): the table it lives in, its id, and
	// the positional line the ingest path would publish for it.
	type storedRow struct {
		shape   string
		table   string
		id      uint32
		payload any
		columns []string
		line    []byte
	}
	var rows []storedRow

	for _, sh := range shapes {
		table := createTable(t, "id UInt32, v "+sh.ddl, "ORDER BY id")
		any_ := false
		for i, payload := range sh.payloads {
			if err := rowFilterInsert(t, table, map[string]any{"id": uint32(i), "v": payload}); err != nil {
				// Un-storable payloads are the documented transient (DLQ) class,
				// out of the parity claim — skip, on the record.
				t.Logf("payload %v not insertable into %s (%v); skipping", payload, sh.ddl, err)
				continue
			}
			any_ = true
			rows = append(rows, storedRow{
				shape: sh.name, table: table, id: uint32(i), payload: payload,
				columns: []string{"id", "v"},
				line:    rowFilterStoredLine(t, table, uint32(i)),
			})
		}
		require.True(t, any_, "corpus for %s must contain insertable payloads", sh.name)
	}

	// ONE engine for every shape, bound to the registry createTable already
	// refreshed — the same discovery output production binds.
	eval := rowFilterEvaluator(t)

	byShape := map[string][]string{}
	for _, sh := range shapes {
		byShape[sh.name] = sh.constants
	}
	for _, r := range rows {
		for _, constant := range byShape[r.shape] {
			for _, op := range ops {
				got := rowFilterStreamVerdict(t, eval, r.table, r.columns, r.line, op, constant)
				want, sqlErr := rowFilterStoredVerdict(t, r.table, r.id, op, constant)
				if got != want {
					t.Errorf("%s: stored %v %s %q — stream says %v, ClickHouse says %v (query err: %v)",
						r.shape, r.payload, op, constant, got, want, sqlErr)
				}
			}
		}
	}
}

// rowFilterEvaluator builds the production row evaluator over a type-layer
// Engine bound to the integration registry's own discovery — server version,
// server timezone and table list all as discovered, never hand-set, so the test
// cannot pass against a binding production would not make.
func rowFilterEvaluator(t *testing.T) stream.RowEvaluator {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := typelayer.NewEngine(typelayer.Config{}, logger)
	require.NoError(t, err, "the 26.6 chtypes artifact must be installed")
	t.Cleanup(eng.Close)
	reg := env(t).registry
	eng.Bind(reg.ServerVersion(), reg.ServerTimezone(), reg.List())
	return stream.NewRowEvaluator(eng, logger)
}

// rowFilterStreamVerdict resolves a one-operator literal filter through the full
// production path (Evaluate → Prepare → Visible) and reports whether the stream
// would deliver this stored row.
func rowFilterStreamVerdict(t *testing.T, eval stream.RowEvaluator, table string, columns []string, line []byte, op, constant string) bool {
	t.Helper()
	f := policy.Filter{}
	switch op {
	case "=":
		f.Eq = &constant
	case "!=":
		f.Neq = &constant
	case ">":
		f.Gt = &constant
	case "<":
		f.Lt = &constant
	case "in":
		// A placeholder-free _in template resolves to a one-element set, so this
		// is the IN (…) renderer on both surfaces with one bound value.
		f.In = &constant
	default:
		t.Fatalf("unknown op %q", op)
	}
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		table: {"r": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"v": f}}}},
	}}
	perms := policy.Evaluate(p, "r", table, "select", nil)
	require.True(t, perms.Allowed)

	view, err := eval.Prepare(table, columns, line)
	if err != nil {
		return false // withheld: no view, no row
	}
	defer view.Close()
	visible, _ := view.Visible(perms)
	return visible
}

// rowFilterStoredVerdict asks ClickHouse whether the stored row satisfies the
// predicate, with the constant bound as a positional parameter exactly like
// predicatesToSQL emits it. A query error (a cast rejecting the constant's
// spelling) means the role reads no rows on that path.
func rowFilterStoredVerdict(t *testing.T, table string, id uint32, op, constant string) (bool, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cnt uint64
	pred := fmt.Sprintf("v %s ?", op)
	if op == "in" {
		pred = "v IN (?)"
	}
	q := fmt.Sprintf("SELECT count() FROM %s WHERE id = ? AND %s", table, pred)
	if err := sharedEnv.chConn.QueryRow(ctx, q, id, constant).Scan(&cnt); err != nil {
		return false, err
	}
	return cnt == 1, nil
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
