//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/stream"
)

// TestRowFilterNumeric_DifferentialAgainstClickHouse pins the stream/query
// row-visibility agreement with ClickHouse itself as the oracle (the #381
// review's storage-narrowing fail-open): for every numeric column shape ×
// insertable payload × filter constant × operator, the in-memory verdict
// (policy.Evaluate → RowVisible over the pre-insert payload, specs built the
// way stream.Hub builds them) must equal what a structured query returns over
// the STORED row — `WHERE v <op> ?` with the constant bound exactly as
// predicatesToSQL binds it. A constant ClickHouse rejects with a type error
// means the role reads no rows on the query path, so the stream must withhold
// too. Inserts go through the worker's exact HTTP surface (JSONEachRow), so
// storage narrowing — Float32/Float64 rounding, Decimal scale truncation — is
// ClickHouse's own, not a lookalike.
func TestRowFilterNumeric_DifferentialAgainstClickHouse(t *testing.T) {
	shapes := []struct {
		name      string
		ddl       string
		payloads  []any
		constants []string
		// looseConstants are out-of-range spellings whose ClickHouse reading
		// was measured to vary by pair on one release — mathematical promotion
		// ('256' vs UInt8), or a width-boundary WRAP that compares against a
		// different value than written (2^63 vs Int64 reads as −2^63). The
		// stream refuses them all (the range gate), so verdicts legitimately
		// diverge in the withholding direction; only the subset half of the
		// guarantee is asserted here: the stream must never admit where SQL
		// hides.
		looseConstants []string
	}{
		{
			name: "uint64",
			ddl:  "UInt64",
			payloads: []any{
				json.Number("16777217"),
				json.Number("9007199254740992"),
				json.Number("9007199254740993"), // 2^53+1: float64 would collapse it onto its neighbor
				"12345678901234567890",          // string-encoded (the JS-precision-loss escape hatch), > 2^63
				json.Number("0"),
			},
			constants: []string{
				"16777216", "16777217",
				"9007199254740992", "9007199254740993",
				"12345678901234567890",
				"1e3", // exponent spelling: ClickHouse's integer cast errors the query
				"1.5", // fractional constant: same
				"-5",  // negative vs unsigned: measured cast error, role reads no rows — strict parity holds
			},
			// Wide or width-boundary constants: ClickHouse's reading varies by
			// pair (promotion vs wrap); the stream refuses — subset assertion.
			looseConstants: []string{"18446744073709551616", "99999999999999999999999"},
		},
		{
			name:     "int64",
			ddl:      "Int64",
			payloads: []any{json.Number("-5"), json.Number("9007199254740993")},
			constants: []string{
				"-4", "-5", "9007199254740992",
			},
			// 2^63 vs Int64 was measured to WRAP (compares as −2^63) — the
			// exact case the range gate exists for; subset assertion only.
			looseConstants: []string{"9223372036854775808"},
		},
		{
			name:     "uint8",
			ddl:      "UInt8",
			payloads: []any{json.Number("0"), json.Number("5"), json.Number("255")},
			constants: []string{
				"5", "255",
				"-1", // negative vs unsigned: measured cast error — strict parity holds
			},
			// Past the width, ClickHouse promotes and compares mathematically
			// while the stream refuses — subset assertion only.
			looseConstants: []string{"256", "300"},
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
				json.Number("1.006"),
				json.Number("1.02"),
				json.Number("-1.005"),
			},
			constants:      []string{"1.005", "1.004", "1", "1.5", "-1", "1.50", "1e3"},
			looseConstants: []string{"999999999"}, // past Precision−Scale: promoted on the SQL side, refused here
		},
	}

	ops := []string{"=", "!=", ">", "<"}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			t.Parallel()
			table := createTable(t, "id UInt32, v "+sh.ddl, "ORDER BY id")
			spec := numericColumnSpec(t, sh.ddl)

			inserted := make(map[int]any, len(sh.payloads))
			for i, payload := range sh.payloads {
				if err := insertBestEffort(t, table, map[string]any{"id": uint32(i), "v": payload}); err != nil {
					// Un-storable payloads are the documented transient (DLQ)
					// class, out of the parity claim — skip, on the record.
					t.Logf("payload %v not insertable into %s (%v); skipping", payload, sh.ddl, err)
					continue
				}
				inserted[i] = payload
			}
			require.NotEmpty(t, inserted, "corpus must contain insertable payloads")

			for id, payload := range inserted {
				for _, constant := range sh.constants {
					for _, op := range ops {
						stream := streamVerdict(t, table, op, constant, payload, spec)
						sql, sqlErr := storedVerdict(t, table, uint32(id), op, constant)
						if stream != sql {
							t.Errorf("%s: payload %v %s %q — stream says %v, ClickHouse says %v (query err: %v)",
								sh.ddl, payload, op, constant, stream, sql, sqlErr)
						}
					}
				}
				for _, constant := range sh.looseConstants {
					for _, op := range ops {
						stream := streamVerdict(t, table, op, constant, payload, spec)
						sql, sqlErr := storedVerdict(t, table, uint32(id), op, constant)
						if stream && !sql {
							t.Errorf("%s: payload %v %s %q — stream admits where ClickHouse hides (query err: %v)",
								sh.ddl, payload, op, constant, sqlErr)
						}
					}
				}
			}
		})
	}
}

// numericColumnSpec builds the policy.ColumnSpec for a numeric ClickHouse type
// through the very mapping production uses (stream.NumericSpecOf, the same
// classifier and spec builder as stream.Hub's columnSpecs), so this oracle can
// never validate a mapping the Hub no longer applies.
func numericColumnSpec(t *testing.T, chType string) policy.ColumnSpec {
	t.Helper()
	st, ok := discovery.NumericStorageOf(chType)
	require.True(t, ok, "corpus types must classify: %s", chType)
	return policy.ColumnSpec{Kind: policy.ColumnNumeric, Numeric: stream.NumericSpecOf(st)}
}

// streamVerdict resolves a one-operator literal filter through the full
// production path (Evaluate → RowVisible) and reports whether the stream
// would deliver the payload's event.
func streamVerdict(t *testing.T, table, op, constant string, payload any, spec policy.ColumnSpec) bool {
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
	default:
		t.Fatalf("unknown op %q", op)
	}
	p := &policy.Policy{Tables: map[string]policy.TablePolicy{
		table: {"r": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"v": f}}}},
	}}
	perms := policy.Evaluate(p, "r", table, "select", nil)
	require.True(t, perms.Allowed)
	return perms.RowVisible(map[string]any{"v": payload}, map[string]policy.ColumnSpec{"v": spec})
}

// storedVerdict asks ClickHouse whether the stored row satisfies the predicate,
// with the constant bound as a positional parameter exactly like
// predicatesToSQL emits it. A query error (an exact-domain cast rejecting the
// constant's spelling) means the role reads no rows on that path.
func storedVerdict(t *testing.T, table string, id uint32, op, constant string) (bool, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cnt uint64
	q := fmt.Sprintf("SELECT count() FROM %s WHERE id = ? AND v %s ?", table, op)
	if err := sharedEnv.chConn.QueryRow(ctx, q, id, constant).Scan(&cnt); err != nil {
		return false, err
	}
	return cnt == 1, nil
}
