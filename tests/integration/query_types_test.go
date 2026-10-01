//go:build integration

package tests

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// queryTypesGolden is the pinned `/v1/query` response body for one row of
// every ClickHouse type family. It is a byte-for-byte pin, not a semantic
// one: the JSON *rendering* of a value is the endpoint's public contract,
// so a change from `"12.5"` to `12.5` — or a reordering of the keys — must
// show up as a failing test and be re-recorded deliberately.
//
// Regenerate with `WAVEHOUSE_UPDATE_PIN=1 make test-integration` (or
// `-run TestQuery_TypeRendering_Pin`) after an intentional contract change,
// and put the diff in the PR description.
const queryTypesGolden = "testdata/query_types_pin.json"

// queryTypesDDL is one column per rendering family. Order matters: it is the
// order `SELECT *` projects, which is part of what the pin records.
const queryTypesDDL = `
	i8 Int8, i16 Int16, i32 Int32, i64 Int64,
	u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64,
	f32 Float32, f64 Float64,
	dec Decimal(10, 2), dec64 Decimal64(3),
	s String, ls LowCardinality(String), fs FixedString(4),
	uu UUID, en Enum8('a' = 1, 'b' = 2), bl Bool,
	ip4 IPv4, ip6 IPv6,
	d Date, dt DateTime('UTC'), dt64 DateTime64(3, 'UTC'),
	arr Array(String), m Map(String, UInt8),
	nn Nullable(Int32), nnull Nullable(String)`

// queryTypesRow is the single row, written as SQL literals so the values
// reach ClickHouse without passing through a driver's own type mapping —
// the pin must describe ClickHouse's storage, not clickhouse-go's encoder.
// i64/u64 sit past 2^53 so the pin also records how 64-bit integers are
// spelled; s carries a `/` so it records that one is not escaped; fs is
// shorter than its FixedString(4) so the NUL padding shows.
const queryTypesRow = `(
	-8, -16, -32, -9007199254740993,
	8, 16, 32, 18446744073709551615,
	0.1, 0.1,
	12.50, 1.500,
	'hello/world', 'lc', 'ab',
	toUUID('11111111-2222-3333-4444-555555555555'), 'a', true,
	'10.0.0.1', '::1',
	'2026-01-15', '2026-01-15 10:30:00', '2026-01-15 10:30:00.123',
	['a', 'b'], map('k', 7),
	42, NULL)`

// TestQuery_TypeRendering_Pin snapshots the exact JSON `/v1/query` returns for
// one row of every ClickHouse type family, against the server the suite pins.
//
// It exists because the structured-query response is the SDK's data contract
// and nothing else asserts on the *spelling* of a value: the e2e tables are
// all String/UInt32/DateTime64, so a Decimal silently changing from a JSON
// number to a JSON string, or a FixedString from a string to a byte array,
// would reach consumers with a green suite. Any diff here is a breaking API
// change and belongs in the CHANGELOG.
//
// ClickHouse renders the body (FORMAT JSONEachRow under the reader's pinned
// output settings): keys in SELECT order, Decimal as a JSON number, `/`
// unescaped (output_format_json_escape_forward_slashes=0), DateTime as RFC
// 3339 in UTC at the column's scale (date_time_output_format=iso). Measured
// on 26.8.15.10.
func TestQuery_TypeRendering_Pin(t *testing.T) {
	e := env(t)

	table := createTable(t, queryTypesDDL, "ORDER BY i8")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("INSERT INTO `%s` VALUES %s", table, queryTypesRow)),
		"seed the one typed row")

	// The suite's default caller is the admin role, an unrestricted grant, so
	// select_all stays a bare SELECT * and the projection is the DDL order.
	got := postJSON(t, e.baseURL+"/v1/query?table="+table, `{"select_all":true}`)
	require.Equal(t, http.StatusOK, got.status, "body: %s", got.raw)
	body := strings.TrimRight(got.raw, "\n")

	if os.Getenv("WAVEHOUSE_UPDATE_PIN") == "1" {
		require.NoError(t, os.MkdirAll(filepath.Dir(queryTypesGolden), 0o750))
		require.NoError(t, os.WriteFile(queryTypesGolden, []byte(body+"\n"), 0o600))
		t.Logf("pin updated: %s", queryTypesGolden)
		return
	}

	want, err := os.ReadFile(queryTypesGolden)
	require.NoError(t, err, "read pin; regenerate with WAVEHOUSE_UPDATE_PIN=1")
	require.Equal(t, strings.TrimRight(string(want), "\n"), body,
		"the /v1/query type-rendering contract changed — re-record deliberately "+
			"with WAVEHOUSE_UPDATE_PIN=1 and document the diff")
}

// TestOpsQuery_SlashRendering: the raw SQL proxy spells `/` as /v1/query
// does, not as ClickHouse's default `\/`.
func TestOpsQuery_SlashRendering(t *testing.T) {
	e := env(t)

	got := postJSON(t, e.baseURL+"/v1/ops/query", `{"sql":"SELECT '/home' AS p"}`)
	require.Equal(t, http.StatusOK, got.status, "body: %s", got.raw)
	require.Contains(t, got.raw, `"/home"`)
	require.NotContains(t, got.raw, `\/`)
}
