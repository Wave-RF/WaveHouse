//go:build integration

package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
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
// order `SELECT *` projects, which some marshallers preserve and others do
// not — part of what the pin records.
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
// spelled; fs is shorter than its FixedString(4) so the NUL padding shows.
const queryTypesRow = `(
	-8, -16, -32, -9007199254740993,
	8, 16, 32, 18446744073709551615,
	0.1, 0.1,
	12.50, 1.500,
	'hello', 'lc', 'ab',
	toUUID('11111111-2222-3333-4444-555555555555'), 'a', true,
	'10.0.0.1', '::1',
	'2026-01-15', '2026-01-15 10:30:00', '2026-01-15 10:30:00.123',
	['a', 'b'], map('k', 7),
	42, NULL)`

// TestQuery_TypeRendering_Pin snapshots the exact JSON `/v1/query` returns for
// one row of every ClickHouse type family, against the real server the suite
// pins (26.6.3.62).
//
// It exists because the structured-query response is the SDK's data contract
// and nothing else asserts on the *spelling* of a value: the e2e tables are
// all String/UInt32/DateTime64, so a Decimal silently changing from a JSON
// string to a JSON number, or a FixedString from a byte array to a string,
// would reach consumers with a green suite. Any diff here is a breaking API
// change and belongs in the CHANGELOG.
//
// The pin was recorded once against the old native-driver path and re-recorded
// when the endpoint moved onto ClickHouse's own renderer. Exactly two things
// moved, and NOTHING else did — every other family is byte-identical:
//   - Decimal* is a JSON number (12.5) where it was a JSON string ("12.5"),
//     which is what the SDK codegen already claimed it was;
//   - the object keys are in SELECT order rather than alphabetical, because
//     Go's map marshaller sorted them and ClickHouse does not.
func TestQuery_TypeRendering_Pin(t *testing.T) {
	e := env(t)

	table := createTable(t, queryTypesDDL, "ORDER BY i8")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, e.chConn.Exec(ctx,
		fmt.Sprintf("INSERT INTO `%s` VALUES %s", table, queryTypesRow)),
		"seed the one typed row")

	// Admin resolves to an unrestricted grant, so select_all stays a bare
	// SELECT * and the projection is the DDL order above. Cache is nil so the
	// body is always freshly rendered.
	store := policy.Static(&policy.Policy{AdminRole: "admin"})
	h := api.NewStructuredQueryHandler(
		func() chconn.Target {
			return chconn.Target{URL: e.chHTTPURL, Username: testCHUser, Password: testCHPassword, Database: testCHDatabase}
		},
		nil, e.registry, store,
		func() int { return 60 },
		func() time.Duration { return 30 * time.Second },
		nil, testutil.NopLogger(),
	)

	req := httptest.NewRequest(http.MethodPost,
		"/v1/query?table="+table, strings.NewReader(`{"select_all":true}`))
	req = req.WithContext(auth.WithRole(req.Context(), "admin"))
	rec := httptest.NewRecorder()

	h.Handle(rec, req)

	body := rec.Body.String()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", body)

	if os.Getenv("WAVEHOUSE_UPDATE_PIN") == "1" {
		require.NoError(t, os.MkdirAll(filepath.Dir(queryTypesGolden), 0o755))
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
