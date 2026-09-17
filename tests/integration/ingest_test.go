//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// TestIngest_FlowsToClickHouseWithoutDLQ exercises the happy path: POST
// /v1/ingest?table={table} is acknowledged synchronously, ingest worker
// batches the event to ClickHouse, and the DLQ stays empty for that table.
func TestIngest_FlowsToClickHouseWithoutDLQ(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	table := createTable(t,
		"user_id String, event_type String, value Float64",
		"ORDER BY user_id",
	)

	body := `{"user_id":"alice","event_type":"click","value":42.5}`
	resp, err := http.Post(
		e.server.URL+"/v1/ingest?table="+url.QueryEscape(table),
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var ingestResp map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ingestResp))
	assert.Equal(t, true, ingestResp["ok"])

	// 30s upper bound for ingest worker's 5s batch window plus loaded-runner slack.
	assert.Eventually(t, func() bool {
		var count uint64
		err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM %s WHERE user_id = 'alice'", table),
		).Scan(&count)
		return err == nil && count > 0
	}, 30*time.Second, 500*time.Millisecond, "event should appear in ClickHouse")

	// Confirm the success path didn't tee anything into the DLQ for this
	// table — that's the actual contract we're asserting (no silent
	// duplicate writes to dlq.<table> alongside the real INSERT).
	dlqResp, err := http.Get(e.server.URL + "/v1/ops/dlq/stats")
	require.NoError(t, err)
	defer dlqResp.Body.Close()

	var stats map[string]any
	require.NoError(t, json.NewDecoder(dlqResp.Body).Decode(&stats))
	tables, ok := stats["tables"].(map[string]any)
	require.True(t, ok)
	_, hasDLQ := tables["dlq."+table]
	assert.False(t, hasDLQ, "successful inserts should not produce DLQ entries")
}

// TestIngest_ComputedColumns_FlowToClickHouse is the CI-visible guard for the
// class that broke ingest outright once the worker began naming columns
// explicitly: ClickHouse refuses a MATERIALIZED column in an INSERT column list
// (code 44, and insert_allow_materialized_columns defaults to 0) and an ALIAS
// one (code 16). A table carrying either could ingest under the old
// column-less `FORMAT JSONEachRow` and could not under the new statement —
// every row to the DLQ, or redelivered forever where the DLQ is off.
//
// No fixture in the suite declared such a column, which is exactly why the
// whole pipeline was green while this was broken. This is that fixture: it
// drives the real path — HTTP ingest, NATS, the worker's INSERT — and asserts
// the row lands AND the server computed the derived values.
func TestIngest_ComputedColumns_FlowToClickHouse(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	table := createTable(t,
		"user_id String, value Float64, "+
			"digest String MATERIALIZED concat('d:', user_id), "+
			"doubled Float64 ALIAS value * 2",
		"ORDER BY user_id",
	)

	body := `{"user_id":"carol","value":21}`
	resp, err := http.Post(
		e.server.URL+"/v1/ingest?table="+url.QueryEscape(table),
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Eventually(t, func() bool {
		var count uint64
		err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM %s WHERE user_id = 'carol'", table),
		).Scan(&count)
		return err == nil && count == 1
	}, 30*time.Second, 250*time.Millisecond,
		"row never landed — a computed column in the INSERT column list fails the whole batch")

	// The point of leaving them out of the statement: ClickHouse still fills
	// them. If the row inserted but these were empty, the column list was wrong
	// in the other direction.
	var digest string
	var doubled float64
	require.NoError(t, e.chConn.QueryRow(ctx,
		fmt.Sprintf("SELECT digest, doubled FROM %s WHERE user_id = 'carol'", table),
	).Scan(&digest, &doubled))
	assert.Equal(t, "d:carol", digest, "MATERIALIZED column computed by the server")
	assert.InDelta(t, 42.0, doubled, 0.001, "ALIAS column resolved by the server")
}

// TestIngest_SuppliedComputedColumn_Rejected: the other half — a record that
// names a computed column is refused at the API with a 400 naming it, rather
// than having the value silently dropped by the positional encoder.
//
// The refusal is now ClickHouse's own, not WaveHouse's phrasing of it: the
// column is not one a JSONEachRow record may carry, so the parser answers
// UNKNOWN_FIELD (117) and the code rides back with the message. The old
// "is materialized and cannot be inserted" wording is gone deliberately.
func TestIngest_SuppliedComputedColumn_Rejected(t *testing.T) {
	e := env(t)

	table := createTable(t,
		"user_id String, digest String MATERIALIZED concat('d:', user_id)",
		"ORDER BY user_id",
	)

	resp, err := http.Post(
		e.server.URL+"/v1/ingest?table="+url.QueryEscape(table),
		"application/json",
		strings.NewReader(`{"user_id":"dave","digest":"forged"}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Contains(t, body["error"], "digest")
	assert.EqualValues(t, 117, body["code"], "ClickHouse's UNKNOWN_FIELD, not a gateway guess")
}

// postIngest is the shared HTTP call for the format/policy cases below: one
// POST to /v1/ingest with a verbatim body, an explicit Content-Type, and the
// optional test role/claims headers (see setup_test.go).
func postIngest(t *testing.T, table, contentType, body, role, claims string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		env(t).server.URL+"/v1/ingest?table="+url.QueryEscape(table), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	if role != "" {
		req.Header.Set(testRoleHeader, role)
	}
	if claims != "" {
		req.Header.Set(testClaimsHeader, claims)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var decoded map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))
	return resp.StatusCode, decoded
}

// eventuallyRows waits for the ingest worker's batch window and reports the row
// count matching a predicate.
func eventuallyRows(t *testing.T, table, where string, want uint64) {
	t.Helper()
	ctx := context.Background()
	assert.Eventually(t, func() bool {
		var count uint64
		err := env(t).chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM %s WHERE %s", table, where)).Scan(&count)
		return err == nil && count == want
	}, 30*time.Second, 250*time.Millisecond, "expected %d row(s) in %s WHERE %s", want, table, where)
}

// TestIngest_CompactArray_OneBadRecord_TheOthersStillLand is the §0.2
// regression guard end to end, against a real ClickHouse.
//
// Measured on the artifact: a single-line JSON array with one bad record makes
// chtypes reject the WHOLE batch and export nothing — the two good records are
// lost with the bad one, which breaks #195's promise. Ingest rewrites the
// array's depth-1 commas to newlines in place, which restores per-record
// salvage. This asserts the two survivors actually reach the table, not merely
// that the response said so.
func TestIngest_CompactArray_OneBadRecord_TheOthersStillLand(t *testing.T) {
	table := createTable(t, "user_id String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "application/json",
		`[{"user_id":"a1","value":1},{"user_id":"a2","value":"not-a-number"},{"user_id":"a3","value":3}]`,
		"", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 3, body["total"])
	assert.EqualValues(t, 2, body["succeeded"])
	assert.EqualValues(t, 1, body["failed"])

	eventuallyRows(t, table, "user_id IN ('a1','a3')", 2)
	eventuallyRows(t, table, "user_id = 'a2'", 0)
}

// TestIngest_CSVBody_LandsInClickHouse: CSV is header-less and positional in
// the table's declaration order. The end-to-end assertion is what makes the
// positional contract real — a column-order bug would still answer 200.
func TestIngest_CSVBody_LandsInClickHouse(t *testing.T) {
	table := createTable(t, "user_id String, event_type String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/csv",
		"\"c1\",\"click\",7\n\"c2\",\"view\",9\n", "", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 2, body["succeeded"])

	eventuallyRows(t, table, "user_id = 'c1' AND event_type = 'click' AND value = 7", 1)
	eventuallyRows(t, table, "user_id = 'c2' AND event_type = 'view' AND value = 9", 1)
}

// TestIngest_TSVBody_LandsInClickHouse is CSV's tab-separated twin, with a bad
// row alongside a good one so the per-record salvage is covered for the
// positional formats too.
func TestIngest_TSVBody_LandsInClickHouse(t *testing.T) {
	table := createTable(t, "user_id String, event_type String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/tab-separated-values",
		"t1\tclick\t5\nt2\tview\tnope\n", "", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 1, body["succeeded"])
	assert.EqualValues(t, 1, body["failed"])

	eventuallyRows(t, table, "user_id = 't1' AND value = 5", 1)
	eventuallyRows(t, table, "user_id = 't2'", 0)
}

// TestIngest_DeniedColumn_IsClickHouseCode117 is decision D1 end to end: column
// policy is answered by compiling the ROLE's own schema without the denied
// columns, so a record naming one is ClickHouse's per-record UNKNOWN_FIELD —
// a 400 with code 117, where it used to be the gateway's 403
// `column "x" not allowed for insert`.
func TestIngest_DeniedColumn_IsClickHouseCode117(t *testing.T) {
	table := createTable(t, "user_id String, secret String", "ORDER BY user_id")
	withIngestPolicy(t, &policy.Policy{
		AdminRole: "admin",
		Tables: map[string]policy.TablePolicy{
			table: {"writer": {Insert: &policy.InsertPermissions{DenyColumns: []string{"secret"}}}},
		},
	})

	status, body := postIngest(t, table, "application/json",
		`{"user_id":"d1","secret":"leak"}`, "writer", "")
	require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
	assert.Contains(t, body["error"], "secret")
	assert.EqualValues(t, 117, body["code"])

	// The same role WITHOUT the denied column still writes, and the column takes
	// the server's default rather than the caller's value.
	status, body = postIngest(t, table, "application/json", `{"user_id":"d2"}`, "writer", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	eventuallyRows(t, table, "user_id = 'd2' AND secret = ''", 1)
	eventuallyRows(t, table, "user_id = 'd1'", 0)
}

// TestIngest_AutoInject_FillsAnAbsentCheckColumn covers both halves of the
// per-role DEFAULT mechanism (AUDIT §A.3): a record that omits the checked
// column is filled from the claim, and a record that supplies a value keeps its
// own — the caller's value wins over the injected default, measured.
func TestIngest_AutoInject_FillsAnAbsentCheckColumn(t *testing.T) {
	table := createTable(t, "user_id String, tenant String", "ORDER BY user_id")
	tmpl := "{{ jwt.tenant }}"
	withIngestPolicy(t, &policy.Policy{
		AdminRole: "admin",
		Tables: map[string]policy.TablePolicy{
			table: {"writer": {Insert: &policy.InsertPermissions{
				Check: map[string]policy.Filter{"tenant": {Eq: &tmpl}},
			}}},
		},
	})
	const claims = `{"tenant":"acme"}`

	// Absent → injected.
	status, body := postIngest(t, table, "application/json", `{"user_id":"i1"}`, "writer", claims)
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	// Supplied and matching → the caller's own value rides through.
	status, body = postIngest(t, table, "application/json",
		`{"user_id":"i2","tenant":"acme"}`, "writer", claims)
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	// Supplied and NOT matching → the check refuses it, 403, nothing stored.
	status, body = postIngest(t, table, "application/json",
		`{"user_id":"i3","tenant":"other"}`, "writer", claims)
	require.Equal(t, http.StatusForbidden, status, "body=%v", body)
	assert.Contains(t, body["error"], "check failed")

	eventuallyRows(t, table, "user_id = 'i1' AND tenant = 'acme'", 1)
	eventuallyRows(t, table, "user_id = 'i2' AND tenant = 'acme'", 1)
	eventuallyRows(t, table, "user_id = 'i3'", 0)
}

// TestIngest_CheckIn_AbsentColumn_TestsTheTableDefault is decision D3, the one
// behaviour change auto-inject cannot cover: an `_in` check has no single value
// to inject, so a record omitting the column is judged on the TABLE's own
// default rather than failing closed on absence. Both directions are asserted,
// because the difference is entirely in what the column's default happens to be.
func TestIngest_CheckIn_AbsentColumn_TestsTheTableDefault(t *testing.T) {
	tmpl := "{{ jwt.tenants }}"
	perms := func(table string) *policy.Policy {
		return &policy.Policy{
			AdminRole: "admin",
			Tables: map[string]policy.TablePolicy{
				table: {"writer": {Insert: &policy.InsertPermissions{
					Check: map[string]policy.Filter{"tenant": {In: &tmpl}},
				}}},
			},
		}
	}

	t.Run("a default outside the set is refused", func(t *testing.T) {
		table := createTable(t, "user_id String, tenant String", "ORDER BY user_id")
		withIngestPolicy(t, perms(table))
		status, body := postIngest(t, table, "application/json",
			`{"user_id":"n1"}`, "writer", `{"tenants":["acme","globex"]}`)
		require.Equal(t, http.StatusForbidden, status, "body=%v", body)
		assert.Contains(t, body["error"], "check failed")
		eventuallyRows(t, table, "user_id = 'n1'", 0)
	})

	t.Run("a default inside the set is admitted", func(t *testing.T) {
		table := createTable(t, "user_id String, tenant String DEFAULT 'acme'", "ORDER BY user_id")
		withIngestPolicy(t, perms(table))
		status, body := postIngest(t, table, "application/json",
			`{"user_id":"n2"}`, "writer", `{"tenants":["acme","globex"]}`)
		require.Equal(t, http.StatusOK, status, "body=%v", body)
		eventuallyRows(t, table, "user_id = 'n2' AND tenant = 'acme'", 1)
	})
}
