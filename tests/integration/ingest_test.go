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
		e.baseURL+"/v1/ingest?table="+url.QueryEscape(table),
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
	// duplicate parking on the DLQ alongside the real INSERT).
	dlqResp, err := http.Get(e.baseURL + "/v1/ops/dlq/stats")
	require.NoError(t, err)
	defer dlqResp.Body.Close()

	var stats map[string]any
	require.NoError(t, json.NewDecoder(dlqResp.Body).Decode(&stats))
	tables, ok := stats["tables"].(map[string]any)
	require.True(t, ok)
	_, hasDLQ := tables[table]
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
		e.baseURL+"/v1/ingest?table="+url.QueryEscape(table),
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
// than having the value silently dropped by the positional encoder. The
// refusal is ClickHouse's own: a MATERIALIZED column is not one a record may
// carry, so the parser answers with its own message and code (117).
func TestIngest_SuppliedComputedColumn_Rejected(t *testing.T) {
	table := createTable(t,
		"user_id String, digest String MATERIALIZED concat('d:', user_id)",
		"ORDER BY user_id",
	)

	status, body := postIngest(t, table, "application/json", `{"user_id":"dave","digest":"forged"}`, "")
	require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
	assert.Contains(t, body["error"], "digest")
	assert.EqualValues(t, 117, body["exception_code"], "ClickHouse's own code, not a gateway guess")
}

// postIngest is one POST to /v1/ingest with a verbatim body and an explicit
// Content-Type, as the suite's default admin caller or, with authorization
// set (bearer), as the role its token names.
func postIngest(t *testing.T, table, contentType, body, authorization string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		env(t).baseURL+"/v1/ingest?table="+url.QueryEscape(table), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var decoded map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))
	return resp.StatusCode, decoded
}

// eventuallyRows waits out the ingest worker's batch window for the count of
// rows matching where to be want. A want of 0 holds only once something posted
// later has landed, so the tests asserting an absence post a record that
// lands after the refused ones and wait for it first.
//
// Each wait is most of the worker's batch window, idle, so the tests here
// that wait run in parallel: each writes and reads a table of its own, and a
// policy it adopts grants on that table alone (withPolicy holds the union).
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

// A single-line JSON array with one bad record: the other two still land.
// Measured on the artifact, an unframed array with one bad record makes the
// parser refuse the whole batch and export nothing, which would break the
// per-record promise of #195; ingest rewrites the array's depth-1 commas to
// newlines in place, which restores per-record salvage. Asserted in the table,
// not only in the response.
func TestIngest_CompactArray_OneBadRecord_TheOthersStillLand(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "application/json",
		`[{"user_id":"a1","value":1},{"user_id":"a2","value":"not-a-number"},{"user_id":"a3","value":3}]`, "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 3, body["total"])
	assert.EqualValues(t, 2, body["succeeded"])
	assert.EqualValues(t, 1, body["failed"])

	eventuallyRows(t, table, "user_id IN ('a1','a3')", 2)
	eventuallyRows(t, table, "user_id = 'a2'", 0)
}

// CSV is positional in the table's declaration order. The end-to-end
// assertion is what makes the positional contract real: a column-order bug
// would still answer 200.
func TestIngest_CSVBody_LandsInClickHouse(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, event_type String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/csv", "\"c1\",\"click\",7\n\"c2\",\"view\",9\n", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 2, body["succeeded"])

	eventuallyRows(t, table, "user_id = 'c1' AND event_type = 'click' AND value = 7", 1)
	eventuallyRows(t, table, "user_id = 'c2' AND event_type = 'view' AND value = 9", 1)
}

// A bare text/csv is ClickHouse's default CSV, so a first line spelling the
// column names is consumed as a header and only the data rows land.
func TestIngest_BareCSVHeader_LandsInClickHouse(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, event_type String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/csv", "user_id,event_type,value\n\"d1\",\"click\",7\n", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 1, body["total"], "the detected header is not a record")
	assert.EqualValues(t, 1, body["succeeded"])

	eventuallyRows(t, table, "user_id = 'd1' AND value = 7", 1)
	eventuallyRows(t, table, "user_id = 'user_id'", 0)
}

// header=absent is strictly positional, so the same header line is one
// refused record and never reaches the table.
func TestIngest_CSVHeaderAbsent_LandsInClickHouse(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, event_type String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/csv; header=absent", "user_id,event_type,value\n\"a1\",\"click\",7\n", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 2, body["total"])
	assert.EqualValues(t, 1, body["succeeded"])
	assert.EqualValues(t, 1, body["failed"])

	eventuallyRows(t, table, "user_id = 'a1' AND value = 7", 1)
	eventuallyRows(t, table, "user_id = 'user_id'", 0)
}

// TSV is CSV's tab-separated twin, with a bad row beside a good one, so
// per-record salvage is covered for the positional formats too.
func TestIngest_TSVBody_LandsInClickHouse(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, event_type String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/tab-separated-values", "t1\tclick\t5\nt2\tview\tnope\n", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 1, body["succeeded"])
	assert.EqualValues(t, 1, body["failed"])

	eventuallyRows(t, table, "user_id = 't1' AND value = 5", 1)
	eventuallyRows(t, table, "user_id = 't2'", 0)
}

// text/csv; header=present addresses the columns by the header, in any order:
// the header is not a record, a column it omits takes the table's own
// DEFAULT, and a bad row is salvaged like any other.
func TestIngest_CSVWithNamesBody_LandsInClickHouse(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, event_type String, value UInt32 DEFAULT 42", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/csv; header=present", "event_type,user_id\nclick,h1\nview,h2\n", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 2, body["total"], "the header line is not a record")
	assert.EqualValues(t, 2, body["succeeded"])

	eventuallyRows(t, table, "user_id = 'h1' AND event_type = 'click' AND value = 42", 1)
	eventuallyRows(t, table, "user_id = 'h2' AND event_type = 'view' AND value = 42", 1)
}

// The tab-separated twin of the header=present case, with a bad row beside a
// good one.
func TestIngest_TSVWithNamesBody_LandsInClickHouse(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, event_type String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/tab-separated-values; header=present",
		"value\tuser_id\tevent_type\n5\tn1\tclick\nnope\tn2\tview\n", "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.EqualValues(t, 1, body["succeeded"])
	assert.EqualValues(t, 1, body["failed"])

	eventuallyRows(t, table, "user_id = 'n1' AND value = 5", 1)
	eventuallyRows(t, table, "user_id = 'n2'", 0)
}

// A header naming a column the table does not have is ClickHouse's own
// refusal of the body, before any record: a whole-request 400 carrying its
// code, and nothing stored.
func TestIngest_WithNamesUnknownHeader_Is400WithCode117(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, value UInt32", "ORDER BY user_id")

	status, body := postIngest(t, table, "text/csv; header=present", "user_id,nosuch\nu1,1\n", "")
	require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
	assert.Equal(t, "clickhouse.rejected", body["code"])
	assert.EqualValues(t, 117, body["exception_code"])
	assert.Contains(t, body["error"], "nosuch")

	status, body = postIngest(t, table, "application/json", `{"user_id":"after","value":2}`, "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	eventuallyRows(t, table, "user_id = 'after'", 1)
	eventuallyRows(t, table, "1", 1)
}

// A column the role may not write is not in the schema its records are
// parsed against, so a record naming one is ClickHouse's per-record refusal —
// 400 with code 117 — where it used to be the gateway's 403. The same role
// writing without it still lands, the column taking the table's default
// rather than any value of the caller's.
func TestIngest_DeniedColumn_IsClickHouseCode117(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, secret String", "ORDER BY user_id")
	withPolicy(t, policy.Policy{Tables: map[string]policy.TablePolicy{
		table: {"writer": {Insert: &policy.InsertPermissions{DenyColumns: []string{"secret"}}}},
	}})
	writer := bearer(t, "writer", nil)

	status, body := postIngest(t, table, "application/json", `{"user_id":"d1","secret":"leak"}`, writer)
	require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
	assert.Contains(t, body["error"], "secret")
	assert.EqualValues(t, 117, body["exception_code"])

	status, body = postIngest(t, table, "application/json", `{"user_id":"d2"}`, writer)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	eventuallyRows(t, table, "user_id = 'd2' AND secret = ''", 1)
	eventuallyRows(t, table, "user_id = 'd1'", 0)
}

// An _eq check fills a record that omits its column from the claim, keeps a
// record's own value when it matches, and refuses one that does not (403,
// nothing stored).
func TestIngest_AutoInject_FillsAnAbsentCheckColumn(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, tenant String", "ORDER BY user_id")
	tmpl := "{{ jwt.tenant }}"
	withPolicy(t, policy.Policy{Tables: map[string]policy.TablePolicy{
		table: {"writer": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{"tenant": {Eq: &tmpl}}}}},
	}})
	writer := bearer(t, "writer", map[string]any{"tenant": "acme"})

	status, body := postIngest(t, table, "application/json", `{"user_id":"i1"}`, writer)
	require.Equal(t, http.StatusOK, status, "absent → injected; body=%v", body)
	status, body = postIngest(t, table, "application/json", `{"user_id":"i2","tenant":"acme"}`, writer)
	require.Equal(t, http.StatusOK, status, "supplied and matching; body=%v", body)
	status, body = postIngest(t, table, "application/json", `{"user_id":"i3","tenant":"other"}`, writer)
	require.Equal(t, http.StatusForbidden, status, "supplied and not matching; body=%v", body)
	assert.Contains(t, body["error"], "check failed")

	eventuallyRows(t, table, "user_id = 'i1' AND tenant = 'acme'", 1)
	eventuallyRows(t, table, "user_id = 'i2' AND tenant = 'acme'", 1)
	eventuallyRows(t, table, "user_id = 'i3'", 0)
}

// An _in check has no single value to inject, so a record omitting its column
// is judged on the TABLE's default rather than refused for the absence. Both
// directions, because the outcome is entirely what the default happens to be.
func TestIngest_CheckIn_AbsentColumn_TestsTheTableDefault(t *testing.T) {
	t.Parallel()
	tmpl := "{{ jwt.tenants }}"
	policyFor := func(table string) policy.Policy {
		return policy.Policy{Tables: map[string]policy.TablePolicy{
			table: {"writer": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{"tenant": {In: &tmpl}}}}},
		}}
	}
	claims := map[string]any{"tenants": []any{"acme", "globex"}}

	t.Run("a default outside the set is refused", func(t *testing.T) {
		t.Parallel()
		table := createTable(t, "user_id String, tenant String", "ORDER BY user_id")
		withPolicy(t, policyFor(table))
		writer := bearer(t, "writer", claims)
		status, body := postIngest(t, table, "application/json", `{"user_id":"n1"}`, writer)
		require.Equal(t, http.StatusForbidden, status, "body=%v", body)
		assert.Contains(t, body["error"], "check failed")
		status, body = postIngest(t, table, "application/json", `{"user_id":"n2","tenant":"globex"}`, writer)
		require.Equal(t, http.StatusOK, status, "body=%v", body)
		eventuallyRows(t, table, "user_id = 'n2'", 1)
		eventuallyRows(t, table, "user_id = 'n1'", 0)
	})

	t.Run("a default inside the set is admitted", func(t *testing.T) {
		t.Parallel()
		table := createTable(t, "user_id String, tenant String DEFAULT 'acme'", "ORDER BY user_id")
		withPolicy(t, policyFor(table))
		status, body := postIngest(t, table, "application/json", `{"user_id":"n3"}`, bearer(t, "writer", claims))
		require.Equal(t, http.StatusOK, status, "body=%v", body)
		eventuallyRows(t, table, "user_id = 'n3' AND tenant = 'acme'", 1)
	})
}

// An _eq check on an integer column compares the claim through the strict
// round-trip cast. 2^64+5 does not fit a UInt64, yet a plain String binding
// wrapped it onto 5 in the check — and the injected DEFAULT wraps it onto 5
// too — so a record used to land under tenant 5. Now the check refuses it
// (403) and nothing is published, whether the record omits the column or
// supplies the wrapped value itself; the same policy with a claim that fits
// lands as before.
func TestIngest_IntegerCheckClaimThatDoesNotFit_IsRefused(t *testing.T) {
	t.Parallel()
	table := createTable(t, "user_id String, tenant UInt64", "ORDER BY user_id")
	tmpl := "{{ jwt.tenant }}"
	withPolicy(t, policy.Policy{Tables: map[string]policy.TablePolicy{
		table: {"writer": {Insert: &policy.InsertPermissions{Check: map[string]policy.Filter{"tenant": {Eq: &tmpl}}}}},
	}})
	over := bearer(t, "writer", map[string]any{"tenant": "18446744073709551621"})
	fits := bearer(t, "writer", map[string]any{"tenant": "5"})

	for _, rec := range []string{`{"user_id":"o1"}`, `{"user_id":"o2","tenant":5}`, `{"user_id":"o3","tenant":"18446744073709551621"}`} {
		status, body := postIngest(t, table, "application/json", rec, over)
		require.Equal(t, http.StatusForbidden, status, "%s: body=%v", rec, body)
		assert.Contains(t, body["error"], "check failed", rec)
	}

	status, body := postIngest(t, table, "application/json", `{"user_id":"i1"}`, fits)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = postIngest(t, table, "application/json", `{"user_id":"i2","tenant":5}`, fits)
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	// The admitted records were posted after the refused ones, so once they
	// have landed a published refusal would have landed too.
	eventuallyRows(t, table, "user_id IN ('i1', 'i2') AND tenant = 5", 2)
	eventuallyRows(t, table, "user_id IN ('o1', 'o2', 'o3')", 0)
	eventuallyRows(t, table, "1 = 1", 2)
}
