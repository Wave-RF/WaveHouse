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
	assert.Contains(t, body["error"], "cannot be inserted")
}
