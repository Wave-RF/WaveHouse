//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pollCount returns the row count for SELECT count() FROM `<table>` WHERE id = ?,
// or -1 on query error. Used by the happy-path delete test to poll for both
// the insert landing and the row disappearing post-delete.
//
// `id` is bound via the driver's parameterized form rather than interpolated
// into the SQL, mirroring the production pattern in internal/ingest/bento.go.
// `table` still uses fmt.Sprintf because ClickHouse identifiers can't be bound
// as parameters — backtick quoting is the same defense bento.go uses.
func pollCount(t *testing.T, table, id string) int64 {
	t.Helper()
	var count uint64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := env(t).chConn.QueryRow(ctx,
		fmt.Sprintf("SELECT count() FROM `%s` WHERE id = ?", table),
		id,
	).Scan(&count)
	if err != nil {
		return -1
	}
	return int64(count)
}

// TestIngest_FlowsToClickHouseWithoutDLQ exercises the happy path: POST
// /v1/ingest/{table} is acknowledged synchronously, Bento batches the event
// to ClickHouse, and the DLQ stays empty for that table.
func TestIngest_FlowsToClickHouseWithoutDLQ(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	table := createTable(t,
		"user_id String, event_type String, value Float64",
		"ORDER BY user_id",
	)

	body := `{"user_id":"alice","event_type":"click","value":42.5}`
	resp, err := http.Post(
		e.server.URL+"/v1/ingest/"+table,
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var ingestResp map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ingestResp))
	assert.Equal(t, true, ingestResp["ok"])

	// 30s upper bound for Bento's 5s batch window plus loaded-runner slack.
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
	dlqResp, err := http.Get(e.server.URL + "/v1/dlq/stats")
	require.NoError(t, err)
	defer dlqResp.Body.Close()

	var stats map[string]any
	require.NoError(t, json.NewDecoder(dlqResp.Body).Decode(&stats))
	tables, ok := stats["tables"].(map[string]any)
	require.True(t, ok)
	_, hasDLQ := tables["dlq."+table]
	assert.False(t, hasDLQ, "successful inserts should not produce DLQ entries")
}

// TestDelete_HappyPath exercises the worker's success branch for
// `action: "delete"`: an inserted row is removed end-to-end via a delete
// envelope published to ingest.<table>. There's no public SDK / HTTP delete
// surface today — the delete envelope is a NATS-only contract — so the test
// publishes directly to JetStream and observes ClickHouse state via the
// shared connection.
//
// Pairs with TestDelete_FailureRoutesToDLQWithHeader in dlq_test.go, which
// covers the failure branch. Together they give the worker's delete path
// real end-to-end coverage — previously zero.
func TestDelete_HappyPath(t *testing.T) {
	e := env(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	table := createTable(t,
		"id String, page String",
		"ORDER BY id",
	)
	const rowID = "row-to-delete"

	// Insert via the public API so the test exercises the full ingest path
	// (validation → JetStream → Bento → ClickHouse) before the delete.
	body := fmt.Sprintf(`{"id":%q,"page":"/about"}`, rowID)
	resp, err := http.Post(
		e.server.URL+"/v1/ingest/"+table,
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for the insert to land. 30s upper bound on Bento's 5s batch
	// window + loaded-runner slack, matching other tests in this package.
	require.Eventually(t, func() bool {
		return pollCount(t, table, rowID) == 1
	}, 30*time.Second, 500*time.Millisecond, "insert should land before delete")

	// Publish the delete envelope directly to JetStream. The worker reads
	// it via the buffer-consumer, executes DELETE FROM `<table>` WHERE id=?,
	// and DoubleAcks on Exec success (internal/ingest/bento.go).
	envelope := map[string]any{
		"action":     "delete",
		"table_name": table,
		"id":         rowID,
	}
	payload, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = e.embeddedMQ.JetStream().Publish(ctx, "ingest."+table, payload)
	require.NoError(t, err)

	// ClickHouse lightweight DELETE (default in modern CH) marks rows
	// invisible immediately after the mutation is applied. 30s upper bound
	// is generous for a single-row mutation on the test runner.
	require.Eventually(t, func() bool {
		return pollCount(t, table, rowID) == 0
	}, 30*time.Second, 500*time.Millisecond,
		"action:\"delete\" envelope should remove the row from ClickHouse")

	// Confirm the success branch didn't tee anything into the DLQ — the
	// permanent-error policy only fires on Exec failures.
	dlqResp, err := http.Get(e.server.URL + "/v1/dlq/stats")
	require.NoError(t, err)
	defer dlqResp.Body.Close()
	var stats map[string]any
	require.NoError(t, json.NewDecoder(dlqResp.Body).Decode(&stats))
	tables, ok := stats["tables"].(map[string]any)
	require.True(t, ok)
	_, hasDLQ := tables["dlq."+table]
	assert.False(t, hasDLQ, "successful deletes must not produce DLQ entries")
}
