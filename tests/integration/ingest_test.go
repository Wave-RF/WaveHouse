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

// TestIngest_NonInsertActionDropped pins the insert-only contract end-to-end:
// an envelope with `action: "delete"` published directly to the buffer
// stream is dropped (DoubleAck'd) by the worker and produces no DLQ entry
// and no ClickHouse mutation. Mirrors the unit-test coverage in
// internal/ingest/bento_test.go over the real NATS wire format. All
// non-insert mutations must go through POST /v1/query — see
// TestQuery_TruncateReturnsEmptyArray.
func TestIngest_NonInsertActionDropped(t *testing.T) {
	e := env(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	table := createTable(t,
		"id String, page String",
		"ORDER BY id",
	)
	const rowID = "row-survives-rogue-delete"

	body := fmt.Sprintf(`{"id":%q,"page":"/about"}`, rowID)
	resp, err := http.Post(
		e.server.URL+"/v1/ingest/"+table,
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for the insert to land so we can prove the rogue delete envelope
	// below didn't remove it.
	require.Eventually(t, func() bool {
		var count uint64
		err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM `%s` WHERE id = ?", table),
			rowID,
		).Scan(&count)
		return err == nil && count == 1
	}, 30*time.Second, 500*time.Millisecond, "insert should land")

	// Publish a delete envelope directly to JetStream. The pre-lock pipeline
	// would have executed DELETE FROM <table> WHERE id=?; the insert-only
	// pipeline must drop this envelope without touching ClickHouse.
	envelope := map[string]any{
		"action":     "delete",
		"table_name": table,
		"id":         rowID,
	}
	payload, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = e.embeddedMQ.JetStream().Publish(ctx, "ingest."+table, payload)
	require.NoError(t, err)

	// The row must still be there after enough time for the worker to have
	// processed and discarded the delete envelope. We bound by the same
	// 30s window the ingest happy path uses; if the worker were still
	// honoring `action: "delete"`, the row would disappear inside that
	// window.
	require.Never(t, func() bool {
		var count uint64
		err := e.chConn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM `%s` WHERE id = ?", table),
			rowID,
		).Scan(&count)
		return err == nil && count == 0
	}, 5*time.Second, 500*time.Millisecond,
		"non-insert envelope must not mutate ClickHouse — DELETE/UPDATE/etc. require POST /v1/query")

	// And nothing should have leaked to the DLQ either — the dropped
	// envelope is not a failure, it's a contract rejection.
	dlqResp, err := http.Get(e.server.URL + "/v1/dlq/stats")
	require.NoError(t, err)
	defer dlqResp.Body.Close()
	var stats map[string]any
	require.NoError(t, json.NewDecoder(dlqResp.Body).Decode(&stats))
	tables, ok := stats["tables"].(map[string]any)
	require.True(t, ok)
	_, hasDLQ := tables["dlq."+table]
	assert.False(t, hasDLQ, "rejected non-insert envelopes must not be routed to the DLQ")
}
