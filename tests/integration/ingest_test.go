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

// TestIngest_InvalidatesCacheTag is the read-your-writes regression guard.
// A regression in the invalidation call site — InvalidateByTags silently not
// called, the wrong tableName used as the tag, or Wait() removed from
// InvalidateByTags so the Del races the still-buffered Set — would otherwise
// pass every other test in this package. We plant a cache entry tagged with
// the destination table, drive a real ingest through the worker, and assert
// the entry is gone before the row's TTL would have ever expired it.
func TestIngest_InvalidatesCacheTag(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	table := createTable(t,
		"user_id String, event_type String, value Float64",
		"ORDER BY user_id",
	)

	cacheKey := "ryw-test:" + table
	cacheVal := []byte(`{"planted":true}`)
	require.NoError(t, e.cache.Set(ctx, cacheKey, cacheVal, time.Hour, []string{table}))

	require.Eventually(t, func() bool {
		data, _, err := e.cache.Get(ctx, cacheKey)
		return err == nil && data != nil
	}, 5*time.Second, 50*time.Millisecond, "planted cache entry must be visible before ingest")

	body := `{"user_id":"bob","event_type":"click","value":7}`
	resp, err := http.Post(
		e.server.URL+"/v1/ingest/"+table,
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Eventually(t, func() bool {
		data, _, err := e.cache.Get(ctx, cacheKey)
		return err == nil && data == nil
	}, 30*time.Second, 200*time.Millisecond, "ingest must invalidate the table-tagged cache entry")
}
