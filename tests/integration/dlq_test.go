//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDLQ_StatsEmptyOnFreshStart verifies the DLQ exposes an empty result
// before any failures have been routed to it. Runs first because a later
// test in the same package may publish a failed-table message that
// permanently bumps the global counter for `dlq.<table>` while the
// embedded NATS lives.
func TestDLQ_StatsEmptyOnFreshStart(t *testing.T) {
	e := env(t)

	resp, err := http.Get(e.server.URL + "/v1/dlq/stats")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	tables, ok := body["tables"].(map[string]any)
	require.True(t, ok, "DLQ response missing tables field")
	assert.Empty(t, tables, "DLQ should have no entries on fresh start")
	assert.Equal(t, float64(0), body["total"])
}

// TestDLQ_PopulatedOnBentoFailure verifies that publishing an event for a
// non-existent table routes the failure into the DLQ. Bypasses the API's
// schema validation by publishing directly to JetStream — Bento's batch
// INSERT then fails, fallback fires, and the DLQ output records the entry
// under `dlq.<table>`.
func TestDLQ_PopulatedOnBentoFailure(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// A table name that intentionally doesn't exist in ClickHouse. Per-test
	// suffix keeps tests independent if more DLQ tests get added later.
	table := fmt.Sprintf("nonexistent_table_%d", time.Now().UnixNano())

	evt := map[string]any{
		"table_name":         table,
		"received_timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"data":               map[string]any{"key": "value"},
	}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	_, err = e.embeddedMQ.JetStream().Publish(ctx, "ingest."+query.EncodeTable(table), payload)
	require.NoError(t, err)

	// Bento batches every 5s; 30s upper bound gives generous slack on a
	// loaded CI runner. The condition polls the API rather than the
	// stream so this also exercises the read path.
	dlqSubject := "dlq." + table
	assert.Eventually(t, func() bool {
		resp, err := http.Get(e.server.URL + "/v1/dlq/stats")
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return false
		}
		tables, ok := body["tables"].(map[string]any)
		if !ok {
			return false
		}
		_, present := tables[dlqSubject]
		return present
	}, 30*time.Second, 500*time.Millisecond, "DLQ should receive failed events within timeout")
}

func TestDLQ_PopulatedOnBentoFailureWithBadName(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// A table name that intentionally doesn't exist in ClickHouse AND is invalid as a NATS subject. This tests that Bento's DLQ can handle subjects that are not valid NATS subjects.
	// Per-test suffix keeps tests independent if more DLQ tests get added later.

	table := fmt.Sprintf("no table.!@#&*()_=/_`%d", time.Now().UnixNano())

	evt := map[string]any{
		"table_name":         table,
		"received_timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"data":               map[string]any{"key": "value"},
	}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	_, err = e.embeddedMQ.JetStream().Publish(ctx, "ingest."+query.EncodeTable(table), payload)
	require.NoError(t, err)

	dlqSubject := "dlq." + table

	// Bento batches every 5s; 30s upper bound gives generous slack on a
	// loaded CI runner. The condition polls the API rather than the
	// stream so this also exercises the read path.
	assert.Eventually(t, func() bool {
		resp, err := http.Get(e.server.URL + "/v1/dlq/stats")
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return false
		}
		tables, ok := body["tables"].(map[string]any)
		if !ok {
			return false
		}
		// iterate over all table names to try query.DecodeTable()
		for k := range tables {
			rawTable, err := query.DecodeTable(k)
			if err == nil && rawTable == dlqSubject {
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "DLQ should receive failed events within timeout")
}
