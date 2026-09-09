//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
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

	resp, err := http.Get(e.server.URL + "/v1/ops/dlq/stats")
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

// TestDLQ_PopulatedOnIngestWorkerFailure verifies that publishing an event for a
// non-existent table routes the failure into the DLQ. Bypasses the API's
// schema validation by publishing directly to JetStream — ingest worker's batch
// INSERT then fails, fallback fires, and the DLQ output records the entry
// under `dlq.<table>`.
func TestDLQ_PopulatedOnIngestWorkerFailure(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// A table name that intentionally doesn't exist in ClickHouse. Per-test
	// suffix keeps tests independent if more DLQ tests get added later.
	rawTableName := fmt.Sprintf("nonexistent_table_%d", time.Now().UnixNano())
	safeTableName := query.SafeEncodeNATS(rawTableName)

	payload, err := json.Marshal(ingest.EventMessage{
		TableName:         rawTableName,
		ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Format:            ingest.FormatJSONCompactEachRow,
		Columns:           []string{"key"},
		Row:               json.RawMessage(`["value"]`),
	})
	require.NoError(t, err)

	_, err = e.embeddedMQ.JetStream().Publish(ctx, "ingest."+safeTableName, payload)
	require.NoError(t, err)

	// Ingest worker batches every 5s; 30s upper bound gives generous slack on a
	// loaded CI runner. The condition polls the API rather than the
	// stream so this also exercises the read path.
	assert.Eventually(t, func() bool {
		resp, err := http.Get(e.server.URL + "/v1/ops/dlq/stats")
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
		_, present := tables[rawTableName]
		return present
	}, 30*time.Second, 500*time.Millisecond, "DLQ should receive failed events within timeout")
}

func TestDLQ_PopulatedOnIngestWorkerFailureWithBadName(t *testing.T) {
	e := env(t)
	ctx := context.Background()

	// A table name that intentionally doesn't exist in ClickHouse AND is invalid as a NATS subject. This tests that ingest worker's DLQ can handle subjects that are not valid NATS subjects.
	// Per-test suffix keeps tests independent if more DLQ tests get added later.

	rawTableName := fmt.Sprintf("no table.!@#&*()_=/_`%d", time.Now().UnixNano())
	safeTableName := query.SafeEncodeNATS(rawTableName)

	payload, err := json.Marshal(ingest.EventMessage{
		TableName:         rawTableName,
		ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Format:            ingest.FormatJSONCompactEachRow,
		Columns:           []string{"key"},
		Row:               json.RawMessage(`["value"]`),
	})
	require.NoError(t, err)

	_, err = e.embeddedMQ.JetStream().Publish(ctx, "ingest."+safeTableName, payload)
	require.NoError(t, err)

	// Ingest worker batches every 5s; 30s upper bound gives generous slack on a
	// loaded CI runner. The condition polls the API rather than the
	// stream so this also exercises the read path.
	assert.Eventually(t, func() bool {
		resp, err := http.Get(e.server.URL + "/v1/ops/dlq/stats")
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
		_, exists := tables[rawTableName]
		return exists
	}, 30*time.Second, 500*time.Millisecond, "DLQ should receive failed events within timeout")
}
