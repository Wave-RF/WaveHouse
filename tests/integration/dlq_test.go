//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
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

	_, err = e.embeddedMQ.JetStream().Publish(ctx, "ingest."+table, payload)
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

// TestDelete_FailureRoutesToDLQWithHeader verifies the end-to-end Phase-1
// permanent-delete-error policy (#91):
//
//  1. A bad delete (table doesn't exist in ClickHouse) reaches the worker.
//  2. The worker publishes the original NATS envelope to `dlq.<table>`.
//  3. The envelope carries a `Wave-DLQ-Type: delete-envelope` NATS header
//     that survives the NATS round-trip — that's the BYOS-safe discriminator
//     a downstream consumer must use (per PR #122 review).
//  4. The buffer consumer DoubleAcks the original message — no Nak loop.
//
// The unit tests in internal/ingest/bento_test.go assert the worker called
// PublishMsg with the right header on its mock. This test proves the same
// invariants over real JetStream wire format and consumer-state semantics.
func TestDelete_FailureRoutesToDLQWithHeader(t *testing.T) {
	e := env(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	js := e.embeddedMQ.JetStream()

	// Use a table name that intentionally doesn't exist in ClickHouse so the
	// worker's DELETE Exec fails with the permanent-error path under test.
	// Per-test suffix keeps state independent across runs / from other tests.
	table := fmt.Sprintf("nonexistent_delete_dlq_%d", time.Now().UnixNano())
	deleteID := fmt.Sprintf("test-id-%d", time.Now().UnixNano())

	// Ephemeral DLQ consumer scoped to our subject (no Durable name → no
	// state leak across tests). DeliverAll covers the case where the worker
	// publishes before we start listening on busy CI.
	dlqCons, err := js.CreateOrUpdateConsumer(ctx, mq.DLQStreamName(), jetstream.ConsumerConfig{
		FilterSubject: "dlq." + table,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	require.NoError(t, err)

	// Snapshot the buffer consumer's ack state before publishing. After the
	// DLQ receive we'll assert AckFloor caught up to Delivered — a DoubleAck
	// advances both; a Nak advances Delivered but leaves AckFloor stuck
	// until AckWait elapses (30s default), the symptom of the infinite-Nak
	// loop this PR fixes.
	bufferCons, err := js.Consumer(ctx, mq.StreamName(), ingest.BufferConsumerName)
	require.NoError(t, err)
	preInfo, err := bufferCons.Info(ctx)
	require.NoError(t, err)

	// Publish the delete envelope directly to ingest.<table>. The worker
	// picks it up, fails the CH Exec (no such table), and routes the
	// envelope to dlq.<table> via PublishMsg + Wave-DLQ-Type header.
	envelope := map[string]any{
		"action":     "delete",
		"table_name": table,
		"id":         deleteID,
	}
	payload, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = js.Publish(ctx, "ingest."+table, payload)
	require.NoError(t, err)

	// Wait up to 30s for the DLQ message — covers Bento's batch window plus
	// loaded-CI slack. Fetch(1, …) is the simplest "pull next" primitive.
	batch, err := dlqCons.Fetch(1, jetstream.FetchMaxWait(30*time.Second))
	require.NoError(t, err)
	var got jetstream.Msg
	for m := range batch.Messages() {
		got = m
		break
	}
	require.NoError(t, batch.Error())
	require.NotNil(t, got, "DLQ should receive the failed delete envelope within timeout")
	defer func() { _ = got.Ack() }()

	// BYOS-safe discriminator: header is set by the worker (non-user-
	// controlled). A user row with an `action` column can't forge this.
	assert.Equal(t, "delete-envelope", got.Headers().Get("Wave-DLQ-Type"),
		"Wave-DLQ-Type header must survive the NATS round-trip end-to-end")

	// Subject and payload are preserved bit-for-bit so a Phase-2 consumer
	// can re-issue the delete with full context.
	assert.Equal(t, "dlq."+table, got.Subject())
	assert.JSONEq(t, string(payload), string(got.Data()),
		"DLQ envelope must equal the original ingest.<table> envelope verbatim")

	// Note: we intentionally do NOT assert on the W3C `traceparent` header
	// here. observability.InjectNATS is called by the worker and writes
	// traceparent in production (where cmd/wavehouse runs
	// observability.InitProvider, installing a real TracerProvider +
	// TraceContext propagator). The integration test harness in
	// setup_test.go intentionally leaves both globals unset so otel_test.go
	// can save/restore them around its own InitProvider invocations. Adding
	// trace-propagation coverage to this test would require either wiring
	// InitProvider into the shared harness (touches otel_test.go's
	// save/restore dance) or installing a per-test recording tracer.
	// The propagator round-trip is fully covered by
	// observability/tracer_test.go:TestInjectExtractNATS_Roundtrip, so the
	// signal-per-effort here is low.

	// Buffer consumer state: AckFloor must have caught up to Delivered,
	// proving the worker DoubleAck'd our delete rather than Nak'ing it.
	// Use Eventually with a short window — the DoubleAck and ack-floor
	// update happen post-DLQ-publish but typically within a few ms.
	require.Eventually(t, func() bool {
		postInfo, err := bufferCons.Info(ctx)
		if err != nil {
			return false
		}
		// Our publish advanced Delivered by at least 1.
		if postInfo.Delivered.Consumer < preInfo.Delivered.Consumer+1 {
			return false
		}
		// Nothing should be stuck pending an ack — neither our message
		// (which must have been DoubleAck'd) nor any earlier test's.
		return postInfo.NumAckPending == 0
	}, 5*time.Second, 100*time.Millisecond,
		"buffer consumer must DoubleAck the failed delete (NumAckPending=0); a Nak would leave it pending until AckWait elapses")
}
