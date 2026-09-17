package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parkedMsg is a message as the ingest worker would hand it to the DLQ.
func parkedMsg(table string) *mq.Message {
	return (&testutil.MockMessage{
		MsgTopic: mq.Topic{Table: table},
		MsgData:  []byte(`{"table_name":"` + table + `"}`),
	}).Message()
}

func TestDLQStats_EmptyWhenNoStream(t *testing.T) {
	// The embedded MQ always has a dead-letter queue, so its absence comes
	// from a mock.
	handler := NewDLQHandler(&testutil.MockDeadLetterStats{Err: mq.ErrNoDeadLetterQueue}, slog.Default())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/ops/dlq/stats", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["tables"].(map[string]any)
	require.True(t, ok)
	assert.Empty(t, tables)
	assert.Equal(t, float64(0), resp["total"])
}

func TestDLQStats_ReturnsCorrectCounts(t *testing.T) {
	dir := t.TempDir()
	emb, err := mq.NewEmbedded(dir, 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	defer func() { _ = emb.Close() }()

	ctx := context.Background()

	// Park messages on the dead-letter queue.
	for i := 0; i < 3; i++ {
		require.NoError(t, emb.DeadLetter(ctx, parkedMsg("events")))
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, emb.DeadLetter(ctx, parkedMsg("users")))
	}

	handler := NewDLQHandler(emb, slog.Default())
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/ops/dlq/stats", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["tables"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(3), tables["events"])
	assert.Equal(t, float64(2), tables["users"])
	assert.Equal(t, float64(5), resp["total"])
}

func TestDLQStats_SingleTable(t *testing.T) {
	dir := t.TempDir()
	emb, err := mq.NewEmbedded(dir, 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	defer func() { _ = emb.Close() }()

	ctx := context.Background()

	require.NoError(t, emb.DeadLetter(ctx, parkedMsg("orders")))

	handler := NewDLQHandler(emb, slog.Default())
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/ops/dlq/stats", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables := resp["tables"].(map[string]any)
	assert.Equal(t, float64(1), tables["orders"])
	assert.Equal(t, float64(1), resp["total"])
}

func TestDLQStats_BrokerFailureIsAnError(t *testing.T) {
	handler := NewDLQHandler(&testutil.MockDeadLetterStats{Err: errors.New("broker unavailable")}, testutil.NopLogger())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/ops/dlq/stats", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code, "a failed read is not an empty queue")
}

func TestDLQStats_PassesTheTableFilter(t *testing.T) {
	emb, err := mq.NewEmbedded(t.TempDir(), 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	defer func() { _ = emb.Close() }()

	ctx := context.Background()
	require.NoError(t, emb.DeadLetter(ctx, parkedMsg("default.orders")))
	require.NoError(t, emb.DeadLetter(ctx, parkedMsg("users")))

	handler := NewDLQHandler(emb, slog.Default())
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/ops/dlq/stats?table=default.orders", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, map[string]any{"default.orders": float64(1)}, resp["tables"])
	assert.Equal(t, float64(2), resp["total"])
}
