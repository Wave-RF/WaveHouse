package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDLQStats_EmptyWhenNoStream(t *testing.T) {
	dir := t.TempDir()
	emb, err := mq.NewEmbedded(dir, 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	defer func() { _ = emb.Close() }()

	handler := NewDLQHandler(emb, slog.Default())

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

	// Create the DLQ stream.
	require.NoError(t, emb.EnsureDLQStream(ctx, 1024*1024))

	// Publish messages to DLQ subjects.
	for i := 0; i < 3; i++ {
		require.NoError(t, emb.Publish(ctx, "dlq.events", []byte(`{"table_name":"events"}`)))
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, emb.Publish(ctx, "dlq.users", []byte(`{"table_name":"users"}`)))
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

	require.NoError(t, emb.EnsureDLQStream(ctx, 1024*1024))

	require.NoError(t, emb.Publish(ctx, "dlq.orders", []byte(`{"table_name":"orders"}`)))

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
