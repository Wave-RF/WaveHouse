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
	streamName := "WAVEHOUSE"
	emb, err := mq.NewEmbedded(dir, "WAVEHOUSE", 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	defer emb.Close()

	handler := NewDLQHandler(emb.JetStream(), streamName, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/v1/dlq/stats", nil)
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
	streamName := "WAVEHOUSE"
	emb, err := mq.NewEmbedded(dir, "WAVEHOUSE", 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	defer emb.Close()

	js := emb.JetStream()
	ctx := context.Background()

	// Create the DLQ stream.
	require.NoError(t, EnsureDLQStream(ctx, js, streamName, 1024*1024))

	// Publish messages to DLQ subjects.
	for i := 0; i < 3; i++ {
		_, err := js.Publish(ctx, "dlq.events", []byte(`{"table_name":"events"}`))
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		_, err := js.Publish(ctx, "dlq.users", []byte(`{"table_name":"users"}`))
		require.NoError(t, err)
	}

	handler := NewDLQHandler(js, streamName, slog.Default())
	req := httptest.NewRequest(http.MethodGet, "/v1/dlq/stats", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["tables"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(3), tables["dlq.events"])
	assert.Equal(t, float64(2), tables["dlq.users"])
	assert.Equal(t, float64(5), resp["total"])
}

func TestDLQStats_SingleTable(t *testing.T) {
	dir := t.TempDir()
	streamName := "WAVEHOUSE"
	emb, err := mq.NewEmbedded(dir, "WAVEHOUSE", 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	defer emb.Close()

	js := emb.JetStream()
	ctx := context.Background()

	require.NoError(t, EnsureDLQStream(ctx, js, streamName, 1024*1024))

	_, err = js.Publish(ctx, "dlq.orders", []byte(`{"table_name":"orders"}`))
	require.NoError(t, err)

	handler := NewDLQHandler(js, streamName, slog.Default())
	req := httptest.NewRequest(http.MethodGet, "/v1/dlq/stats", nil)
	rec := httptest.NewRecorder()

	handler.Stats(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables := resp["tables"].(map[string]any)
	assert.Equal(t, float64(1), tables["dlq.orders"])
	assert.Equal(t, float64(1), resp["total"])
}

func TestEnsureDLQStream_Idempotent(t *testing.T) {
	dir := t.TempDir()
	streamName := "WAVEHOUSE"
	emb, err := mq.NewEmbedded(dir, "WAVEHOUSE", 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	defer emb.Close()

	js := emb.JetStream()
	ctx := context.Background()

	// Calling twice should not error.
	require.NoError(t, EnsureDLQStream(ctx, js, streamName, 1024*1024))
    require.NoError(t, EnsureDLQStream(ctx, js, streamName, 1024*1024))

	handler := NewDLQHandler(js, streamName, slog.Default())
	
	// Stream should be accessible.
	stream, err := js.Stream(ctx, handler.DLQStreamName)
	require.NoError(t, err)

	info, err := stream.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, handler.DLQStreamName, info.Config.Name)
	assert.Equal(t, []string{"dlq.>"}, info.Config.Subjects)
}
