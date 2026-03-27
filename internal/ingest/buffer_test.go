package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// emptyRegistry returns a SchemaRegistry with no tables.
// Get() returns nil without panicking.
func emptyRegistry() *discovery.SchemaRegistry {
	return discovery.NewSchemaRegistry(nil, "", 0, slog.Default())
}

// mockPublisher is a test double for mq.Publisher that records all published messages.
type mockPublisher struct {
	mu       sync.Mutex
	messages []publishedMessage
}

type publishedMessage struct {
	Subject string
	Data    []byte
}

func (m *mockPublisher) Publish(_ context.Context, subject string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, publishedMessage{Subject: subject, Data: data})
	return nil
}

func (m *mockPublisher) Close() error { return nil }

func (m *mockPublisher) get() []publishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]publishedMessage, len(m.messages))
	copy(out, m.messages)
	return out
}

func TestSendToDLQ_PublishesToCorrectSubjects(t *testing.T) {
	pub := &mockPublisher{}
	logger := slog.Default()

	bc := NewBufferConsumer(nil, nil, nil, 100, time.Second, logger)
	bc.SetDLQ(pub)

	events := []EventMessage{
		{TableName: "clicks", ReceivedTimestamp: "2026-01-01T00:00:00Z", Data: map[string]any{"user_id": "alice"}},
		{TableName: "clicks", ReceivedTimestamp: "2026-01-01T00:00:01Z", Data: map[string]any{"user_id": "bob"}},
	}

	bc.sendToDLQ(context.Background(), "clicks", events)

	msgs := pub.get()
	require.Len(t, msgs, 2, "expected one DLQ message per event")

	for i, msg := range msgs {
		assert.Equal(t, "dlq.clicks", msg.Subject)

		var got EventMessage
		require.NoError(t, json.Unmarshal(msg.Data, &got))
		assert.Equal(t, events[i].TableName, got.TableName)
		assert.Equal(t, events[i].ReceivedTimestamp, got.ReceivedTimestamp)
		assert.Equal(t, events[i].Data["user_id"], got.Data["user_id"])
	}
}

func TestSendToDLQ_DifferentTables(t *testing.T) {
	pub := &mockPublisher{}
	logger := slog.Default()

	bc := NewBufferConsumer(nil, nil, nil, 100, time.Second, logger)
	bc.SetDLQ(pub)

	bc.sendToDLQ(context.Background(), "clicks", []EventMessage{
		{TableName: "clicks", Data: map[string]any{"id": "1"}},
	})
	bc.sendToDLQ(context.Background(), "users", []EventMessage{
		{TableName: "users", Data: map[string]any{"id": "2"}},
	})

	msgs := pub.get()
	require.Len(t, msgs, 2)
	assert.Equal(t, "dlq.clicks", msgs[0].Subject)
	assert.Equal(t, "dlq.users", msgs[1].Subject)
}

func TestFlushBatch_RoutesToDLQOnInsertFailure(t *testing.T) {
	pub := &mockPublisher{}
	logger := slog.Default()

	// Empty registry → Get(table) returns nil → insertTableBatch fails with "unknown table"
	bc := NewBufferConsumer(nil, nil, emptyRegistry(), 100, time.Second, logger)
	bc.SetDLQ(pub)

	events := []EventMessage{
		{TableName: "nonexistent", ReceivedTimestamp: "2026-01-01T00:00:00Z", Data: map[string]any{"k": "v"}},
		{TableName: "nonexistent", ReceivedTimestamp: "2026-01-01T00:00:01Z", Data: map[string]any{"k": "v2"}},
	}

	var acked int
	acks := make([]func(), len(events))
	for i := range acks {
		acks[i] = func() { acked++ }
	}

	bc.flushBatch(context.Background(), events, acks)

	// Events should be in DLQ.
	msgs := pub.get()
	require.Len(t, msgs, 2, "all failed events should be sent to DLQ")
	for _, msg := range msgs {
		assert.Equal(t, "dlq.nonexistent", msg.Subject)
	}

	// Original messages should be ACKed (so they don't retry forever).
	assert.Equal(t, 2, acked, "all original messages should be ACKed when DLQ is enabled")
}

func TestFlushBatch_NoACKWhenDLQDisabled(t *testing.T) {
	logger := slog.Default()

	// DLQ not enabled (no SetDLQ call), empty registry → insert fails
	bc := NewBufferConsumer(nil, nil, emptyRegistry(), 100, time.Second, logger)

	events := []EventMessage{
		{TableName: "nonexistent", Data: map[string]any{"k": "v"}},
	}

	var acked int
	acks := []func(){func() { acked++ }}

	bc.flushBatch(context.Background(), events, acks)

	// Without DLQ, messages should NOT be ACKed (left for retry).
	assert.Equal(t, 0, acked, "messages should not be ACKed when DLQ is disabled")
}

func TestFlushBatch_MixedTables_PartialDLQ(t *testing.T) {
	pub := &mockPublisher{}
	logger := slog.Default()

	// Empty registry → all tables fail, but we verify grouping works
	bc := NewBufferConsumer(nil, nil, emptyRegistry(), 100, time.Second, logger)
	bc.SetDLQ(pub)

	events := []EventMessage{
		{TableName: "table_a", Data: map[string]any{"id": "1"}},
		{TableName: "table_b", Data: map[string]any{"id": "2"}},
		{TableName: "table_a", Data: map[string]any{"id": "3"}},
	}

	var mu sync.Mutex
	var acked int
	acks := make([]func(), len(events))
	for i := range acks {
		acks[i] = func() {
			mu.Lock()
			acked++
			mu.Unlock()
		}
	}

	bc.flushBatch(context.Background(), events, acks)

	msgs := pub.get()
	require.Len(t, msgs, 3, "all events from both tables should be DLQ'd")

	// Verify subjects.
	subjects := map[string]int{}
	for _, msg := range msgs {
		subjects[msg.Subject]++
	}
	assert.Equal(t, 2, subjects["dlq.table_a"])
	assert.Equal(t, 1, subjects["dlq.table_b"])

	mu.Lock()
	assert.Equal(t, 3, acked, "all messages should be ACKed when DLQ is enabled")
	mu.Unlock()
}
