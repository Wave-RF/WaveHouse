package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

// ---------------------------------------------------------------------------
// Mock types for bento component tests.
// Different names from sweeper_test.go mocks to avoid redeclaration.
// ---------------------------------------------------------------------------

// bentoMockJS satisfies jetstream.JetStream; only Publish is overridden.
type bentoMockJS struct {
	jetstream.JetStream
	published []publishedMsg
}

type publishedMsg struct {
	Subject string
	Data    []byte
}

func (m *bentoMockJS) Publish(_ context.Context, subject string, payload []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	m.published = append(m.published, publishedMsg{Subject: subject, Data: payload})
	return &jetstream.PubAck{}, nil
}

// bentoMockMsg satisfies jetstream.Msg for testing jsInput.Read.
type bentoMockMsg struct {
	jetstream.Msg
	data        []byte
	subject     string
	headers     nats.Header
	acked       bool
	naked       bool
	doubleAcked bool
}

func (m *bentoMockMsg) Data() []byte         { return m.data }
func (m *bentoMockMsg) Subject() string      { return m.subject }
func (m *bentoMockMsg) Headers() nats.Header { return m.headers }
func (m *bentoMockMsg) Ack() error           { m.acked = true; return nil }
func (m *bentoMockMsg) Nak() error           { m.naked = true; return nil }

func (m *bentoMockMsg) DoubleAck(ctx context.Context) error {
	m.doubleAcked = true
	return nil
}

// bentoMockIter satisfies jetstream.MessagesContext for testing jsInput.
type bentoMockIter struct {
	jetstream.MessagesContext
	msgs chan jetstream.Msg
}

func (m *bentoMockIter) Next(_ ...jetstream.NextOpt) (jetstream.Msg, error) {
	msg, ok := <-m.msgs
	if !ok {
		return nil, errors.New("iterator closed")
	}
	return msg, nil
}

func (m *bentoMockIter) Stop() {}

// bentoMockConn satisfies driver.Conn; only Exec is overridden.
type bentoMockConn struct {
	driver.Conn
	execFn func(ctx context.Context, query string, args ...any) error
}

func (m *bentoMockConn) Exec(ctx context.Context, query string, args ...any) error {
	if m.execFn != nil {
		return m.execFn(ctx, query, args...)
	}
	return nil
}

// ---------------------------------------------------------------------------
// safeIdentifierRe tests
// ---------------------------------------------------------------------------

func TestSafeIdentifierRe(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{"simple", "clicks", true},
		{"underscore prefix", "_private", true},
		{"mixed case", "Table123", true},
		{"underscore in middle", "user_events", true},
		{"single char", "x", true},
		{"digits after letter", "a1b2", true},
		{"empty string", "", false},
		{"starts with digit", "123start", false},
		{"contains dash", "table-name", false},
		{"contains dot", "schema.table", false},
		{"contains space", "my table", false},
		{"SQL injection", "; DROP TABLE users", false},
		{"semicolon", "table;", false},
		{"parens", "table()", false},
		{"star", "table*", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := safeIdentifierRe.MatchString(tt.input)
			assert.Equal(t, tt.valid, got)
		})
	}
}

// ---------------------------------------------------------------------------
// dlqOutput.WriteBatch tests
// ---------------------------------------------------------------------------

func TestDLQOutput_WriteBatch_WithTableName(t *testing.T) {
	t.Parallel()
	js := &bentoMockJS{}
	d := &dlqOutput{js: js, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	msg := service.NewMessage([]byte(`{"data": "test"}`))
	msg.MetaSet("table_name", "clicks")

	err := d.WriteBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, js.published, 1)
	assert.Equal(t, "dlq.clicks", js.published[0].Subject)
	assert.Equal(t, []byte(`{"data": "test"}`), js.published[0].Data)
}

func TestDLQOutput_WriteBatch_MissingTableName(t *testing.T) {
	t.Parallel()
	js := &bentoMockJS{}
	d := &dlqOutput{js: js, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	msg := service.NewMessage([]byte(`{"data": "orphan"}`))
	// No table_name metadata set.

	err := d.WriteBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, js.published, 1)
	assert.Equal(t, "dlq.unknown", js.published[0].Subject)
}

func TestDLQOutput_WriteBatch_MultipleMessages(t *testing.T) {
	t.Parallel()
	js := &bentoMockJS{}
	d := &dlqOutput{js: js, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	m1 := service.NewMessage([]byte(`{"id":"1"}`))
	m1.MetaSet("table_name", "clicks")
	m2 := service.NewMessage([]byte(`{"id":"2"}`))
	m2.MetaSet("table_name", "events")
	m3 := service.NewMessage([]byte(`{"id":"3"}`))
	// m3 has no table_name.

	err := d.WriteBatch(context.Background(), service.MessageBatch{m1, m2, m3})
	require.NoError(t, err)

	require.Len(t, js.published, 3)
	assert.Equal(t, "dlq.clicks", js.published[0].Subject)
	assert.Equal(t, "dlq.events", js.published[1].Subject)
	assert.Equal(t, "dlq.unknown", js.published[2].Subject)
}

// ---------------------------------------------------------------------------
// jsInput.Read tests
// ---------------------------------------------------------------------------

func TestJsInput_Read_InsertMessage(t *testing.T) {
	t.Parallel()
	evt := EventMessage{
		TableName:         "clicks",
		ReceivedTimestamp: "2026-01-01T00:00:00Z",
		Data:              map[string]any{"page": "/home"},
	}
	data, _ := json.Marshal(evt)

	natsMsg := &bentoMockMsg{data: data}
	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 1)}
	iter.msgs <- natsMsg

	input := &jsInput{iter: iter}
	msg, ackFn, err := input.Read(context.Background())
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Verify table_name metadata was set.
	table, exists := msg.MetaGet("table_name")
	assert.True(t, exists)
	assert.Equal(t, "clicks", table)

	// Verify inFlight counter incremented.
	assert.Equal(t, int32(1), input.inFlight.Load())

	// Ack with nil error — should increment Ack on NATS msg.
	require.NoError(t, ackFn(context.Background(), nil))
	assert.True(t, natsMsg.acked)
	assert.Equal(t, int32(0), input.inFlight.Load())
}

func TestJsInput_Read_InsertNack(t *testing.T) {
	t.Parallel()
	evt := EventMessage{TableName: "clicks", Data: map[string]any{"id": "1"}}
	data, _ := json.Marshal(evt)

	natsMsg := &bentoMockMsg{data: data}
	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 1)}
	iter.msgs <- natsMsg

	input := &jsInput{iter: iter}
	_, ackFn, err := input.Read(context.Background())
	require.NoError(t, err)

	// Ack with error — should Nak the NATS message.
	_ = ackFn(context.Background(), errors.New("insert failed"))
	assert.True(t, natsMsg.naked)
	assert.Equal(t, int32(0), input.inFlight.Load())
}

func TestJsInput_Read_UnsafeTableNameDropped(t *testing.T) {
	t.Parallel()
	bad := EventMessage{TableName: "; DROP TABLE users", Data: map[string]any{}}
	badData, _ := json.Marshal(bad)
	badMsg := &bentoMockMsg{data: badData}

	good := EventMessage{TableName: "safe_table", Data: map[string]any{"id": "1"}}
	goodData, _ := json.Marshal(good)
	goodMsg := &bentoMockMsg{data: goodData}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 2)}
	iter.msgs <- badMsg
	iter.msgs <- goodMsg

	input := &jsInput{iter: iter}
	msg, _, err := input.Read(context.Background())
	require.NoError(t, err)

	// Bad message should have been acked (dropped).
	assert.True(t, badMsg.acked, "unsafe message should be acked and dropped")

	// Returned message should be the safe one.
	table, _ := msg.MetaGet("table_name")
	assert.Equal(t, "safe_table", table)
}

func TestJsInput_Read_DeleteMessage(t *testing.T) {
	t.Parallel()
	del := map[string]any{"action": "delete", "table_name": "clicks", "id": "abc123"}
	delData, _ := json.Marshal(del)
	delMsg := &bentoMockMsg{data: delData}

	insert := EventMessage{TableName: "events", Data: map[string]any{"x": "1"}}
	insertData, _ := json.Marshal(insert)
	insertMsg := &bentoMockMsg{data: insertData}

	var execCalled bool
	var execQuery string
	var execArgs []any
	conn := &bentoMockConn{
		execFn: func(_ context.Context, query string, args ...any) error {
			execCalled = true
			execQuery = query
			execArgs = args
			return nil
		},
	}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 2)}
	iter.msgs <- delMsg
	iter.msgs <- insertMsg

	input := &jsInput{iter: iter, chConn: conn}
	msg, _, err := input.Read(context.Background())
	require.NoError(t, err)

	// Delete should have been executed.
	assert.True(t, execCalled)
	assert.Equal(t, "DELETE FROM clicks WHERE id = ?", execQuery)
	require.Len(t, execArgs, 1)
	assert.Equal(t, "abc123", execArgs[0])
	assert.True(t, delMsg.acked, "delete message should be acked on success")

	// Returned message should be the insert.
	table, _ := msg.MetaGet("table_name")
	assert.Equal(t, "events", table)
}

func TestJsInput_Read_DeleteExecError(t *testing.T) {
	t.Parallel()
	del := map[string]any{"action": "delete", "table_name": "clicks", "id": "fail"}
	delData, _ := json.Marshal(del)
	delMsg := &bentoMockMsg{data: delData}

	insert := EventMessage{TableName: "events", Data: map[string]any{}}
	insertData, _ := json.Marshal(insert)
	insertMsg := &bentoMockMsg{data: insertData}

	conn := &bentoMockConn{
		execFn: func(context.Context, string, ...any) error { return errors.New("db error") },
	}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 2)}
	iter.msgs <- delMsg
	iter.msgs <- insertMsg

	input := &jsInput{iter: iter, chConn: conn}
	_, _, err := input.Read(context.Background())
	require.NoError(t, err)

	// On exec error, message should be Nak'd for retry.
	assert.True(t, delMsg.naked, "delete message should be nak'd on exec error")
}

func TestJsInput_Read_IteratorError(t *testing.T) {
	t.Parallel()
	iter := &bentoMockIter{msgs: make(chan jetstream.Msg)}
	close(iter.msgs) // Will cause Next to return error.

	input := &jsInput{iter: iter}
	_, _, err := input.Read(context.Background())
	assert.ErrorIs(t, err, service.ErrNotConnected)
}

func TestJsInput_Close_NilIter(t *testing.T) {
	t.Parallel()
	input := &jsInput{}
	assert.NoError(t, input.Close(context.Background()))
}

// ---------------------------------------------------------------------------
// jsInput.Connect tests
// ---------------------------------------------------------------------------

// bentoMockConsumer satisfies jetstream.Consumer; only Messages is overridden.
type bentoMockConsumer struct {
	jetstream.Consumer
	msgsFn func(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error)
}

func (m *bentoMockConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return m.msgsFn(opts...)
}

func TestJsInput_Connect_Success(t *testing.T) {
	t.Parallel()
	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 1)}
	consumer := &bentoMockConsumer{
		msgsFn: func(_ ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
			return iter, nil
		},
	}

	input := &jsInput{consumer: consumer}
	require.NoError(t, input.Connect(context.Background()))
	assert.NotNil(t, input.iter, "iter should be set after Connect")
}

func TestJsInput_Connect_Error(t *testing.T) {
	t.Parallel()
	consumer := &bentoMockConsumer{
		msgsFn: func(_ ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
			return nil, errors.New("consumer unavailable")
		},
	}

	input := &jsInput{consumer: consumer}
	err := input.Connect(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "jetstream iterator")
}

// ---------------------------------------------------------------------------
// jsInput.Read — malformed JSON
// ---------------------------------------------------------------------------

func TestJsInput_Read_MalformedJSON(t *testing.T) {
	t.Parallel()
	badMsg := &bentoMockMsg{data: []byte(`not valid json`)}

	good := EventMessage{TableName: "safe_table", Data: map[string]any{"id": "1"}}
	goodData, _ := json.Marshal(good)
	goodMsg := &bentoMockMsg{data: goodData}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 2)}
	iter.msgs <- badMsg
	iter.msgs <- goodMsg

	input := &jsInput{iter: iter}
	msg, _, err := input.Read(context.Background())
	require.NoError(t, err)

	// Bad message should be acked and dropped.
	assert.True(t, badMsg.acked, "malformed JSON message should be acked and dropped")

	// Returned message should be the valid one.
	table, _ := msg.MetaGet("table_name")
	assert.Equal(t, "safe_table", table)
}

// ---------------------------------------------------------------------------
// jsInput.Read — empty table_name
// ---------------------------------------------------------------------------

func TestJsInput_Read_EmptyTableName(t *testing.T) {
	t.Parallel()
	emptyData, _ := json.Marshal(map[string]any{"data": map[string]any{"x": 1}})
	emptyMsg := &bentoMockMsg{data: emptyData}

	good := EventMessage{TableName: "events", Data: map[string]any{"id": "2"}}
	goodData, _ := json.Marshal(good)
	goodMsg := &bentoMockMsg{data: goodData}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 2)}
	iter.msgs <- emptyMsg
	iter.msgs <- goodMsg

	input := &jsInput{iter: iter}
	msg, _, err := input.Read(context.Background())
	require.NoError(t, err)

	assert.True(t, emptyMsg.acked, "empty table_name message should be acked and dropped")

	table, _ := msg.MetaGet("table_name")
	assert.Equal(t, "events", table)
}

// ---------------------------------------------------------------------------
// jsInput.Read — delete with unsafe table name
// ---------------------------------------------------------------------------

func TestJsInput_Read_DeleteUnsafeTableName(t *testing.T) {
	t.Parallel()
	del := map[string]any{"action": "delete", "table_name": "; DROP TABLE users", "id": "x"}
	delData, _ := json.Marshal(del)
	delMsg := &bentoMockMsg{data: delData}

	good := EventMessage{TableName: "safe_table", Data: map[string]any{"id": "1"}}
	goodData, _ := json.Marshal(good)
	goodMsg := &bentoMockMsg{data: goodData}

	var execCalled bool
	conn := &bentoMockConn{
		execFn: func(context.Context, string, ...any) error {
			execCalled = true
			return nil
		},
	}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 2)}
	iter.msgs <- delMsg
	iter.msgs <- goodMsg

	input := &jsInput{iter: iter, chConn: conn}
	msg, _, err := input.Read(context.Background())
	require.NoError(t, err)

	// Delete with unsafe table name should be dropped, NOT executed.
	assert.True(t, delMsg.acked, "unsafe delete should be acked and dropped")
	assert.False(t, execCalled, "delete should not be executed for unsafe table name")

	table, _ := msg.MetaGet("table_name")
	assert.Equal(t, "safe_table", table)
}

// ---------------------------------------------------------------------------
// jsInput.Read — delete with context cancellation during flush-wait
// ---------------------------------------------------------------------------

func TestJsInput_Read_DeleteCancelledCtx(t *testing.T) {
	t.Parallel()
	del := map[string]any{"action": "delete", "table_name": "clicks", "id": "abc"}
	delData, _ := json.Marshal(del)
	delMsg := &bentoMockMsg{data: delData}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 1)}
	iter.msgs <- delMsg

	input := &jsInput{iter: iter, chConn: &bentoMockConn{}}
	// Simulate in-flight messages so the flush-wait loop blocks.
	input.inFlight.Store(1)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay to unblock the flush-wait.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, _, err := input.Read(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, delMsg.naked, "delete should be Nak'd when context is cancelled")
}

// ---------------------------------------------------------------------------
// dlqOutput — DLQ publish error is logged, WriteBatch still returns nil
// ---------------------------------------------------------------------------

type bentoMockJSWithError struct {
	jetstream.JetStream
	publishErr error
}

func (m *bentoMockJSWithError) Publish(_ context.Context, _ string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, m.publishErr
}

func TestDLQOutput_WriteBatch_PublishError(t *testing.T) {
	t.Parallel()
	js := &bentoMockJSWithError{publishErr: errors.New("NATS unavailable")}
	d := &dlqOutput{js: js, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	msg := service.NewMessage([]byte(`{"data": "orphan"}`))
	msg.MetaSet("table_name", "clicks")

	err := d.WriteBatch(context.Background(), service.MessageBatch{msg})
	assert.NoError(t, err, "WriteBatch should still return nil even when publish fails")
}

// ---------------------------------------------------------------------------
// dlqOutput — Connect, Wait, Close are all no-ops
// ---------------------------------------------------------------------------

func TestDLQOutput_NoOps(t *testing.T) {
	t.Parallel()
	d := &dlqOutput{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	assert.NoError(t, d.Connect(context.Background()))
	assert.NoError(t, d.Wait(context.Background()))
	assert.NoError(t, d.Close(context.Background()))
}
