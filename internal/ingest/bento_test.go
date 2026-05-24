package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

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
// Publish is used by dlqOutput.WriteBatch (insert-failure path).
type bentoMockJS struct {
	jetstream.JetStream
	published []publishedMsg
}

type publishedMsg struct {
	Subject string
	Data    []byte
	Header  nats.Header
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

func (m *bentoMockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	// Return an empty, safe metadata object to prevent nil pointer panics
	return &jetstream.MsgMetadata{Timestamp: time.Now()}, nil
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

// ---------------------------------------------------------------------------
// dlqOutput.WriteBatch tests
// ---------------------------------------------------------------------------

func TestDLQOutput_WriteBatch_WithTableName(t *testing.T) {
	t.Parallel()
	js := &bentoMockJS{}
	d := &dlqOutput{js: js}

	msg := service.NewMessage([]byte(`{"data": "test"}`))
	msg.MetaSet("table_name", "clicks")

	err := d.WriteBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, js.published, 1)
	assert.Equal(t, "dlq.clicks", js.published[0].Subject)
	assert.Equal(t, []byte(`{"data": "test"}`), js.published[0].Data)
}

func TestDLQOutput_WriteBatch_WithEmptyTableName(t *testing.T) {
	t.Parallel()
	js := &bentoMockJS{}
	d := &dlqOutput{js: js}

	msg := service.NewMessage([]byte(`{"data": "test"}`))
	msg.MetaSet("table_name", "")

	err := d.WriteBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, js.published, 1)
	assert.Equal(t, "dlq.unknown", js.published[0].Subject)
	assert.Equal(t, []byte(`{"data": "test"}`), js.published[0].Data)
}

func TestDLQOutput_WriteBatch_MissingTableName(t *testing.T) {
	t.Parallel()
	js := &bentoMockJS{}
	d := &dlqOutput{js: js}

	msg := service.NewMessage([]byte(`{"data": "orphan"}`))
	// No table_name metadata set.

	err := d.WriteBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, js.published, 1)
	assert.Equal(t, "dlq.unknown", js.published[0].Subject)
	assert.Equal(t, []byte(`{"data": "orphan"}`), js.published[0].Data)
}

func TestDLQOutput_WriteBatch_MultipleMessages(t *testing.T) {
	t.Parallel()
	js := &bentoMockJS{}
	d := &dlqOutput{js: js}

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

	// Ack with nil error — should increment Ack on NATS msg.
	require.NoError(t, ackFn(context.Background(), nil))
	assert.True(t, natsMsg.doubleAcked, "insert message should use DoubleAck, not Ack")
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
}

func TestJsInput_Read_UnsafeTableNameAllowed(t *testing.T) {
	t.Parallel()
	bad := EventMessage{TableName: "; DROP TABLE users", Data: map[string]any{"x": 1}}
	badData, _ := json.Marshal(bad)
	badMsg := &bentoMockMsg{data: badData}

	good := EventMessage{TableName: "safe_table", Data: map[string]any{"id": "1"}}
	goodData, _ := json.Marshal(good)
	goodMsg := &bentoMockMsg{data: goodData}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 2)}
	iter.msgs <- badMsg
	iter.msgs <- goodMsg

	input := &jsInput{iter: iter}
	ctx := context.Background()

	msg1, _, err := input.Read(ctx)
	require.NoError(t, err)

	assert.False(t, badMsg.doubleAcked, "malicious message should NOT be dropped")
	table1, _ := msg1.MetaGet("table_name")
	assert.Equal(t, "; DROP TABLE users", table1)

	msg2, _, err := input.Read(ctx)
	require.NoError(t, err)

	assert.False(t, goodMsg.doubleAcked, "safe message should NOT be dropped")
	table2, _ := msg2.MetaGet("table_name")
	assert.Equal(t, "safe_table", table2)
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

	assert.True(t, badMsg.doubleAcked, "malformed JSON message should be double-acked and dropped")

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

	assert.True(t, emptyMsg.doubleAcked, "empty table name should be double-acked and dropped")

	table, _ := msg.MetaGet("table_name")
	assert.Equal(t, "events", table)
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
	d := &dlqOutput{js: js}

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
	d := &dlqOutput{}

	assert.NoError(t, d.Connect(context.Background()))
	assert.NoError(t, d.Wait(context.Background()))
	assert.NoError(t, d.Close(context.Background()))
}

// ---------------------------------------------------------------------------
// Mock HTTP Transport for clickhouseOutput tests
// ---------------------------------------------------------------------------

// mockRoundTripper allows us to intercept and mock http.Client responses.
type mockRoundTripper struct {
	roundTripFn func(req *http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.roundTripFn != nil {
		return m.roundTripFn(req)
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
}

// ---------------------------------------------------------------------------
// clickhouseOutput tests
// ---------------------------------------------------------------------------

func TestClickhouseOutput_ConnectClose(t *testing.T) {
	t.Parallel()
	c := &clickhouseOutput{}
	assert.NoError(t, c.Connect(context.Background()))
	assert.NoError(t, c.Close(context.Background()))
}

func TestClickhouseOutput_WriteBatch_EmptyBatch(t *testing.T) {
	t.Parallel()
	c := &clickhouseOutput{}
	err := c.WriteBatch(context.Background(), service.MessageBatch{})
	assert.NoError(t, err, "empty batch should return nil immediately")
}

func TestClickhouseOutput_WriteBatch_MissingTableName(t *testing.T) {
	t.Parallel()
	c := &clickhouseOutput{}
	msg1 := service.NewMessage([]byte(`{"x": 1}`))
	// Intentionally omitting table_name metadata

	err1 := c.WriteBatch(context.Background(), service.MessageBatch{msg1})
	assert.ErrorContains(t, err1, "missing table_name in message metadata")

	msg2 := service.NewMessage([]byte(`{"x": 1}`))
	msg2.MetaSet("table_name", "")

	err2 := c.WriteBatch(context.Background(), service.MessageBatch{msg2})
	assert.ErrorContains(t, err2, "missing table_name in message metadata")
}

func TestClickhouseOutput_WriteBatch_SafelyParametersTableName(t *testing.T) {
	t.Parallel()

	maliciousName := "users; DROP TABLE users;"

	mockTransport := &mockRoundTripper{
		roundTripFn: func(req *http.Request) (*http.Response, error) {
			// Verify the SQL query itself is 100% static and immune to injection
			assert.Equal(t, "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow", req.URL.Query().Get("query"))

			// Verify the malicious payload was safely tucked into the out-of-band parameter
			assert.Equal(t, maliciousName, req.URL.Query().Get("param_target_table"))

			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString("OK")),
			}, nil
		},
	}

	c := &clickhouseOutput{
		httpClient: &http.Client{Transport: mockTransport},
		scheme:     "http",
		host:       "localhost",
		port:       "8123",
		db:         "test_db",
	}

	msg := service.NewMessage([]byte(`{"x": 1}`))
	msg.MetaSet("table_name", maliciousName)

	err := c.WriteBatch(context.Background(), service.MessageBatch{msg})
	assert.NoError(t, err)
}

func TestClickhouseOutput_WriteBatch_Success(t *testing.T) {
	t.Parallel()

	// Mock an HTTP 200 OK response from ClickHouse
	mockTransport := &mockRoundTripper{
		roundTripFn: func(req *http.Request) (*http.Response, error) {
			// Verify URL was constructed correctly
			assert.Equal(t, "POST", req.Method)
			assert.Equal(t, "test_db", req.URL.Query().Get("database"))

			// Verify the query is now using the static parameter syntax
			assert.Equal(t, "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow", req.URL.Query().Get("query"))

			// Verify the actual table name is passed safely as a param
			assert.Equal(t, "test_table", req.URL.Query().Get("param_target_table"))

			// Verify headers
			assert.Equal(t, "test_user", req.Header.Get("X-ClickHouse-User"))
			assert.Equal(t, "test_pass", req.Header.Get("X-ClickHouse-Key"))

			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString("OK")),
			}, nil
		},
	}

	c := &clickhouseOutput{
		httpClient: &http.Client{Transport: mockTransport},
		scheme:     "http",
		host:       "localhost",
		port:       "8123",
		user:       "test_user",
		password:   "test_pass",
		db:         "test_db",
	}

	msg := service.NewMessage([]byte(`{"metric": 99}`))
	msg.MetaSet("table_name", "test_table")
	msg.MetaSet("bento_start_time", "1700000000000") // Valid unix milli

	err := c.WriteBatch(context.Background(), service.MessageBatch{msg})
	assert.NoError(t, err)
}

func TestClickhouseOutput_WriteBatch_HTTPErrorResponse(t *testing.T) {
	t.Parallel()

	// Mock an HTTP 400 Bad Request response from ClickHouse
	mockTransport := &mockRoundTripper{
		roundTripFn: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(bytes.NewBufferString("Code: 117. DB::Exception: Unknown table.")),
			}, nil
		},
	}

	c := &clickhouseOutput{
		httpClient: &http.Client{Transport: mockTransport},
		scheme:     "http",
	}

	msg := service.NewMessage([]byte(`{"metric": 99}`))
	msg.MetaSet("table_name", "missing_table")

	err := c.WriteBatch(context.Background(), service.MessageBatch{msg})
	assert.ErrorContains(t, err, "clickhouse error 400")
	assert.ErrorContains(t, err, "Unknown table")
}

func TestClickhouseOutput_WriteBatch_NetworkError(t *testing.T) {
	t.Parallel()

	// Mock a hard network failure (e.g. connection refused)
	mockTransport := &mockRoundTripper{
		roundTripFn: func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("connection refused")
		},
	}

	c := &clickhouseOutput{
		httpClient: &http.Client{Transport: mockTransport},
		scheme:     "http",
	}

	msg := service.NewMessage([]byte(`{"metric": 99}`))
	msg.MetaSet("table_name", "events")

	err := c.WriteBatch(context.Background(), service.MessageBatch{msg})
	assert.ErrorContains(t, err, "execute clickhouse request")
	assert.ErrorContains(t, err, "connection refused")
}

func TestClickhouseOutput_WriteBatch_MalformedStartTime(t *testing.T) {
	t.Parallel()

	mockTransport := &mockRoundTripper{
		roundTripFn: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}

	c := &clickhouseOutput{
		httpClient: &http.Client{Transport: mockTransport},
		scheme:     "http",
		host:       "localhost",
		port:       "8123",
	}

	msg := service.NewMessage([]byte(`{"metric": 99}`))
	msg.MetaSet("table_name", "test_table")
	msg.MetaSet("bento_start_time", "not-a-unix-timestamp") // Malformed

	err := c.WriteBatch(context.Background(), service.MessageBatch{msg})

	// Should succeed without crashing, falling back to time.Now() for the trace span
	assert.NoError(t, err, "WriteBatch should succeed even if start time is malformed")
}

func TestClickhouseOutput_WriteBatch_MultiMessage(t *testing.T) {
	t.Parallel()

	mockTransport := &mockRoundTripper{
		roundTripFn: func(req *http.Request) (*http.Response, error) {
			bodyBytes, err := io.ReadAll(req.Body)
			assert.NoError(t, err)

			// Verify newline-separated JSONEachRow format
			expectedBody := `{"id":1}` + "\n" + `{"id":2}` + "\n"
			assert.Equal(t, expectedBody, string(bodyBytes))

			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString("OK")),
			}, nil
		},
	}

	c := &clickhouseOutput{
		httpClient: &http.Client{Transport: mockTransport},
		scheme:     "http",
		host:       "localhost",
		port:       "8123",
	}

	msg1 := service.NewMessage([]byte(`{"id":1}`))
	msg1.MetaSet("table_name", "multi_table")
	msg1.MetaSet("bento_start_time", "1700000000000") // Valid timestamp

	msg2 := service.NewMessage([]byte(`{"id":2}`))
	msg2.MetaSet("table_name", "multi_table")
	// Second message doesn't need bento_start_time, but this exercises the trace.Link loop!

	err := c.WriteBatch(context.Background(), service.MessageBatch{msg1, msg2})
	assert.NoError(t, err, "WriteBatch should succeed with multi-message batch")
}

// ---------------------------------------------------------------------------
// jsInput payload extraction validation
// ---------------------------------------------------------------------------

func TestJsInput_Read_EmptyPayloadRejected(t *testing.T) {
	t.Parallel()

	// Message missing the "data" payload completely. Should be rejected.
	badData := []byte(`{"table_name": "events"}`)
	badMsg := &bentoMockMsg{data: badData}

	// Good message so the Read loop has something to successfully return
	goodData := []byte(`{"table_name": "events", "data": {"id": 1}}`)
	goodMsg := &bentoMockMsg{data: goodData}

	iter := &bentoMockIter{msgs: make(chan jetstream.Msg, 2)}
	iter.msgs <- badMsg
	iter.msgs <- goodMsg

	input := &jsInput{iter: iter}
	msg, _, err := input.Read(context.Background())
	require.NoError(t, err)

	// The bad message should be DoubleAcked (dropped)
	assert.True(t, badMsg.doubleAcked, "empty payload message should be acked and dropped")

	// We should have received the good message
	payload, err := msg.AsBytes()
	require.NoError(t, err)
	assert.JSONEq(t, `{"id": 1}`, string(payload))
}
