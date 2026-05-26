package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared mocks come from internal/testutil: MockJetStreamMsg, MockJetStream,
// MockRoundTripper, MockCache, NopLogger.

// newTestWorker builds an IngestWorker wired to in-process mocks. wait() blocks
// until all background ack goroutines kicked off by handleSuccess finish.
func newTestWorker(rt http.RoundTripper) (*IngestWorker, *testutil.MockJetStream, *testutil.MockCache, func()) {
	js := &testutil.MockJetStream{}
	cache := &testutil.MockCache{}
	w := &IngestWorker{
		js:         js,
		httpClient: &http.Client{Transport: rt},
		cache:      cache,
		logger:     testutil.NopLogger(),
		chURL:      "http://test-clickhouse:8123",
		user:       "test_user",
		password:   "test_pass",
		db:         "test_db",
	}
	return w, js, cache, func() { w.wg.Wait() }
}

// makeEnvelope returns the JSON wire format the worker reads off NATS.
func makeEnvelope(t *testing.T, tableName, scope string, data map[string]any) []byte {
	t.Helper()
	rawData, err := json.Marshal(data)
	require.NoError(t, err)
	env := struct {
		TableName         string          `json:"table_name"`
		Scope             string          `json:"scope,omitempty"`
		ReceivedTimestamp string          `json:"received_timestamp"`
		Data              json.RawMessage `json:"data"`
	}{
		TableName:         tableName,
		Scope:             scope,
		ReceivedTimestamp: "2026-01-01T00:00:00Z",
		Data:              rawData,
	}
	out, err := json.Marshal(env)
	require.NoError(t, err)
	return out
}

// ---------------------------------------------------------------------------
// StartIngestWorker — constructor
// ---------------------------------------------------------------------------

func TestStartIngestWorker_NilNATSConn(t *testing.T) {
	t.Parallel()
	_, err := StartIngestWorker(context.Background(), nil, &testutil.MockCache{},
		"localhost", "8123", "http", "user", "pass", "db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nats connection is nil")
}

func TestStartIngestWorker_NilCache(t *testing.T) {
	t.Parallel()
	emb, err := mq.NewEmbedded(t.TempDir(), 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	_, err = StartIngestWorker(context.Background(), emb.NatsConn(), nil,
		"localhost", "8123", "http", "user", "pass", "db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cache is nil")
}

// TestStartIngestWorker_EndToEnd exercises the full happy path:
// embedded NATS + httptest-backed ClickHouse stub. We publish an envelope on
// the ingest subject, then verify (a) the worker POSTs the inner payload to
// ClickHouse, (b) the message is ACKed (consumer's AckFloor moves), and
// (c) the cache is invalidated for the table + scope.
//
// This also covers runLoop + StartIngestWorker's setup logic.
func TestStartIngestWorker_EndToEnd(t *testing.T) {
	t.Parallel()

	// ── Embedded NATS (creates the WAVEHOUSE stream w/ "ingest.>") ──
	emb, err := mq.NewEmbedded(t.TempDir(), 4*1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	// ── ClickHouse stub: capture each request body, return 200 ──
	var (
		gotMu  sync.Mutex
		bodies []string
		params []string
	)
	chSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotMu.Lock()
		bodies = append(bodies, string(body))
		params = append(params, r.URL.Query().Get("param_target_table"))
		gotMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(chSrv.Close)

	u, err := url.Parse(chSrv.URL)
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)

	cache := &testutil.MockCache{}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stopFn, err := StartIngestWorker(ctx, emb.NatsConn(), cache,
		host, port, "http", "u", "p", "db")
	require.NoError(t, err)
	require.NotNil(t, stopFn)

	// ── Publish an envelope on ingest.events ──
	envelope := makeEnvelope(t, "events", "org_42", map[string]any{"id": 1, "v": "x"})
	js, err := jetstream.New(emb.NatsConn())
	require.NoError(t, err)
	_, err = js.Publish(ctx, "ingest.events", envelope)
	require.NoError(t, err)

	// ── Wait for the worker to insert + ack ──
	require.Eventually(t, func() bool {
		gotMu.Lock()
		defer gotMu.Unlock()
		return len(bodies) >= 1
	}, 6*time.Second, 25*time.Millisecond, "ClickHouse stub never received the insert")

	gotMu.Lock()
	require.Len(t, bodies, 1)
	assert.Equal(t, `{"id":1,"v":"x"}`+"\n", bodies[0])
	assert.Equal(t, "events", params[0])
	gotMu.Unlock()

	// Cache invalidation should run with the table + the (encoded) subject.
	require.Eventually(t, func() bool {
		keys := cache.GetKeys()
		hasTable := false
		hasSubject := false
		for _, k := range keys {
			if k == "events" {
				hasTable = true
			}
			if k == "events" || k == "events%2Eorg_42" || k == "events.org_42" {
				hasSubject = true
			}
		}
		return hasTable && hasSubject
	}, 4*time.Second, 25*time.Millisecond, "cache must be invalidated for table + subject")

	// ── stopFn must return cleanly ──
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(shutCancel)
	require.NoError(t, stopFn(shutCtx))
}

// TestStartIngestWorker_StopFunc_RespectsShutdownDeadline verifies that if
// the shutdown context expires before background ack goroutines drain, stopFn
// returns the context error instead of blocking indefinitely.
func TestStartIngestWorker_StopFunc_RespectsShutdownDeadline(t *testing.T) {
	t.Parallel()

	emb, err := mq.NewEmbedded(t.TempDir(), 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	// ClickHouse stub that blocks until we say go — keeps the worker's
	// flush goroutine alive past the stop call.
	release := make(chan struct{})
	chSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		close(release)
		chSrv.Close()
	})
	u, _ := url.Parse(chSrv.URL)
	host, port, _ := net.SplitHostPort(u.Host)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stopFn, err := StartIngestWorker(ctx, emb.NatsConn(), &testutil.MockCache{},
		host, port, "http", "u", "p", "db")
	require.NoError(t, err)

	// Publish so there's an in-flight insert blocking on `release`.
	js, _ := jetstream.New(emb.NatsConn())
	_, err = js.Publish(ctx, "ingest.events", makeEnvelope(t, "events", "", map[string]any{"id": 1}))
	require.NoError(t, err)

	// Give the worker a moment to pick up the message and start the request.
	time.Sleep(150 * time.Millisecond)

	// Shut down with a tight deadline — the blocked HTTP request keeps the
	// background goroutine alive longer than this, so stopFn must return
	// context.DeadlineExceeded.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	t.Cleanup(shutCancel)

	err = stopFn(shutCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// ---------------------------------------------------------------------------
// insertToClickHouse — HTTP request shape
// ---------------------------------------------------------------------------

func TestInsertToClickHouse_BuildsCorrectRequest(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "POST", req.Method)
			assert.Equal(t, "test-clickhouse:8123", req.URL.Host)
			assert.Equal(t, "http", req.URL.Scheme)

			q := req.URL.Query()
			assert.Equal(t, "test_db", q.Get("database"))
			assert.Equal(t, "events", q.Get("param_target_table"))
			assert.Equal(t, "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow", q.Get("query"))

			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			assert.Equal(t, "test_user", req.Header.Get("X-ClickHouse-User"))
			assert.Equal(t, "test_pass", req.Header.Get("X-ClickHouse-Key"))

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			assert.Equal(t, `{"id":1}`+"\n", string(body))

			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), "events", []parsedMsg{
		{rawJSON: []byte(`{"id":1}`)},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), rt.Hits())
}

func TestInsertToClickHouse_MultipleMessages_JSONEachRow(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			// JSONEachRow: newline-separated, each row's own JSON object.
			assert.Equal(t, `{"id":1}`+"\n"+`{"id":2}`+"\n"+`{"id":3}`+"\n", string(body))
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), "events", []parsedMsg{
		{rawJSON: []byte(`{"id":1}`)},
		{rawJSON: []byte(`{"id":2}`)},
		{rawJSON: []byte(`{"id":3}`)},
	})
	require.NoError(t, err)
}

func TestInsertToClickHouse_SafelyParameterizesTableName(t *testing.T) {
	t.Parallel()
	malicious := "users; DROP TABLE users;--"
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			q := req.URL.Query()
			// Static query string — no interpolation of the table name.
			assert.Equal(t, "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow", q.Get("query"))
			// Malicious table name lives in the out-of-band parameter,
			// where ClickHouse will quote it as an Identifier.
			assert.Equal(t, malicious, q.Get("param_target_table"))
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), malicious, []parsedMsg{
		{rawJSON: []byte(`{"id":1}`)},
	})
	require.NoError(t, err)
}

func TestInsertToClickHouse_HTTPError_ReturnsBody(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(bytes.NewBufferString("Code: 60. DB::Exception: Unknown table.")),
			}, nil
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), "missing_table", []parsedMsg{
		{rawJSON: []byte(`{"id":1}`)},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 400")
	assert.Contains(t, err.Error(), "Unknown table")
}

func TestInsertToClickHouse_500Error(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(bytes.NewBufferString("internal error")),
			}, nil
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), "events", []parsedMsg{
		{rawJSON: []byte(`{"id":1}`)},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

func TestInsertToClickHouse_NetworkError(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return nil, errors.New("connection refused")
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), "events", []parsedMsg{
		{rawJSON: []byte(`{"id":1}`)},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

// ---------------------------------------------------------------------------
// handleSuccess — cache invalidation + DoubleAck
// ---------------------------------------------------------------------------

func TestHandleSuccess_InvalidatesCacheAndAcks(t *testing.T) {
	t.Parallel()
	w, _, cache, wait := newTestWorker(&testutil.MockRoundTripper{})

	m1 := &testutil.MockJetStreamMsg{MsgSubject: "org_1"}
	m2 := &testutil.MockJetStreamMsg{MsgSubject: "org_2"}

	w.handleSuccess(context.Background(), "events", []parsedMsg{
		{natsMsg: m1, natsSafeSubject: "org_1"},
		{natsMsg: m2, natsSafeSubject: "org_2"},
	})
	wait()

	// Cache should be invalidated with the table name + each unique subject.
	keys := cache.GetKeys()
	assert.ElementsMatch(t, []string{"events", "org_1", "org_2"}, keys)

	// Both messages should have been DoubleAcked.
	assert.True(t, m1.DoubleAcked.Load(), "m1 must be DoubleAcked")
	assert.True(t, m2.DoubleAcked.Load(), "m2 must be DoubleAcked")
}

func TestHandleSuccess_DeduplicatesVersionKeys(t *testing.T) {
	t.Parallel()
	w, _, cache, wait := newTestWorker(&testutil.MockRoundTripper{})

	// Three messages, but only two distinct subjects.
	m1 := &testutil.MockJetStreamMsg{MsgSubject: "org_1"}
	m2 := &testutil.MockJetStreamMsg{MsgSubject: "org_1"} // duplicate scope
	m3 := &testutil.MockJetStreamMsg{MsgSubject: "org_2"}

	w.handleSuccess(context.Background(), "events", []parsedMsg{
		{natsMsg: m1, natsSafeSubject: "org_1"},
		{natsMsg: m2, natsSafeSubject: "org_1"},
		{natsMsg: m3, natsSafeSubject: "org_2"},
	})
	wait()

	keys := cache.GetKeys()
	// Expect "events" + "org_1" + "org_2" — exactly 3 entries (no duplicate org_1).
	assert.Len(t, keys, 3)
	assert.ElementsMatch(t, []string{"events", "org_1", "org_2"}, keys)
}

func TestHandleSuccess_TableNameAlsoActsAsSubjectKey(t *testing.T) {
	t.Parallel()
	// If a parsed msg's NATS-safe subject equals the encoded table name (a
	// table-scoped publisher), the table key is added once — not twice.
	w, _, cache, wait := newTestWorker(&testutil.MockRoundTripper{})

	m := &testutil.MockJetStreamMsg{MsgSubject: "events"}
	w.handleSuccess(context.Background(), "events", []parsedMsg{
		{natsMsg: m, natsSafeSubject: "events"},
	})
	wait()

	keys := cache.GetKeys()
	assert.Equal(t, []string{"events"}, keys)
}

func TestHandleSuccess_CacheError_StillAcks(t *testing.T) {
	t.Parallel()
	w, _, cache, wait := newTestWorker(&testutil.MockRoundTripper{})
	cache.InvErr = errors.New("cache backend down")

	m := &testutil.MockJetStreamMsg{MsgSubject: "org_1"}
	// Should not panic even when cache invalidation fails.
	w.handleSuccess(context.Background(), "events", []parsedMsg{
		{natsMsg: m, natsSafeSubject: "org_1"},
	})
	wait()

	// Ack still happens; cache failure is logged but non-fatal.
	assert.True(t, m.DoubleAcked.Load(), "ack must happen even when cache invalidation fails")
}

func TestHandleSuccess_DoubleAckError_DoesNotPanic(t *testing.T) {
	t.Parallel()
	w, _, _, wait := newTestWorker(&testutil.MockRoundTripper{})

	m := &testutil.MockJetStreamMsg{
		MsgSubject:   "org_1",
		DoubleAckErr: errors.New("server unavailable"),
	}
	// DoubleAck error is logged but must not crash the worker.
	w.handleSuccess(context.Background(), "events", []parsedMsg{
		{natsMsg: m, natsSafeSubject: "org_1"},
	})
	wait()

	assert.True(t, m.DoubleAcked.Load(), "DoubleAck still attempted")
}

// ---------------------------------------------------------------------------
// sendToDLQ — DLQ publish + headers + ack-only-on-success
// ---------------------------------------------------------------------------

func TestSendToDLQ_PublishesWithHeaders(t *testing.T) {
	t.Parallel()
	w, js, _, _ := newTestWorker(&testutil.MockRoundTripper{})

	original := &testutil.MockJetStreamMsg{MsgData: []byte(`{"bad":"row"}`), MsgSubject: "events"}
	pm := parsedMsg{
		natsMsg:         original,
		natsSafeSubject: "events",
	}

	w.sendToDLQ(context.Background(), "events", pm, "Code: 60. DB::Exception: ...")

	published := js.Published()
	require.Len(t, published, 1)

	got := published[0]
	assert.Equal(t, "dlq.events", got.Subject)
	assert.Equal(t, []byte(`{"bad":"row"}`), got.Data)
	assert.Equal(t, "events", got.Header.Get("X-DLQ-Table"))
	assert.Contains(t, got.Header.Get("X-DLQ-Error"), "Code: 60")
	assert.NotEmpty(t, got.Header.Get("X-DLQ-Timestamp"))

	// Timestamp should parse as RFC3339.
	_, err := time.Parse(time.RFC3339, got.Header.Get("X-DLQ-Timestamp"))
	assert.NoError(t, err, "X-DLQ-Timestamp must be RFC3339")

	// Original message acked so NATS stops redelivering the bad row.
	assert.True(t, original.DoubleAcked.Load(), "original must be DoubleAcked after DLQ publish")
}

func TestSendToDLQ_PublishFailure_NoAck(t *testing.T) {
	t.Parallel()
	w, js, _, _ := newTestWorker(&testutil.MockRoundTripper{})
	js.PubErr = errors.New("nats unavailable")

	original := &testutil.MockJetStreamMsg{MsgData: []byte(`{"bad":"row"}`), MsgSubject: "events"}
	pm := parsedMsg{
		natsMsg:         original,
		natsSafeSubject: "events",
	}

	w.sendToDLQ(context.Background(), "events", pm, "boom")

	// No ack on original — NATS must redeliver until DLQ recovers.
	assert.False(t, original.DoubleAcked.Load(),
		"original must NOT be acked when DLQ publish fails; otherwise we'd lose the row")
}

func TestSendToDLQ_EncodedSubject(t *testing.T) {
	t.Parallel()
	// The DLQ subject uses the already-NATS-safe subject from the upstream
	// envelope. We don't re-encode here, so callers must pass safe values.
	w, js, _, _ := newTestWorker(&testutil.MockRoundTripper{})

	pm := parsedMsg{
		natsMsg:         &testutil.MockJetStreamMsg{MsgData: []byte("payload")},
		natsSafeSubject: "events%2Estaging", // already encoded by upstream
	}

	w.sendToDLQ(context.Background(), "events.staging", pm, "err")

	require.Len(t, js.Published(), 1)
	assert.Equal(t, "dlq.events%2Estaging", js.Published()[0].Subject)
}

// ---------------------------------------------------------------------------
// flush — orchestration: parse → group → insert → ack / fall back / DLQ
// ---------------------------------------------------------------------------

func TestFlush_EmptyBatch_NoOp(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{}
	w, _, _, _ := newTestWorker(rt)

	// Must not panic, must not call HTTP.
	w.flush(context.Background(), nil)
	w.flush(context.Background(), []jetstream.Msg{})
	assert.Equal(t, int32(0), rt.Hits())
}

func TestFlush_SingleTable_HappyPath(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, cache, wait := newTestWorker(rt)

	m1 := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    makeEnvelope(t, "events", "org_1", map[string]any{"id": 1}),
	}
	m2 := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    makeEnvelope(t, "events", "org_1", map[string]any{"id": 2}),
	}

	w.flush(context.Background(), []jetstream.Msg{m1, m2})
	wait()

	// One HTTP request — both rows batched together.
	assert.Equal(t, int32(1), rt.Hits())

	// Both messages acked.
	assert.True(t, m1.DoubleAcked.Load())
	assert.True(t, m2.DoubleAcked.Load())

	// Cache invalidated for the table.
	assert.Contains(t, cache.GetKeys(), "events")
}

func TestFlush_MalformedEnvelope_DropsAndContinues(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			// Only the good row should survive into the request.
			assert.Equal(t, `{"id":2}`+"\n", string(body))
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, wait := newTestWorker(rt)

	bad := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    []byte("not valid json"),
	}
	good := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    makeEnvelope(t, "events", "", map[string]any{"id": 2}),
	}

	w.flush(context.Background(), []jetstream.Msg{bad, good})
	wait()

	// Bad envelope is DoubleAcked synchronously inside flush (poison-pill drop).
	assert.True(t, bad.DoubleAcked.Load(), "malformed envelope must be acked and dropped")
	// Good message also acked after a successful bulk insert.
	assert.True(t, good.DoubleAcked.Load())
}

func TestFlush_MultipleTables_GroupedPerTable(t *testing.T) {
	t.Parallel()
	var tables sync.Map
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			table := req.URL.Query().Get("param_target_table")
			tables.Store(table, true)
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, cache, wait := newTestWorker(rt)

	msgs := []jetstream.Msg{
		&testutil.MockJetStreamMsg{MsgSubject: "ingest.events", MsgData: makeEnvelope(t, "events", "", map[string]any{"id": 1})},
		&testutil.MockJetStreamMsg{MsgSubject: "ingest.users", MsgData: makeEnvelope(t, "users", "", map[string]any{"id": 2})},
		&testutil.MockJetStreamMsg{MsgSubject: "ingest.events", MsgData: makeEnvelope(t, "events", "", map[string]any{"id": 3})},
	}

	w.flush(context.Background(), msgs)
	wait()

	// One request per table — bulk batches per table.
	assert.Equal(t, int32(2), rt.Hits())

	_, eventsHit := tables.Load("events")
	_, usersHit := tables.Load("users")
	assert.True(t, eventsHit, "events table must have been targeted")
	assert.True(t, usersHit, "users table must have been targeted")

	// Cache invalidated for both tables.
	keys := cache.GetKeys()
	assert.Contains(t, keys, "events")
	assert.Contains(t, keys, "users")
}

func TestFlush_BulkFails_FallsBackToOneByOne(t *testing.T) {
	t.Parallel()
	// Mock: fail on multi-row body, succeed on single-row body.
	// This forces flush into the 1-by-1 isolation path and verifies it
	// successfully acks each row that inserts cleanly on its own.
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			lines := strings.Count(string(body), "\n")
			if lines > 1 {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewBufferString("Code: 117. bulk failed")),
				}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, js, _, wait := newTestWorker(rt)

	m1 := &testutil.MockJetStreamMsg{MsgSubject: "ingest.events", MsgData: makeEnvelope(t, "events", "", map[string]any{"id": 1})}
	m2 := &testutil.MockJetStreamMsg{MsgSubject: "ingest.events", MsgData: makeEnvelope(t, "events", "", map[string]any{"id": 2})}

	w.flush(context.Background(), []jetstream.Msg{m1, m2})
	wait()

	// 1 bulk attempt (failed) + 2 single-row retries = 3 HTTP requests.
	assert.Equal(t, int32(3), rt.Hits())

	// Both messages acked via the per-row success path.
	assert.True(t, m1.DoubleAcked.Load())
	assert.True(t, m2.DoubleAcked.Load())

	// Nothing went to the DLQ — every row inserted cleanly on its own.
	assert.Empty(t, js.Published(), "no DLQ publishes when 1-by-1 retries all succeed")
}

func TestFlush_BadRow_Isolated_GoesToDLQ(t *testing.T) {
	t.Parallel()
	// Bulk insert fails; on per-row retry, the row carrying `"poison":true`
	// fails again. That single row should end up in the DLQ while the rest
	// of the batch is acked normally.
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			s := string(body)

			// Multi-row body: bulk attempt, always fails.
			if strings.Count(s, "\n") > 1 {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewBufferString("Code: 117. bulk failed")),
				}, nil
			}

			// Single-row body: fail only if it contains the poison marker.
			if strings.Contains(s, `"poison":true`) {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewBufferString("Code: 60. bad row")),
				}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, js, _, wait := newTestWorker(rt)

	goodA := &testutil.MockJetStreamMsg{MsgSubject: "ingest.events", MsgData: makeEnvelope(t, "events", "", map[string]any{"id": 1})}
	poison := &testutil.MockJetStreamMsg{MsgSubject: "ingest.events", MsgData: makeEnvelope(t, "events", "", map[string]any{"id": 2, "poison": true})}
	goodB := &testutil.MockJetStreamMsg{MsgSubject: "ingest.events", MsgData: makeEnvelope(t, "events", "", map[string]any{"id": 3})}

	w.flush(context.Background(), []jetstream.Msg{goodA, poison, goodB})
	wait()

	// 1 bulk + 3 single-row retries = 4 HTTP requests.
	assert.Equal(t, int32(4), rt.Hits())

	// Good rows are acked via handleSuccess.
	assert.True(t, goodA.DoubleAcked.Load(), "good row A must be acked")
	assert.True(t, goodB.DoubleAcked.Load(), "good row B must be acked")

	// Poison row is DLQ'd, and sendToDLQ also DoubleAcks the original.
	assert.True(t, poison.DoubleAcked.Load(), "poison row must be acked after DLQ publish")

	published := js.Published()
	require.Len(t, published, 1, "exactly one row should reach the DLQ")
	assert.Equal(t, "dlq.events", published[0].Subject)
	assert.Equal(t, "events", published[0].Header.Get("X-DLQ-Table"))
	assert.Contains(t, published[0].Header.Get("X-DLQ-Error"), "Code: 60")
}

func TestFlush_StripsIngestPrefixFromSubject(t *testing.T) {
	t.Parallel()
	// The worker uses the subject (sans "ingest." prefix) as the version key
	// for cache invalidation. Verify that prefix stripping happens.
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, cache, wait := newTestWorker(rt)

	m := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events.org_42",
		MsgData:    makeEnvelope(t, "events", "org_42", map[string]any{"id": 1}),
	}
	w.flush(context.Background(), []jetstream.Msg{m})
	wait()

	keys := cache.GetKeys()
	assert.Contains(t, keys, "events", "table version key must be present")
	assert.Contains(t, keys, "events.org_42", "NATS-safe subject (sans ingest. prefix) must be invalidated")
}
