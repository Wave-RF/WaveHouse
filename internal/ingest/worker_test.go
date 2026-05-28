package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/nats-io/nats.go"
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

// newIngestMsg builds a MockJetStreamMsg shaped exactly the way the
// /v1/ingest producer (internal/api/ingest.go) publishes events:
//
//	subject  = "ingest." + SafeEncodeNATS(table)                     // scopeless
//	subject  = "ingest." + SafeEncodeNATS(table) + "." + SafeEncodeNATS(scope)
//	envelope = { table_name: table, scope: scope, ... }              // raw, not encoded
//
// Tests should use this helper instead of hand-rolling MsgSubject/MsgData pairs
// so the subject and envelope can't silently drift from the producer's contract.
func newIngestMsg(t *testing.T, table, scope string, data map[string]any) *testutil.MockJetStreamMsg {
	t.Helper()
	subj := "ingest." + query.SafeEncodeNATS(table)
	if scope != "" {
		subj += "." + query.SafeEncodeNATS(scope)
	}
	return &testutil.MockJetStreamMsg{
		MsgSubject: subj,
		MsgData:    makeEnvelope(t, table, scope, data),
	}
}

// ---------------------------------------------------------------------------
// StartIngestWorker — constructor argument validation
// ---------------------------------------------------------------------------

func TestStartIngestWorker_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T) (*nats.Conn, cache.Cache)
		wantErrSub string
	}{
		{
			name:       "nil nats connection",
			setup:      func(*testing.T) (*nats.Conn, cache.Cache) { return nil, &testutil.MockCache{} },
			wantErrSub: "nats connection is nil",
		},
		{
			name: "nil cache",
			setup: func(t *testing.T) (*nats.Conn, cache.Cache) {
				emb, err := mq.NewEmbedded(t.TempDir(), 1024*1024, testutil.NopLogger())
				require.NoError(t, err)
				t.Cleanup(func() { _ = emb.Close() })
				return emb.NatsConn(), nil
			},
			wantErrSub: "cache is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			nc, c := tt.setup(t)
			_, err := StartIngestWorker(context.Background(), nc, c,
				"localhost", "8123", "http", "user", "pass", "db")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrSub)
		})
	}
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
	_, err = js.Publish(ctx, "ingest.events.org_42", envelope)
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
			if k == "events.org_42" {
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

// TestRunLoop_PerTableBatching_NoCrossTableContamination reproduces the
// flake described in #191: when the worker uses a single global batch for
// every table, a low-volume publisher to table A immediately followed by a
// maxBatch-sized publish to table B "leaks" A's events into B's batch.
// The size trigger then fires on `(A + part of B)`, stranding the last few
// B events in a fresh batch that only flushes when maxWait elapses.
//
// Sequence (priorMsgs=2, triggerMsgs=maxBatch=5):
//
//	old global batch:                       new per-table batch:
//	  [A1 A2 B1 B2 B3] → size trigger         tableA: [A1 A2]       (waits maxWait)
//	  [B4 B5]          → maxWait timer        tableB: [B1..B5]      → size trigger
//	                                                                   (flushes now)
//
// The assertion polls for B's count to hit triggerMsgs within
// `maxWait - epsilon`. The old code's last two B rows can only land after
// the maxWait timer expires, so the assertion deterministically fails;
// per-table batching lets B hit its own size trigger and flush immediately.
func TestRunLoop_PerTableBatching_NoCrossTableContamination(t *testing.T) {
	t.Parallel()

	const (
		smallBatch  = 5
		smallWait   = 2 * time.Second
		priorMsgs   = 2 // events on tableA published first
		triggerMsgs = smallBatch
		// Poll window. Must be < smallWait so the old code's maxWait-bound
		// flush of the leftover B rows can't sneak in and mask the failure.
		assertDeadline = smallWait - 500*time.Millisecond
	)

	emb, err := mq.NewEmbedded(t.TempDir(), 8*1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	// CH stub: count rows (newlines in the JSONEachRow body) per target table.
	var (
		mu          sync.Mutex
		rowsByTable = map[string]int{}
	)
	chSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		table := r.URL.Query().Get("param_target_table")
		mu.Lock()
		rowsByTable[table] += bytes.Count(body, []byte("\n"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(chSrv.Close)

	u, err := url.Parse(chSrv.URL)
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)

	js, err := jetstream.New(emb.NatsConn())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	cons, err := js.CreateOrUpdateConsumer(ctx, mq.StreamName(), jetstream.ConsumerConfig{
		Durable:       BufferConsumerName,
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 1000,
	})
	require.NoError(t, err)

	// Build worker directly so we can dial maxBatch / maxWait small enough
	// to reproduce the contamination in ~2s instead of the production 5s.
	worker := &IngestWorker{
		js:         js,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		cache:      &testutil.MockCache{},
		logger:     testutil.NopLogger(),
		chURL:      fmt.Sprintf("http://%s:%s", host, port),
		user:       "u",
		password:   "p",
		db:         "db",
		maxBatch:   smallBatch,
		maxWait:    smallWait,
	}
	worker.wg.Add(1)
	go worker.runLoop(ctx, cons)
	t.Cleanup(func() {
		cancel()
		worker.wg.Wait()
	})

	// 1. Prime table A — too few events to hit either trigger on its own.
	for i := 0; i < priorMsgs; i++ {
		_, err = js.Publish(ctx, "ingest.tableA",
			makeEnvelope(t, "tableA", "", map[string]any{"id": i}))
		require.NoError(t, err)
	}

	// 2. Then publish exactly maxBatch events to table B — should hit B's
	//    own size trigger and flush immediately, regardless of what A did.
	for i := 0; i < triggerMsgs; i++ {
		_, err = js.Publish(ctx, "ingest.tableB",
			makeEnvelope(t, "tableB", "", map[string]any{"id": i}))
		require.NoError(t, err)
	}

	// All triggerMsgs B rows must reach ClickHouse within maxWait − epsilon.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return rowsByTable["tableB"] >= triggerMsgs
	}, assertDeadline, 25*time.Millisecond,
		"table B should flush %d rows within %s of being published "+
			"(table A's prior events must not strand B rows in a batch "+
			"that waits for the maxWait timer)",
		triggerMsgs, assertDeadline,
	)
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

func TestHandleSuccess(t *testing.T) {
	t.Parallel()

	// natsSafeSubject is what the worker derives by stripping "ingest." from
	// the NATS subject the producer publishes to (see internal/api/ingest.go).
	// In production that is always "<SafeEncodeNATS(table)>[.<SafeEncodeNATS(scope)>]",
	// so e.g. table "events" + scope "org_1" yields natsSafeSubject "events.org_1".
	// handleSuccess invalidates the encoded table key + each unique scoped key,
	// deduping when the scopeless subject already equals the table key.

	tests := []struct {
		name             string
		table            string
		natsSafeSubjects []string // realistic shape: "<encoded_table>[.<encoded_scope>]"
		cacheErr         error    // applied via MockCache.InvErr before the call
		msgDoubleAckErr  error    // applied to every MockJetStreamMsg
		wantCacheKeys    []string // expected (ElementsMatch) keys in cache.GetKeys()
	}{
		{
			name:             "invalidates table + each unique scope",
			table:            "events",
			natsSafeSubjects: []string{"events.org_1", "events.org_2"},
			wantCacheKeys:    []string{"events", "events.org_1", "events.org_2"},
		},
		{
			// 3 msgs, 2 distinct scoped subjects → table + 2 scoped = 3 keys.
			name:             "deduplicates repeated scope keys",
			table:            "events",
			natsSafeSubjects: []string{"events.org_1", "events.org_1", "events.org_2"},
			wantCacheKeys:    []string{"events", "events.org_1", "events.org_2"},
		},
		{
			// Scopeless: producer publishes to "ingest.<table>", so
			// natsSafeSubject equals the encoded table → one key.
			name:             "scopeless subject dedups against table key",
			table:            "events",
			natsSafeSubjects: []string{"events"},
			wantCacheKeys:    []string{"events"},
		},
		{
			// Table name containing a '.' is percent-encoded on both sides of
			// the version key — keys must use the encoded form for cache hits
			// to line up with the reader (internal/api/structured_query.go).
			name:             "table with special chars is percent-encoded",
			table:            "events.staging",
			natsSafeSubjects: []string{"events%2Estaging.org_1"},
			wantCacheKeys:    []string{"events%2Estaging", "events%2Estaging.org_1"},
		},
		{
			// Cache failure must not prevent ack — failure is logged, non-fatal.
			name:             "cache error still acks",
			table:            "events",
			natsSafeSubjects: []string{"events.org_1"},
			cacheErr:         errors.New("cache backend down"),
			wantCacheKeys:    []string{"events", "events.org_1"},
		},
		{
			// DoubleAck error is logged but DoubleAcked flag still flips.
			name:             "double ack error does not panic",
			table:            "events",
			natsSafeSubjects: []string{"events.org_1"},
			msgDoubleAckErr:  errors.New("server unavailable"),
			wantCacheKeys:    []string{"events", "events.org_1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w, _, cache, wait := newTestWorker(&testutil.MockRoundTripper{})
			cache.InvErr = tt.cacheErr

			msgs := make([]*testutil.MockJetStreamMsg, len(tt.natsSafeSubjects))
			parsed := make([]parsedMsg, len(tt.natsSafeSubjects))
			for i, sub := range tt.natsSafeSubjects {
				// MsgSubject intentionally left blank — handleSuccess reads
				// natsSafeSubject off parsedMsg, not the NATS msg itself.
				msgs[i] = &testutil.MockJetStreamMsg{DoubleAckErr: tt.msgDoubleAckErr}
				parsed[i] = parsedMsg{natsMsg: msgs[i], natsSafeSubject: sub}
			}

			w.handleSuccess(context.Background(), tt.table, parsed)
			wait()

			assert.ElementsMatch(t, tt.wantCacheKeys, cache.GetKeys())
			for i, m := range msgs {
				assert.True(t, m.DoubleAcked.Load(),
					"msg %d (natsSafeSubject=%q) must be DoubleAcked", i, tt.natsSafeSubjects[i])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// sendToDLQ — DLQ publish + headers + ack-only-on-success
// ---------------------------------------------------------------------------

func TestSendToDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		table         string
		safeSubject   string
		msgData       []byte
		msgSubject    string
		errMsg        string
		pubErr        error // applied to MockJetStream.PubErr before the call
		wantPublished int   // expected number of PublishMsg calls recorded
		wantSubject   string
		wantData      []byte
		wantAcked     bool
		extraCheck    func(t *testing.T, m *nats.Msg)
	}{
		{
			name:          "publishes with headers and acks original",
			table:         "events",
			safeSubject:   "events",
			msgData:       []byte(`{"bad":"row"}`),
			msgSubject:    "events",
			errMsg:        "Code: 60. DB::Exception: ...",
			wantPublished: 1,
			wantSubject:   "dlq.events",
			wantData:      []byte(`{"bad":"row"}`),
			wantAcked:     true,
			extraCheck: func(t *testing.T, m *nats.Msg) {
				assert.Equal(t, "events", m.Header.Get("X-DLQ-Table"))
				assert.Contains(t, m.Header.Get("X-DLQ-Error"), "Code: 60")
				assert.NotEmpty(t, m.Header.Get("X-DLQ-Timestamp"))
				_, err := time.Parse(time.RFC3339, m.Header.Get("X-DLQ-Timestamp"))
				assert.NoError(t, err, "X-DLQ-Timestamp must be RFC3339")
			},
		},
		{
			// No ack on original — NATS must redeliver until DLQ recovers,
			// otherwise we'd lose the row.
			name:          "publish failure means no ack",
			table:         "events",
			safeSubject:   "events",
			msgData:       []byte(`{"bad":"row"}`),
			msgSubject:    "events",
			errMsg:        "boom",
			pubErr:        errors.New("nats unavailable"),
			wantPublished: 0,
			wantAcked:     false,
		},
		{
			// The DLQ subject uses the already-NATS-safe subject from the
			// upstream envelope; we don't re-encode here.
			name:          "uses already-encoded safe subject",
			table:         "events.staging",
			safeSubject:   "events%2Estaging",
			msgData:       []byte("payload"),
			errMsg:        "err",
			wantPublished: 1,
			wantSubject:   "dlq.events%2Estaging",
			wantData:      []byte("payload"),
			wantAcked:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w, js, _, _ := newTestWorker(&testutil.MockRoundTripper{})
			js.PubErr = tt.pubErr

			original := &testutil.MockJetStreamMsg{
				MsgData:    tt.msgData,
				MsgSubject: tt.msgSubject,
			}
			pm := parsedMsg{natsMsg: original, natsSafeSubject: tt.safeSubject}

			w.sendToDLQ(context.Background(), tt.table, pm, tt.errMsg)

			published := js.Published()
			require.Len(t, published, tt.wantPublished)
			if tt.wantPublished > 0 {
				got := published[0]
				assert.Equal(t, tt.wantSubject, got.Subject)
				if tt.wantData != nil {
					assert.Equal(t, tt.wantData, got.Data)
				}
				if tt.extraCheck != nil {
					tt.extraCheck(t, got)
				}
			}
			assert.Equal(t, tt.wantAcked, original.DoubleAcked.Load(), "DoubleAcked flag")
		})
	}
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

	m1 := newIngestMsg(t, "events", "org_1", map[string]any{"id": 1})
	m2 := newIngestMsg(t, "events", "org_1", map[string]any{"id": 2})

	w.flush(context.Background(), []jetstream.Msg{m1, m2})
	wait()

	// One HTTP request — both rows batched together.
	assert.Equal(t, int32(1), rt.Hits())

	// Both messages acked.
	assert.True(t, m1.DoubleAcked.Load())
	assert.True(t, m2.DoubleAcked.Load())

	// Cache invalidation: table-level + the shared scoped key.
	assert.ElementsMatch(t,
		[]string{"events", "events.org_1"},
		cache.GetKeys(),
	)
}

// TestFlush_MultiScope_InvalidatesPerScope covers the multi-scope batching path:
// distinct scopes for the same table get grouped into one HTTP insert (per-table
// grouping), but each unique scope still gets its own cache version key bumped
// alongside the global table key.
func TestFlush_MultiScope_InvalidatesPerScope(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, cache, wait := newTestWorker(rt)

	msgs := []jetstream.Msg{
		newIngestMsg(t, "events", "org_1", map[string]any{"id": 1}),
		newIngestMsg(t, "events", "org_2", map[string]any{"id": 2}),
		newIngestMsg(t, "events", "org_1", map[string]any{"id": 3}),
	}

	w.flush(context.Background(), msgs)
	wait()

	// Per-table grouping → one HTTP request despite multiple scopes.
	assert.Equal(t, int32(1), rt.Hits())

	// Cache: table key + each unique scope key, deduped.
	assert.ElementsMatch(t,
		[]string{"events", "events.org_1", "events.org_2"},
		cache.GetKeys(),
	)
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

	// Bad message: intentionally malformed envelope, can't go through
	// newIngestMsg because that helper builds a valid envelope.
	bad := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    []byte("not valid json"),
	}
	good := newIngestMsg(t, "events", "", map[string]any{"id": 2})

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
		newIngestMsg(t, "events", "", map[string]any{"id": 1}),
		newIngestMsg(t, "users", "", map[string]any{"id": 2}),
		newIngestMsg(t, "events", "", map[string]any{"id": 3}),
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

	m1 := newIngestMsg(t, "events", "", map[string]any{"id": 1})
	m2 := newIngestMsg(t, "events", "", map[string]any{"id": 2})

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

	goodA := newIngestMsg(t, "events", "", map[string]any{"id": 1})
	poison := newIngestMsg(t, "events", "", map[string]any{"id": 2, "poison": true})
	goodB := newIngestMsg(t, "events", "", map[string]any{"id": 3})

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

	m := newIngestMsg(t, "events", "org_42", map[string]any{"id": 1})
	w.flush(context.Background(), []jetstream.Msg{m})
	wait()

	keys := cache.GetKeys()
	assert.Contains(t, keys, "events", "table version key must be present")
	assert.Contains(t, keys, "events.org_42", "NATS-safe subject (sans ingest. prefix) must be invalidated")
}
