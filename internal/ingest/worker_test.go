package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
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
		target: func() chconn.Target {
			return chconn.Target{URL: "http://test-clickhouse:8123", Username: "test_user", Password: "test_pass", Database: "test_db"}
		},
	}
	return w, js, cache, func() { w.ackWg.Wait() }
}

// makeEnvelope returns the JSON wire format the worker reads off NATS. Column
// order is the sorted key order, which is deterministic; production uses the
// table's declaration order.
func makeEnvelope(t *testing.T, tableName, scope string, data map[string]any) []byte {
	t.Helper()
	return makeEnvelopeCols(t, tableName, scope, slices.Sorted(maps.Keys(data)), data)
}

// makeEnvelopeCols is makeEnvelope with an explicit column list, for the tests
// that publish two different schemas for one table.
func makeEnvelopeCols(t *testing.T, tableName, scope string, cols []string, data map[string]any) []byte {
	t.Helper()
	schema := make([]discovery.Column, len(cols))
	for i, c := range cols {
		schema[i] = discovery.Column{Name: c, Position: uint64(i + 1)}
	}
	row, err := EncodeCompactRow(schema, data)
	require.NoError(t, err)
	out, err := json.Marshal(EventMessage{
		TableName:         tableName,
		Scope:             scope,
		ReceivedTimestamp: "2026-01-01T00:00:00Z",
		Format:            FormatJSONCompactEachRow,
		Columns:           cols,
		Row:               row,
	})
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
				func() chconn.Target { return chconn.Target{URL: "http://localhost:8123"} }, nil)
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

	js, err := jetstream.New(emb.NatsConn())
	require.NoError(t, err)
	cons, err := js.CreateOrUpdateConsumer(ctx, mq.StreamName(), jetstream.ConsumerConfig{
		Durable:       BufferConsumerName,
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 1000,
	})
	require.NoError(t, err)

	// Construct the worker directly so the flush timer can use a short maxWait —
	// the single-event batch then flushes on the timer in ~200ms instead of the
	// 5s default. StartIngestWorker's own setup is covered by
	// TestStartIngestWorker_Validation + _StopFunc.
	worker := &IngestWorker{
		js:         js,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		cache:      cache,
		logger:     testutil.NopLogger(),
		target: func() chconn.Target {
			return chconn.Target{URL: fmt.Sprintf("http://%s:%s", host, port), Username: "u", Password: "p", Database: "db"}
		},
		maxBatch: defaultMaxBatch,
		maxWait:  200 * time.Millisecond,
	}
	worker.wg.Add(1)
	go worker.dispatchLoop(ctx, cons)
	t.Cleanup(func() {
		cancel()
		worker.wg.Wait()
	})

	// ── Publish an envelope on ingest.events ──
	envelope := makeEnvelope(t, "events", "org_42", map[string]any{"id": 1, "v": "x"})
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
	assert.Equal(t, "[1,\"x\"]\n", bodies[0])
	assert.Equal(t, "events", params[0])
	gotMu.Unlock()

	// Cache invalidation should run with the table+scope namespace.
	require.Eventually(t, func() bool {
		for _, ns := range cache.GetNamespaces() {
			if ns.Table == "events" && ns.Scope == "org_42" {
				return true
			}
		}
		return false
	}, 4*time.Second, 25*time.Millisecond, "cache must be invalidated for the table+scope namespace")

	// Shutdown (cancel + drain) is handled by t.Cleanup above.
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
		func() chconn.Target {
			return chconn.Target{URL: fmt.Sprintf("http://%s:%s", host, port), Username: "u", Password: "p", Database: "db"}
		}, nil)
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

// TestStartIngestWorker_StopFunc_CleanShutdown verifies the graceful path: with
// no in-flight work, stopFn drains the dispatch loop and returns nil well within
// the deadline — the success arm of waitOrDeadline. Complements the
// deadline-exceeded test above.
func TestStartIngestWorker_StopFunc_CleanShutdown(t *testing.T) {
	t.Parallel()

	emb, err := mq.NewEmbedded(t.TempDir(), 1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	// chURL is never dialed: with no messages there is no flush, so a dummy
	// host/port is fine.
	stopFn, err := StartIngestWorker(context.Background(), emb.NatsConn(), &testutil.MockCache{},
		func() chconn.Target { return chconn.Target{URL: "http://localhost:8123"} }, nil)
	require.NoError(t, err)

	// Nothing to flush, so shutdown drains immediately and returns nil before the
	// ample deadline.
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, stopFn(shutCtx))
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
			assert.Equal(t, "INSERT INTO {target_table:Identifier} (`id`) FORMAT JSONCompactEachRow", q.Get("query"))
			// #372: the insert pins best_effort — the server default since
			// ClickHouse 26.5; older 'basic' defaults reject the canonical
			// form's zone suffix.
			assert.Equal(t, "best_effort", q.Get("date_time_input_format"))
			// A field the record omitted rides as null in its column's slot;
			// this is what turns it back into the column's default.
			assert.Equal(t, "1", q.Get("input_format_null_as_default"))

			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			assert.Equal(t, "test_user", req.Header.Get("X-ClickHouse-User"))
			assert.Equal(t, "test_pass", req.Header.Get("X-ClickHouse-Key"))

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			assert.Equal(t, "[1]\n", string(body))

			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), "events", []string{"id"}, []parsedMsg{
		{row: []byte(`[1]`)},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), rt.Hits())
}

func TestInsertToClickHouse_MultipleMessages_JSONCompactEachRow(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			// JSONCompactEachRow: newline-separated, each row a positional array.
			assert.Equal(t, "[1]\n[2]\n[3]\n", string(body))
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), "events", []string{"id"}, []parsedMsg{
		{row: []byte(`[1]`)},
		{row: []byte(`[2]`)},
		{row: []byte(`[3]`)},
	})
	require.NoError(t, err)
}

func TestInsertToClickHouse_SafelyParameterizesTableName(t *testing.T) {
	t.Parallel()
	malicious := "users; DROP TABLE users;--"
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			q := req.URL.Query()
			// Static table reference — no interpolation of the table name.
			assert.Equal(t, "INSERT INTO {target_table:Identifier} (`id`) FORMAT JSONCompactEachRow", q.Get("query"))
			// Malicious table name lives in the out-of-band parameter,
			// where ClickHouse will quote it as an Identifier.
			assert.Equal(t, malicious, q.Get("param_target_table"))
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, _ := newTestWorker(rt)

	err := w.insertToClickHouse(context.Background(), malicious, []string{"id"}, []parsedMsg{
		{row: []byte(`[1]`)},
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

	err := w.insertToClickHouse(context.Background(), "missing_table", []string{"id"}, []parsedMsg{
		{row: []byte(`[1]`)},
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

	err := w.insertToClickHouse(context.Background(), "events", []string{"id"}, []parsedMsg{
		{row: []byte(`[1]`)},
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

	err := w.insertToClickHouse(context.Background(), "events", []string{"id"}, []parsedMsg{
		{row: []byte(`[1]`)},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

// ---------------------------------------------------------------------------
// handleSuccess — cache invalidation + DoubleAck
// ---------------------------------------------------------------------------

func TestHandleSuccess(t *testing.T) {
	t.Parallel()

	// handleSuccess invalidates one namespace per distinct scope in the batch: the
	// encoded table paired with the encoded scope (envelope.scope). Invalidate turns
	// an empty scope into a whole-table bump and a non-empty scope into a per-scope
	// bump, so the worker only needs to emit the table+scope pairs it saw.

	tests := []struct {
		name            string
		table           string
		scopes          []string // raw per-message scopes (envelope.scope)
		cacheErr        error    // applied via MockCache.InvErr before the call
		msgDoubleAckErr error    // applied to every MockJetStreamMsg
		wantNamespaces  []cache.Namespace
	}{
		{
			name:           "invalidates each unique scope",
			table:          "events",
			scopes:         []string{"org_1", "org_2"},
			wantNamespaces: []cache.Namespace{{Table: "events", Scope: "org_1"}, {Table: "events", Scope: "org_2"}},
		},
		{
			// 3 msgs, 2 distinct scopes → 2 namespaces.
			name:           "deduplicates repeated scopes",
			table:          "events",
			scopes:         []string{"org_1", "org_1", "org_2"},
			wantNamespaces: []cache.Namespace{{Table: "events", Scope: "org_1"}, {Table: "events", Scope: "org_2"}},
		},
		{
			// Scopeless message → empty-scope namespace (a whole-table bump).
			name:           "scopeless message yields whole-table namespace",
			table:          "events",
			scopes:         []string{""},
			wantNamespaces: []cache.Namespace{{Table: "events", Scope: ""}},
		},
		{
			// A scopeless message bumps the whole table, which subsumes every scope,
			// so the worker collapses the batch to just the whole-table namespace.
			name:           "scopeless message subsumes other scopes",
			table:          "events",
			scopes:         []string{"org_1", "", "org_2"},
			wantNamespaces: []cache.Namespace{{Table: "events", Scope: ""}},
		},
		{
			// Table and scope are percent-encoded so keys line up with the reader
			// (internal/api/structured_query.go), which encodes the table too.
			name:           "table and scope are percent-encoded",
			table:          "events.staging",
			scopes:         []string{"org.1"},
			wantNamespaces: []cache.Namespace{{Table: "events%2Estaging", Scope: "org%2E1"}},
		},
		{
			// Cache failure must not prevent ack — failure is logged, non-fatal.
			name:           "cache error still acks",
			table:          "events",
			scopes:         []string{"org_1"},
			cacheErr:       errors.New("cache backend down"),
			wantNamespaces: []cache.Namespace{{Table: "events", Scope: "org_1"}},
		},
		{
			// DoubleAck error is logged but DoubleAcked flag still flips.
			name:            "double ack error does not panic",
			table:           "events",
			scopes:          []string{"org_1"},
			msgDoubleAckErr: errors.New("server unavailable"),
			wantNamespaces:  []cache.Namespace{{Table: "events", Scope: "org_1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w, _, mc, wait := newTestWorker(&testutil.MockRoundTripper{})
			mc.InvErr = tt.cacheErr

			msgs := make([]*testutil.MockJetStreamMsg, len(tt.scopes))
			parsed := make([]parsedMsg, len(tt.scopes))
			for i, scope := range tt.scopes {
				// handleSuccess reads scope off parsedMsg, not the NATS msg itself.
				msgs[i] = &testutil.MockJetStreamMsg{DoubleAckErr: tt.msgDoubleAckErr}
				parsed[i] = parsedMsg{natsMsg: msgs[i], scope: scope}
			}

			w.handleSuccess(context.Background(), tt.table, parsed)
			wait()

			assert.ElementsMatch(t, tt.wantNamespaces, mc.GetNamespaces())
			for i, m := range msgs {
				assert.True(t, m.DoubleAcked.Load(),
					"msg %d (scope=%q) must be DoubleAcked", i, tt.scopes[i])
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
// parseMsg + flushTable — parse/route, then per-table insert / ack / DLQ
// ---------------------------------------------------------------------------

// parseAll runs each mock message through the worker's real parseMsg path and
// returns the parsedMsgs, failing the test if any envelope is malformed.
func parseAll(t *testing.T, w *IngestWorker, msgs ...*testutil.MockJetStreamMsg) []parsedMsg {
	t.Helper()
	out := make([]parsedMsg, 0, len(msgs))
	for _, m := range msgs {
		pm, ok := w.parseMsg(context.Background(), m)
		require.True(t, ok, "parseMsg unexpectedly dropped a message")
		out = append(out, pm)
	}
	return out
}

func TestParseMsg(t *testing.T) {
	t.Parallel()

	t.Run("valid envelope populates routing fields", func(t *testing.T) {
		t.Parallel()
		w, _, _, _ := newTestWorker(&testutil.MockRoundTripper{})
		m := newIngestMsg(t, "events", "org_42", map[string]any{"id": 1})

		pm, ok := w.parseMsg(context.Background(), m)
		require.True(t, ok)
		assert.Equal(t, "events", pm.tableName, "raw table name drives per-table routing")
		assert.Equal(t, "org_42", pm.scope)
		// natsSafeSubject is the subject sans the "ingest." prefix (cache version key).
		assert.Equal(t, "events.org_42", pm.natsSafeSubject)
		assert.JSONEq(t, `[1]`, string(pm.row))
		assert.Equal(t, []string{"id"}, pm.columns)
		assert.False(t, m.DoubleAcked.Load(), "valid message must not be acked by parseMsg")
	})

	t.Run("malformed envelope is acked-and-dropped", func(t *testing.T) {
		t.Parallel()
		w, _, _, _ := newTestWorker(&testutil.MockRoundTripper{})
		bad := &testutil.MockJetStreamMsg{
			MsgSubject: "ingest.events",
			MsgData:    []byte("not valid json"),
		}

		_, ok := w.parseMsg(context.Background(), bad)
		assert.False(t, ok, "malformed envelope must be dropped")
		assert.True(t, bad.DoubleAcked.Load(), "poison pill must be acked so NATS won't redeliver it")
	})
}

func TestFlushTable_EmptyBatch_NoOp(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{}
	w, _, _, _ := newTestWorker(rt)

	// Must not panic, must not call HTTP.
	w.flushTable(context.Background(), "events", nil)
	w.flushTable(context.Background(), "events", []parsedMsg{})
	assert.Equal(t, int32(0), rt.Hits())
}

func TestFlushTable_HappyPath(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, mc, wait := newTestWorker(rt)

	m1 := newIngestMsg(t, "events", "org_1", map[string]any{"id": 1})
	m2 := newIngestMsg(t, "events", "org_1", map[string]any{"id": 2})

	w.flushTable(context.Background(), "events", parseAll(t, w, m1, m2))
	wait()

	// One HTTP request — both rows in a single bulk insert.
	assert.Equal(t, int32(1), rt.Hits())

	// Both messages acked.
	assert.True(t, m1.DoubleAcked.Load())
	assert.True(t, m2.DoubleAcked.Load())

	// Cache invalidation: one namespace for the single shared scope.
	assert.ElementsMatch(t,
		[]cache.Namespace{{Table: "events", Scope: "org_1"}},
		mc.GetNamespaces(),
	)
}

// TestFlushTable_MultiScope covers a single table with multiple scopes: one bulk
// HTTP insert, but each unique scope still gets its own namespace invalidated.
func TestFlushTable_MultiScope(t *testing.T) {
	t.Parallel()
	rt := &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, mc, wait := newTestWorker(rt)

	msgs := parseAll(t, w,
		newIngestMsg(t, "events", "org_1", map[string]any{"id": 1}),
		newIngestMsg(t, "events", "org_2", map[string]any{"id": 2}),
		newIngestMsg(t, "events", "org_1", map[string]any{"id": 3}),
	)

	w.flushTable(context.Background(), "events", msgs)
	wait()

	// One bulk HTTP request despite multiple scopes.
	assert.Equal(t, int32(1), rt.Hits())

	// Cache: one namespace per unique scope, deduped.
	assert.ElementsMatch(t,
		[]cache.Namespace{
			{Table: "events", Scope: "org_1"},
			{Table: "events", Scope: "org_2"},
		},
		mc.GetNamespaces(),
	)
}

func TestFlushTable_BulkFails_FallsBackToOneByOne(t *testing.T) {
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

	w.flushTable(context.Background(), "events", parseAll(t, w, m1, m2))
	wait()

	// 1 bulk attempt (failed) + 2 single-row retries = 3 HTTP requests.
	assert.Equal(t, int32(3), rt.Hits())

	// Both messages acked via the per-row success path.
	assert.True(t, m1.DoubleAcked.Load())
	assert.True(t, m2.DoubleAcked.Load())

	// Nothing went to the DLQ — every row inserted cleanly on its own.
	assert.Empty(t, js.Published(), "no DLQ publishes when 1-by-1 retries all succeed")
}

func TestFlushTable_BadRow_Isolated_GoesToDLQ(t *testing.T) {
	t.Parallel()
	// Bulk insert fails; on per-row retry, the row whose `poison` column is true
	// fails again. That single row should end up in the DLQ while the rest of the
	// batch is acked normally. Every row carries the same columns, so the batch is
	// one group and one bulk attempt — see groupByColumns.
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

			// Single-row body: fail only if the poison column is set. The row is
			// positional now, so the marker is the value, not a named field.
			if strings.Contains(s, "true") {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewBufferString("Code: 60. bad row")),
				}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, js, _, wait := newTestWorker(rt)

	goodA := newIngestMsg(t, "events", "", map[string]any{"id": 1, "poison": false})
	poison := newIngestMsg(t, "events", "", map[string]any{"id": 2, "poison": true})
	goodB := newIngestMsg(t, "events", "", map[string]any{"id": 3, "poison": false})

	w.flushTable(context.Background(), "events", parseAll(t, w, goodA, poison, goodB))
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

func TestFlushTable_BadRow_DLQDisabledForTable_LeftUnacked(t *testing.T) {
	t.Parallel()
	// Same poison batch, but the settings say the DLQ is off for this table:
	// the poison row must be neither published nor acked, so NATS redelivers
	// it — nothing is ever dropped — while the good rows still ack normally.
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			s := string(body)
			if strings.Count(s, "\n") > 1 || strings.Contains(s, "true") {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewBufferString("Code: 60. bad row")),
				}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, js, _, wait := newTestWorker(rt)
	w.dlqEnabled = func(table string) bool { return table != "events" }

	good := newIngestMsg(t, "events", "", map[string]any{"id": 1, "poison": false})
	poison := newIngestMsg(t, "events", "", map[string]any{"id": 2, "poison": true})

	w.flushTable(context.Background(), "events", parseAll(t, w, good, poison))
	wait()

	assert.True(t, good.DoubleAcked.Load(), "good row must still be acked")
	assert.False(t, poison.DoubleAcked.Load(), "poison row must stay unacked for redelivery")
	assert.Empty(t, js.Published(), "nothing reaches the DLQ while it is disabled for the table")
}

// ---------------------------------------------------------------------------
// tableBatcher — coalescing, the flushQueued latch, and drain.

//
// These are white-box: they drive the per-table state machine's methods
// directly (the way tableLoop's select does), setting `flushing`/`flushQueued`
// to simulate "a flush is in flight" so the coalesce/latch/drain branches are
// exercised deterministically without timing races.
// ---------------------------------------------------------------------------

func okRoundTripper() *testutil.MockRoundTripper {
	return &testutil.MockRoundTripper{
		Fn: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
}

// closedChan returns an already-closed signal channel, simulating a flush that
// has finished (so "<-b.flushing" returns immediately).
func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// newTestBatcher builds a tableBatcher backed by mocks. maxWait is long so the
// deadline timer never fires during these tests; wait() drains background acks.
func newTestBatcher(t *testing.T, rt http.RoundTripper) (b *tableBatcher, w *IngestWorker, wait func()) {
	t.Helper()
	w, _, _, wait = newTestWorker(rt)
	w.maxBatch = 3
	w.maxWait = time.Hour
	b = newTableBatcher(w, "events")
	t.Cleanup(func() { b.timer.Stop() })
	return b, w, wait
}

func TestTableBatcher_RequestFlush_EmptyBatchNoOp(t *testing.T) {
	t.Parallel()
	rt := okRoundTripper()
	b, _, _ := newTestBatcher(t, rt)

	b.requestFlush(context.Background()) // nothing buffered

	assert.Equal(t, int32(0), rt.Hits(), "no insert for an empty batch")
	assert.Nil(t, b.flushing)
	assert.False(t, b.flushQueued)
}

func TestTableBatcher_RequestFlush_LatchesWhileFlushing(t *testing.T) {
	t.Parallel()
	rt := okRoundTripper()
	b, w, _ := newTestBatcher(t, rt)

	// Simulate a flush already in flight (tableLoop would have set this).
	b.flushing = make(chan struct{})
	b.batch = parseAll(t, w, newIngestMsg(t, "events", "", map[string]any{"id": 1}))

	b.requestFlush(context.Background())

	assert.True(t, b.flushQueued, "a trigger during an in-flight flush must latch, not start a 2nd flush")
	assert.Len(t, b.batch, 1, "rows stay buffered for the deferred flush")
	assert.Equal(t, int32(0), rt.Hits(), "at most one concurrent insert per table")
}

func TestTableBatcher_OnFlushDone(t *testing.T) {
	t.Parallel()

	t.Run("queued flush starts when the slot frees", func(t *testing.T) {
		t.Parallel()
		rt := okRoundTripper()
		b, w, wait := newTestBatcher(t, rt)

		// A flush just finished (closed channel), another was latched, rows wait.
		b.flushing = closedChan()
		b.flushQueued = true
		b.batch = parseAll(t, w, newIngestMsg(t, "events", "", map[string]any{"id": 1}))

		b.onFlushDone(context.Background())
		require.NotNil(t, b.flushing, "latched flush must start")
		<-b.flushing // let it complete
		wait()       // drain background acks

		assert.Equal(t, int32(1), rt.Hits())
		assert.False(t, b.flushQueued)
		assert.Empty(t, b.batch)
	})

	t.Run("no queued flush leaves a partial batch waiting", func(t *testing.T) {
		t.Parallel()
		rt := okRoundTripper()
		b, w, _ := newTestBatcher(t, rt)

		b.flushing = closedChan()
		b.flushQueued = false
		b.batch = parseAll(t, w, newIngestMsg(t, "events", "", map[string]any{"id": 1}))

		b.onFlushDone(context.Background())

		assert.Nil(t, b.flushing, "slot freed")
		assert.Equal(t, int32(0), rt.Hits(), "partial batch waits for its own size/timer")
		assert.Len(t, b.batch, 1)
	})
}

func TestTableBatcher_DrainAndExit(t *testing.T) {
	t.Parallel()

	t.Run("waits for in-flight flush then flushes leftover", func(t *testing.T) {
		t.Parallel()
		rt := okRoundTripper()
		b, w, wait := newTestBatcher(t, rt)

		// An in-flight flush (already completed → closed channel) plus a leftover row.
		b.flushing = closedChan()
		b.batch = parseAll(t, w, newIngestMsg(t, "events", "", map[string]any{"id": 1}))

		b.drainAndExit(context.Background())
		wait()

		assert.Equal(t, int32(1), rt.Hits(), "leftover row is flushed on the way out")
	})

	t.Run("idle with empty batch flushes nothing", func(t *testing.T) {
		t.Parallel()
		rt := okRoundTripper()
		b, _, _ := newTestBatcher(t, rt)

		b.drainAndExit(context.Background()) // flushing nil, batch empty

		assert.Equal(t, int32(0), rt.Hits())
	})
}

func TestDispatchLoop_PerTableBatching_NoCrossTableContamination(t *testing.T) {
	t.Parallel()

	const (
		// worker.go config values. maxWait is deliberately far longer than the
		// assertion window below (production's defaultMaxWait), so a
		// size-trigger flush and a batch stranded waiting out the timer (the
		// regression pinned here) can't blur together on a slow CI runner.
		maxBatch = 100
		maxWait  = 30 * time.Second

		// test values
		batchA = 5 // intentionally under maxBatch
		batchB = maxBatch
	)

	emb, err := mq.NewEmbedded(t.TempDir(), 8*1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	// CH stub: count rows (newlines in the JSONCompactEachRow body) per target table.
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

	worker := &IngestWorker{
		js:         js,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		cache:      &testutil.MockCache{},
		logger:     testutil.NopLogger(),
		target: func() chconn.Target {
			return chconn.Target{URL: fmt.Sprintf("http://%s:%s", host, port), Username: "u", Password: "p", Database: "db"}
		},
		maxBatch: maxBatch,
		maxWait:  maxWait,
	}
	worker.wg.Add(1)
	go worker.dispatchLoop(ctx, cons)
	t.Cleanup(func() {
		cancel()
		worker.wg.Wait()
	})

	// 1. Prime table A — too few events to hit either trigger on its own.
	for i := range batchA {
		_, err = js.Publish(ctx, "ingest.tableA",
			makeEnvelope(t, "tableA", "", map[string]any{"id": i}))
		require.NoError(t, err)
	}

	// 2. Then publish exactly maxBatch events to table B — should hit B's
	//    own size trigger and flush immediately, regardless of what A did.
	for i := range batchB {
		_, err = js.Publish(ctx, "ingest.tableB",
			makeEnvelope(t, "tableB", "", map[string]any{"id": i}))
		require.NoError(t, err)
	}

	// All B rows must reach ClickHouse within production's defaultMaxWait —
	// proof they flushed on B's own size trigger faster than the real 5s
	// timer bound, not on this test's (30s) timer.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return rowsByTable["tableB"] >= batchB
	}, defaultMaxWait, 25*time.Millisecond,
		"table B hit maxBatch and should flush on its own size trigger "+
			"before production's defaultMaxWait timer bound "+
			"(table A's prior events must not strand B rows in a batch "+
			"that waits for the maxWait timer)",
	)
}

// TestDispatchLoop_PartialBatchWaitsForOwnTrigger pins the leftover-after-a-size-
// flush behavior: when a full batch flushes on the size trigger and a few rows
// remain, those rows must NOT flush merely because the first flush completed —
// they wait for their own size or timer trigger. maxWait is long here, so the
// leftover should stay buffered. (The old code flushed it on flush completion.)
func TestDispatchLoop_PartialBatchWaitsForOwnTrigger(t *testing.T) {
	t.Parallel()

	const (
		maxBatch = 3
		maxWait  = 30 * time.Second // long: the leftover row's timer never fires during this test
		total    = 4                // 3 → one full batch on the size trigger; 1 leftover
	)

	emb, err := mq.NewEmbedded(t.TempDir(), 8*1024*1024, testutil.NopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })

	// CH stub counts rows and sleeps briefly, so the 4th row is reliably buffered
	// before the first (3-row) flush completes — that's when the old code would
	// have wrongly flushed it.
	var (
		mu   sync.Mutex
		rows int
	)
	chSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		rows += bytes.Count(body, []byte("\n"))
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

	worker := &IngestWorker{
		js:         js,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		cache:      &testutil.MockCache{},
		logger:     testutil.NopLogger(),
		target: func() chconn.Target {
			return chconn.Target{URL: fmt.Sprintf("http://%s:%s", host, port), Username: "u", Password: "p", Database: "db"}
		},
		maxBatch: maxBatch,
		maxWait:  maxWait,
	}
	worker.wg.Add(1)
	go worker.dispatchLoop(ctx, cons)
	t.Cleanup(func() {
		cancel()
		worker.wg.Wait()
	})

	for i := range total {
		_, err = js.Publish(ctx, "ingest.tableX",
			makeEnvelope(t, "tableX", "", map[string]any{"id": i}))
		require.NoError(t, err)
	}

	// The first full batch (maxBatch rows) flushes on the size trigger.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return rows >= maxBatch
	}, 3*time.Second, 25*time.Millisecond, "first full batch should flush on the size trigger")

	// The leftover row must NOT flush just because that flush completed — with a
	// long maxWait it stays buffered until shutdown drains it.
	assert.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return rows > maxBatch
	}, 750*time.Millisecond, 50*time.Millisecond,
		"leftover row must wait for its own size/timer, not flush when the prior flush completes")
}

// v1Envelope is the pre-v2 wire shape: table_name/scope/received_timestamp plus
// a `data` object, and crucially no `format` field at all.
func v1Envelope(t *testing.T, table string, data map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(data)
	require.NoError(t, err)
	out, err := json.Marshal(map[string]any{
		"table_name":         table,
		"received_timestamp": "2026-01-01T00:00:00Z",
		"data":               json.RawMessage(raw),
	})
	require.NoError(t, err)
	return out
}

// TestParseMsg_PoisonEnvelope_ParkedOnDLQ: an envelope the worker can never
// insert — a pre-v2 message left in the queue across an upgrade, malformed
// JSON, or columns and a row that can't be paired — is preserved on the DLQ
// rather than dropped, so a missed pre-deploy drain costs an operator a replay
// rather than the rows themselves.
func TestParseMsg_PoisonEnvelope_ParkedOnDLQ(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
	}{
		{"pre-v2 envelope (no format)", v1Envelope(t, "events", map[string]any{"id": 1})},
		{"malformed json", []byte(`{"table_name":`)},
		{"columns without a row", []byte(`{"table_name":"events","format":"JSONCompactEachRow","columns":["id"]}`)},
		// The pairing is checked here rather than left to fail at ClickHouse, so
		// the DLQ entry names the real problem instead of an INSERT error.
		{"row shorter than its columns", []byte(`{"table_name":"events","format":"JSONCompactEachRow","columns":["id","v"],"row":[1]}`)},
		{"row longer than its columns", []byte(`{"table_name":"events","format":"JSONCompactEachRow","columns":["id"],"row":[1,2]}`)},
		{"row is not an array", []byte(`{"table_name":"events","format":"JSONCompactEachRow","columns":["id"],"row":{"id":1}}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w, js, _, _ := newTestWorker(&testutil.MockRoundTripper{})
			m := &testutil.MockJetStreamMsg{MsgSubject: "ingest.events", MsgData: tt.data}

			_, ok := w.parseMsg(context.Background(), m)
			require.False(t, ok, "poison must not be routed to a table loop")

			published := js.Published()
			require.Len(t, published, 1, "the row is parked, not dropped")
			assert.Equal(t, "dlq.events", published[0].Subject)
			assert.Equal(t, tt.data, published[0].Data, "the original bytes are preserved verbatim")
			assert.NotEmpty(t, published[0].Header.Get("X-DLQ-Error"))
			assert.True(t, m.DoubleAcked.Load(), "acked once parked, so NATS stops redelivering it")
		})
	}
}

// TestParseMsg_PoisonEnvelope_DLQDisabled_AckedAndDropped: with the DLQ off for
// the table there is nowhere to park it, and redelivering a message that can
// never insert would wedge the consumer behind it — so it is acked and dropped,
// loudly.
func TestParseMsg_PoisonEnvelope_DLQDisabled_AckedAndDropped(t *testing.T) {
	t.Parallel()
	w, js, _, _ := newTestWorker(&testutil.MockRoundTripper{})
	w.dlqEnabled = func(string) bool { return false }

	m := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    v1Envelope(t, "events", map[string]any{"id": 1}),
	}
	_, ok := w.parseMsg(context.Background(), m)
	require.False(t, ok)

	assert.Empty(t, js.Published(), "nothing reaches the DLQ while it is disabled")
	assert.True(t, m.DoubleAcked.Load(), "dropped rather than redelivered forever")
}

// TestParseMsg_PoisonEnvelope_DLQPublishFails_LeftUnacked: a DLQ outage is
// transient, so the message stays unacked and retries — losing a row to a
// temporarily unavailable DLQ would be worse than redelivering it.
func TestParseMsg_PoisonEnvelope_DLQPublishFails_LeftUnacked(t *testing.T) {
	t.Parallel()
	w, js, _, _ := newTestWorker(&testutil.MockRoundTripper{})
	js.PubErr = errors.New("jetstream unavailable")

	m := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    v1Envelope(t, "events", map[string]any{"id": 1}),
	}
	_, ok := w.parseMsg(context.Background(), m)
	require.False(t, ok)
	assert.False(t, m.DoubleAcked.Load(), "left unacked so it retries once the DLQ recovers")
}

// TestGroupByColumns splits a table's batch by column list — the whole reason
// it exists is that a schema change mid-stream must produce separate INSERTs
// rather than one statement whose column list fits only some of the rows.
func TestGroupByColumns(t *testing.T) {
	t.Parallel()
	msg := func(sig string, cols ...string) parsedMsg {
		return parsedMsg{colSig: sig, columns: cols, row: json.RawMessage(`[` + sig + `]`)}
	}
	a := columnSignature([]string{"id", "page"})
	b := columnSignature([]string{"id", "page", "country"})

	t.Run("one signature is one group", func(t *testing.T) {
		t.Parallel()
		got := groupByColumns([]parsedMsg{msg(a, "id", "page"), msg(a, "id", "page")})
		require.Len(t, got, 1)
		assert.Len(t, got[0], 2)
	})

	t.Run("interleaved signatures split, order preserved within each", func(t *testing.T) {
		t.Parallel()
		got := groupByColumns([]parsedMsg{
			{colSig: a, columns: []string{"id", "page"}, row: json.RawMessage(`[1]`)},
			{colSig: b, columns: []string{"id", "page", "country"}, row: json.RawMessage(`[2]`)},
			{colSig: a, columns: []string{"id", "page"}, row: json.RawMessage(`[3]`)},
		})
		require.Len(t, got, 2, "two column lists cannot share one INSERT")

		// Groups follow first appearance; rows keep arrival order within a group.
		assert.Equal(t, []string{"id", "page"}, got[0][0].columns)
		require.Len(t, got[0], 2)
		assert.JSONEq(t, `[1]`, string(got[0][0].row))
		assert.JSONEq(t, `[3]`, string(got[0][1].row))

		assert.Equal(t, []string{"id", "page", "country"}, got[1][0].columns)
		require.Len(t, got[1], 1)
		assert.JSONEq(t, `[2]`, string(got[1][0].row))
	})

	t.Run("empty batch yields no groups", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, groupByColumns(nil))
	})
}

// TestFlushTable_MixedColumnLists_TwoInserts drives the split end to end: a
// batch carrying two column lists must produce two INSERT statements, each
// naming its own columns, with every row acked.
func TestFlushTable_MixedColumnLists_TwoInserts(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var queries, bodies []string
	rt := &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			mu.Lock()
			queries = append(queries, req.URL.Query().Get("query"))
			bodies = append(bodies, string(body))
			mu.Unlock()
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
	w, _, _, wait := newTestWorker(rt)

	narrow := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    makeEnvelopeCols(t, "events", "", []string{"id"}, map[string]any{"id": 1}),
	}
	wide := &testutil.MockJetStreamMsg{
		MsgSubject: "ingest.events",
		MsgData:    makeEnvelopeCols(t, "events", "", []string{"id", "v"}, map[string]any{"id": 2, "v": "x"}),
	}
	w.flushTable(context.Background(), "events", parseAll(t, w, narrow, wide))
	wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, queries, 2, "one INSERT per column list")
	assert.Contains(t, queries, "INSERT INTO {target_table:Identifier} (`id`) FORMAT JSONCompactEachRow")
	assert.Contains(t, queries, "INSERT INTO {target_table:Identifier} (`id`, `v`) FORMAT JSONCompactEachRow")
	assert.ElementsMatch(t, []string{"[1]\n", "[2,\"x\"]\n"}, bodies)
	assert.True(t, narrow.DoubleAcked.Load())
	assert.True(t, wide.DoubleAcked.Load())
}
