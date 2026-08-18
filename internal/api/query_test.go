package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// safeHandle calls a handler and recovers from panics.
// Used when tests verify validation logic but pass nil for dependencies,
// which would panic once the handler reaches them. Shared with
// structured_query_test.go and pipes_test.go.
func safeHandle(handler http.HandlerFunc, w *httptest.ResponseRecorder, r *http.Request) {
	defer func() { _ = recover() }()
	handler(w, r)
}

func newProxyHandler(t *testing.T, fakeCH http.Handler) *QueryHandler {
	t.Helper()
	srv := httptest.NewServer(fakeCH)
	t.Cleanup(srv.Close)
	return NewQueryHandler(srv.URL, "default", "secret", "default", time.Second*time.Duration(30))
}

func postQuery(h *QueryHandler, body []byte) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ops/query", bytes.NewReader(body))
	h.Handle(w, r)
	return w
}

// assertSecurityHeaders confirms Cache-Control: no-store and
// X-Content-Type-Options: nosniff are set. Both are set unconditionally at
// handler entry (see query.go:Handle), so every response — 200, 4xx, 5xx,
// 413 — must carry them. A future refactor that hoists either into a
// branch-specific spot (tempting cleanup) would silently regress without
// this check.
func assertSecurityHeaders(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	assert.Equal(t, "no-store", w.Header().Get("Cache-Control"),
		"Cache-Control: no-store must be set on every response, not only the 200 path")
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"),
		"X-Content-Type-Options: nosniff must be set on every response")
}

// TestQueryHandler_RejectsMalformedRequests pins every contract-rejection
// path on /v1/ops/query: the handler must surface a 400 with a JSON
// error envelope, the standard security headers, AND a specific error
// message that lets clients tell the failure modes apart. None of these
// cases involve an upstream call — the handler should reject before the
// proxy.
//
// Each case documents WHY it matters at the test site rather than in a
// separate top-level test, so future contract additions drop in as a
// new row instead of a new function.
func TestQueryHandler_RejectsMalformedRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			// Empty sql field shouldn't reach ClickHouse.
			name:    "missing sql",
			body:    `{"sql":""}`,
			wantErr: "missing sql",
		},
		{
			// Malformed JSON at the top level.
			name:    "invalid json",
			body:    `{bad}`,
			wantErr: "invalid json",
		},
		{
			// The pre-proxy /v1/admin/query handler accepted a `params` array
			// bound to positional `?` placeholders. The new HTTP proxy
			// dropped that and DisallowUnknownFields rejects it loudly
			// — silently ignoring the field would let old clients run
			// for months thinking their bound params took effect.
			name:    "unknown field — dropped `params` array",
			body:    `{"sql":"SELECT 1","params":[1,2,3]}`,
			wantErr: "invalid json",
		},
		{
			// A buggy client that double-encodes
			// (`{"sql":"a"}{"sql":"b"}`) would otherwise have its
			// second envelope dropped on the floor — invisible to the
			// caller, hard to diagnose. The trailing-Decode check
			// catches it.
			name:    "trailing JSON tokens after first envelope",
			body:    `{"sql":"SELECT 1"}{"sql":"SELECT 2"}`,
			wantErr: "invalid json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := NewQueryHandler("http://unused.invalid", "", "", "", time.Second*time.Duration(30))
			w := httptest.NewRecorder()
			r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ops/query", bytes.NewReader([]byte(tt.body)))
			h.Handle(w, r)

			testutil.AssertJSONContains(t, w, http.StatusBadRequest, map[string]any{"error": tt.wantErr})
			testutil.AssertJSONErrorResponse(t, w)
			assertSecurityHeaders(t, w)
		})
	}
}

// TestQueryHandler_NilHTTPClientReturnsError pins the defensive nil-check
// CR flagged: a zero-value QueryHandler{} (constructed without
// NewQueryHandler) would panic when Handle reached HTTPClient.Do. The
// router-only routing tests use that shape to verify the role gate
// fires BEFORE the handler runs — but a future routing test that
// accidentally reaches the handler should get a clean 500, not a panic
// that surfaces via the chi recoverer.
func TestQueryHandler_NilHTTPClientReturnsError(t *testing.T) {
	t.Parallel()
	h := &QueryHandler{Endpoint: "http://unused.invalid"} // no HTTPClient
	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})
	w := postQuery(h, body)

	testutil.AssertJSONContains(t, w, http.StatusInternalServerError, map[string]any{"error": "query handler not configured: HTTPClient is nil"})
	testutil.AssertJSONErrorResponse(t, w)
	assertSecurityHeaders(t, w)
}

// TestQueryHandler_ForwardsSQLToClickHouse pins the proxy contract:
//   - Request SQL is sent as the HTTP body verbatim.
//   - default_format=JSON and date_time_output_format=iso are set so the
//     response envelope is predictable.
//   - Database from constructor lands in the query string.
//   - X-ClickHouse-User / X-ClickHouse-Key headers carry the credentials.
//
// A regression that swapped any of these would either change the URL the
// fake ClickHouse sees or the body it receives, and the sub-assertions
// below would fail loudly.
func TestQueryHandler_ForwardsSQLToClickHouse(t *testing.T) {
	t.Parallel()

	var (
		gotMethod  string
		gotPath    string
		gotQuery   string
		gotBody    string
		gotUser    string
		gotKey     string
		gotContent string
	)
	fake := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotUser = r.Header.Get("X-ClickHouse-User")
		gotKey = r.Header.Get("X-ClickHouse-Key")
		gotContent = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"meta":[{"name":"n","type":"UInt64"}],"data":[{"n":1}],"rows":1}`))
	})
	h := newProxyHandler(t, fake)

	const sql = "SELECT count() AS n FROM clicks"
	body, _ := json.Marshal(queryRequest{SQL: sql})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, http.MethodPost, gotMethod, "ClickHouse HTTP requires POST for arbitrary SQL")
	assert.Equal(t, "/", gotPath)
	assert.Contains(t, gotQuery, "default_format=JSON")
	assert.Contains(t, gotQuery, "date_time_output_format=iso")
	assert.Contains(t, gotQuery, "database=default")
	assert.Equal(t, sql, gotBody, "SQL body must be forwarded verbatim, no parsing")
	assert.Equal(t, "default", gotUser, "username must be sent via X-ClickHouse-User")
	assert.Equal(t, "secret", gotKey, "password must be sent via X-ClickHouse-Key")
	assert.Equal(t, "text/plain; charset=utf-8", gotContent)
}

// TestQueryHandler_ExtractsDataArray confirms the proxy strips ClickHouse's
// {meta, data, rows, statistics} envelope and returns just the data array
// — preserving the [{...}, {...}] shape callers expected from the previous
// (pre-proxy) handler. If we stopped extracting `data`, clients would
// suddenly see an object with `meta`/`rows`/`statistics` fields and a
// `.length` check would silently misbehave.
func TestQueryHandler_ExtractsDataArray(t *testing.T) {
	t.Parallel()
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"meta":[{"name":"id","type":"String"},{"name":"v","type":"UInt64"}],
			"data":[{"id":"a","v":1},{"id":"b","v":2}],
			"rows":2,
			"statistics":{"elapsed":0.001}
		}`))
	})
	h := newProxyHandler(t, fake)

	body, _ := json.Marshal(queryRequest{SQL: "SELECT id, v FROM t"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code)
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rows))
	assert.Equal(t, []map[string]any{
		{"id": "a", "v": float64(1)},
		{"id": "b", "v": float64(2)},
	}, rows)
}

// TestQueryHandler_EmptyBodyMutationReturnsArray pins the "mutation → []"
// contract. ClickHouse returns an empty body for TRUNCATE/INSERT/DELETE/
// etc. — the proxy must turn that into `[]` so the response shape stays
// "always an array." Without this, a successful TRUNCATE would return a
// zero-byte body and clients doing `result.length` would crash on null.
func TestQueryHandler_EmptyBodyMutationReturnsArray(t *testing.T) {
	t.Parallel()
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// ClickHouse returns 200 + empty body for mutations regardless of
		// default_format. No Content-Type either.
		w.WriteHeader(http.StatusOK)
	})
	h := newProxyHandler(t, fake)

	body, _ := json.Marshal(queryRequest{SQL: "TRUNCATE TABLE clicks"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, "[]", w.Body.String())
}

// TestQueryHandler_ForwardsCHError covers the "ClickHouse rejected the
// statement" path. ClickHouse returns 4xx/5xx with a plain-text error
// message in the body (e.g. "Code: 60. DB::Exception: Unknown table x").
// The proxy must surface that message AND classify the status: 4xx →
// 400 (caller-fault, the SQL was bad), 5xx → 502 (gateway-fault, the
// upstream had a problem). Admin tooling that retries on 5xx-but-not-4xx
// depends on this distinction.
func TestQueryHandler_ForwardsCHError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		upstreamStatus int
		upstreamBody   string
		wantStatus     int
		wantMsg        string
	}{
		{
			name:           "caller fault — bad SQL → 400",
			upstreamStatus: http.StatusBadRequest,
			upstreamBody:   "Code: 60. DB::Exception: Table default.no_such_table doesn't exist.\n",
			wantStatus:     http.StatusBadRequest,
			wantMsg:        "Table default.no_such_table doesn't exist",
		},
		{
			name:           "caller fault — type error → 400",
			upstreamStatus: http.StatusUnprocessableEntity,
			upstreamBody:   "Code: 53. DB::Exception: Type mismatch.\n",
			wantStatus:     http.StatusBadRequest,
			wantMsg:        "Type mismatch",
		},
		{
			name:           "upstream fault — ClickHouse 500 → 502",
			upstreamStatus: http.StatusInternalServerError,
			upstreamBody:   "Code: 999. DB::Exception: Internal error.\n",
			wantStatus:     http.StatusBadGateway,
			wantMsg:        "Internal error",
		},
		{
			name:           "upstream fault — ClickHouse 503 → 502",
			upstreamStatus: http.StatusServiceUnavailable,
			upstreamBody:   "Server is overloaded.\n",
			wantStatus:     http.StatusBadGateway,
			wantMsg:        "Server is overloaded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
				w.WriteHeader(tt.upstreamStatus)
				_, _ = w.Write([]byte(tt.upstreamBody))
			})
			h := newProxyHandler(t, fake)

			body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM no_such_table"})
			w := postQuery(h, body)

			require.Equal(t, tt.wantStatus, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantMsg, "ClickHouse's error message must reach the admin verbatim")
			assertSecurityHeaders(t, w)
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

// TestQueryHandler_SetsSecurityHeadersOn200 pins Cache-Control: no-store
// AND X-Content-Type-Options: nosniff on the success path. Raw SQL is
// admin-only and admins call it for read-your-writes verification, so any
// downstream HTTP cache (CDN, browser, corp proxy) caching a SELECT would
// re-introduce the staleness class of bug the in-process cache strip just
// removed. nosniff defangs the FORMAT-pass-through branch where ClickHouse
// could return a renderable MIME like text/html.
func TestQueryHandler_SetsSecurityHeadersOn200(t *testing.T) {
	t.Parallel()
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"meta":[],"data":[],"rows":0}`))
	})
	h := newProxyHandler(t, fake)

	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code)
	assertSecurityHeaders(t, w)
}

// TestQueryHandler_NoAuthHeadersWhenBlank confirms the proxy doesn't emit
// empty X-ClickHouse-User / X-ClickHouse-Key headers when the constructor
// got blank credentials (dev mode / unauth ClickHouse). Sending blank
// headers would let an attacker who can MITM the upstream link tell that
// the call is unauthenticated, and some ClickHouse setups validate that
// User is present and reject blank-user requests with a confusing 4xx.
func TestQueryHandler_NoAuthHeadersWhenBlank(t *testing.T) {
	t.Parallel()
	var gotUser, gotKey string
	var hasUserHeader, hasKeyHeader bool
	fake := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hasUserHeader = r.Header["X-Clickhouse-User"]
		_, hasKeyHeader = r.Header["X-Clickhouse-Key"]
		gotUser = r.Header.Get("X-ClickHouse-User")
		gotKey = r.Header.Get("X-ClickHouse-Key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"meta":[],"data":[],"rows":0}`))
	})
	srv := httptest.NewServer(fake)
	defer srv.Close()
	h := NewQueryHandler(srv.URL, "", "", "", time.Second*time.Duration(30))

	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code)
	assert.False(t, hasUserHeader, "no X-ClickHouse-User header should be set when username is blank, got %q", gotUser)
	assert.False(t, hasKeyHeader, "no X-ClickHouse-Key header should be set when password is blank, got %q", gotKey)
}

// TestQueryHandler_ForwardsRawBodyAndContentType covers the
// FORMAT-override branch: ClickHouse returned 200 + a body that isn't
// the {meta, data, rows} envelope. Verified empirically against a real
// ClickHouse: `SELECT 1 FORMAT CSV` with default_format=JSON on the URL
// returns raw `1\n` + Content-Type: text/csv (inline FORMAT wins). The
// proxy must (a) forward the body verbatim, and (b) pass through the
// upstream Content-Type — not stamp application/json on it, or the SDK's
// `await response.json()` crashes on a non-JSON payload.
func TestQueryHandler_ForwardsRawBodyAndContentType(t *testing.T) {
	t.Parallel()
	const csvBody = "1\n2\n3\n"
	const upstreamCT = "text/csv; charset=UTF-8; header=absent"
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", upstreamCT)
		_, _ = w.Write([]byte(csvBody))
	})
	h := newProxyHandler(t, fake)

	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1 FORMAT CSV"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, csvBody, w.Body.String(), "non-JSON 200 body must be forwarded raw, not dropped")
	assert.Equal(t, upstreamCT, w.Header().Get("Content-Type"),
		"upstream Content-Type must pass through — labelling a CSV body as application/json would break SDK consumers")
	assertSecurityHeaders(t, w)
}

// TestQueryHandler_ResponseSizeCap pins the memory-safety cap. The proxy
// buffers the upstream response in memory (no streaming today), so a
// runaway SELECT could OOM the API server. We bound that at the
// configured cap and surface a clear 502 on overflow rather than silently
// truncating into a parse error.
//
// The test overrides h.maxResponseBytes to a tiny value so we don't have
// to allocate 64 MiB+1 per run (which on a parallel suite is real RAM
// pressure / flake risk on loaded CI runners).
func TestQueryHandler_ResponseSizeCap(t *testing.T) {
	t.Parallel()

	const testCap = 1024
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		oversized := bytes.Repeat([]byte{'a'}, testCap+1)
		_, _ = w.Write(oversized)
	})
	h := newProxyHandler(t, fake)
	h.maxResponseBytes = testCap

	body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM huge_table"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusBadGateway, w.Code, "oversized response must 502, not OOM")
	assert.Contains(t, w.Body.String(), "exceeded")
	testutil.AssertJSONErrorResponse(t, w)
	assertSecurityHeaders(t, w)
}

// TestQueryHandler_RequestBodyCap pins the inbound 16 MiB body cap. The
// handler wraps r.Body in http.MaxBytesReader before the JSON decode and
// returns 413 with a "request body exceeded N bytes" message — distinct
// from "invalid json", so admin scripts can tell "you sent garbage" from
// "you sent too much".
//
// Like the response-cap test, this overrides h.maxRequestBytes to a tiny
// value so we don't allocate 16 MiB per run.
func TestQueryHandler_RequestBodyCap(t *testing.T) {
	t.Parallel()

	const testCap = 64
	h := NewQueryHandler("http://unused.invalid", "", "", "", time.Second*time.Duration(30))
	h.maxRequestBytes = testCap

	body, _ := json.Marshal(queryRequest{SQL: strings.Repeat("x", 200)})
	require.Greater(t, len(body), testCap, "test body must exceed the cap")
	w := postQuery(h, body)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, "oversized request must 413, not 400")
	assert.Contains(t, w.Body.String(), "request body exceeded")
	testutil.AssertJSONErrorResponse(t, w)
	assertSecurityHeaders(t, w)
}

// TestQueryHandler_ContextCancelBoundedAt30s is a sanity check that the
// 30s context bound is in fact applied — if a future refactor accidentally
// dropped the WithTimeout call, an upstream-stuck ClickHouse would block
// the handler forever and tests like this one would time out at the Go
// test timeout instead of failing cleanly.
//
// We don't actually wait 30s — we cancel from the outside and confirm the
// handler propagates the cancellation, which proves the request was made
// with a derivable context (the only way the cancel can reach it).
func TestQueryHandler_ContextCancelPropagates(t *testing.T) {
	t.Parallel()
	upstreamReached := make(chan struct{})
	allowReturn := make(chan struct{})
	fake := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(upstreamReached)
		select {
		case <-r.Context().Done():
			// upstream saw the cancellation — exactly what we want
			return
		case <-allowReturn:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(fake)
	defer srv.Close()
	defer close(allowReturn)

	h := NewQueryHandler(srv.URL, "", "", "", time.Second*time.Duration(30))
	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})

	w := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/ops/query", bytes.NewReader(body))

	done := make(chan struct{})
	go func() {
		h.Handle(w, r)
		close(done)
	}()

	select {
	case <-upstreamReached:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream was not reached before cancellation — handler did not propagate request context to the upstream call")
	}
	cancel()
	// Symmetric bound on the post-cancel wait: if the handler ever
	// deadlocks instead of returning when the request context cancels
	// (e.g. a future refactor drops the context.WithTimeout in
	// query.go), this fails fast with a clear message instead of
	// hanging until the suite timeout.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after request cancellation — context propagation likely broken")
	}

	// Either 502 (proxy reported the upstream cancellation as a transport
	// failure) or 500 (cancellation surfaced from the read path) is fine —
	// what we're pinning is that the handler returned promptly after
	// cancel(), proving the request context made it to the upstream call.
	assert.NotEqual(t, http.StatusOK, w.Code, "cancelled request must not return 200")
}

// TODO: new test for very short max query duration to test
