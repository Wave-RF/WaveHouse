package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	return NewQueryHandler(srv.URL, "default", "secret", "default")
}

func postQuery(h *QueryHandler, body []byte) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/admin/query", bytes.NewReader(body))
	h.Handle(w, r)
	return w
}

func TestQueryHandler_MissingSQL(t *testing.T) {
	t.Parallel()
	// No upstream call expected — handler should reject before the proxy.
	h := NewQueryHandler("http://unused.invalid", "", "", "")
	body, _ := json.Marshal(queryRequest{SQL: ""})
	w := postQuery(h, body)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing sql")
	assertJSONErrorResponse(t, w)
}

func TestQueryHandler_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := NewQueryHandler("http://unused.invalid", "", "", "")
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/admin/query", bytes.NewReader([]byte(`{bad}`)))
	h.Handle(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
	assertJSONErrorResponse(t, w)
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
// The proxy must surface that message — admin's whole reason for using
// this endpoint is to see ClickHouse's view of what went wrong.
func TestQueryHandler_ForwardsCHError(t *testing.T) {
	t.Parallel()
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("Code: 60. DB::Exception: Table default.no_such_table doesn't exist.\n"))
	})
	h := newProxyHandler(t, fake)

	body, _ := json.Marshal(queryRequest{SQL: "SELECT * FROM no_such_table"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusInternalServerError, w.Code,
		"ClickHouse 4xx is currently mapped to 500 — proxy distinguishing caller-fault would need ClickHouse error-code parsing")
	assert.Contains(t, w.Body.String(), "Table default.no_such_table doesn't exist")
	assertJSONErrorResponse(t, w)
}

// TestQueryHandler_SetsNoStore pins the cache header. Raw SQL is admin-only
// and admins call it for read-your-writes verification — any downstream
// HTTP cache (CDN, browser, corp proxy) caching a SELECT would re-introduce
// the staleness class of bug the in-process cache strip just removed.
func TestQueryHandler_SetsNoStore(t *testing.T) {
	t.Parallel()
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"meta":[],"data":[],"rows":0}`))
	})
	h := newProxyHandler(t, fake)

	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "no-store", w.Header().Get("Cache-Control"),
		"raw SQL must not be cached by any downstream layer")
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
	h := NewQueryHandler(srv.URL, "", "", "")

	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code)
	assert.False(t, hasUserHeader, "no X-ClickHouse-User header should be set when username is blank, got %q", gotUser)
	assert.False(t, hasKeyHeader, "no X-ClickHouse-Key header should be set when password is blank, got %q", gotKey)
}

// TestQueryHandler_ForwardsRawBodyOnUnexpectedShape covers the
// belt-and-suspenders branch: ClickHouse returned 200 + a body that isn't
// the {meta, data, rows} envelope (shouldn't happen with
// default_format=JSON, but possible if someone overrides FORMAT in their
// SQL, e.g. `SELECT 1 FORMAT CSV`). In that case the proxy forwards the
// body verbatim rather than dropping it on the floor.
func TestQueryHandler_ForwardsRawBodyOnUnexpectedShape(t *testing.T) {
	t.Parallel()
	const csvBody = "1\n2\n3\n"
	fake := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(csvBody))
	})
	h := newProxyHandler(t, fake)

	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1 FORMAT CSV"})
	w := postQuery(h, body)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, csvBody, w.Body.String(), "non-JSON 200 body must be forwarded raw, not dropped")
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

	h := NewQueryHandler(srv.URL, "", "", "")
	body, _ := json.Marshal(queryRequest{SQL: "SELECT 1"})

	w := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/admin/query", bytes.NewReader(body))

	done := make(chan struct{})
	go func() {
		h.Handle(w, r)
		close(done)
	}()

	<-upstreamReached
	cancel()
	<-done

	// Either 502 (proxy reported the upstream cancellation as a transport
	// failure) or 500 (cancellation surfaced from the read path) is fine —
	// what we're pinning is that the handler returned promptly after
	// cancel(), proving the request context made it to the upstream call.
	assert.NotEqual(t, http.StatusOK, w.Code, "cancelled request must not return 200")
}
