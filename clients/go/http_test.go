package wavehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

// errIs checks if err wraps a *Error with the given code.
func errIs(err error, code string) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == code
	}
	return false
}

// testCtx and queryTestCtx are the two entry points every test in this package
// uses to reach a throwaway server: the bare transport context, and a full
// Client wired to the same server.
func testCtx(t *testing.T, handler http.Handler) httpContext {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return httpContext{
		baseURL:    srv.URL,
		maxRetries: 0,
		httpClient: srv.Client(),
	}
}

func queryTestCtx(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewClient(Config{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
		Options:    &ClientOptions{MaxRetries: 0},
	})
}

// recordedRequest is one request as the test server saw it — everything a test
// might assert on, copied on the server goroutine.
type recordedRequest struct {
	method string
	path   string
	query  url.Values
	header http.Header
	body   []byte
}

// jsonBody decodes the recorded body as a JSON object. UseNumber keeps int64
// cursor values exact on the assertion side too.
func (r recordedRequest) jsonBody(t *testing.T) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(r.body))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode request body %q: %v", r.body, err)
	}
	return m
}

// recordRequests wraps handler so every request it serves lands on the
// returned buffered channel — a channel, not a shared variable, so -race sees
// the edge between the server goroutine and the test. handler still reads an
// intact Body.
func recordRequests(t *testing.T, handler http.Handler) (http.Handler, <-chan recordedRequest) {
	t.Helper()
	seen := make(chan recordedRequest, 16)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		seen <- recordedRequest{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.Query(),
			header: r.Header.Clone(),
			body:   raw,
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		handler.ServeHTTP(w, r)
	}), seen
}

func recordingCtx(t *testing.T, handler http.Handler) (httpContext, <-chan recordedRequest) {
	t.Helper()
	h, seen := recordRequests(t, handler)
	return testCtx(t, h), seen
}

func recordingClient(t *testing.T, handler http.Handler) (*Client, <-chan recordedRequest) {
	t.Helper()
	h, seen := recordRequests(t, handler)
	return queryTestCtx(t, h), seen
}

// ok200 answers with a bare 200; jsonArray with an empty JSON array — the
// minimal valid reply for a list endpoint.
var (
	ok200 = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	jsonArray = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
)

// jsonResponse answers any request with body encoded as JSON.
func jsonResponse(body any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(body)
	}
}

func TestDoRequest_SuccessfulGET(t *testing.T) {
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))

	var result map[string]string
	err := doRequest(context.Background(), hctx, requestOptions{
		method: "GET",
		path:   "/health",
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result["status"] != "ok" {
		t.Fatalf("want ok, got %v", result)
	}
}

// TestDoRequest_RequestShape: what the transport puts on the wire for each
// body/auth combination.
func TestDoRequest_RequestShape(t *testing.T) {
	tests := []struct {
		name     string
		auth     func(context.Context) (string, error)
		opts     requestOptions
		wantCT   string
		wantBody string
		wantAuth string
	}{
		{
			name:     "a struct body is marshaled as JSON",
			opts:     requestOptions{method: "POST", path: "/v1/ingest", body: map[string]string{"page": "/home"}},
			wantCT:   "application/json",
			wantBody: `{"page":"/home"}`,
		},
		{
			name:     "a raw body keeps the caller's content type",
			opts:     requestOptions{method: "POST", path: "/v1/ingest", rawBody: `{"page":"/a"}`, contentType: "application/x-ndjson"},
			wantCT:   "application/x-ndjson",
			wantBody: `{"page":"/a"}`,
		},
		{
			name:     "the auth provider's token becomes a Bearer header",
			auth:     StaticToken("my-token"),
			opts:     requestOptions{method: "GET", path: "/v1/ops/schema"},
			wantCT:   "application/json",
			wantAuth: "Bearer my-token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hctx, reqs := recordingCtx(t, ok200)
			hctx.auth = tt.auth

			if err := doRequest(context.Background(), hctx, tt.opts, nil); err != nil {
				t.Fatal(err)
			}
			got := <-reqs
			if got.method != tt.opts.method || got.path != tt.opts.path {
				t.Fatalf("want %s %s, got %s %s", tt.opts.method, tt.opts.path, got.method, got.path)
			}
			if ct := got.header.Get("Content-Type"); ct != tt.wantCT {
				t.Fatalf("want Content-Type %q, got %q", tt.wantCT, ct)
			}
			if string(got.body) != tt.wantBody {
				t.Fatalf("want body %q, got %q", tt.wantBody, got.body)
			}
			if auth := got.header.Get("Authorization"); auth != tt.wantAuth {
				t.Fatalf("want Authorization %q, got %q", tt.wantAuth, auth)
			}
		})
	}
}

// TestDoRequest_RetryPolicy: which statuses are retried, how many attempts
// they take, and whether Retry-After is honored.
func TestDoRequest_RetryPolicy(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		retryAfter     string
		maxRetries     int
		wantAttempts   int32
		wantErrCode    string
		wantMinElapsed time.Duration
	}{
		{name: "4xx is not retried", status: 404, maxRetries: 2, wantAttempts: 1, wantErrCode: "HTTP_404"},
		{name: "5xx retries until it succeeds", status: 500, maxRetries: 2, wantAttempts: 3},
		{
			name: "429 waits out Retry-After", status: 429, retryAfter: "1",
			maxRetries: 1, wantAttempts: 2, wantMinElapsed: 900 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) >= tt.wantAttempts && tt.wantErrCode == "" {
					_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if tt.retryAfter != "" {
					w.Header().Set("Retry-After", tt.retryAfter)
				}
				w.WriteHeader(tt.status)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "boom"})
			}))
			hctx.maxRetries = tt.maxRetries

			start := time.Now()
			var result map[string]string
			err := doRequest(context.Background(), hctx, requestOptions{method: "GET", path: "/health"}, &result)
			if tt.wantErrCode == "" && err != nil {
				t.Fatalf("want success after retries, got %v", err)
			}
			if tt.wantErrCode != "" && !errIs(err, tt.wantErrCode) {
				t.Fatalf("want %s error, got %v", tt.wantErrCode, err)
			}
			if got := calls.Load(); got != tt.wantAttempts {
				t.Fatalf("want %d attempts, got %d", tt.wantAttempts, got)
			}
			if elapsed := time.Since(start); elapsed < tt.wantMinElapsed {
				t.Fatalf("Retry-After not honored — retried after only %v", elapsed)
			}
		})
	}
}

func TestDoRequest_AbortedOnCancel(t *testing.T) {
	hctx := testCtx(t, http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Second)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := doRequest(ctx, hctx, requestOptions{method: "GET", path: "/health"}, nil)
	if !errIs(err, "ABORTED") {
		t.Fatalf("want ABORTED, got %v", err)
	}
}

func TestDoRequest_EmptyResponse(t *testing.T) {
	var result map[string]string
	err := doRequest(context.Background(), testCtx(t, ok200), requestOptions{
		method: "POST",
		path:   "/v1/ops/schema/refresh",
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
	// Empty body = no decode, result stays zero value.
	if result != nil {
		t.Fatalf("want nil, got %v", result)
	}
}

func TestBackoff(t *testing.T) {
	tests := []struct {
		name    string
		attempt int
		base    time.Duration
	}{
		{"Attempt0", 0, 1 * time.Second},
		{"Attempt1", 1, 2 * time.Second},
		{"Attempt2", 2, 4 * time.Second},
		{"Attempt3", 3, 8 * time.Second},
		{"CappedAt30s", 10, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := backoff(tt.attempt)
			if tt.name == "CappedAt30s" {
				// The cap is applied *after* jitter, so this is exact — a ±20%
				// window here would also accept capping before the jitter,
				// which lets the documented 30s max drift to 36s.
				if got != 30*time.Second {
					t.Errorf("backoff(%d) = %v, want exactly 30s", tt.attempt, got)
				}
				return
			}
			// backoff applies ±20% jitter around the exponential base.
			lo := time.Duration(float64(tt.base) * 0.8)
			hi := time.Duration(float64(tt.base) * 1.2)
			if got < lo || got > hi {
				t.Errorf("backoff(%d) = %v, want within [%v, %v]", tt.attempt, got, lo, hi)
			}
		})
	}
}

func TestRetryAfterDelay(t *testing.T) {
	tests := []struct {
		name string
		ra   string
		want time.Duration
	}{
		{"DeltaSeconds", "5", 5 * time.Second},
		{"ClampedToMax", "3600", maxRetryAfter},
		// time.Duration(secs) * time.Second wraps negative past ~9.2e9s; an
		// unguarded min() then picks the negative and retries instantly.
		{"OverflowClamped", "10000000000", maxRetryAfter},
		{"MaxIntClamped", "9223372036854775807", maxRetryAfter},
		{"HTTPDateFuture", time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat), 0}, // range-checked below
		{"Garbage", "not-a-delay", 0},                                                         // range-checked below
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retryAfterDelay(tt.ra, 0)
			switch tt.name {
			case "HTTPDateFuture":
				// Lower bound well clear of the backoff(0) fallback (~1s), so a
				// broken HTTP-date branch can't pass by falling through to it.
				if got < 5*time.Second || got > 10*time.Second {
					t.Fatalf("want ~10s, got %v", got)
				}
			case "Garbage":
				// Falls back to backoff(0): 1s ±20% jitter.
				if got < 800*time.Millisecond || got > 1200*time.Millisecond {
					t.Fatalf("want backoff(0) fallback, got %v", got)
				}
			default:
				if got != tt.want {
					t.Fatalf("want %v, got %v", tt.want, got)
				}
			}
		})
	}
}

// A BaseURL carrying a path prefix must survive on both transports — the bug
// #428 fixed in the TS client, which Go avoids by concatenating rather than
// resolving. Guards against a future switch to url.JoinPath/ResolveReference.
func TestBaseURLPathPrefixIsPreserved(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/warehouse/v1/query", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	srv := httptest.NewServer(mux) // anything off-prefix 404s
	t.Cleanup(srv.Close)

	for _, base := range []string{srv.URL + "/api/warehouse", srv.URL + "/api/warehouse/"} {
		client := NewClient(Config{BaseURL: base, Options: &ClientOptions{}, HTTPClient: srv.Client()})

		var result map[string]string
		if err := doRequest(context.Background(), client.ctx, requestOptions{
			method: "POST",
			path:   "/v1/query",
		}, &result); err != nil {
			t.Fatalf("base %q: %v", base, err)
		}
		if result["status"] != "ok" {
			t.Fatalf("base %q: want ok, got %v", base, result)
		}
	}
}

// headerCaptureClient is a Client with configured headers and auth pointed at
// a server that records what it received.
func headerCaptureClient(t *testing.T, opts *ClientOptions, auth func(context.Context) (string, error)) (*Client, <-chan recordedRequest) {
	t.Helper()
	h, reqs := recordRequests(t, jsonArray)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return NewClient(Config{BaseURL: srv.URL, Auth: auth, HTTPClient: srv.Client(), Options: opts}), reqs
}

// TestConfiguredHeadersOnRESTRequests: ClientOptions.Headers apply to every
// REST call, are matched case-insensitively, and always lose to the SDK's own
// headers rather than appending alongside them.
func TestConfiguredHeadersOnRESTRequests(t *testing.T) {
	tests := []struct {
		name       string
		configured map[string]string
		auth       func(context.Context) (string, error)
		header     string
		want       string
	}{
		{
			name:       "custom header is forwarded",
			configured: map[string]string{"X-Operator-Key": "op-secret"},
			header:     "X-Operator-Key",
			want:       "op-secret",
		},
		{
			name:       "name matching is case-insensitive",
			configured: map[string]string{"x-tenant-id": "acme"},
			header:     "X-Tenant-Id",
			want:       "acme",
		},
		{
			name:       "SDK Accept outranks a configured one",
			configured: map[string]string{"Accept": "text/plain"},
			header:     "Accept",
			want:       "application/json",
		},
		{
			name:       "SDK Authorization outranks a configured one",
			configured: map[string]string{"Authorization": "Bearer configured"},
			auth:       StaticToken("real-token"),
			header:     "Authorization",
			want:       "Bearer real-token",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, reqs := headerCaptureClient(t, &ClientOptions{Headers: tc.configured}, tc.auth)
			if _, err := client.Schema.List(context.Background()); err != nil {
				t.Fatalf("schema list: %v", err)
			}
			got := (<-reqs).header
			if v := got.Values(tc.header); len(v) != 1 {
				t.Fatalf("want exactly one %s header, got %v", tc.header, v)
			}
			if v := got.Get(tc.header); v != tc.want {
				t.Fatalf("want %s: %q, got %q", tc.header, tc.want, v)
			}
		})
	}
}

// TestConfiguredHeadersAreCopied: mutating the caller's map after NewClient
// must not change what later requests send.
func TestConfiguredHeadersAreCopied(t *testing.T) {
	headers := map[string]string{"X-Tenant-Id": "acme"}
	client, reqs := headerCaptureClient(t, &ClientOptions{Headers: headers}, nil)
	headers["X-Tenant-Id"] = "attacker"
	delete(headers, "X-Tenant-Id")

	if _, err := client.Schema.List(context.Background()); err != nil {
		t.Fatalf("schema list: %v", err)
	}
	if v := (<-reqs).header.Get("X-Tenant-Id"); v != "acme" {
		t.Fatalf("want the value captured at construction, got %q", v)
	}
}
