package wavehouse

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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

func TestDoRequest_POSTWithBody(t *testing.T) {
	var gotBody map[string]string
	var gotCT string
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(200)
	}))

	err := doRequest(context.Background(), hctx, requestOptions{
		method: "POST",
		path:   "/v1/ingest",
		body:   map[string]string{"page": "/home"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if gotCT != "application/json" {
		t.Fatalf("want application/json, got %s", gotCT)
	}
	if gotBody["page"] != "/home" {
		t.Fatalf("want /home, got %v", gotBody)
	}
}

func TestDoRequest_RawBody(t *testing.T) {
	var gotBody string
	var gotCT string
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_ = json.NewEncoder(w).Encode(map[string]int{"total": 1})
	}))

	err := doRequest(context.Background(), hctx, requestOptions{
		method:      "POST",
		path:        "/v1/ingest",
		rawBody:     `{"page":"/a"}`,
		contentType: "application/x-ndjson",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if gotCT != "application/x-ndjson" {
		t.Fatalf("want ndjson content type, got %s", gotCT)
	}
	if gotBody != `{"page":"/a"}` {
		t.Fatalf("want raw body, got %s", gotBody)
	}
}

func TestDoRequest_AuthInjection(t *testing.T) {
	var gotAuth string
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	hctx.auth = StaticToken("my-token")

	err := doRequest(context.Background(), hctx, requestOptions{
		method: "GET",
		path:   "/v1/ops/schema",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer my-token" {
		t.Fatalf("want 'Bearer my-token', got %s", gotAuth)
	}
}

func TestDoRequest_4xxNotRetried(t *testing.T) {
	var count atomic.Int32
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	}))
	hctx.maxRetries = 2

	err := doRequest(context.Background(), hctx, requestOptions{
		method: "GET",
		path:   "/v1/ops/schema",
	}, nil)

	if !errIs(err, "HTTP_404") {
		t.Fatalf("want HTTP_404 error, got %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("4xx should not retry, got %d attempts", count.Load())
	}
}

func TestDoRequest_5xxRetried(t *testing.T) {
	var count atomic.Int32
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := count.Add(1)
		if n < 3 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "internal"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	}))
	hctx.maxRetries = 2

	var result map[string]string
	err := doRequest(context.Background(), hctx, requestOptions{
		method: "GET",
		path:   "/health",
	}, &result)
	if err != nil {
		t.Fatalf("want success after retries, got %v", err)
	}
	if count.Load() != 3 {
		t.Fatalf("want 3 attempts, got %d", count.Load())
	}
}

func TestDoRequest_AbortedOnCancel(t *testing.T) {
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Second)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := doRequest(ctx, hctx, requestOptions{
		method: "GET",
		path:   "/health",
	}, nil)

	if !errIs(err, "ABORTED") {
		t.Fatalf("want ABORTED, got %v", err)
	}
}

func TestDoRequest_EmptyResponse(t *testing.T) {
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))

	var result map[string]string
	err := doRequest(context.Background(), hctx, requestOptions{
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

func TestDoRequest_429RetriesWithRetryAfter(t *testing.T) {
	var calls atomic.Int64
	hctx := testCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	hctx.maxRetries = 1

	start := time.Now()
	var result map[string]string
	err := doRequest(context.Background(), hctx, requestOptions{method: "GET", path: "/x"}, &result)
	if err != nil {
		t.Fatalf("want success after 429 retry, got %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("want 2 attempts, got %d", got)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("Retry-After: 1 not honored — retried after only %v", elapsed)
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
			var got http.Header
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Clone()
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `[]`)
			}))
			defer srv.Close()

			client := NewClient(Config{
				BaseURL:    srv.URL,
				Auth:       tc.auth,
				HTTPClient: srv.Client(),
				Options:    &ClientOptions{Headers: tc.configured},
			})
			if _, err := client.Schema.List(context.Background()); err != nil {
				t.Fatalf("schema list: %v", err)
			}
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
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer srv.Close()

	headers := map[string]string{"X-Tenant-Id": "acme"}
	client := NewClient(Config{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
		Options:    &ClientOptions{Headers: headers},
	})
	headers["X-Tenant-Id"] = "attacker"
	delete(headers, "X-Tenant-Id")

	if _, err := client.Schema.List(context.Background()); err != nil {
		t.Fatalf("schema list: %v", err)
	}
	if v := got.Get("X-Tenant-Id"); v != "acme" {
		t.Fatalf("want the value captured at construction, got %q", v)
	}
}
