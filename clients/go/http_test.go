package wavehouse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

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
		raw := make([]byte, 1024)
		n, _ := r.Body.Read(raw)
		gotBody = string(raw[:n])
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
		path:   "/v1/schema",
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
		path:   "/v1/schema",
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
		path:   "/v1/schema/refresh",
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
			// backoff applies ±20% jitter around the exponential base.
			lo := time.Duration(float64(tt.base) * 0.8)
			hi := time.Duration(float64(tt.base) * 1.2)
			got := backoff(tt.attempt)
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
		{"HTTPDateFuture", time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat), 0}, // range-checked below
		{"Garbage", "not-a-delay", 0},                                                         // range-checked below
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retryAfterDelay(tt.ra, 0)
			switch tt.name {
			case "HTTPDateFuture":
				if got <= 0 || got > 10*time.Second {
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
