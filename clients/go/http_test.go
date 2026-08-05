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

func testCtx(handler http.Handler) httpContext {
	srv := httptest.NewServer(handler)
	return httpContext{
		baseURL:    srv.URL,
		maxRetries: 0,
		httpClient: srv.Client(),
	}
}

func TestDoRequest_SuccessfulGET(t *testing.T) {
	hctx := testCtx(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))

	var result map[string]string
	err := doRequest(hctx, context.Background(), requestOptions{
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
	hctx := testCtx(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(200)
	}))

	err := doRequest(hctx, context.Background(), requestOptions{
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
	hctx := testCtx(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		raw := make([]byte, 1024)
		n, _ := r.Body.Read(raw)
		gotBody = string(raw[:n])
		json.NewEncoder(w).Encode(map[string]int{"total": 1})
	}))

	err := doRequest(hctx, context.Background(), requestOptions{
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
	hctx := testCtx(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	hctx.auth = StaticToken("my-token")

	err := doRequest(hctx, context.Background(), requestOptions{
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
	hctx := testCtx(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	}))
	hctx.maxRetries = 2

	err := doRequest(hctx, context.Background(), requestOptions{
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
	hctx := testCtx(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := count.Add(1)
		if n < 3 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]string{"error": "internal"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	}))
	hctx.maxRetries = 2

	var result map[string]string
	err := doRequest(hctx, context.Background(), requestOptions{
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
	hctx := testCtx(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Second)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := doRequest(hctx, ctx, requestOptions{
		method: "GET",
		path:   "/health",
	}, nil)

	if !errIs(err, "ABORTED") {
		t.Fatalf("want ABORTED, got %v", err)
	}
}

func TestDoRequest_EmptyResponse(t *testing.T) {
	hctx := testCtx(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))

	var result map[string]string
	err := doRequest(hctx, context.Background(), requestOptions{
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
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{10, 30 * time.Second}, // capped at 30s
	}
	for _, tt := range tests {
		got := backoff(tt.attempt)
		if got != tt.want {
			t.Errorf("backoff(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}
