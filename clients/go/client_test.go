package wavehouse

import (
	"context"
	"encoding/json"
	"testing"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name           string
		cfg            Config
		wantBaseURL    string
		wantMaxRetries int
	}{
		{
			name:           "defaults",
			cfg:            Config{BaseURL: "http://localhost:8080"},
			wantBaseURL:    "http://localhost:8080",
			wantMaxRetries: 2,
		},
		{
			name:           "trailing slashes are stripped",
			cfg:            Config{BaseURL: "http://localhost:8080///"},
			wantBaseURL:    "http://localhost:8080",
			wantMaxRetries: 2,
		},
		{
			name:           "MaxRetries is honored",
			cfg:            Config{BaseURL: "http://localhost:8080", Options: &ClientOptions{MaxRetries: Ptr(5)}},
			wantBaseURL:    "http://localhost:8080",
			wantMaxRetries: 5,
		},
		{
			// The regression a plain int caused: options set for an unrelated
			// reason silently disabled retries, because 0 is both "unset" and
			// "none". Headers is the field the docs push operators toward.
			name:           "options set only for Headers keep the default",
			cfg:            Config{BaseURL: "http://localhost:8080", Options: &ClientOptions{Headers: map[string]string{"X-Operator-Key": "k"}}},
			wantBaseURL:    "http://localhost:8080",
			wantMaxRetries: 2,
		},
		{
			name:           "Ptr(0) disables retries",
			cfg:            Config{BaseURL: "http://localhost:8080", Options: &ClientOptions{MaxRetries: Ptr(0)}},
			wantBaseURL:    "http://localhost:8080",
			wantMaxRetries: 0,
		},
		{
			name:           "a negative MaxRetries clamps to 0",
			cfg:            Config{BaseURL: "http://localhost:8080", Options: &ClientOptions{MaxRetries: Ptr(-3)}},
			wantBaseURL:    "http://localhost:8080",
			wantMaxRetries: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(tt.cfg)
			if c.ctx.baseURL != tt.wantBaseURL {
				t.Errorf("want baseURL %q, got %q", tt.wantBaseURL, c.ctx.baseURL)
			}
			if c.ctx.maxRetries != tt.wantMaxRetries {
				t.Errorf("want maxRetries %d, got %d", tt.wantMaxRetries, c.ctx.maxRetries)
			}
		})
	}
}

func TestNewClient_HasNamespaces(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://localhost:8080"})
	// Compared as concrete typed pointers, not boxed into map[string]any: a nil
	// typed pointer in an interface is never == nil, so the map form passed
	// even if NewClient stopped assigning a namespace entirely.
	if c.Sys == nil {
		t.Error("Sys namespace is nil")
	}
	if c.Schema == nil {
		t.Error("Schema namespace is nil")
	}
	if c.Policy == nil {
		t.Error("Policy namespace is nil")
	}
	if c.Pipes == nil {
		t.Error("Pipes namespace is nil")
	}
	if c.DLQ == nil {
		t.Error("DLQ namespace is nil")
	}
}

func TestClient_SQL(t *testing.T) {
	c, reqs := recordingClient(t, jsonResponse([]map[string]any{{"x": 1}}))
	rows, err := SQL[map[string]any](context.Background(), c, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	got := <-reqs
	if got.method != "POST" || got.path != "/v1/ops/query" {
		t.Fatalf("want POST /v1/ops/query, got %s %s", got.method, got.path)
	}
	if body := string(got.body); body != `{"sql":"SELECT 1"}` {
		t.Fatalf(`want {"sql":"SELECT 1"}, got %s`, body)
	}
}

func TestPolicyFilter_MarshalOperators(t *testing.T) {
	empty := ""
	raw, err := json.Marshal(PolicyFilter{Eq: &empty})
	if err != nil {
		t.Fatal(err)
	}
	// Intentional empty-string comparison survives; unset operators are
	// omitted entirely, never sent as null.
	if string(raw) != `{"_eq":""}` {
		t.Fatalf(`want {"_eq":""}, got %s`, raw)
	}
}
