package wavehouse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClient_Defaults(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://localhost:8080"})
	if c.ctx.maxRetries != 2 {
		t.Fatalf("want default maxRetries=2, got %d", c.ctx.maxRetries)
	}
	if c.ctx.baseURL != "http://localhost:8080" {
		t.Fatalf("want baseURL, got %s", c.ctx.baseURL)
	}
}

func TestNewClient_StripsTrailingSlashes(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://localhost:8080///"})
	if c.ctx.baseURL != "http://localhost:8080" {
		t.Fatalf("want stripped URL, got %s", c.ctx.baseURL)
	}
}

func TestNewClient_CustomMaxRetries(t *testing.T) {
	c := NewClient(Config{
		BaseURL: "http://localhost:8080",
		Options: &ClientOptions{MaxRetries: 5},
	})
	if c.ctx.maxRetries != 5 {
		t.Fatalf("want 5, got %d", c.ctx.maxRetries)
	}
}

func TestNewClient_HasNamespaces(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://localhost:8080"})
	if c.Schema == nil {
		t.Fatal("Schema namespace nil")
	}
	if c.Policy == nil {
		t.Fatal("Policy namespace nil")
	}
	if c.DLQ == nil {
		t.Fatal("DLQ namespace nil")
	}
	if c.Sys == nil {
		t.Fatal("Sys namespace nil")
	}
	if c.Pipes == nil {
		t.Fatal("Pipes namespace nil")
	}
}

func TestClient_From(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the table name appears in the URL.
		if r.URL.Query().Get("table") != "events" {
			t.Errorf("want table=events, got %s", r.URL.Query().Get("table"))
		}
		json.NewEncoder(w).Encode([]map[string]any{})
	}))
	c := NewClient(Config{BaseURL: srv.URL, HTTPClient: srv.Client()})
	_, _ = c.From("events").Fetch(context.Background())
}

func TestClient_SQL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/admin/query" {
			t.Errorf("want /v1/admin/query, got %s", r.URL.Path)
		}
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if body["sql"] != "SELECT 1" {
			t.Errorf("want sql=SELECT 1, got %s", body["sql"])
		}
		json.NewEncoder(w).Encode([]map[string]any{{"x": 1}})
	}))
	c := NewClient(Config{BaseURL: srv.URL, HTTPClient: srv.Client()})
	rows, err := SQL[map[string]any](context.Background(), c, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

func TestStaticToken(t *testing.T) {
	fn := StaticToken("abc")
	token, err := fn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != "abc" {
		t.Fatalf("want abc, got %s", token)
	}
}
