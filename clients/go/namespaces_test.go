package wavehouse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func nsClient(handler http.Handler) *Client {
	srv := httptest.NewServer(handler)
	return NewClient(Config{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
		Options:    &ClientOptions{MaxRetries: 0},
	})
}

func TestSysNamespace_Health(t *testing.T) {
	c := nsClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" {
			t.Errorf("want /v1/health, got %s", r.URL.Path)
		}
		w.WriteHeader(200)
	}))
	err := c.Sys.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}

func TestSchemaNamespace_List(t *testing.T) {
	c := nsClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/schema" {
			t.Errorf("want /v1/schema, got %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode([]TableSchema{
			{Name: "clicks", Columns: []Column{{Name: "page", Type: "String"}}},
		})
	}))
	schemas, err := c.Schema.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := schemas["clicks"]; !ok {
		t.Fatal("want clicks in schemas")
	}
}

func TestSchemaNamespace_Refresh(t *testing.T) {
	var gotMethod string
	c := nsClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(200)
	}))
	err := c.Schema.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != "POST" {
		t.Fatalf("want POST, got %s", gotMethod)
	}
}

func TestPolicyNamespace_GetSetValidate(t *testing.T) {
	c := nsClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET":
			json.NewEncoder(w).Encode(Policy{Tables: map[string]TablePolicy{}})
		case r.Method == "PUT":
			w.WriteHeader(200)
		case r.Method == "POST":
			json.NewEncoder(w).Encode(ValidationResult{Valid: true})
		}
	}))

	pol, err := c.Policy.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pol.Tables == nil {
		t.Fatal("want tables map")
	}

	err = c.Policy.Set(context.Background(), pol)
	if err != nil {
		t.Fatal(err)
	}

	v, err := c.Policy.Validate(context.Background(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Valid {
		t.Fatal("want valid=true")
	}
}

func TestDLQNamespace_List(t *testing.T) {
	c := nsClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(DLQStats{Tables: map[string]int{"clicks": 3}, Total: 3})
	}))
	stats, err := c.DLQ.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 3 {
		t.Fatalf("want total=3, got %d", stats.Total)
	}
}

func TestDLQNamespace_Table(t *testing.T) {
	var gotParam string
	c := nsClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotParam = r.URL.Query().Get("table")
		json.NewEncoder(w).Encode(DLQStats{Tables: map[string]int{"clicks": 2}, Total: 2})
	}))
	_, err := c.DLQ.Table(context.Background(), "clicks")
	if err != nil {
		t.Fatal(err)
	}
	if gotParam != "clicks" {
		t.Fatalf("want table=clicks, got %s", gotParam)
	}
}

func TestPipesNamespace_CRUD(t *testing.T) {
	c := nsClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			if r.URL.Path == "/v1/admin/pipes" {
				json.NewEncoder(w).Encode([]Pipe{{Name: "p1", SQL: "SELECT 1"}})
			} else {
				json.NewEncoder(w).Encode(Pipe{Name: "p1", SQL: "SELECT 1"})
			}
		case "PUT":
			w.WriteHeader(200)
		case "DELETE":
			w.WriteHeader(200)
		}
	}))

	pipes, err := c.Pipes.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pipes) != 1 || pipes[0].Name != "p1" {
		t.Fatalf("want [p1], got %v", pipes)
	}

	p, err := c.Pipes.Get(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "p1" {
		t.Fatalf("want p1, got %s", p.Name)
	}

	err = c.Pipes.Set(context.Background(), "p1", PipeDef{SQL: "SELECT 1"})
	if err != nil {
		t.Fatal(err)
	}

	err = c.Pipes.Delete(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestPipeRef_Fetch(t *testing.T) {
	var gotPath, gotMethod string
	var gotBody map[string]any
	c := nsClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		json.NewDecoder(r.Body).Decode(&gotBody)
		json.NewEncoder(w).Encode([]map[string]any{{"count": 42}})
	}))
	rows, err := Fetch[map[string]any](context.Background(), c.Pipe("top_pages", map[string]any{"limit": 10}))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if gotPath != "/v1/pipes/top_pages" {
		t.Fatalf("want /v1/pipes/top_pages, got %s", gotPath)
	}
	if gotMethod != "POST" {
		t.Fatalf("want POST, got %s", gotMethod)
	}
}
