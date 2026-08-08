package wavehouse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
)

func TestTableRef_InsertSingle(t *testing.T) {
	// mu guards handler captures throughout this file: the handler runs on the
	// server goroutine and no happens-before edge exists via the TCP socket.
	var mu sync.Mutex
	var gotBody map[string]any
	var gotPath string
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	result, err := c.From("clicks").Insert(context.Background(), map[string]any{"page": "/home"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatal("want ok=true")
	}
	mu.Lock()
	defer mu.Unlock()
	if gotPath != "/v1/ingest" {
		t.Fatalf("want /v1/ingest, got %s", gotPath)
	}
	if gotBody["page"] != "/home" {
		t.Fatalf("want page=/home, got %v", gotBody)
	}
}

func TestTableRef_InsertBatch(t *testing.T) {
	var mu sync.Mutex
	var gotCT string
	var gotBody string
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotCT = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": 2, "succeeded": 2, "failed": 0, "duplicates": 0,
		})
	}))
	result, err := c.From("clicks").Insert(context.Background(), []map[string]any{
		{"page": "/a"},
		{"page": "/b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatal("want ok=true")
	}
	mu.Lock()
	defer mu.Unlock()
	if gotCT != "application/x-ndjson" {
		t.Fatalf("want ndjson content type, got %s", gotCT)
	}
	if gotBody != `{"page":"/a"}`+"\n"+`{"page":"/b"}` {
		t.Fatalf("want NDJSON body, got %s", gotBody)
	}
}

// TestTableRef_InsertTypedSlice covers the P1 finding: a typed slice (e.g. a
// generated or user-defined row type such as []ClickRow) must take the batch
// NDJSON path — not fall through to insertSingle, which would send the slice
// as a single JSON body and silently ignore any per-record failures the
// server reports.
func TestTableRef_InsertTypedSlice(t *testing.T) {
	type ClickRow struct {
		Page string `json:"page"`
	}

	var mu sync.Mutex
	var gotCT string
	var gotBody string
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotCT = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": 2, "succeeded": 1, "failed": 1, "duplicates": 0,
		})
	}))
	result, err := c.From("clicks").Insert(context.Background(), []ClickRow{
		{Page: "/a"},
		{Page: "/b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotCT != "application/x-ndjson" {
		t.Fatalf("want ndjson content type, got %s", gotCT)
	}
	if gotBody != `{"page":"/a"}`+"\n"+`{"page":"/b"}` {
		t.Fatalf("want NDJSON body, got %s", gotBody)
	}
	if result.OK {
		t.Fatal("want ok=false when a batch record fails")
	}
	if result.Failed == nil || *result.Failed != 1 {
		t.Fatalf("want failed=1, got %v", result.Failed)
	}
	if result.Total == nil || *result.Total != 2 {
		t.Fatalf("want total=2, got %v", result.Total)
	}
}

// TestTableRef_InsertByteSliceNotBatch ensures []byte keeps going through
// insertSingle rather than being (mis)treated as a slice of per-byte rows.
func TestTableRef_InsertByteSliceNotBatch(t *testing.T) {
	var mu sync.Mutex
	var gotPath string
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotPath = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	result, err := c.From("clicks").Insert(context.Background(), []byte(`{"page":"/home"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatal("want ok=true")
	}
	mu.Lock()
	defer mu.Unlock()
	if gotPath != "/v1/ingest" {
		t.Fatalf("want /v1/ingest, got %s", gotPath)
	}
}

func TestTableRef_InsertEmptyBatch(t *testing.T) {
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("should not make a request for empty batch")
	}))
	result, err := c.From("clicks").Insert(context.Background(), []map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatal("want ok=true")
	}
	if result.Total == nil || *result.Total != 0 {
		t.Fatal("want total=0")
	}
}

func TestTableRef_InsertNDJSON(t *testing.T) {
	var mu sync.Mutex
	var gotBody string
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": 2, "succeeded": 2, "failed": 0, "duplicates": 0,
		})
	}))
	ndjson := `{"page":"/a"}` + "\n" + `{"page":"/b"}`
	result, err := c.From("clicks").InsertNDJSON(context.Background(), ndjson)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total == nil || *result.Total != 2 {
		t.Fatalf("want total=2, got %v", result.Total)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotBody != ndjson {
		t.Fatalf("want raw NDJSON, got %s", gotBody)
	}
}

func TestTableRef_Schema(t *testing.T) {
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("table") != "clicks" {
			t.Errorf("want table=clicks")
		}
		_ = json.NewEncoder(w).Encode(TableSchema{
			Name: "clicks",
			Columns: []Column{
				{Name: "page", Type: "String"},
			},
		})
	}))
	schema, err := c.From("clicks").Schema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if schema.Name != "clicks" {
		t.Fatalf("want clicks, got %s", schema.Name)
	}
	if len(schema.Columns) != 1 || schema.Columns[0].Name != "page" {
		t.Fatalf("unexpected columns: %v", schema.Columns)
	}
}

func TestTableRef_InsertDuplicate(t *testing.T) {
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"duplicate": true})
	}))
	result, err := c.From("clicks").Insert(context.Background(), map[string]any{"page": "/dup"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Duplicate == nil || !*result.Duplicate {
		t.Fatal("want duplicate=true")
	}
}
