package wavehouse

import (
	"context"
	"net/http"
	"testing"
)

// TestTableRef_Insert: which wire format each argument shape takes on the way
// to /v1/ingest, and how the server's reply maps onto InsertResult.
func TestTableRef_Insert(t *testing.T) {
	type ClickRow struct {
		Page string `json:"page"`
	}
	const ndjson = `{"page":"/a"}` + "\n" + `{"page":"/b"}`

	wantOK := func(t *testing.T, r *InsertResult) {
		t.Helper()
		if !r.OK {
			t.Fatalf("want ok=true, got %+v", r)
		}
	}
	batchReply := map[string]any{"total": 2, "succeeded": 2, "failed": 0, "duplicates": 0}

	tests := []struct {
		name     string
		insert   func(context.Context, *TableRef) (*InsertResult, error)
		reply    map[string]any
		wantCT   string
		wantBody string
		check    func(*testing.T, *InsertResult)
	}{
		{
			name: "a single map is sent as one JSON value",
			insert: func(ctx context.Context, tr *TableRef) (*InsertResult, error) {
				return tr.Insert(ctx, map[string]any{"page": "/home"})
			},
			reply:    map[string]any{"ok": true},
			wantCT:   "application/json",
			wantBody: `{"page":"/home"}`,
			check:    wantOK,
		},
		{
			name: "a []map batch is sent as NDJSON",
			insert: func(ctx context.Context, tr *TableRef) (*InsertResult, error) {
				return tr.Insert(ctx, []map[string]any{{"page": "/a"}, {"page": "/b"}})
			},
			reply:    batchReply,
			wantCT:   "application/x-ndjson",
			wantBody: ndjson,
			check:    wantOK,
		},
		{
			// A typed slice (a generated or user-defined row type such as
			// []ClickRow) must take the batch NDJSON path — not fall through to
			// insertSingle, which would send the slice as one JSON body and
			// silently ignore any per-record failures the server reports.
			name: "a typed slice takes the batch path and surfaces per-record failures",
			insert: func(ctx context.Context, tr *TableRef) (*InsertResult, error) {
				return tr.Insert(ctx, []ClickRow{{Page: "/a"}, {Page: "/b"}})
			},
			reply:    map[string]any{"total": 2, "succeeded": 1, "failed": 1, "duplicates": 0},
			wantCT:   "application/x-ndjson",
			wantBody: ndjson,
			check: func(t *testing.T, r *InsertResult) {
				t.Helper()
				if r.OK {
					t.Fatal("want ok=false when a batch record fails")
				}
				if r.Failed == nil || *r.Failed != 1 {
					t.Fatalf("want failed=1, got %v", r.Failed)
				}
				if r.Total == nil || *r.Total != 2 {
					t.Fatalf("want total=2, got %v", r.Total)
				}
			},
		},
		{
			// The batch path posts to the same URL and also yields ok=true, so
			// the wire format is the only thing that distinguishes them: one
			// opaque JSON value vs. NDJSON of 16 per-byte rows. encoding/json
			// base64s a []byte — documented in queries.md as a value the server
			// rejects (use InsertNDJSON for raw bytes); pinned here because it
			// proves the batch path wasn't taken.
			name: "a []byte stays a single opaque value, not 16 rows",
			insert: func(ctx context.Context, tr *TableRef) (*InsertResult, error) {
				return tr.Insert(ctx, []byte(`{"page":"/home"}`))
			},
			reply:    map[string]any{"ok": true},
			wantCT:   "application/json",
			wantBody: `"eyJwYWdlIjoiL2hvbWUifQ=="`,
			check:    wantOK,
		},
		{
			name: "InsertNDJSON forwards pre-formatted lines verbatim",
			insert: func(ctx context.Context, tr *TableRef) (*InsertResult, error) {
				return tr.InsertNDJSON(ctx, ndjson)
			},
			reply:    batchReply,
			wantCT:   "application/x-ndjson",
			wantBody: ndjson,
			check: func(t *testing.T, r *InsertResult) {
				t.Helper()
				if r.Total == nil || *r.Total != 2 {
					t.Fatalf("want total=2, got %v", r.Total)
				}
			},
		},
		{
			name: "a deduplicated row reports duplicate=true",
			insert: func(ctx context.Context, tr *TableRef) (*InsertResult, error) {
				return tr.Insert(ctx, map[string]any{"page": "/dup"})
			},
			reply:    map[string]any{"duplicate": true},
			wantCT:   "application/json",
			wantBody: `{"page":"/dup"}`,
			check: func(t *testing.T, r *InsertResult) {
				t.Helper()
				if r.Duplicate == nil || !*r.Duplicate {
					t.Fatal("want duplicate=true")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, reqs := recordingClient(t, jsonResponse(tt.reply))
			result, err := tt.insert(context.Background(), c.From("clicks"))
			if err != nil {
				t.Fatal(err)
			}
			got := <-reqs
			if got.path != "/v1/ingest" {
				t.Fatalf("want /v1/ingest, got %s", got.path)
			}
			if ct := got.header.Get("Content-Type"); ct != tt.wantCT {
				t.Fatalf("want Content-Type %q, got %q", tt.wantCT, ct)
			}
			if string(got.body) != tt.wantBody {
				t.Fatalf("body:\n got %s\nwant %s", got.body, tt.wantBody)
			}
			tt.check(t, result)
		})
	}
}

func TestTableRef_InsertEmptyBatch(t *testing.T) {
	c := queryTestCtx(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("should not make a request for empty batch")
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

func TestTableRef_Schema(t *testing.T) {
	c, reqs := recordingClient(t, jsonResponse(TableSchema{
		Name:    "clicks",
		Columns: []Column{{Name: "page", Type: "String"}},
	}))
	schema, err := c.From("clicks").Schema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := (<-reqs).query.Get("table"); got != "clicks" {
		t.Fatalf("want table=clicks, got %q", got)
	}
	if schema.Name != "clicks" {
		t.Fatalf("want clicks, got %s", schema.Name)
	}
	if len(schema.Columns) != 1 || schema.Columns[0].Name != "page" {
		t.Fatalf("unexpected columns: %v", schema.Columns)
	}
}
