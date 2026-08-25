package wavehouse

import (
	"context"
	"testing"
)

// TestNamespaces_RequestShape: the method, path and query parameters every
// namespace helper puts on the wire, and the shape it decodes the reply into.
func TestNamespaces_RequestShape(t *testing.T) {
	ctx := context.Background()
	policy := &Policy{Tables: map[string]TablePolicy{}}

	tests := []struct {
		name       string
		reply      any // nil replies with a bare 200
		call       func(*testing.T, *Client)
		wantMethod string
		wantPath   string
		wantTable  string // expected ?table= parameter, if any
	}{
		{
			name:       "Sys.Health",
			call:       func(t *testing.T, c *Client) { mustNoErr(t, c.Sys.Health(ctx)) },
			wantMethod: "GET",
			wantPath:   "/v1/health",
		},
		{
			name:  "Schema.List keys tables by name",
			reply: []TableSchema{{Name: "clicks", Columns: []Column{{Name: "page", Type: "String"}}}},
			call: func(t *testing.T, c *Client) {
				schemas, err := c.Schema.List(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := schemas["clicks"]; !ok {
					t.Fatalf("want clicks in schemas, got %v", schemas)
				}
			},
			wantMethod: "GET",
			wantPath:   "/v1/ops/schema",
		},
		{
			name:       "Schema.Refresh",
			call:       func(t *testing.T, c *Client) { mustNoErr(t, c.Schema.Refresh(ctx)) },
			wantMethod: "POST",
			wantPath:   "/v1/ops/schema/refresh",
		},
		{
			name:  "Policy.Get",
			reply: policy,
			call: func(t *testing.T, c *Client) {
				pol, err := c.Policy.Get(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if pol.Tables == nil {
					t.Fatal("want a tables map")
				}
			},
			wantMethod: "GET",
			wantPath:   "/v1/ops/policy",
		},
		{
			name:       "Policy.Set",
			call:       func(t *testing.T, c *Client) { mustNoErr(t, c.Policy.Set(ctx, policy)) },
			wantMethod: "PUT",
			wantPath:   "/v1/ops/policy",
		},
		{
			name:  "Policy.Validate",
			reply: ValidationResult{Valid: true},
			call: func(t *testing.T, c *Client) {
				v, err := c.Policy.Validate(ctx, policy)
				if err != nil {
					t.Fatal(err)
				}
				if !v.Valid {
					t.Fatal("want valid=true")
				}
			},
			wantMethod: "POST",
			wantPath:   "/v1/ops/policy/validate",
		},
		{
			name:  "DLQ.List totals every table",
			reply: DLQStats{Tables: map[string]int{"clicks": 3}, Total: 3},
			call: func(t *testing.T, c *Client) {
				stats, err := c.DLQ.List(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if stats.Total != 3 {
					t.Fatalf("want total=3, got %d", stats.Total)
				}
			},
			wantMethod: "GET",
			wantPath:   "/v1/ops/dlq/stats",
		},
		{
			name:  "DLQ.Table filters by table",
			reply: DLQStats{Tables: map[string]int{"clicks": 2}, Total: 2},
			call: func(t *testing.T, c *Client) {
				_, err := c.DLQ.Table(ctx, "clicks")
				mustNoErr(t, err)
			},
			wantMethod: "GET",
			wantPath:   "/v1/ops/dlq/stats",
			wantTable:  "clicks",
		},
		{
			name:  "Pipes.List",
			reply: []Pipe{{Name: "p1", SQL: "SELECT 1"}},
			call: func(t *testing.T, c *Client) {
				pipes, err := c.Pipes.List(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(pipes) != 1 || pipes[0].Name != "p1" {
					t.Fatalf("want [p1], got %v", pipes)
				}
			},
			wantMethod: "GET",
			wantPath:   "/v1/ops/pipes",
		},
		{
			name:  "Pipes.Get",
			reply: Pipe{Name: "p1", SQL: "SELECT 1"},
			call: func(t *testing.T, c *Client) {
				p, err := c.Pipes.Get(ctx, "p1")
				if err != nil {
					t.Fatal(err)
				}
				if p.Name != "p1" {
					t.Fatalf("want p1, got %s", p.Name)
				}
			},
			wantMethod: "GET",
			wantPath:   "/v1/ops/pipes/p1",
		},
		{
			name:       "Pipes.Set",
			call:       func(t *testing.T, c *Client) { mustNoErr(t, c.Pipes.Set(ctx, "p1", PipeDef{SQL: "SELECT 1"})) },
			wantMethod: "PUT",
			wantPath:   "/v1/ops/pipes/p1",
		},
		{
			name:       "Pipes.Delete",
			call:       func(t *testing.T, c *Client) { mustNoErr(t, c.Pipes.Delete(ctx, "p1")) },
			wantMethod: "DELETE",
			wantPath:   "/v1/ops/pipes/p1",
		},
		{
			name:  "PipeRef.Fetch posts the pipe's parameters",
			reply: []map[string]any{{"count": 42}},
			call: func(t *testing.T, c *Client) {
				rows, err := Fetch[map[string]any](ctx, c.Pipe("top_pages", map[string]any{"limit": 10}))
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) != 1 {
					t.Fatalf("want 1 row, got %d", len(rows))
				}
			},
			wantMethod: "POST",
			wantPath:   "/v1/pipes/top_pages",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			respond := ok200
			if tt.reply != nil {
				respond = jsonResponse(tt.reply)
			}
			c, reqs := recordingClient(t, respond)
			tt.call(t, c)

			got := <-reqs
			if got.method != tt.wantMethod || got.path != tt.wantPath {
				t.Fatalf("want %s %s, got %s %s", tt.wantMethod, tt.wantPath, got.method, got.path)
			}
			if tbl := got.query.Get("table"); tbl != tt.wantTable {
				t.Fatalf("want table=%q, got %q", tt.wantTable, tbl)
			}
		})
	}
}

func mustNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
