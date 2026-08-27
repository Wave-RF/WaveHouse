package wavehouse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
)

// oneRow answers any query with a single-row result set.
var oneRow = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode([]map[string]any{{"page": "/home"}})
})

func TestQueryBuilder_Immutability(t *testing.T) {
	b1 := queryTestCtx(t, oneRow).From("clicks").Select("page")
	if b2 := b1.Where("score", OpGt, 10); b1 == b2 {
		t.Fatal("builder should be immutable — chain methods return new instances")
	}
}

// TestQueryBuilder_RequestBody pins the exact StructuredQuery each chain puts
// on the wire. The table name always travels as a query parameter, never in
// the body — and CacheTTL is client-side only, so it appears in neither.
func TestQueryBuilder_RequestBody(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *TableRef) error
		want string
	}{
		{
			name: "the Fetch shortcut selects everything",
			run:  func(ctx context.Context, tr *TableRef) error { _, err := tr.Fetch(ctx); return err },
			want: `{"select_all":true,"limit":1000}`,
		},
		{
			name: "Select projects the named columns",
			run:  func(ctx context.Context, tr *TableRef) error { return runFetch(ctx, tr.Select("page", "button")) },
			want: `{"columns":["page","button"],"limit":1000}`,
		},
		{
			name: "SelectAll asks for every readable column",
			run:  func(ctx context.Context, tr *TableRef) error { return runFetch(ctx, tr.SelectAll()) },
			want: `{"select_all":true,"limit":1000}`,
		},
		{
			name: "a bare query defaults to select_all",
			run:  func(ctx context.Context, tr *TableRef) error { return runFetch(ctx, tr.Select()) },
			want: `{"select_all":true,"limit":1000}`,
		},
		{
			name: "an aggregation-only query does not set select_all",
			run:  func(ctx context.Context, tr *TableRef) error { return runFetch(ctx, tr.Select().Count("*", "n")) },
			want: `{"aggregations":[{"fn":"count","column":"*","alias":"n"}],"limit":1000}`,
		},
		{
			name: "Where appends a filter",
			run: func(ctx context.Context, tr *TableRef) error {
				return runFetch(ctx, tr.Select("page").Where("score", OpGt, 10))
			},
			want: `{"columns":["page"],"filters":[{"column":"score","op":"gt","value":10}],"limit":1000}`,
		},
		{
			name: "every aggregation helper, with and without an explicit alias",
			run: func(ctx context.Context, tr *TableRef) error {
				return runFetch(ctx, tr.Select().Count("*", "total").Sum("score", "").Avg("score", "").
					Min("score", "").Max("score", "").CountDistinct("page", "").
					Aggregate("uniqExact", "user_id", "unique_users"))
			},
			want: `{"aggregations":[{"fn":"count","column":"*","alias":"total"},` +
				`{"fn":"sum","column":"score","alias":"sum_score"},` +
				`{"fn":"avg","column":"score","alias":"avg_score"},` +
				`{"fn":"min","column":"score","alias":"min_score"},` +
				`{"fn":"max","column":"score","alias":"max_score"},` +
				`{"fn":"countDistinct","column":"page","alias":"count_distinct_page"},` +
				`{"fn":"uniqExact","column":"user_id","alias":"unique_users"}],"limit":1000}`,
		},
		{
			name: "GroupBy",
			run:  func(ctx context.Context, tr *TableRef) error { return runFetch(ctx, tr.Select("page").GroupBy("page")) },
			want: `{"columns":["page"],"group_by":["page"],"limit":1000}`,
		},
		{
			name: "OrderBy",
			run: func(ctx context.Context, tr *TableRef) error {
				return runFetch(ctx, tr.Select("page").OrderBy("page", "desc"))
			},
			want: `{"columns":["page"],"order_by":[{"column":"page","dir":"desc"}],"limit":1000}`,
		},
		{
			name: "Limit overrides the default",
			run:  func(ctx context.Context, tr *TableRef) error { return runFetch(ctx, tr.Select("page").Limit(50)) },
			want: `{"columns":["page"],"limit":50}`,
		},
		{
			name: "TimeRange omits an empty until",
			run: func(ctx context.Context, tr *TableRef) error {
				return runFetch(ctx, tr.Select("page").TimeRange("received_timestamp", "1h", ""))
			},
			want: `{"columns":["page"],"limit":1000,"time_range":{"column":"received_timestamp","since":"1h"}}`,
		},
		{
			name: "a fully chained query composes every clause",
			run: func(ctx context.Context, tr *TableRef) error {
				return runFetch(ctx, tr.Select("page").Where("score", OpGt, 10).Count("*", "total").
					GroupBy("page").OrderBy("total", "desc").Limit(50).
					TimeRange("received_timestamp", "1h", "").CacheTTL(60))
			},
			want: `{"columns":["page"],"aggregations":[{"fn":"count","column":"*","alias":"total"}],` +
				`"filters":[{"column":"score","op":"gt","value":10}],"group_by":["page"],` +
				`"order_by":[{"column":"total","dir":"desc"}],"limit":50,` +
				`"time_range":{"column":"received_timestamp","since":"1h"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, reqs := recordingClient(t, oneRow)
			if err := tt.run(context.Background(), c.From("clicks")); err != nil {
				t.Fatal(err)
			}
			got := <-reqs
			if got.method != "POST" || got.path != "/v1/query" {
				t.Fatalf("want POST /v1/query, got %s %s", got.method, got.path)
			}
			if tbl := got.query.Get("table"); tbl != "clicks" {
				t.Fatalf("want table=clicks in the query string, got %q", tbl)
			}
			if string(got.body) != tt.want {
				t.Fatalf("body:\n got %s\nwant %s", got.body, tt.want)
			}
		})
	}
}

// runFetch runs the query and discards the page — the request body is what these
// tests assert on.
func runFetch(ctx context.Context, q *QueryBuilder) error {
	_, err := q.FetchUntyped(ctx)
	return err
}

// TestQueryBuilder_AllOperators pins each SDK operator's wire spelling.
func TestQueryBuilder_AllOperators(t *testing.T) {
	ops := []struct {
		sdk  FilterOp
		wire string
	}{
		{OpEq, "eq"},
		{OpNeq, "neq"},
		{OpGt, "gt"},
		{OpGte, "gte"},
		{OpLt, "lt"},
		{OpLte, "lte"},
		{OpIn, "in"},
		{OpLike, "like"},
		{OpNotLike, "not_like"},
	}
	for _, tt := range ops {
		t.Run(tt.wire, func(t *testing.T) {
			c, reqs := recordingClient(t, oneRow)
			if err := runFetch(context.Background(), c.From("clicks").Select("x").Where("col", tt.sdk, "v")); err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf(`{"columns":["x"],"filters":[{"column":"col","op":%q,"value":"v"}],"limit":1000}`, tt.wire)
			if got := string((<-reqs).body); got != want {
				t.Fatalf("want %s, got %s", want, got)
			}
		})
	}
}

// pagingServer answers each request with the next fixture page (an empty page
// once they run out) and records every request, so tests can walk page.Next
// and inspect the cursor filters sent.
func pagingServer(t *testing.T, pages [][]map[string]any) (*Client, <-chan recordedRequest) {
	t.Helper()
	var mu sync.Mutex
	call := 0
	return recordingClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		idx := call
		call++
		mu.Unlock()
		page := []map[string]any{}
		if idx < len(pages) {
			page = pages[idx]
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
}

func filtersOf(t *testing.T, req recordedRequest) []map[string]any {
	t.Helper()
	raw, _ := req.jsonBody(t)["filters"].([]any)
	out := make([]map[string]any, len(raw))
	for i, f := range raw {
		out[i] = f.(map[string]any)
	}
	return out
}

// A full page always means HasMore, but Next only exists when there is an
// order column to build a cursor from. The ordered half is covered by
// TestQueryBuilder_PaginationCursor, which walks the cursor it hands back.
func TestQueryBuilder_Pagination_NoOrderNoNext(t *testing.T) {
	c, _ := pagingServer(t, [][]map[string]any{{{"id": "a"}, {"id": "b"}}})
	page, err := c.From("clicks").Select("id").Limit(2).FetchUntyped(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !page.HasMore {
		t.Fatal("a full page means hasMore=true")
	}
	if page.Next != nil {
		t.Fatal("want nil next — no order column for cursor")
	}
}

// TestQueryBuilder_PaginationCursor: every follow-up request carries exactly
// ONE cursor filter — replaced, never stacked — holding the previous page's
// last value, with the operator implied by the sort direction.
func TestQueryBuilder_PaginationCursor(t *testing.T) {
	tests := []struct {
		name    string
		dir     string
		op      string
		pages   [][]map[string]any
		cursors []string // expected cursor value per follow-up request
	}{
		{
			name: "ascending walks forward with gt", dir: "asc", op: "gt",
			pages:   [][]map[string]any{{{"id": "a"}, {"id": "b"}}, {{"id": "c"}, {"id": "d"}}, {{"id": "e"}}},
			cursors: []string{"b", "d"},
		},
		{
			name: "descending walks back with lt", dir: "desc", op: "lt",
			pages:   [][]map[string]any{{{"id": "z"}, {"id": "y"}}, {{"id": "x"}}},
			cursors: []string{"y"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, reqs := pagingServer(t, tt.pages)
			page, err := c.From("clicks").Select("id").OrderBy("id", tt.dir).Limit(2).FetchUntyped(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if f := filtersOf(t, <-reqs); len(f) != 0 {
				t.Fatalf("page 1 must have no cursor filter, got %v", f)
			}
			for i, want := range tt.cursors {
				if page, err = page.Next(context.Background()); err != nil {
					t.Fatal(err)
				}
				if got, want := page.Data[0]["id"], tt.pages[i+1][0]["id"]; got != want {
					t.Fatalf("page %d: want first row %v, got %v", i+2, want, got)
				}
				f := filtersOf(t, <-reqs)
				if len(f) != 1 {
					t.Fatalf("page %d: want exactly 1 cursor filter, got %v", i+2, f)
				}
				if f[0]["column"] != "id" || f[0]["op"] != tt.op || f[0]["value"] != want {
					t.Fatalf("page %d: unexpected cursor filter %v", i+2, f[0])
				}
			}
			if page.HasMore {
				t.Fatal("the last fixture page is short — want hasMore=false")
			}
			if n := len(reqs); n != 0 {
				t.Fatalf("want no requests beyond the pages walked, got %d more", n)
			}
		})
	}
}

func TestQueryBuilder_Pagination_CursorColumnMissingEndsQuietly(t *testing.T) {
	c, _ := pagingServer(t, [][]map[string]any{
		{{"other": "1"}, {"other": "2"}}, // projection omits the order column
	})

	page, err := c.From("clicks").Select("other").OrderBy("id", "asc").Limit(2).FetchUntyped(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	next, err := page.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Data) != 0 || next.HasMore || next.Next != nil {
		t.Fatalf("want quiet empty page, got %+v", next)
	}
}

// TestQueryBuilder_PaginationCursorPrecision: the cursor value is read back off
// the decoded row, so how the row decoded decides how much precision survives.
// Typed rows keep int64 exactly; the untyped path decodes to float64 and loses
// everything past 2^53 (the same ceiling the TS SDK's JS numbers have). Use
// FetchTyped with an int64 field past 2^53 — documented in queries.mdx.
func TestQueryBuilder_PaginationCursorPrecision(t *testing.T) {
	type idRow struct {
		ID int64 `json:"id"`
	}
	const bigID = int64(9007199254740993) // 2^53 + 1: a float64 round-trip corrupts it

	tests := []struct {
		name string
		walk func(context.Context, *QueryBuilder) error
		want string
	}{
		{
			name: "typed rows keep int64 exactly",
			walk: func(ctx context.Context, q *QueryBuilder) error {
				page, err := FetchTyped[idRow](ctx, q)
				if err != nil {
					return err
				}
				_, err = page.Next(ctx)
				return err
			},
			want: "9007199254740993",
		},
		{
			name: "untyped rows hit the float64 ceiling",
			walk: func(ctx context.Context, q *QueryBuilder) error {
				page, err := q.FetchUntyped(ctx)
				if err != nil {
					return err
				}
				_, err = page.Next(ctx)
				return err
			},
			want: "9007199254740992",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, reqs := pagingServer(t, [][]map[string]any{{{"id": 1}, {"id": bigID}}, {}})
			q := c.From("clicks").Select("id").OrderBy("id", "asc").Limit(2)
			if err := tt.walk(context.Background(), q); err != nil {
				t.Fatal(err)
			}
			<-reqs // page 1 carries no cursor
			f := filtersOf(t, <-reqs)
			if len(f) != 1 {
				t.Fatalf("want 1 cursor filter, got %v", f)
			}
			if got := fmt.Sprint(f[0]["value"]); got != tt.want {
				t.Fatalf("cursor value: want %s, got %s (update docs if intentional)", tt.want, got)
			}
		})
	}
}

// The cursor round-trip re-marshals the last row to read its cursor value. A
// Row that unmarshals cleanly but can't be marshaled back (an exported func
// field, absent from the response) must surface an error rather than an empty
// page, which is indistinguishable from real exhaustion.
func TestQueryBuilder_Pagination_UnmarshalableRowErrors(t *testing.T) {
	type row struct {
		ID string `json:"id"`
		Cb func() `json:"cb"`
	}
	c, _ := pagingServer(t, [][]map[string]any{{{"id": "a"}, {"id": "b"}}})
	page, err := FetchTyped[row](context.Background(),
		c.From("clicks").Select("id").OrderBy("id", "asc").Limit(2))
	if err != nil {
		t.Fatal(err)
	}
	if page.Next == nil {
		t.Fatal("want a Next cursor")
	}
	if _, err := page.Next(context.Background()); err == nil {
		t.Fatal("want a marshal error, got a silently empty page")
	}
}
