package wavehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func queryTestCtx(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewClient(Config{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
		Options:    &ClientOptions{MaxRetries: 0},
	})
}

func captureQueryBody(t *testing.T, handler http.Handler) (*Client, func() map[string]any) {
	t.Helper()
	// body is written on the server goroutine and read on the test goroutine;
	// the mutex is what makes that visible under -race.
	var mu sync.Mutex
	var body []byte
	wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		mu.Lock()
		body = raw
		mu.Unlock()
		handler.ServeHTTP(w, r)
	})
	c := queryTestCtx(t, wrapper)
	return c, func() map[string]any {
		mu.Lock()
		defer mu.Unlock()
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		return m
	}
}

var emptyRows = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode([]map[string]any{{"page": "/home"}})
})

func TestQueryBuilder_Immutability(t *testing.T) {
	c := queryTestCtx(t, emptyRows)
	b1 := c.From("clicks").Select("page")
	b2 := b1.Where("score", OpGt, 10)
	if b1 == b2 {
		t.Fatal("builder should be immutable — chain methods return new instances")
	}
}

func TestQueryBuilder_SelectColumns(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select("page", "button").FetchUntyped(context.Background())

	body := getBody()
	cols, ok := body["columns"].([]any)
	if !ok || len(cols) != 2 {
		t.Fatalf("want [page, button], got %v", body["columns"])
	}
}

func TestQueryBuilder_SelectAll(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").SelectAll().FetchUntyped(context.Background())

	body := getBody()
	if body["select_all"] != true {
		t.Fatalf("want select_all=true, got %v", body)
	}
}

func TestQueryBuilder_BareQueryDefaultsToSelectAll(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select().FetchUntyped(context.Background())

	body := getBody()
	if body["select_all"] != true {
		t.Fatalf("bare query should default to select_all, got %v", body)
	}
}

func TestQueryBuilder_AggregationOnlyNoSelectAll(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select().Count("*", "n").FetchUntyped(context.Background())

	body := getBody()
	if body["select_all"] != nil {
		t.Fatalf("aggregation-only query should not set select_all, got %v", body)
	}
}

func TestQueryBuilder_Where(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select("page").Where("score", OpGt, 10).FetchUntyped(context.Background())

	body := getBody()
	filters, ok := body["filters"].([]any)
	if !ok || len(filters) != 1 {
		t.Fatalf("want 1 filter, got %v", body["filters"])
	}
	f := filters[0].(map[string]any)
	if f["column"] != "score" || f["op"] != "gt" {
		t.Fatalf("want score/gt filter, got %v", f)
	}
}

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
			c, getBody := captureQueryBody(t, emptyRows)
			_, _ = c.From("clicks").Select("x").Where("col", tt.sdk, "v").FetchUntyped(context.Background())
			body := getBody()
			filters := body["filters"].([]any)
			f := filters[0].(map[string]any)
			if f["op"] != tt.wire {
				t.Errorf("want wire op %s, got %s", tt.wire, f["op"])
			}
		})
	}
}

func TestQueryBuilder_Aggregations(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select().
		Count("*", "total").
		Sum("score", "").
		Avg("score", "").
		Min("score", "").
		Max("score", "").
		CountDistinct("page", "").
		Aggregate("uniqExact", "user_id", "unique_users").
		FetchUntyped(context.Background())

	body := getBody()
	aggs, ok := body["aggregations"].([]any)
	if !ok || len(aggs) != 7 {
		t.Fatalf("want 7 aggregations, got %v", body["aggregations"])
	}
}

func TestQueryBuilder_GroupBy(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select("page").GroupBy("page").FetchUntyped(context.Background())

	body := getBody()
	gb, ok := body["group_by"].([]any)
	if !ok || len(gb) != 1 || gb[0] != "page" {
		t.Fatalf("want [page], got %v", body["group_by"])
	}
}

func TestQueryBuilder_OrderBy(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select("page").OrderBy("page", "desc").FetchUntyped(context.Background())

	body := getBody()
	ob := body["order_by"].([]any)
	o := ob[0].(map[string]any)
	if o["column"] != "page" || o["dir"] != "desc" {
		t.Fatalf("want page/desc, got %v", o)
	}
}

func TestQueryBuilder_Limit(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select("page").Limit(50).FetchUntyped(context.Background())

	body := getBody()
	if body["limit"] != float64(50) {
		t.Fatalf("want 50, got %v", body["limit"])
	}
}

func TestQueryBuilder_TimeRange(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").Select("page").
		TimeRange("received_timestamp", "1h", "").
		FetchUntyped(context.Background())

	body := getBody()
	tr := body["time_range"].(map[string]any)
	if tr["column"] != "received_timestamp" || tr["since"] != "1h" {
		t.Fatalf("want received_timestamp/1h, got %v", tr)
	}
}

func TestQueryBuilder_Pagination_HasMore(t *testing.T) {
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "a"}, {"id": "b"}})
	}))

	page, err := c.From("clicks").Select("id").OrderBy("id", "asc").Limit(2).FetchUntyped(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !page.HasMore {
		t.Fatal("want hasMore=true")
	}
	if page.Next == nil {
		t.Fatal("want next function")
	}
}

func TestQueryBuilder_Pagination_NoOrderNoNext(t *testing.T) {
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "a"}, {"id": "b"}})
	}))

	page, err := c.From("clicks").Select("id").Limit(2).FetchUntyped(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !page.HasMore {
		t.Fatal("want hasMore=true")
	}
	if page.Next != nil {
		t.Fatal("want nil next — no order column for cursor")
	}
}

func TestQueryBuilder_ComplexQuery(t *testing.T) {
	c, getBody := captureQueryBody(t, emptyRows)
	_, _ = c.From("clicks").
		Select("page").
		Where("score", OpGt, 10).
		Count("*", "total").
		GroupBy("page").
		OrderBy("total", "desc").
		Limit(50).
		TimeRange("received_timestamp", "1h", "").
		CacheTTL(60).
		FetchUntyped(context.Background())

	body := getBody()
	if body["columns"].([]any)[0] != "page" {
		t.Fatal("missing page column")
	}
	if body["limit"] != float64(50) {
		t.Fatal("wrong limit")
	}
	if body["group_by"].([]any)[0] != "page" {
		t.Fatal("wrong group_by")
	}
}

// pagingServer returns limit-sized pages of rows and captures each request
// body, so tests can walk page.Next and inspect the cursor filters sent.
func pagingServer(t *testing.T, pages [][]map[string]any) (*Client, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var bodies []map[string]any
	call := 0
	c := queryTestCtx(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber() // keep int64 cursor values exact on the capture side too
		_ = dec.Decode(&body)
		mu.Lock()
		bodies = append(bodies, body)
		idx := call
		call++
		mu.Unlock()
		page := []map[string]any{}
		if idx < len(pages) {
			page = pages[idx]
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	return c, func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), bodies...)
	}
}

func filtersOf(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, ok := body["filters"].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, len(raw))
	for i, f := range raw {
		out[i] = f.(map[string]any)
	}
	return out
}

func TestQueryBuilder_Pagination_NextWalksPages(t *testing.T) {
	c, getBodies := pagingServer(t, [][]map[string]any{
		{{"id": "a"}, {"id": "b"}},
		{{"id": "c"}, {"id": "d"}},
		{{"id": "e"}},
	})

	page, err := c.From("clicks").Select("id").OrderBy("id", "asc").Limit(2).FetchUntyped(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	page2, err := page.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if page2.Data[0]["id"] != "c" || !page2.HasMore || page2.Next == nil {
		t.Fatalf("unexpected page 2: %+v", page2)
	}
	page3, err := page2.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(page3.Data) != 1 || page3.HasMore {
		t.Fatalf("unexpected page 3: %+v", page3)
	}

	bodies := getBodies()
	if len(bodies) != 3 {
		t.Fatalf("want 3 requests, got %d", len(bodies))
	}
	if f := filtersOf(t, bodies[0]); len(f) != 0 {
		t.Fatalf("page 1 must have no cursor filter, got %v", f)
	}
	// Page 2 and 3: exactly ONE cursor filter (replaced, not stacked), with
	// the ascending op and the previous page's last cursor value.
	for i, want := range []string{"b", "d"} {
		f := filtersOf(t, bodies[i+1])
		if len(f) != 1 {
			t.Fatalf("page %d: want exactly 1 cursor filter, got %v", i+2, f)
		}
		if f[0]["column"] != "id" || f[0]["op"] != "gt" || f[0]["value"] != want {
			t.Fatalf("page %d: unexpected cursor filter %v", i+2, f[0])
		}
	}
}

func TestQueryBuilder_Pagination_DescUsesLt(t *testing.T) {
	c, getBodies := pagingServer(t, [][]map[string]any{
		{{"id": "z"}, {"id": "y"}},
		{{"id": "x"}},
	})

	page, err := c.From("clicks").Select("id").OrderBy("id", "desc").Limit(2).FetchUntyped(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := page.Next(context.Background()); err != nil {
		t.Fatal(err)
	}

	f := filtersOf(t, getBodies()[1])
	if len(f) != 1 || f[0]["op"] != "lt" || f[0]["value"] != "y" {
		t.Fatalf("desc cursor filter wrong: %v", f)
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

func TestQueryBuilder_Pagination_TypedInt64CursorKeepsPrecision(t *testing.T) {
	type idRow struct {
		ID int64 `json:"id"`
	}
	const bigID = int64(9007199254740993) // 2^53 + 1: float64 round-trip corrupts it
	c, getBodies := pagingServer(t, [][]map[string]any{
		{{"id": 1}, {"id": bigID}},
		{},
	})

	q := c.From("clicks").Select("id").OrderBy("id", "asc").Limit(2)
	page, err := FetchTyped[idRow](context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := page.Next(context.Background()); err != nil {
		t.Fatal(err)
	}

	f := filtersOf(t, getBodies()[1])
	if len(f) != 1 {
		t.Fatalf("want 1 cursor filter, got %v", f)
	}
	// json.Number survives the round-trip; float64 would have sent ...992.
	if got := fmt.Sprint(f[0]["value"]); got != "9007199254740993" {
		t.Fatalf("cursor value lost precision: %s", got)
	}
}

// TestQueryBuilder_Pagination_UntypedCursorFloat64Ceiling documents the known
// ceiling on the untyped path: rows decode to float64, so an integer cursor
// past 2^53 loses precision before pagination sees it (same as the TS SDK's
// JS-number ceiling). Use FetchTyped or codegen structs past 2^53.
func TestQueryBuilder_Pagination_UntypedCursorFloat64Ceiling(t *testing.T) {
	c, getBodies := pagingServer(t, [][]map[string]any{
		{{"id": 1}, {"id": int64(9007199254740993)}},
		{},
	})

	page, err := c.From("clicks").Select("id").OrderBy("id", "asc").Limit(2).FetchUntyped(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := page.Next(context.Background()); err != nil {
		t.Fatal(err)
	}

	f := filtersOf(t, getBodies()[1])
	if len(f) != 1 {
		t.Fatalf("want 1 cursor filter, got %v", f)
	}
	// float64 rounds 2^53+1 down to 2^53 — the documented untyped ceiling.
	if got := fmt.Sprint(f[0]["value"]); got != "9007199254740992" {
		t.Fatalf("untyped ceiling changed (update docs if intentional): %s", got)
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
