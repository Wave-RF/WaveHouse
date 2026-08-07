package wavehouse

import (
	"context"
	"encoding/json"
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
