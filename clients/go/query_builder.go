package wavehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
)

// DefaultLimit is applied when no explicit limit is set — deliberately tighter
// than the backend's DefaultMaxRows (10000) safety cap.
const DefaultLimit = 1000

// queryState is the immutable core of a QueryBuilder.
type queryState struct {
	table        string
	columns      []string
	selectAll    bool
	aggregations []Aggregation
	filters      []QueryFilter
	groupBy      []string
	orderBy      []OrderClause
	limit        *int
	timeRange    *TimeRange
	cacheTTL     *int // ponytail: client-side only, not sent to server (#280)
}

// QueryBuilder builds structured queries. Immutable — every chain method
// returns a new builder. Use Fetch or FetchUntyped to execute.
type QueryBuilder struct {
	ctx          httpContext
	createStream func(table string, opts *StreamOptions) *StreamController
	state        queryState
}

func (q *QueryBuilder) clone(mutate func(*queryState)) *QueryBuilder {
	s := q.state
	// Deep-copy slices so mutations don't alias.
	s.columns = append([]string(nil), s.columns...)
	s.aggregations = append([]Aggregation(nil), s.aggregations...)
	s.filters = append([]QueryFilter(nil), s.filters...)
	s.groupBy = append([]string(nil), s.groupBy...)
	s.orderBy = append([]OrderClause(nil), s.orderBy...)
	mutate(&s)
	return &QueryBuilder{ctx: q.ctx, createStream: q.createStream, state: s}
}

// Select appends columns to the projection.
func (q *QueryBuilder) Select(columns ...string) *QueryBuilder {
	return q.clone(func(s *queryState) {
		s.columns = append(s.columns, columns...)
	})
}

// SelectAll requests every column the caller's role may read.
func (q *QueryBuilder) SelectAll() *QueryBuilder {
	return q.clone(func(s *queryState) {
		s.selectAll = true
	})
}

// Where adds a filter condition.
func (q *QueryBuilder) Where(column string, op FilterOp, value any) *QueryBuilder {
	wireOp, ok := opMap[op]
	if !ok {
		wireOp = string(op)
	}
	return q.clone(func(s *queryState) {
		s.filters = append(s.filters, QueryFilter{Column: column, Op: wireOp, Value: value})
	})
}

// Count adds a COUNT aggregation.
func (q *QueryBuilder) Count(column, alias string) *QueryBuilder {
	if column == "" {
		column = "*"
	}
	if alias == "" {
		alias = "count"
	}
	return q.addAgg("count", column, alias)
}

// Sum adds a SUM aggregation.
func (q *QueryBuilder) Sum(column, alias string) *QueryBuilder {
	return q.aggDefault("sum", "sum_", column, alias)
}

// Avg adds an AVG aggregation.
func (q *QueryBuilder) Avg(column, alias string) *QueryBuilder {
	return q.aggDefault("avg", "avg_", column, alias)
}

// Min adds a MIN aggregation.
func (q *QueryBuilder) Min(column, alias string) *QueryBuilder {
	return q.aggDefault("min", "min_", column, alias)
}

// Max adds a MAX aggregation.
func (q *QueryBuilder) Max(column, alias string) *QueryBuilder {
	return q.aggDefault("max", "max_", column, alias)
}

// CountDistinct adds a COUNT DISTINCT aggregation.
func (q *QueryBuilder) CountDistinct(column, alias string) *QueryBuilder {
	return q.aggDefault("countDistinct", "count_distinct_", column, alias)
}

// Aggregate adds a custom aggregation function.
func (q *QueryBuilder) Aggregate(fn, column, alias string) *QueryBuilder {
	return q.addAgg(fn, column, alias)
}

// GroupBy appends columns to the GROUP BY clause.
func (q *QueryBuilder) GroupBy(columns ...string) *QueryBuilder {
	return q.clone(func(s *queryState) {
		s.groupBy = append(s.groupBy, columns...)
	})
}

// OrderBy appends an ORDER BY clause. dir defaults to "asc".
func (q *QueryBuilder) OrderBy(column, dir string) *QueryBuilder {
	if dir == "" {
		dir = "asc"
	}
	return q.clone(func(s *queryState) {
		s.orderBy = append(s.orderBy, OrderClause{Column: column, Dir: dir})
	})
}

// Limit sets the maximum number of rows to return.
func (q *QueryBuilder) Limit(n int) *QueryBuilder {
	return q.clone(func(s *queryState) {
		s.limit = &n
	})
}

// TimeRange filters by a time window. since and until accept RFC3339 timestamps
// or relative durations ("1h", "30m", "7d", "2w").
func (q *QueryBuilder) TimeRange(column, since, until string) *QueryBuilder {
	return q.clone(func(s *queryState) {
		s.timeRange = &TimeRange{Column: column, Since: since, Until: until}
	})
}

// CacheTTL records a desired result-cache TTL. Currently client-side only —
// the server derives TTLs adaptively (#280).
func (q *QueryBuilder) CacheTTL(seconds int) *QueryBuilder {
	return q.clone(func(s *queryState) {
		s.cacheTTL = &seconds
	})
}

// FetchTyped executes the query and decodes rows into []T.
func FetchTyped[Row any](ctx context.Context, q *QueryBuilder) (*Page[Row], error) {
	limit := DefaultLimit
	if q.state.limit != nil {
		limit = *q.state.limit
	}
	ast := q.buildAST(limit)

	var rows []Row
	if err := doRequest(ctx, q.ctx, requestOptions{
		method: "POST",
		path:   "/v1/query",
		params: url.Values{"table": {q.state.table}},
		body:   ast,
	}, &rows); err != nil {
		return nil, err
	}

	hasMore := limit > 0 && len(rows) >= limit
	page := &Page[Row]{Data: rows, HasMore: hasMore}

	// Attach Next whenever we have an order column to build a cursor from.
	// This doesn't check that the order column is present in the row
	// projection — a Select() that omits it means fetchNextTyped can't find
	// a cursor value and will quietly return an empty page (matches the TS
	// SDK's QueryBuilder.fetch()/_fetchNext(), which has the same limitation).
	if hasMore && len(q.state.orderBy) > 0 {
		page.Next = func(ctx context.Context) (*Page[Row], error) {
			return fetchNextTyped[Row](ctx, q, rows, limit)
		}
	}

	return page, nil
}

// FetchUntyped executes the query and returns rows as []map[string]any.
func (q *QueryBuilder) FetchUntyped(ctx context.Context) (*Page[map[string]any], error) {
	return FetchTyped[map[string]any](ctx, q)
}

// Stream opens a live SSE event stream for this query's table.
// Filters and column projections are applied client-side.
func (q *QueryBuilder) Stream(opts *StreamOptions) *StreamController {
	raw := q.createStream(q.state.table, opts)
	if len(q.state.filters) == 0 && len(q.state.columns) == 0 {
		return raw
	}
	return newFilteredStreamController(raw, q.state.filters, q.state.columns)
}

// LiveQuery starts a live query: fetches historical data, then streams live
// updates. The subscriber's Initial is called once, then Next fires for each
// live event. Returns a LiveQuery handle with a Close method.
func (q *QueryBuilder) LiveQuery(sub *StreamSubscriber, opts *StreamOptions) *LiveQueryHandle {
	stream := q.Stream(opts)
	fetchFn := func(ctx context.Context) ([]map[string]any, error) {
		page, err := q.FetchUntyped(ctx)
		if err != nil {
			return nil, err
		}
		return page.Data, nil
	}
	return newLiveQuery(stream, fetchFn, sub)
}

func (q *QueryBuilder) aggDefault(fn, prefix, column, alias string) *QueryBuilder {
	if alias == "" {
		alias = prefix + column
	}
	return q.addAgg(fn, column, alias)
}

func (q *QueryBuilder) addAgg(fn, column, alias string) *QueryBuilder {
	return q.clone(func(s *queryState) {
		s.aggregations = append(s.aggregations, Aggregation{Fn: fn, Column: column, Alias: alias})
	})
}

func (q *QueryBuilder) buildAST(effectiveLimit int) *StructuredQuery {
	ast := &StructuredQuery{}
	hasColumns := len(q.state.columns) > 0
	hasAggs := len(q.state.aggregations) > 0

	// Projection: explicit select_all, then explicit columns, else — for a bare
	// query with no projection and no aggregations — default to select_all so
	// from(t).fetch() returns rows.
	switch {
	case q.state.selectAll:
		ast.SelectAll = true
	case hasColumns:
		ast.Columns = q.state.columns
	case !hasAggs:
		ast.SelectAll = true
	}

	if hasAggs {
		ast.Aggregations = q.state.aggregations
	}
	if len(q.state.filters) > 0 {
		ast.Filters = q.state.filters
	}
	if len(q.state.groupBy) > 0 {
		ast.GroupBy = q.state.groupBy
	}
	if len(q.state.orderBy) > 0 {
		ast.OrderBy = q.state.orderBy
	}
	ast.Limit = &effectiveLimit
	if q.state.timeRange != nil {
		ast.TimeRange = q.state.timeRange
	}
	return ast
}

func fetchNextTyped[Row any](ctx context.Context, q *QueryBuilder, prevRows []Row, limit int) (*Page[Row], error) {
	if len(q.state.orderBy) == 0 {
		return &Page[Row]{}, nil
	}
	cursor := q.state.orderBy[0]

	// Extract the last row's value for the cursor column.
	lastRow := any(prevRows[len(prevRows)-1])
	m, ok := lastRow.(map[string]any)
	if !ok {
		// ponytail: marshal/unmarshal round-trip to get a map — optimize with reflect if perf matters.
		// UseNumber keeps typed int64 cursor values exact past 2^53. The
		// untyped path (FetchUntyped / TableRef.Fetch) doesn't get this
		// protection: its rows were already decoded to float64 by
		// encoding/json, so precision above 2^53 is gone before we get here —
		// the same ceiling the TS SDK has with JS numbers. Use FetchTyped (or
		// codegen structs — their 64-bit int columns are int64/uint64, and
		// 128/256-bit are json.Number) when paging on >2^53 integer cursors.
		raw, _ := json.Marshal(lastRow)
		m = make(map[string]any)
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		_ = dec.Decode(&m)
	}
	lastValue, exists := m[cursor.Column]
	if !exists {
		// Cursor column wasn't in the projection (e.g. Select() omitted it) —
		// no cursor value to page from, so end pagination quietly rather than
		// erroring. Matches the TS SDK's _fetchNext().
		return &Page[Row]{}, nil
	}

	cursorOp := "gt"
	if cursor.Dir == "desc" {
		cursorOp = "lt"
	}

	next := q.clone(func(s *queryState) {
		// Replace an existing cursor filter instead of appending — otherwise
		// page N carries N stacked filters on the cursor column.
		for i := range s.filters {
			if s.filters[i].Column == cursor.Column && s.filters[i].Op == cursorOp {
				s.filters[i].Value = lastValue
				return
			}
		}
		s.filters = append(s.filters, QueryFilter{
			Column: cursor.Column,
			Op:     cursorOp,
			Value:  lastValue,
		})
	})
	return FetchTyped[Row](ctx, next)
}
