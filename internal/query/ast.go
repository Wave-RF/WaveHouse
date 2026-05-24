package query

// StructuredQuery represents a type-safe query AST that gets translated to SQL.
type StructuredQuery struct {
	Columns      []string      `json:"columns,omitempty"`
	Aggregations []Aggregation `json:"aggregations,omitempty"`
	Filters      []Filter      `json:"filters,omitempty"`
	GroupBy      []string      `json:"group_by,omitempty"`
	OrderBy      []OrderClause `json:"order_by,omitempty"`
	Limit        int           `json:"limit,omitempty"`
	TimeRange    *TimeRange    `json:"time_range,omitempty"`
}

// Aggregation represents an aggregation function call.
type Aggregation struct {
	Fn     string `json:"fn"`     // count, sum, avg, min, max, countDistinct, etc.
	Column string `json:"column"` // "*" for count(*)
	Alias  string `json:"alias"`  // result column name
}

// Filter represents a WHERE condition.
type Filter struct {
	Column string `json:"column"`
	Op     string `json:"op"`    // eq, neq, gt, gte, lt, lte, in, like
	Value  any    `json:"value"` // scalar or array (for "in")
}

// OrderClause specifies sort order.
type OrderClause struct {
	Column string `json:"column"`
	Dir    string `json:"dir"` // "asc" or "desc"
}

// TimeRange constrains a query to a time window.
type TimeRange struct {
	Column string `json:"column"` // timestamp column name
	Since  string `json:"since"`  // RFC3339 or relative (e.g. "1h", "30m")
	Until  string `json:"until"`  // RFC3339 or empty (=now)
}
