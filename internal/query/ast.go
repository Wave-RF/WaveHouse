package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
)

// StructuredQuery represents a type-safe query AST that gets translated to SQL.
type StructuredQuery struct {
	// Columns is the explicit list of row columns to project. Empty (omitted, [],
	// "", or null) means "no row columns" — combined with no aggregations and no
	// SelectAll, the query returns nothing (see SelectAll). "*" here is a literal
	// column name, not a wildcard.
	Columns Columns `json:"columns,omitempty"`
	// SelectAll requests every column the caller's role may read (the all-columns
	// wildcard, expanded to the role's allowed/denied columns). It is mutually
	// exclusive with a non-empty Columns. This is the only way to get a full-row
	// read — omitting Columns does NOT select all, so a hidden column can never
	// leak by simply leaving Columns out.
	SelectAll    bool          `json:"select_all,omitempty"`
	Aggregations []Aggregation `json:"aggregations,omitempty"`
	Filters      []Filter      `json:"filters,omitempty"`
	GroupBy      []string      `json:"group_by,omitempty"`
	OrderBy      []OrderClause `json:"order_by,omitempty"`
	Limit        int           `json:"limit,omitempty"`
	TimeRange    *TimeRange    `json:"time_range,omitempty"`
}

// Columns is the list of row columns a query projects. It accepts either a JSON
// array of strings (`["a","b"]`) or a single JSON string (`"a"`, treated as
// `["a"]`) so a hand-written query meaning one column can drop the brackets.
//
// An omitted field, null, "", or [] all decode to an empty list — a request for
// no columns. Column names must be strings: a non-string scalar (number, bool),
// an object, or a non-string array element is rejected rather than coerced. An
// explicit empty-string element (`[""]`) is rejected too — that is a malformed
// column name, distinct from the "no columns" forms above (a whitespace name
// like `[" "]` is a legal, if unusual, ClickHouse column and is kept).
type Columns []string

// UnmarshalJSON implements the string-or-array decoding described on Columns.
func (c *Columns) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		*c = nil
		return nil
	}
	switch trimmed[0] {
	case '"': // single string form: "col" or ""
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return err
		}
		if s == "" {
			*c = nil // "" means "no columns", not a column literally named ""
			return nil
		}
		*c = Columns{s}
		return nil
	case '[': // array form: must be an array of strings
		var arr []string
		if err := json.Unmarshal(trimmed, &arr); err != nil {
			return fmt.Errorf("columns must be a string or array of strings: %w", err)
		}
		if slices.Contains(arr, "") {
			return fmt.Errorf("columns must not contain an empty string")
		}
		*c = Columns(arr)
		return nil
	default:
		return fmt.Errorf("columns must be a string or array of strings")
	}
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
	Since  string `json:"since"`  // RFC3339 or relative (e.g. "1h", "30m", "7d", "2w")
	Until  string `json:"until"`  // RFC3339, relative (e.g. "1h"), or empty (=now)
}
