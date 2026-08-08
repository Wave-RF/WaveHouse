package wavehouse

import "context"

// ── Structured query AST (matches backend wire format) ────────────────────

// StructuredQuery is the wire format for POST /v1/query.
type StructuredQuery struct {
	// Columns to project. A literal "*" is a column named "*", not a wildcard.
	// Omitting columns (with no aggregations and no select_all) selects nothing.
	Columns []string `json:"columns,omitempty"`
	// SelectAll requests every column the caller's role may read.
	// Mutually exclusive with a non-empty Columns list.
	SelectAll bool `json:"select_all,omitempty"`
	// Aggregations (count, sum, avg, etc.).
	Aggregations []Aggregation `json:"aggregations,omitempty"`
	// Filters (WHERE conditions, ANDed).
	Filters []QueryFilter `json:"filters,omitempty"`
	// GroupBy columns.
	GroupBy []string `json:"group_by,omitempty"`
	// OrderBy clauses.
	OrderBy []OrderClause `json:"order_by,omitempty"`
	// Limit caps the result set.
	Limit *int `json:"limit,omitempty"`
	// TimeRange filters by a time window.
	TimeRange *TimeRange `json:"time_range,omitempty"`
}

// Aggregation describes a single aggregation (e.g. count, sum).
type Aggregation struct {
	Fn     string `json:"fn"`
	Column string `json:"column"`
	Alias  string `json:"alias"`
}

// QueryFilter describes a single WHERE condition.
type QueryFilter struct {
	Column string `json:"column"`
	Op     string `json:"op"`
	Value  any    `json:"value"`
}

// OrderClause describes a single ORDER BY clause.
type OrderClause struct {
	Column string `json:"column"`
	Dir    string `json:"dir"` // "asc" or "desc"
}

// TimeRange filters by a time window on a column.
type TimeRange struct {
	Column string `json:"column"`
	Since  string `json:"since"`
	Until  string `json:"until,omitempty"`
}

// FilterOp is an SDK-facing filter operator.
type FilterOp string

const (
	OpEq      FilterOp = "="
	OpNeq     FilterOp = "!="
	OpGt      FilterOp = ">"
	OpGte     FilterOp = ">="
	OpLt      FilterOp = "<"
	OpLte     FilterOp = "<="
	OpIn      FilterOp = "in"
	OpLike    FilterOp = "like"
	OpNotLike FilterOp = "not_like"
)

// opMap translates SDK operators to backend wire tokens.
var opMap = map[FilterOp]string{
	OpEq:      "eq",
	OpNeq:     "neq",
	OpGt:      "gt",
	OpGte:     "gte",
	OpLt:      "lt",
	OpLte:     "lte",
	OpIn:      "in",
	OpLike:    "like",
	OpNotLike: "not_like",
}

// ── Schema types ──────────────────────────────────────────────────────────

// Column describes a single column in a table schema.
type Column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	IsNullable bool   `json:"is_nullable"`
	HasDefault bool   `json:"has_default"`
}

// TableSchema describes a table's schema.
type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// Schemas maps table names to their schemas.
type Schemas map[string]TableSchema

// ── Insert result ─────────────────────────────────────────────────────────

// InsertRecordResult is a per-record outcome from a batch insert.
type InsertRecordResult struct {
	Index     int    `json:"index"`
	OK        *bool  `json:"ok,omitempty"`
	Duplicate *bool  `json:"duplicate,omitempty"`
	Error     string `json:"error,omitempty"`
}

// InsertResult is the outcome of an insert operation.
type InsertResult struct {
	OK         bool                 `json:"ok"`
	Duplicate  *bool                `json:"duplicate,omitempty"`
	Total      *int                 `json:"total,omitempty"`
	Succeeded  *int                 `json:"succeeded,omitempty"`
	Failed     *int                 `json:"failed,omitempty"`
	Duplicates *int                 `json:"duplicates,omitempty"`
	Results    []InsertRecordResult `json:"results,omitempty"`
}

// ── DLQ types ─────────────────────────────────────────────────────────────

// DLQStats describes dead-letter-queue statistics.
type DLQStats struct {
	Tables map[string]int `json:"tables"`
	Total  int            `json:"total"`
}

// ── Pipe types ────────────────────────────────────────────────────────────

// Pipe describes a named query pipe definition.
type Pipe struct {
	Name         string     `json:"name"`
	SQL          string     `json:"sql"`
	Parameters   []ParamDef `json:"parameters,omitempty"`
	Description  string     `json:"description,omitempty"`
	AllowedRoles []string   `json:"allowed_roles,omitempty"`
}

// ParamDef describes a pipe parameter.
type ParamDef struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
	Default  any    `json:"default,omitempty"`
}

// ── Policy types ──────────────────────────────────────────────────────────

// Policy describes the server's access-control policy.
type Policy struct {
	DefaultRole string `json:"default_role,omitempty"`
	// AdminRole is the role granted full access and the allowlist bypass.
	// Empty means the server's default ("admin") applies.
	AdminRole string                 `json:"admin_role,omitempty"`
	Tables    map[string]TablePolicy `json:"tables"`
}

// TablePolicy describes per-table access control.
type TablePolicy struct {
	Select map[string]RolePermissions `json:"select,omitempty"`
	Insert map[string]RolePermissions `json:"insert,omitempty"`
}

// RolePermissions describes a role's access to a table.
type RolePermissions struct {
	AllowColumns        []string                `json:"allow_columns,omitempty"`
	DenyColumns         []string                `json:"deny_columns,omitempty"`
	Filter              map[string]PolicyFilter `json:"filter,omitempty"`
	Check               map[string]PolicyFilter `json:"check,omitempty"`
	AllowedAggregations []string                `json:"allowed_aggregations,omitempty"`
	DeniedAggregations  []string                `json:"denied_aggregations,omitempty"`
	MaxRows             *int                    `json:"max_rows,omitempty"`
	MaxExecutionTime    any                     `json:"max_execution_time,omitempty"`
	MaxRowsToRead       *int64                  `json:"max_rows_to_read,omitempty"`
	MaxMemoryUsage      any                     `json:"max_memory_usage,omitempty"`
}

// PolicyFilter describes a policy filter predicate. Fields are pointers with
// omitempty so an intentional empty-string comparison (e.g. Eq pointing at "")
// is sent as "", while an unset operator is omitted entirely — never null —
// matching the server's absent-operator semantics.
type PolicyFilter struct {
	Eq  *string `json:"_eq,omitempty"`
	Neq *string `json:"_neq,omitempty"`
	Gt  *string `json:"_gt,omitempty"`
	Lt  *string `json:"_lt,omitempty"`
	In  *string `json:"_in,omitempty"`
}

// ValidationResult is the response from policy validation.
type ValidationResult struct {
	Valid bool `json:"valid"`
}

// ── Streaming types ───────────────────────────────────────────────────────

// StreamStatus represents the connection state of a stream.
type StreamStatus string

const (
	StatusConnecting   StreamStatus = "connecting"
	StatusLive         StreamStatus = "live"
	StatusReconnecting StreamStatus = "reconnecting"
	StatusClosed       StreamStatus = "closed"
)

// StreamEvent is a single event from an SSE stream.
type StreamEvent struct {
	Table     string         `json:"table"`
	Timestamp string         `json:"timestamp"`
	Data      map[string]any `json:"data"`
}

// StreamSubscriber receives events from a stream.
type StreamSubscriber struct {
	// Initial is called once with historical backfill data (live queries only).
	Initial func(rows []map[string]any, err error)
	// Next is called for each live event.
	Next func(event StreamEvent)
	// Status is called when the connection status changes.
	Status func(status StreamStatus)
	// Error is called on stream errors.
	Error func(err error)
}

// StreamOptions configures a stream.
type StreamOptions struct {
	// Since is an RFC3339 timestamp for gap-fill replay.
	Since string
}

// ── Fetch/page types ──────────────────────────────────────────────────────

// Page wraps a result set with pagination metadata.
type Page[T any] struct {
	// Data is the result rows.
	Data []T
	// HasMore is true if more rows may be available.
	HasMore bool
	// Next fetches the next page. Nil when no cursor is available.
	Next func(ctx context.Context) (*Page[T], error)
}
