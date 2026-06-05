package query

import (
	"errors"
	"fmt"
)

// The errors below distinguish an authorization failure (the role is not
// permitted to reference a column or run an aggregation — map to HTTP 403) from
// a malformed query (unknown column, bad operator — map to HTTP 400). Build
// returns them so a handler can pick the right status without re-deriving the
// policy decision. See StructuredQueryHandler.Handle for the mapping.

// ErrNoReadableColumns is returned by Build for a SelectAll read when the role
// may select the table but its column allowlist permits no columns at all.
// Rather than fall back to a bare SELECT * — the fail-open behind #223 — Build
// refuses the query (→ 403). This is distinct from ErrEmptyProjection: the caller
// explicitly asked for all columns and is entitled to none. Mirrors the stream
// path, where a fully column-restricted role receives an empty event.
var ErrNoReadableColumns = errors.New("no columns readable for role")

// ErrEmptyProjection is returned by Build when a query selects nothing — no
// columns, no aggregations, and SelectAll is false. That is a request for no
// data, so the handler returns an empty result (HTTP 200 []) rather than building
// an invalid SELECT with no projection. Omitting columns is therefore safe-by-
// default: you get nothing unless you name columns or set SelectAll.
var ErrEmptyProjection = errors.New("query selects no columns")

// ErrColumnsAndSelectAll is returned when a query sets both an explicit Columns
// list and SelectAll — an ambiguous request the handler maps to 400. Pick one:
// name the columns, or ask for all of them.
var ErrColumnsAndSelectAll = errors.New("columns and select_all are mutually exclusive")

// ForbiddenColumnError reports that a query referenced a column the role's
// allowlist denies. It can surface from any clause — projection, aggregation
// argument, filter, group_by, order_by, or time_range — because Build authorizes
// every column reference, not just the SELECT list.
type ForbiddenColumnError struct {
	Column string
}

func (e *ForbiddenColumnError) Error() string {
	return fmt.Sprintf("column %q not allowed", e.Column)
}

// ForbiddenAggregationError reports that a query used an aggregation function
// the role's policy denies (allowed_aggregations / denied_aggregations).
type ForbiddenAggregationError struct {
	Fn string
}

func (e *ForbiddenAggregationError) Error() string {
	return fmt.Sprintf("aggregation %q not allowed", e.Fn)
}
