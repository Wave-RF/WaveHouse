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

// ErrNoReadableColumns is returned by Build for a full-row read (no explicit
// columns and no aggregations) when the role may select the table but its column
// allowlist permits no columns at all. Rather than fall back to a bare SELECT *
// — the fail-open behind #223 — Build refuses the query. This mirrors the stream
// path, where a fully column-restricted role receives an empty event rather than
// the raw row.
var ErrNoReadableColumns = errors.New("no columns readable for role")

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
