package api

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// clickhouseDriverErrCode returns the numeric ClickHouse server error code
// from a clickhouse-go error, or "0" for transport-level failures.
func clickhouseDriverErrCode(err error) string {
	if exc, ok := errors.AsType[*clickhouse.Exception](err); ok {
		return strconv.Itoa(int(exc.Code))
	}
	return "0"
}

// executeCHQuery runs sql against the native-protocol driver conn, picking
// Exec vs Query by the leading SQL verb (clickhouse-go's Query errors on
// statements with no result set, so dispatch is correctness, not perf).
// Mutations return `[]` to preserve the always-an-array response shape.
//
// `operation` is the high-level feature name used as the metric/span label
// (structured_query, pipes). Used by the cached read paths only; the raw-SQL
// /v1/admin/query endpoint proxies over HTTP and never calls this.
func executeCHQuery(ctx context.Context, conn driver.Conn, sql string, params []any, operation string) ([]map[string]any, error) {
	verb := "query"
	if isMutation(sql) {
		verb = "exec"
	}

	// Raw SQL is intentionally omitted — it can carry user-provided values
	// that leak PII into long-lived trace storage.
	ctx, span := observability.Tracer().Start(ctx, "clickhouse."+operation)
	span.SetAttributes(
		attribute.String("db.system", "clickhouse"),
		attribute.String("clickhouse.verb", verb),
		attribute.String("clickhouse.operation", operation),
	)
	defer span.End()

	metricAttrs := metric.WithAttributes(
		attribute.String("operation", operation),
	)
	errAttrs := func(code string) metric.MeasurementOption {
		return metric.WithAttributes(
			attribute.String("operation", operation),
			attribute.String("clickhouse_code", code),
		)
	}
	start := time.Now()

	if verb == "exec" {
		if err := conn.Exec(ctx, sql, params...); err != nil {
			observability.ClickHouseDuration.Record(ctx, time.Since(start).Seconds(), metricAttrs)
			observability.ClickHouseErrors.Add(ctx, 1, errAttrs(clickhouseDriverErrCode(err)))
			span.RecordError(err)
			return nil, fmt.Errorf("clickhouse exec: %w", err)
		}
		observability.ClickHouseDuration.Record(ctx, time.Since(start).Seconds(), metricAttrs)
		return []map[string]any{}, nil
	}

	rows, err := conn.Query(ctx, sql, params...)
	if err != nil {
		observability.ClickHouseDuration.Record(ctx, time.Since(start).Seconds(), metricAttrs)
		observability.ClickHouseErrors.Add(ctx, 1, errAttrs(clickhouseDriverErrCode(err)))
		span.RecordError(err)
		return nil, fmt.Errorf("clickhouse query: %w", err)
	}
	defer func() {
		_ = rows.Close()
		observability.ClickHouseDuration.Record(ctx, time.Since(start).Seconds(), metricAttrs)
	}()

	columns := rows.ColumnTypes()
	// Empty slice (not nil) so a zero-row result marshals to `[]`, not `null`.
	results := []map[string]any{}

	for rows.Next() {
		valPtrs := make([]any, len(columns))
		for i, col := range columns {
			valPtrs[i] = reflect.New(col.ScanType()).Interface()
		}
		if err := rows.Scan(valPtrs...); err != nil {
			observability.ClickHouseErrors.Add(ctx, 1, errAttrs(clickhouseDriverErrCode(err)))
			span.RecordError(err)
			return nil, fmt.Errorf("scan clickhouse row: %w", err)
		}
		row := make(map[string]any)
		for i, col := range columns {
			row[col.Name()] = reflect.ValueOf(valPtrs[i]).Elem().Interface()
		}
		results = append(results, transformRow(row))
	}
	// rows.Next() returns false on both completion and mid-stream error;
	// without rows.Err() a partial result silently looks complete.
	if err := rows.Err(); err != nil {
		observability.ClickHouseErrors.Add(ctx, 1, errAttrs(clickhouseDriverErrCode(err)))
		span.RecordError(err)
		return nil, fmt.Errorf("iterate clickhouse rows: %w", err)
	}
	return results, nil
}

// mutationVerbs are SQL leading keywords with no result set — routed through
// Exec, not Query. Sourced from the ClickHouse statement reference (DML, DDL,
// role/privilege, SYSTEM/KILL/SET). Read-only verbs fall through to Query.
var mutationVerbs = map[string]struct{}{
	"INSERT":   {},
	"UPDATE":   {},
	"DELETE":   {},
	"TRUNCATE": {},
	"DROP":     {},
	"ALTER":    {},
	"CREATE":   {},
	"RENAME":   {},
	"EXCHANGE": {},
	"OPTIMIZE": {},
	"REPLACE":  {},
	"GRANT":    {},
	"REVOKE":   {},
	"ATTACH":   {},
	"DETACH":   {},
	"KILL":     {},
	"SET":      {},
	"USE":      {},
	"SYSTEM":   {},
}

// isMutation reports whether sql's leading statement returns no result set
// (must go through Exec). Skips whitespace/comments, matches the first token
// case-insensitively. A leading WITH (CTE) routes through a paren-aware scan
// because ClickHouse accepts `WITH cte AS (...) INSERT INTO t SELECT * FROM
// cte` — without that scan the WITH form would be misclassified as a read,
// route through Query, silently succeed, and return `[]`.
func isMutation(sql string) bool {
	s := stripLeadingSQLComments(sql)
	end := 0
	for end < len(s) {
		c := s[end]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			break
		}
		end++
	}
	if end == 0 {
		return false
	}
	first := strings.ToUpper(s[:end])
	if first != "WITH" {
		_, ok := mutationVerbs[first]
		return ok
	}
	return containsMutationVerbAtTopLevel(s[end:])
}

// nonMutationVerbs is the read counterpart to mutationVerbs. The CTE scanner
// needs the union to identify *which* token starts the statement — otherwise
// a CTE-list identifier matching a mutation verb name (e.g. `system`,
// `alter`) false-positives the classifier.
var nonMutationVerbs = map[string]struct{}{
	"SELECT":   {},
	"SHOW":     {},
	"DESCRIBE": {},
	"DESC":     {},
	"EXPLAIN":  {},
	"EXISTS":   {},
	"CHECK":    {},
}

// containsMutationVerbAtTopLevel scans s for the statement-introducing keyword
// at paren-depth 0, stepping over string literals, quoted identifiers,
// parenthesized CTE subqueries, and comments.
//
// CTE-name suppression: identifiers followed by `AS` or `(` are CTE names,
// not statement keywords — skipped so a CTE alias that spells like a mutation
// verb (`WITH set AS (…) SELECT …`) doesn't false-positive. Among remaining
// tokens, the FIRST known statement keyword (mutation or non-mutation)
// decides — returning based on mutationVerbs membership.
//
// Returns false when no known keyword appears: safer to treat unknown SQL as
// non-mutation (Query surfaces a clear error) than to Exec it (silent empty
// array on a SELECT).
func containsMutationVerbAtTopLevel(s string) bool {
	depth := 0
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == '(':
			depth++
			i++
		case c == ')':
			if depth > 0 {
				depth--
			}
			i++
		case c == '\'':
			i++
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case c == '"' || c == '`':
			q := c
			i++
			for i < len(s) && s[i] != q {
				i++
			}
			if i < len(s) {
				i++
			}
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'):
			start := i
			for i < len(s) {
				c2 := s[i]
				if (c2 < 'A' || c2 > 'Z') && (c2 < 'a' || c2 > 'z') && (c2 < '0' || c2 > '9') && c2 != '_' {
					break
				}
				i++
			}
			if depth == 0 {
				kw := strings.ToUpper(s[start:i])
				// Read keywords must be checked BEFORE the CTE-name
				// lookahead — `SELECT (a, b) FROM …` (tuple syntax) would
				// otherwise be classified as a CTE alias.
				if _, ok := nonMutationVerbs[kw]; ok {
					return false
				}
				if isCTENameLookahead(s, i) {
					continue
				}
				if _, ok := mutationVerbs[kw]; ok {
					return true
				}
			}
		case c == '-' && i+1 < len(s) && s[i+1] == '-', c == '#':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			i += 2
			for i+1 < len(s) {
				if s[i] == '*' && s[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
		default:
			i++
		}
	}
	return false
}

// isCTENameLookahead returns true when the next non-trivial token at or
// after pos is `AS` or `(` — the identifier just before pos is a CTE name.
func isCTENameLookahead(s string, pos int) bool {
	i := pos
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			i++
		case c == '-' && i+1 < len(s) && s[i+1] == '-', c == '#':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			i += 2
			for i+1 < len(s) {
				if s[i] == '*' && s[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
		case c == '(':
			return true
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'):
			end := i
			for end < len(s) {
				c2 := s[end]
				if (c2 < 'A' || c2 > 'Z') && (c2 < 'a' || c2 > 'z') && (c2 < '0' || c2 > '9') && c2 != '_' {
					break
				}
				end++
			}
			return strings.EqualFold(s[i:end], "AS")
		default:
			return false
		}
	}
	return false
}

// stripLeadingSQLComments trims whitespace + leading line/block comments.
// Unclosed block comments swallow the rest of the string (matches ClickHouse).
func stripLeadingSQLComments(sql string) string {
	s := strings.TrimLeft(sql, " \t\r\n")
	for {
		switch {
		case strings.HasPrefix(s, "--"), strings.HasPrefix(s, "#"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = strings.TrimLeft(s[i+1:], " \t\r\n")
			} else {
				return ""
			}
		case strings.HasPrefix(s, "/*"):
			if i := strings.Index(s[2:], "*/"); i >= 0 {
				s = strings.TrimLeft(s[2+i+2:], " \t\r\n")
			} else {
				return ""
			}
		default:
			return s
		}
	}
}

// transformRow converts ClickHouse-specific types to JSON-friendly values.
func transformRow(row map[string]any) map[string]any {
	for k, v := range row {
		switch val := v.(type) {
		case uuid.UUID:
			row[k] = val.String()
		case [16]byte:
			row[k] = uuid.UUID(val).String()
		case time.Time:
			row[k] = val.UTC().Format(time.RFC3339Nano)
		}
	}
	return row
}
