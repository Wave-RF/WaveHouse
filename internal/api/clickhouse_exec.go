package api

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// executeCHQuery runs sql against the native-protocol driver conn,
// classifying by leading SQL verb to pick the Exec-vs-Query path —
// clickhouse-go's driver.Query() errors on statements that return no
// result set, so the dispatch is correctness, not optimisation. Returns
// a row-array suitable for JSON marshalling; mutations marshal to `[]`,
// preserving the "always-an-array" response shape callers depend on.
//
// Used by the structured-query and pipes handlers — those are the cached
// read paths that need explicit Query/Exec dispatch and per-row scanning.
// The raw-SQL endpoint (/v1/admin/query) proxies straight to ClickHouse
// over HTTP and never calls this; see internal/api/query.go.
func executeCHQuery(ctx context.Context, conn driver.Conn, sql string, params []any) ([]map[string]any, error) {
	if isMutation(sql) {
		if err := conn.Exec(ctx, sql, params...); err != nil {
			return nil, err
		}
		return []map[string]any{}, nil
	}

	rows, err := conn.Query(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	columns := rows.ColumnTypes()
	// Initialize as empty (not nil) so a zero-row result marshals to `[]`,
	// not `null`. SDK consumers do `data!.length` on the response; a `null`
	// crashes the client on every empty fetch.
	results := []map[string]any{}

	for rows.Next() {
		valPtrs := make([]any, len(columns))
		for i, col := range columns {
			valPtrs[i] = reflect.New(col.ScanType()).Interface()
		}
		if err := rows.Scan(valPtrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any)
		for i, col := range columns {
			row[col.Name()] = reflect.ValueOf(valPtrs[i]).Elem().Interface()
		}
		results = append(results, transformRow(row))
	}
	return results, nil
}

// mutationVerbs is a set of SQL leading keywords that don't return a result
// set — anything that mutates schema or data. Routed through Exec rather
// than Query (see executeCHQuery). Sourced from the ClickHouse statement
// reference: DML, DDL, role/privilege management, and runtime control
// (SYSTEM/KILL/SET). Read-only verbs (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN/
// EXISTS) intentionally fall through to the default Query path.
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

// isMutation reports whether sql's leading statement is a non-SELECT — i.e.
// one that returns no result set and must go through Exec, not Query.
// Leading whitespace and SQL line/block comments are skipped, then the first
// alphabetic token is matched case-insensitively against mutationVerbs. A
// leading WITH clause (CTE) routes through a paren-aware scan because
// ClickHouse accepts `WITH cte AS (...) INSERT INTO t SELECT * FROM cte` as
// equivalent to `INSERT INTO t WITH cte AS (...) SELECT * FROM cte` (see
// https://clickhouse.com/docs/sql-reference/statements/insert-into). Without
// the skip, the WITH form would classify as a read, route through Query,
// silently succeed, and return `[]` — the same silent-success class that
// motivated the original cache-bypass guard.
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

// containsMutationVerbAtTopLevel scans s for a mutation verb at paren-depth 0,
// stepping over SQL string literals (`'…'` with `”` escape), quoted
// identifiers (`"…"` and “ `…` “), and parenthesized CTE subqueries so that
// keywords inside a CTE body never count. Non-mutation tokens (CTE names,
// AS / MATERIALIZED / RECURSIVE, SELECT, …) are skipped silently; the absence
// of a mutation verb at top level means the WITH statement is a read.
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
				if _, ok := mutationVerbs[strings.ToUpper(s[start:i])]; ok {
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

// stripLeadingSQLComments trims whitespace plus line comments (`-- …` and
// MySQL-compat `# …`, both accepted by ClickHouse) and `/* block */`
// comments from the front of sql, returning the remainder with no leading
// whitespace. Unclosed block comments swallow the rest of the string —
// matches what ClickHouse itself would do at parse time.
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
