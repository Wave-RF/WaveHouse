package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// QueryHandler handles POST /v1/admin/query.
//
// Authorization for this surface is enforced entirely at the router (the
// /v1/admin/* RequireRole("admin","service") gate in NewRouter). The
// handler trusts that any request reaching it holds an admin-equivalent
// role — there is no per-statement scope check (a full SQL parser would be
// needed to authorize predicates), and the policy engine is not consulted
// here. See internal/api/router.go for the role-gate rationale.
type QueryHandler struct {
	CHConn     driver.Conn
	Cache      *cache.TieredCache
	DefaultTTL time.Duration
	sf         singleflight.Group
}

func NewQueryHandler(conn driver.Conn, c *cache.TieredCache, defaultTTL time.Duration) *QueryHandler {
	return &QueryHandler{CHConn: conn, Cache: c, DefaultTTL: defaultTTL}
}

type queryRequest struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params,omitempty"`
}

func (h *QueryHandler) Handle(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.SQL == "" {
		writeJSONError(w, http.StatusBadRequest, "missing sql")
		return
	}

	// Mutations bypass cache + singleflight entirely. Caching `[]` under
	// the SQL cache key would let a subsequent identical mutation hit the
	// cache and skip ClickHouse within DefaultTTL — silent data loss on
	// the second TRUNCATE/INSERT/etc. Singleflight collapsing concurrent
	// identical mutations would drop one of two writes the caller asked
	// for. Neither is what the caller meant.
	if isMutation(req.SQL) {
		queryCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		result, err := h.executeQuery(queryCtx, req.SQL, req.Params)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		data, err := json.Marshal(result)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "MISS")
		_, _ = w.Write(data)
		return
	}

	cacheKey := queryCacheKey(req.SQL, req.Params)

	// Try cache.
	if h.Cache != nil {
		if data, _, err := h.Cache.Get(r.Context(), cacheKey); err == nil && data != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	// Execute query with singleflight to protect ClickHouse from thundering herds.
	v, err, _ := h.sf.Do(cacheKey, func() (interface{}, error) {
		queryCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		result, err := h.executeQuery(queryCtx, req.SQL, req.Params)
		if err != nil {
			return nil, err
		}

		data, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}

		if h.Cache != nil {
			_ = h.Cache.Set(r.Context(), cacheKey, data, h.DefaultTTL)
		}
		return data, nil
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(v.([]byte))
}

func (h *QueryHandler) executeQuery(ctx context.Context, sql string, params []any) ([]map[string]any, error) {
	if isMutation(sql) {
		if err := h.CHConn.Exec(ctx, sql, params...); err != nil {
			return nil, err
		}
		return []map[string]any{}, nil
	}

	rows, err := h.CHConn.Query(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	columns := rows.ColumnTypes()
	// Initialize as empty (not nil) so a zero-row result marshals to `[]`,
	// not `null`. The SDK does `data!.length` on the response; a `null`
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
// than Query (see executeQuery). Sourced from the ClickHouse statement
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
// silently succeed, and cache `[]` — the same silent-data-loss class the
// mutation-cache-bypass fix closed for TRUNCATE.
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

func queryCacheKey(sql string, params []any) string {
	h := sha256.New()
	h.Write([]byte(sql))
	for _, p := range params {
		_, _ = fmt.Fprintf(h, "\x00%v", p)
	}
	return "query:" + hex.EncodeToString(h.Sum(nil))
}
