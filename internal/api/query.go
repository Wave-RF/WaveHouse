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
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// QueryHandler handles POST /v1/query.
type QueryHandler struct {
	CHConn      driver.Conn
	Cache       *cache.TieredCache
	DefaultTTL  time.Duration
	PolicyStore *policy.Store
	sf          singleflight.Group
}

func NewQueryHandler(conn driver.Conn, c *cache.TieredCache, defaultTTL time.Duration) *QueryHandler {
	return &QueryHandler{CHConn: conn, Cache: c, DefaultTTL: defaultTTL}
}

type queryRequest struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params,omitempty"`
}

func (h *QueryHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Raw SQL is restricted when a policy store is configured.
	if h.PolicyStore != nil {
		role := RoleFromContext(r.Context())
		if role != "" && role != "admin" && role != "service" {
			// Check if the role has raw_sql permission on any table.
			p := h.PolicyStore.Get()
			if p != nil {
				claims, _ := ClaimsFromContext(r.Context())
				allowed := false
				for table := range p.Tables {
					perms := policy.Evaluate(p, role, table, "select", claims)
					if perms.RawSQL {
						allowed = true
						break
					}
				}
				if !allowed {
					writeJSONError(w, http.StatusForbidden, "raw SQL queries require admin role")
					return
				}
			}
		}
	}

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

// isMutation reports whether sql's leading keyword is a non-SELECT statement
// — i.e. one that returns no result set and must go through Exec, not Query.
// Leading whitespace and SQL line/block comments are skipped; the first
// alphabetic token is matched case-insensitively against mutationVerbs.
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
	_, ok := mutationVerbs[strings.ToUpper(s[:end])]
	return ok
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
