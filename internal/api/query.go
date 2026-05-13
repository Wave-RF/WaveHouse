package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/google/uuid"
)

// Regex to detect mutating queries
var mutationRe = regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|ALTER|DROP|TRUNCATE)\b`)

// Unified table extraction regex
var (
	tableExtractionRe = regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b`)
	safeIdentifierRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// extractCacheTags parses raw SQL to find table names, dedupes them, and validates
// them against safe identifier rules to comply with repository security standards.
func extractCacheTags(sql string) []string {
	matches := tableExtractionRe.FindAllStringSubmatch(sql, -1)
	var tags []string
	seen := make(map[string]bool)
	for _, m := range matches {
		if len(m) > 1 {
			tbl := m[1]
			// Validate safe identifier before using it as a cache tag
			if !seen[tbl] && safeIdentifierRe.MatchString(tbl) {
				seen[tbl] = true
				tags = append(tags, tbl)
			}
		}
	}
	return tags
}

// QueryHandler handles POST /v1/query.
type QueryHandler struct {
	CHConn      driver.Conn
	Cache       *cache.TieredCache
	DefaultTTL  time.Duration
	PolicyStore *policy.Store
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

	queryCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Execute immediately (Bypassing the Cache)
	result, err := h.executeQuery(queryCtx, req.SQL, req.Params)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// If this raw SQL was a mutation, extract the table names and invalidate them.
	if h.Cache != nil && mutationRe.MatchString(req.SQL) {
		tags := extractCacheTags(req.SQL)
		if len(tags) > 0 {
			_ = h.Cache.InvalidateByTags(r.Context(), tags)
		}
	}

	data, err := json.Marshal(result)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "BYPASS")
	_, _ = w.Write(data)
}

func (h *QueryHandler) executeQuery(ctx context.Context, sql string, params []any) ([]map[string]any, error) {
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
