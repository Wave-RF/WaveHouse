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
	"github.com/Wave-RF/BeachHouse/internal/cache"
	"github.com/Wave-RF/BeachHouse/internal/schema"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// QueryHandler handles POST /v1/query.
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
	tenantID := TenantIDFromContext(r.Context())
	if tenantID == "" {
		http.Error(w, `{"error":"no tenant"}`, http.StatusForbidden)
		return
	}

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if req.SQL == "" {
		http.Error(w, `{"error":"missing sql"}`, http.StatusBadRequest)
		return
	}

	// Cache key from tenant + query.
	cacheKey := queryCacheKey(tenantID, req.SQL)

	// Try cache.
	if h.Cache != nil {
		if data, _, err := h.Cache.Get(r.Context(), cacheKey); err == nil && data != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			w.Write(data)
			return
		}
	}

	// Execute query with singleflight to protect ClickHouse from thundering herds.
	v, err, _ := h.sf.Do(cacheKey, func() (interface{}, error) {
		// Need to detach from request context to allow query to complete even if client disconnects.
		// TODO: define max safe duration to run detached to prevent runaway queries.
		queryCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result, err := h.executeQuery(queryCtx, tenantID, req.SQL)
		if err != nil {
			return nil, err
		}

		data, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}

		// Populate cache.
		if h.Cache != nil {
			_ = h.Cache.Set(r.Context(), cacheKey, data, h.DefaultTTL)
		}
		return data, nil
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Write(v.([]byte))
}

func (h *QueryHandler) executeQuery(ctx context.Context, tenantID, sql string) ([]map[string]any, error) {
	tenantUUID, err := uuid.Parse(tenantID)
	if err != nil {
		return nil, fmt.Errorf("invalid tenant UUID: %w", err)
	}

	wrapped := injectTenantFilter(sql)
	rows, err := h.CHConn.Query(ctx, wrapped, tenantUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := rows.ColumnTypes()
	var results []map[string]any

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

// eventsTableRe matches "FROM events" or "JOIN events" (case-insensitive).
var eventsTableRe = regexp.MustCompile(`(?i)\b(FROM|JOIN)\s+events\b`)

// injectTenantFilter wraps the user query in a CTE that pre-filters by tenant_id.
// The user never needs to (and should not) include a tenant_id WHERE clause.
func injectTenantFilter(sql string) string {
	rewritten := eventsTableRe.ReplaceAllString(sql, "${1} __tenant_events")
	return "WITH __tenant_events AS (SELECT * FROM events WHERE tenant_id = toUUID(?)) " + rewritten
}

// transformRow converts ClickHouse-specific types to JSON-friendly values,
// unflattens typed map columns into a nested "data" object, and strips
// internal columns (tenant_id) that clients should not see.
func transformRow(row map[string]any) map[string]any {
	// Convert ClickHouse types to JSON-friendly values.
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

	// Unflatten typed map columns into nested "data" object.
	strData, hasStr := row["str_data"].(map[string]string)
	numData, hasNum := row["num_data"].(map[string]float64)
	boolData, hasBool := row["bool_data"].(map[string]bool)

	if hasStr || hasNum || hasBool {
		if !hasStr {
			strData = nil
		}
		if !hasNum {
			numData = nil
		}
		if !hasBool {
			boolData = nil
		}
		row["data"] = schema.Unflatten(strData, numData, boolData)
		delete(row, "str_data")
		delete(row, "num_data")
		delete(row, "bool_data")
	}

	// Strip internal columns.
	delete(row, "tenant_id")

	return row
}

func queryCacheKey(tenantID, sql string) string {
	h := sha256.Sum256([]byte(tenantID + ":" + sql))
	return "query:" + hex.EncodeToString(h[:])
}
