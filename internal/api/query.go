package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/google/uuid"

	"golang.org/x/sync/singleflight"
)

// qualifiedIdent matches one table reference: an optional `db.` schema prefix
// plus the table name, each segment optionally wrapped in backticks or double
// quotes. Shared with tableExtractionRe so the comma-list capture stays in sync.
const qualifiedIdent = `(?:[` + "`" + `"]?[a-zA-Z_][a-zA-Z0-9_]*[` + "`" + `"]?\.)?[` + "`" + `"]?[a-zA-Z_][a-zA-Z0-9_]*[` + "`" + `"]?`

// tableExtractionRe extracts table names following FROM, JOIN, INTO, UPDATE, or TABLE.
// Capture group 1 is the full comma-separated table list (e.g. "t1, db.t2, `t3`")
// so callers can split it via splitTableList — this covers `FROM a, b` joins that
// a single-table capture would silently leave half-tagged (only `a` invalidated).
var tableExtractionRe = regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+(` + qualifiedIdent + `(?:\s*,\s*` + qualifiedIdent + `)*)`)

// mutationRe detects queries that modify data or schema to trigger cache invalidation.
// Run against cleaned SQL with string literals, comments, AND quoted identifiers
// stripped (see stripForMutationDetect) so that a table or column named after a
// DML keyword (e.g., “ `INSERT` “) doesn't false-positive. \b anchors prevent
// false positives on identifiers such as `insert_time` that share a prefix.
var mutationRe = regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|TRUNCATE|DROP|ALTER|REPLACE)\b`)

// mutationTargetRe extracts the table that a mutation writes to, anchored on
// the DML/DDL (Data Manipulation and Data Definition Language) verb so FROM/JOIN
// tables inside SELECT subqueries are ignored. Used to scope cache invalidation
// to only the written table (e.g., `INSERT INTO target SELECT … FROM source`
// evicts `target`, leaving `source` cache entries intact).
var mutationTargetRe = regexp.MustCompile(`(?i)\b(?:` +
	`INSERT\s+INTO|` +
	`REPLACE\s+INTO|` +
	`UPDATE|` +
	`DELETE\s+FROM|` +
	`TRUNCATE(?:\s+TABLE)?(?:\s+IF\s+EXISTS)?|` +
	`DROP\s+TABLE(?:\s+IF\s+EXISTS)?|` +
	`ALTER\s+TABLE(?:\s+IF\s+EXISTS)?` +
	`)\s+(?:[` + "`" + `"]?[a-zA-Z_][a-zA-Z0-9_]*[` + "`" + `"]?\.)?[` + "`" + `"]?([a-zA-Z_][a-zA-Z0-9_]*)[` + "`" + `"]?`)

// These four regexes below are used to strip out /* comments */, -- comments,
// 'string literals', and 'quoted indentifies' so that the SQL parser doesn't
// accidentally read a keyword hidden inside the text.
var (
	blockCommentRe     = regexp.MustCompile(`(?s)/\*.*?\*/`)
	lineCommentRe      = regexp.MustCompile(`--.*`)
	stringLiteralRe    = regexp.MustCompile(`'(?:''|\\'|[^'])*'`)
	quotedIdentifierRe = regexp.MustCompile("`[^`]*`" + `|"[^"]*"`)
)

// stripForMutationDetect returns cleanSQL with backtick- and double-quote-wrapped
// identifiers blanked out, so a table named after a reserved word (e.g.,
// “ SELECT * FROM `INSERT` “) doesn't trick mutationRe into reporting a write.
// The result is suitable only for mutationRe matching; extractMutationTargets
// and extractCacheTagsFromCleaned still need the unblanked cleanSQL so they can
// capture the actual identifier.
func stripForMutationDetect(cleanSQL string) string {
	return quotedIdentifierRe.ReplaceAllString(cleanSQL, " ")
}

// extractCacheTagsFromCleaned expects SQL with comments and string literals already removed.
func extractCacheTagsFromCleaned(cleanedSQL string) []string {
	matches := tableExtractionRe.FindAllStringSubmatch(cleanedSQL, -1)
	var tags []string
	seen := make(map[string]bool)
	for _, m := range matches {
		if len(m) > 1 {
			for _, tbl := range splitTableList(m[1]) {
				if !seen[tbl] && query.SafeIdentifierRe.MatchString(tbl) {
					seen[tbl] = true
					tags = append(tags, tbl)
				}
			}
		}
	}
	return tags
}

// splitTableList splits a captured comma-separated table list ("t1, db.t2, `t3`")
// into bare identifiers ("t1", "t2", "t3"). The schema prefix and any
// backtick/double-quote wrapping are stripped so SafeIdentifierRe accepts the result.
func splitTableList(list string) []string {
	parts := strings.Split(list, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if i := strings.LastIndex(p, "."); i >= 0 {
			p = p[i+1:]
		}
		p = strings.Trim(p, "`\"")
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// extractMutationTargets returns only the tables a mutation writes to.
// Using this on the invalidation path so reads via FROM/JOIN in subqueries
// are not unnecessarily evicted. Input must have comments and string
// literals stripped (see cleanSQLForTags).
func extractMutationTargets(cleanedSQL string) []string {
	matches := mutationTargetRe.FindAllStringSubmatch(cleanedSQL, -1)
	var tags []string
	seen := make(map[string]bool)
	for _, m := range matches {
		if len(m) > 1 {
			tbl := m[1]
			if !seen[tbl] && query.SafeIdentifierRe.MatchString(tbl) {
				seen[tbl] = true
				tags = append(tags, tbl)
			}
		}
	}
	return tags
}

// cleanSQLForTags removes comments and string literals so regex matchers
// don't trigger false positives on keywords embedded inside text.
func cleanSQLForTags(rawSQL string) string {
	clean := blockCommentRe.ReplaceAllString(rawSQL, " ")
	clean = lineCommentRe.ReplaceAllString(clean, " ")
	return stringLiteralRe.ReplaceAllString(clean, " '' ")
}

// QueryHandler handles POST /v1/query.
type QueryHandler struct {
	CHConn      driver.Conn
	Cache       cache.Cache
	DefaultTTL  time.Duration
	PolicyStore *policy.Store
	sf          singleflight.Group
}

func NewQueryHandler(conn driver.Conn, c cache.Cache, defaultTTL time.Duration) *QueryHandler {
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

	cleanSQL := cleanSQLForTags(req.SQL)

	isMutation := mutationRe.MatchString(stripForMutationDetect(cleanSQL))

	var execCtx context.Context
	if isMutation {
		execCtx = context.WithoutCancel(r.Context())
	} else {
		execCtx = r.Context()
	}

	queryCtx, cancel := context.WithTimeout(execCtx, 30*time.Second)
	defer cancel()

	var result []map[string]any
	var execErr error

	if isMutation {
		result, execErr = h.executeQuery(queryCtx, req.SQL, req.Params)
	} else {
		// Coalesce concurrent identical read queries to protect ClickHouse from stampedes.
		key := queryCacheKey(req.SQL, req.Params)
		var val any
		val, execErr, _ = h.sf.Do(key, func() (any, error) {
			return h.executeQuery(queryCtx, req.SQL, req.Params)
		})
		if execErr == nil {
			result = val.([]map[string]any)
		}
	}

	if execErr != nil {
		writeJSONError(w, http.StatusInternalServerError, execErr.Error())
		return
	}

	// Cache Invalidation
	if isMutation && h.Cache != nil {
		tags := extractMutationTargets(cleanSQL)
		if len(tags) > 0 {
			invCtx, invCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer invCancel()

			if err := h.Cache.InvalidateByTags(invCtx, tags); err != nil {
				slog.ErrorContext(r.Context(), "cache invalidation failed for raw SQL mutation",
					"tags", tags,
					"error", err,
				)
			}
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
