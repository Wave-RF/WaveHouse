package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// QueryHandler handles POST /v1/admin/query.
//
// Authorization is enforced at the router (the /v1/admin/* RequireRole gate
// in NewRouter). The handler trusts any caller that reaches it. See
// internal/api/router.go for the role-gate rationale.
//
// Implementation: a thin proxy to ClickHouse's HTTP interface. The SQL
// string is forwarded verbatim with `default_format=JSON`, ClickHouse
// returns either a `{"meta":..., "data":[...], ...}` JSON object for read
// queries or an empty body for mutations, and the handler emits just the
// `data` array (or `[]` for mutations) back to the caller.
//
// Why a proxy instead of clickhouse-go's native Query/Exec:
//   - ClickHouse classifies statements natively, so multi-statement input
//     (`SELECT 1; TRUNCATE t`), arbitrary DDL/DML verbs (current and
//     future), and inline FORMAT directives all just work.
//   - There is no isMutation heuristic to maintain — no leading-verb table,
//     no comment stripper, no CTE-aware paren scanner, no class of bug
//     where a future ClickHouse verb routes the wrong way.
//   - ClickHouse's own error messages reach the admin verbatim, which is
//     exactly what they want from an escape hatch.
//   - The cache (TieredCache + singleflight) was already removed in an
//     earlier commit; this completes the simplification.
type QueryHandler struct {
	HTTPClient *http.Client
	// Endpoint is the ClickHouse HTTP base URL (e.g.
	// `http://localhost:8123`). The handler appends query-string params
	// (`default_format`, `database`, `date_time_output_format`) per request
	// and POSTs the SQL as the request body.
	Endpoint string
	Username string
	Password string
	Database string
}

// NewQueryHandler builds a handler that proxies to ClickHouse over HTTP.
// endpoint should be the base URL (`http://host:8123`); username/password
// are forwarded via ClickHouse's `X-ClickHouse-User` / `X-ClickHouse-Key`
// headers (matching the ingest worker's convention in internal/ingest).
// database is set as the `?database=` query-string parameter when non-empty.
func NewQueryHandler(endpoint, username, password, database string) *QueryHandler {
	return &QueryHandler{
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		Endpoint:   endpoint,
		Username:   username,
		Password:   password,
		Database:   database,
	}
}

type queryRequest struct {
	SQL string `json:"sql"`
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

	u, err := url.Parse(h.Endpoint)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "invalid clickhouse endpoint: "+err.Error())
		return
	}
	q := u.Query()
	q.Set("default_format", "JSON")
	// Format DateTime/DateTime64 as ISO-8601 so JSON consumers don't have to
	// re-parse ClickHouse's default `YYYY-MM-DD HH:MM:SS`. Matches the prior
	// handler's RFC3339Nano output close enough for downstream callers.
	q.Set("date_time_output_format", "iso")
	if h.Database != "" {
		q.Set("database", h.Database)
	}
	u.RawQuery = q.Encode()

	// Bound the upstream call with a deadline derived from the inbound
	// request context — client disconnect cancels the ClickHouse call too.
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(req.SQL))
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpReq.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if h.Username != "" {
		httpReq.Header.Set("X-ClickHouse-User", h.Username)
	}
	if h.Password != "" {
		httpReq.Header.Set("X-ClickHouse-Key", h.Password)
	}

	resp, err := h.HTTPClient.Do(httpReq)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "clickhouse request failed: "+err.Error())
		return
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "read clickhouse response: "+err.Error())
		return
	}

	if resp.StatusCode != http.StatusOK {
		// ClickHouse returns plain-text error messages with non-200 status.
		// Forward the trimmed message as a JSON error so the response shape
		// stays consistent with the rest of the API. 500 isn't always
		// precise (some ClickHouse errors are caller bugs that arguably
		// merit 4xx) but distinguishing here would require parsing
		// ClickHouse error codes — out of scope for the escape hatch.
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("clickhouse returned status %d", resp.StatusCode)
		}
		writeJSONError(w, http.StatusInternalServerError, msg)
		return
	}

	// Discourage downstream HTTP caches. /v1/admin/query has no
	// read-your-writes guarantees beyond ClickHouse's own and the admin
	// expects every request to hit the database — caching at any layer
	// (CDN, browser, corp proxy) would re-introduce the staleness class of
	// bug the in-process cache strip just removed.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")

	// ClickHouse returns an empty body for mutations (TRUNCATE/INSERT/
	// DELETE/etc. with no result set). Marshal those to `[]` so clients
	// don't have to special-case "empty success body".
	if len(body) == 0 {
		_, _ = w.Write([]byte("[]"))
		return
	}

	// Read-statement response shape:
	//   {"meta":[{"name":"x","type":"..."}, ...],
	//    "data":[{"x": ...}, ...],
	//    "rows":N,
	//    "statistics":{...}}
	// Forward just `data` so a caller's `result.length` works regardless of
	// whether the SQL was a read or a mutation, matching the pre-proxy
	// response shape.
	var chResp struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &chResp); err != nil || chResp.Data == nil {
		// Unexpected non-JSON success body or missing `data` key — forward
		// raw so the caller can see what ClickHouse actually returned. Not
		// expected in practice since default_format=JSON guarantees the
		// envelope shape on success.
		_, _ = w.Write(body)
		return
	}
	_, _ = w.Write(chResp.Data)
}
