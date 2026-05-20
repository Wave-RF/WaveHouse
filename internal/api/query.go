package api

import (
	"context"
	"encoding/json"
	"errors"
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
//   - ClickHouse classifies statements natively, so any single statement
//     (arbitrary DDL/DML verbs, current and future) and inline FORMAT
//     directives all just work without WaveHouse-side parsing.
//     Multi-statement input (`SELECT 1; TRUNCATE t`) also works when
//     the upstream ClickHouse has multi-query enabled, which is the
//     default in recent versions; older or restrictively-configured
//     servers may reject the second statement with a clear error.
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
	// maxResponseBytes optionally overrides the default upstream response
	// buffer cap (maxCHResponseBytes). When 0, the default applies. Exists
	// so same-package tests can pin the cap-overflow path without
	// allocating tens of MiB per run; not a production tuning knob, hence
	// unexported.
	maxResponseBytes int64
	// maxRequestBytes optionally overrides the default inbound request
	// body cap (maxRequestBodyBytes). When 0, the default applies. Same
	// test-only purpose as maxResponseBytes.
	maxRequestBytes int64
}

const (
	// maxCHResponseBytes caps how much of the ClickHouse response the proxy
	// will buffer in memory. 64 MiB is generous for any reasonable admin
	// query (a SELECT returning ~64 MiB of JSON is itself a smell — admins
	// should be using FORMAT JSONEachRow + streaming clients for
	// genuinely-large results, or the structured query endpoint with its
	// DefaultMaxRows cap). The cap is here as a safety net against a
	// runaway SELECT exhausting the API server's RAM; admin-only doesn't
	// mean operators won't accidentally OOM themselves.
	maxCHResponseBytes = 64 << 20 // 64 MiB

	// maxRequestBodyBytes caps the inbound SQL request body. 16 MiB is well
	// above any plausible query (production SQL strings are typically
	// < 1 KiB); the bound exists to keep a misbehaving admin script from
	// forcing the handler to buffer arbitrarily large input before the
	// upstream forward. Symmetry with maxCHResponseBytes on the response
	// side.
	maxRequestBodyBytes = 16 << 20 // 16 MiB
)

// NewQueryHandler builds a handler that proxies to ClickHouse over HTTP.
// endpoint should be the base URL (`http://host:8123`); username/password
// are forwarded via ClickHouse's `X-ClickHouse-User` / `X-ClickHouse-Key`
// headers (matching the ingest worker's convention in internal/ingest).
// database is set as the `?database=` query-string parameter when non-empty.
//
// The HTTP client itself has no Timeout — every request gets a 30s deadline
// from a context derived from the inbound request (see Handle), which
// bounds the whole exchange including body read. Setting `Timeout` here
// too would just duplicate that bound (and silently truncate any
// inbound context longer than 30s).
func NewQueryHandler(endpoint, username, password, database string) *QueryHandler {
	return &QueryHandler{
		HTTPClient: &http.Client{
			// ClickHouse's HTTP interface doesn't 3xx in normal operation,
			// and h.Endpoint is operator-controlled config (not user
			// input). Don't chase redirects — if an operator misconfigures
			// the endpoint to point at something that 3xx's, surface the
			// 3xx response as-is. Our status-mapping below classifies
			// anything outside 2xx/4xx as 502.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Endpoint: endpoint,
		Username: username,
		Password: password,
		Database: database,
	}
}

type queryRequest struct {
	SQL string `json:"sql"`
}

func (h *QueryHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Every response from this handler is non-cacheable — admins expect
	// every request to hit ClickHouse, and downstream caches (CDN, browser,
	// corp proxy) re-introduce the staleness class of bug the in-process
	// cache strip already removed. Set it once up top so error responses
	// carry the header too, not only the 200 path.
	w.Header().Set("Cache-Control", "no-store")
	// Tell browsers not to MIME-sniff the body. Admins can ask ClickHouse
	// for arbitrary FORMATs via inline `FORMAT …`, and the proxy passes
	// the upstream Content-Type through verbatim (e.g. text/html for
	// `FORMAT HTML`). nosniff defangs the browser-as-renderer concern;
	// matches writeJSONError's posture on the error path.
	w.Header().Set("X-Content-Type-Options", "nosniff")

	reqCap := int64(maxRequestBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)
	var req queryRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeJSONError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeded %d bytes", reqCap))
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	// Reject trailing top-level JSON tokens. The single Decode above stops
	// after the first complete value, so `{"sql":"a"}{"sql":"b"}` would
	// otherwise silently take the first envelope and drop the second
	// (a real risk if a buggy client double-encodes). A second Decode that
	// doesn't return io.EOF means there was more JSON the client sent
	// expecting us to act on — treat it as malformed input.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeJSONError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeded %d bytes", reqCap))
			return
		}
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

	// Cap the upstream body at the configured response limit. Read +1 so we
	// can detect "exactly cap or more" without a second read.
	respCap := int64(maxCHResponseBytes)
	if h.maxResponseBytes > 0 {
		respCap = h.maxResponseBytes
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, respCap+1))
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "read clickhouse response: "+err.Error())
		return
	}
	if int64(len(body)) > respCap {
		writeJSONError(w, http.StatusBadGateway, fmt.Sprintf("clickhouse response exceeded %d bytes; narrow the query or use FORMAT JSONEachRow with streaming", respCap))
		return
	}

	if resp.StatusCode != http.StatusOK {
		// ClickHouse returns plain-text error messages with non-200 status.
		// Forward the trimmed message as a JSON error, and map the upstream
		// status into one of two buckets so admin tooling can tell
		// caller-fault from upstream-fault:
		//   4xx (bad SQL, missing table, type error, …) → 400 — the
		//                                                  request itself
		//                                                  was bad.
		//   5xx, anything else                          → 502 — we're a
		//                                                  gateway and the
		//                                                  upstream
		//                                                  service had a
		//                                                  problem.
		// Distinguishing ClickHouse's specific error codes (Code: 60 for
		// "table doesn't exist" etc.) would need a parser and is out of
		// scope here. The body carries ClickHouse's exact message so the
		// admin still sees the diagnostic verbatim.
		status := http.StatusBadGateway
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			status = http.StatusBadRequest
		}
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("clickhouse returned status %d", resp.StatusCode)
		}
		writeJSONError(w, status, msg)
		return
	}

	// ClickHouse returns an empty body for mutations (TRUNCATE/INSERT/
	// DELETE/etc. with no result set). Marshal those to `[]` so clients
	// don't have to special-case "empty success body".
	if len(body) == 0 {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
		return
	}

	// Read-statement response shape under default_format=JSON:
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
	if err := json.Unmarshal(body, &chResp); err == nil && chResp.Data != nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chResp.Data)
		return
	}

	// Unexpected shape — happens when the SQL contains an explicit FORMAT
	// directive that overrides default_format=JSON (verified empirically:
	// `SELECT 1 FORMAT CSV` returns raw `1\n` with Content-Type:
	// text/csv, regardless of default_format on the URL). Forward the
	// upstream Content-Type so consumers don't get a CSV body labelled as
	// JSON. Fall back to application/octet-stream only if ClickHouse
	// returned no Content-Type (shouldn't happen, but better than the
	// previous lie of stamping application/json).
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	_, _ = w.Write(body)
}
