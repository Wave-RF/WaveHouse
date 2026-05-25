package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// adminQueryMetricAttrs is the static label set for every /v1/admin/query
// measurement; pre-allocated to keep the hot path alloc-free.
var adminQueryMetricAttrs = metric.WithAttributes(attribute.String("operation", "admin_query"))

// adminQueryErrCodeRe matches the bento worker's parser. ClickHouse HTTP
// error bodies are prefixed with `Code: <N>. DB::Exception: …`.
var adminQueryErrCodeRe = regexp.MustCompile(`^Code: (\d+)`)

func adminQueryErrCode(body []byte) string {
	if m := adminQueryErrCodeRe.FindSubmatch(body); m != nil {
		return string(m[1])
	}
	return "0"
}

// QueryHandler proxies POST /v1/admin/query to ClickHouse's HTTP interface,
// forwarding SQL verbatim with `default_format=JSON` and emitting just the
// `data` array (or `[]` for mutations) back to the caller. The /v1/admin/*
// RequireRole gate enforces authorization upstream.
//
// Proxying (rather than clickhouse-go Query/Exec) lets ClickHouse classify
// statements natively, so any verb, inline FORMAT, or multi-statement input
// works without a WaveHouse-side parser to maintain.
type QueryHandler struct {
	HTTPClient *http.Client
	Endpoint   string
	Username   string
	Password   string
	Database   string
	// maxQueryTimeout bounds the whole upstream exchange.
	maxQueryTimeout time.Duration
	// maxResponseBytes / maxRequestBytes override the package defaults
	// for tests that need to drive the cap-overflow path cheaply.
	maxResponseBytes int64
	maxRequestBytes  int64
}

const (
	// 64 MiB / 16 MiB caps are safety nets against runaway admin queries.
	// Large responses should use FORMAT JSONEachRow + streaming clients.
	maxCHResponseBytes  = 64 << 20
	maxRequestBodyBytes = 16 << 20
)

// NewQueryHandler builds an HTTP proxy to ClickHouse. The client has no
// timeout; the per-request context (Handle) carries queryTimeout.
func NewQueryHandler(endpoint, username, password, database string, queryTimeout time.Duration) *QueryHandler {
	return &QueryHandler{
		HTTPClient: &http.Client{
			// Don't chase redirects — h.Endpoint is operator-controlled, and
			// ClickHouse doesn't 3xx normally. Forward as-is so a misconfig
			// shows up in the response.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Endpoint:        endpoint,
		Username:        username,
		Password:        password,
		Database:        database,
		maxQueryTimeout: queryTimeout,
	}
}

type queryRequest struct {
	SQL string `json:"sql"`
}

func (h *QueryHandler) Handle(w http.ResponseWriter, r *http.Request) {
	r = r.WithContext(observability.WithComponent(r.Context(), "api/admin_query"))
	// no-store on every response: admins expect every request to hit
	// ClickHouse, no downstream CDN/browser/proxy caching.
	w.Header().Set("Cache-Control", "no-store")
	// Admins can request arbitrary FORMAT (e.g. HTML); nosniff defangs the
	// browser-as-renderer concern when we forward upstream Content-Type.
	w.Header().Set("X-Content-Type-Options", "nosniff")

	reqCap := int64(maxRequestBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)
	var req queryRequest
	dec := json.NewDecoder(r.Body)
	// Reject unknown fields so clients still sending the dropped `params`
	// array fail loudly instead of silently dropping their inputs.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeJSONError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeded %d bytes", reqCap))
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	// Reject trailing JSON. `{"sql":"a"}{"sql":"b"}` would otherwise
	// silently take the first envelope and drop the second.
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
	// ISO-8601 for DateTime so JSON consumers don't re-parse ClickHouse's
	// default `YYYY-MM-DD HH:MM:SS`.
	q.Set("date_time_output_format", "iso")
	if h.Database != "" {
		q.Set("database", h.Database)
	}
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(r.Context(), h.maxQueryTimeout)
	defer cancel()

	// Zero-value QueryHandler{} from routing-only tests; surface as a 500
	// rather than rely on the chi recoverer.
	if h.HTTPClient == nil {
		writeJSONError(w, http.StatusInternalServerError, "query handler not configured: HTTPClient is nil")
		return
	}

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

	// Raw SQL is intentionally omitted from the span — admins paste secrets
	// and PII into ad-hoc queries.
	ctx, chSpan := observability.Tracer().Start(ctx, "clickhouse.admin_query")
	chSpan.SetAttributes(
		attribute.String("db.system", "clickhouse"),
		attribute.String("clickhouse.operation", "admin_query"),
	)
	httpReq = httpReq.WithContext(ctx)
	chStart := time.Now()

	resp, err := h.HTTPClient.Do(httpReq)
	if err != nil {
		observability.ClickHouseDuration.Record(ctx, time.Since(chStart).Seconds(), adminQueryMetricAttrs)
		observability.ClickHouseErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("operation", "admin_query"),
			attribute.String("clickhouse_code", "0"),
		))
		chSpan.RecordError(err)
		chSpan.End()
		writeJSONError(w, http.StatusBadGateway, "clickhouse request failed: "+err.Error())
		return
	}
	defer func() {
		_ = resp.Body.Close()
		observability.ClickHouseDuration.Record(ctx, time.Since(chStart).Seconds(), adminQueryMetricAttrs)
		chSpan.End()
	}()

	// Read cap+1 so the "exactly cap or more" branch below doesn't need a
	// second read.
	respCap := int64(maxCHResponseBytes)
	if h.maxResponseBytes > 0 {
		respCap = h.maxResponseBytes
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, respCap+1))
	if err != nil {
		observability.ClickHouseErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("operation", "admin_query"),
			attribute.String("clickhouse_code", "0"),
		))
		chSpan.RecordError(err)
		writeJSONError(w, http.StatusBadGateway, "read clickhouse response: "+err.Error())
		return
	}
	if int64(len(body)) > respCap {
		// caller_oversize keeps these out of the genuine-server-fault bucket
		// on dashboards.
		observability.ClickHouseErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("operation", "admin_query"),
			attribute.String("clickhouse_code", "caller_oversize"),
		))
		writeJSONError(w, http.StatusBadGateway, fmt.Sprintf("clickhouse response exceeded %d bytes; narrow the query or use FORMAT JSONEachRow with streaming", respCap))
		return
	}

	if resp.StatusCode != http.StatusOK {
		// Map 4xx→400 (caller fault) and everything else→502 (gateway
		// fault) so admin tooling can tell them apart. The parsed CH
		// numeric code goes on the metric label.
		chCode := adminQueryErrCode(body)
		observability.ClickHouseErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("operation", "admin_query"),
			attribute.String("clickhouse_code", chCode),
		))
		chSpan.SetAttributes(attribute.String("clickhouse.error_code", chCode))
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

	// Mutations return empty bodies; emit `[]` so clients don't special-case.
	if len(body) == 0 {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
		return
	}

	// Default JSON shape is `{"meta":..., "data":[...], "rows":N, ...}`.
	// Forward just `data` for response-shape compat with the pre-proxy handler.
	var chResp struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &chResp); err == nil && chResp.Data != nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chResp.Data)
		return
	}

	// Inline `FORMAT …` overrides default_format=JSON — forward the upstream
	// Content-Type verbatim so consumers don't get a CSV labelled as JSON.
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	_, _ = w.Write(body)
}
