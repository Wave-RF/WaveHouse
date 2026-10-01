package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// chRendering names the response contract chReadSettingsFixed pins. It leads
// every cached read's key (queryCacheKey), so two builds that render rows
// differently never serve each other's entries from a shared cache during a
// rolling deploy. Change it with any change to the rendering settings below.
const chRendering = "JSONEachRow/3"

// chReadSettingsFixed go on every request the cached read paths send, ahead
// of the role's caps.
//
// Rendering: ClickHouse renders the rows, so a Decimal's spelling, a
// DateTime64's scale, an Enum's name and an IPv6's compression are the
// server's own. These knobs are pinned rather than inherited, because a
// tenant's server or profile may set any of them: 64-bit integers and
// decimals as bare numbers, NaN and Inf as null, a named tuple as an object,
// `/` left unescaped (`"/home"`, where ClickHouse's default writes
// `"\/home"`), and DateTime as RFC 3339 in UTC, `YYYY-MM-DDThh:mm:ss[.fff]Z`
// with the column's scale, whatever the column's or the server's zone. The
// SSE wire spells `/` and DateTime the same way (typelayer's export pins
// both), so neither a client nor the cache needs to know the server's zone
// (#372). Date and Date32 are unaffected. Measured on 26.8.15.10. A profile
// can still change the bytes through a knob not pinned here, such as
// output_format_decimal_trailing_zeros, output_format_json_array_of_rows or
// output_format_trim_fixed_string (measured on 26.8.15.10; the last is
// unknown to 24.8, where sending it would fail every read).
//
// Failure: wait_end_of_query buffers the result server-side until the query
// has finished, and http_write_exception_in_output_format=0 keeps an
// exception out of the JSON, so a query that fails part way answers with a
// non-200 and X-ClickHouse-Exception-Code instead of a 200 carrying rows and
// then an error. Measured on 24.8.14.39 and 26.6.3.62: without them a failure
// after the first block arrived as rows followed by exception text (with a
// 200 on 24.8).
//
// Cancellation: the request's own deadline drops the connection, and
// cancel_http_readonly_queries_on_client_close stops the query on the server
// rather than letting it run on after nobody waits for it.
//
// Every one of these is known to ClickHouse 24.8 and later (measured on
// 24.8.14.39 and 26.8.15.10).
var chReadSettingsFixed = map[string]string{
	"default_format":                               "JSONEachRow",
	"wait_end_of_query":                            "1",
	"http_write_exception_in_output_format":        "0",
	"cancel_http_readonly_queries_on_client_close": "1",
	"output_format_json_quote_64bit_integers":      "0",
	"output_format_json_quote_decimals":            "0",
	"output_format_json_quote_denormals":           "0",
	"output_format_json_escape_forward_slashes":    "0",
	"date_time_output_format":                      "iso",
	"output_format_json_named_tuples_as_objects":   "1",
}

// ClickHouse's own limits on the HTTP interface's fields, at their defaults:
// http_max_field_value_size per field — a query-string parameter, counted
// percent-encoded, or a multipart form field — and http_max_uri_size for the
// whole request line. Measured on 24.8.14.39 and 26.8.15.10: a 131072-byte
// query-string value is read and a 131073-byte one is refused with code 1000
// ("Field value too long"), as is a 200 KiB form field, and a 1 MiB value
// with a codeless 400 — answers chFailureOf would read as an outage and a
// misconfiguration rather than as a request that is too large. An external
// table has no such limit: a 1.2 MiB one was read whole.
const (
	chMaxFieldBytes = 128 << 10
	chMaxURIBytes   = 1 << 20
	// chURIHeadroom is what the path, the settings and the database leave
	// of chMaxURIBytes for the bound values.
	chURIHeadroom = 16 << 10
)

// defaultReadConns caps a tenant's concurrent reads when it names no
// clickhouse.max_open_conns, as the ingest worker's transport does.
const defaultReadConns = 100

// chRequest is one statement for ClickHouse's HTTP interface.
type chRequest struct {
	sql string
	// params supply param_<Name> for the {<Name>:String} placeholders
	// query.BuildResult.Bind emitted, already encoded for ClickHouse's
	// parameter reader. They ride on the query string.
	params []query.Param
	// tables are the external tables the statement's `in` lists read. With
	// any, the request is multipart/form-data: the statement moves from the
	// body into the query form field, one field per table describes it, and
	// each table is a file part.
	tables []query.Table
	// settings are per-query ClickHouse settings (chReadSettings).
	settings map[string]string
	// write runs the statement without readonly=2: a write pipe. Everything
	// else is a read, and ClickHouse refuses it if it writes after all.
	write bool
}

// chReader runs the cached read paths — the structured query and named pipes
// — and write pipes against ClickHouse's HTTP interface, and hands back
// ClickHouse's own JSON rendering of the rows as a JSON array.
type chReader struct {
	// build makes the client for a TLS config and a cap on connections:
	// readerHTTPClient in production, a fake transport in tests.
	build func(tlsCfg *tls.Config, conns int) *http.Client

	mu sync.Mutex
	// clients holds one client per pool (readerPool).
	clients map[readerPool]*http.Client

	// maxResponseBytes optionally overrides maxCHResponseBytes. Test-only
	// seam for the cap-overflow path; not a production knob.
	maxResponseBytes int64
}

func newCHReader(build func(tlsCfg *tls.Config, conns int) *http.Client) *chReader {
	return &chReader{build: build, clients: map[readerPool]*http.Client{}}
}

// readerPool is what one client's connections are shared by: the server, the
// credentials and the database — the tuple a native pool is keyed by — the
// TLS config, and the cap. Tenants naming the same tuple share one cap, as
// they share one native pool; a tenant on another database or user on the
// same server gets a cap of its own. The set grows with the pools ever read
// through, not with requests: a client whose pool is gone keeps only its
// transport, whose idle connections time out on their own.
type readerPool struct {
	url, username, password, database string
	tls                               *tls.Config
	conns                             int
}

// CHReader is the read client the structured-query and pipes handlers run
// their ClickHouse reads through. Give one to both (SetReader) so a tenant's
// reads share one connection cap whichever route they come in by, as they
// shared its native pool; a handler built without one has a private reader.
type CHReader = chReader

// NewCHReader returns a reader over net/http with a client per pool.
func NewCHReader() *CHReader { return newCHReader(readerHTTPClient) }

// readerHTTPClient is the read paths' client: net/http's default transport
// with the target's TLS config and at most conns connections to the server,
// one per pool (readerPool); a read that finds every connection busy waits
// for one until its deadline. Like the proxy's client it has no Timeout — every request
// carries a deadline — and does not chase redirects: the target is operator
// config, and ClickHouse does not redirect in normal operation.
func readerHTTPClient(tlsCfg *tls.Config, conns int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg
	transport.MaxConnsPerHost = conns
	transport.MaxIdleConnsPerHost = conns
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// client returns the client for target's pool under a cap of conns
// connections.
func (c *chReader) client(target chconn.Target, conns int) *http.Client {
	if conns <= 0 {
		conns = defaultReadConns
	}
	key := readerPool{
		url: target.URL, username: target.Username, password: target.Password, database: target.Database,
		tls: target.TLS, conns: conns,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.clients[key]; ok {
		return cl
	}
	// A copy of the TLS config, never the target's own: net/http appends its
	// HTTP/2 protocols to NextProtos in place when a transport first dials,
	// which would race the driver's handshakes on the shared config.
	cl := c.build(target.TLS.Clone(), conns)
	c.clients[key] = cl
	return cl
}

// chResponseTooLargeError is a response past the read paths' buffer cap. The
// same query overflows again, so it is not retryable.
type chResponseTooLargeError struct{ limit int64 }

func (e *chResponseTooLargeError) Error() string {
	return fmt.Sprintf("clickhouse response exceeded %d bytes; narrow the query", e.limit)
}

// do sends req to target and returns the rows as a JSON array. A refusal is a
// *chconn.HTTPError, a failure on the way is wrapped with %w, so
// chconn.Classify can class either; an oversized response is a
// *chResponseTooLargeError.
func (c *chReader) do(ctx context.Context, target chconn.Target, conns int, req chRequest) ([]byte, error) {
	u, err := url.Parse(target.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid clickhouse endpoint: %w", err)
	}
	q := u.Query()
	for k, v := range chReadSettingsFixed {
		q.Set(k, v)
	}
	if !req.write {
		q.Set("readonly", "2")
	}
	if target.Database != "" {
		q.Set("database", target.Database)
	}
	for k, v := range req.settings {
		q.Set(k, v)
	}
	for _, p := range req.params {
		q.Set("param_"+p.Name, p.Value)
	}
	u.RawQuery = q.Encode()

	reqBody, contentType, err := chRequestBody(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), reqBody)
	if err != nil {
		return nil, fmt.Errorf("clickhouse request: %w", err)
	}
	// The configured headers first, so WaveHouse's own win over a same-named
	// one.
	for name, value := range target.Headers {
		httpReq.Header.Set(name, value)
	}
	httpReq.Header.Set("Content-Type", contentType)
	if target.Username != "" {
		httpReq.Header.Set("X-ClickHouse-User", target.Username)
	}
	if target.Password != "" {
		httpReq.Header.Set("X-ClickHouse-Key", target.Password)
	}

	// #nosec G704 -- the destination is operator config, not caller input:
	// the scheme, host and path come from chconn.Target and only RawQuery is
	// replaced, with url.Values.Encode() percent-encoding every key and value.
	resp, err := c.client(target, conns).Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("clickhouse request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respCap := int64(maxCHResponseBytes)
	if c.maxResponseBytes > 0 {
		respCap = c.maxResponseBytes
	}
	// One byte past the cap tells "exactly cap or more" without a second read.
	body, err := io.ReadAll(io.LimitReader(resp.Body, respCap+1))
	if err != nil {
		return nil, fmt.Errorf("read clickhouse response: %w", err)
	}
	if int64(len(body)) > respCap {
		return nil, &chResponseTooLargeError{limit: respCap}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, chconn.NewHTTPError(&http.Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: io.NopCloser(bytes.NewReader(body))})
	}
	rows, tail := jsonEachRowArray(body)
	if tail != nil {
		// Text after the rows is an exception ClickHouse appended once it
		// could no longer revise the 200 — a setting above overridden on the
		// way. It is the error it would have been had it arrived in time.
		if i := bytes.Index(tail, []byte("Code: ")); i > 0 {
			tail = tail[i:]
		}
		return nil, chconn.NewHTTPError(&http.Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: io.NopCloser(bytes.NewReader(tail))})
	}
	return rows, nil
}

// chRequestBody is req's body and its content type: the statement as plain
// text, or — when it reads external tables — a multipart form carrying the
// statement, each table's structure and format, and then the tables. The
// descriptions go first because ClickHouse reads a table's part as it
// arrives, with what it has been told about it so far.
func chRequestBody(req chRequest) (io.Reader, string, error) {
	if len(req.tables) == 0 {
		return strings.NewReader(req.sql), "text/plain; charset=utf-8", nil
	}
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fields := [][2]string{{"query", req.sql}}
	for _, t := range req.tables {
		fields = append(fields, [2]string{t.Name + "_structure", query.TableStructure}, [2]string{t.Name + "_format", query.TableFormat})
	}
	for _, f := range fields {
		if err := w.WriteField(f[0], f[1]); err != nil {
			return nil, "", err
		}
	}
	for _, t := range req.tables {
		part, err := w.CreateFormFile(t.Name, t.Name)
		if err != nil {
			return nil, "", err
		}
		if _, err := part.Write(t.Data); err != nil {
			return nil, "", err
		}
	}
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	return &buf, w.FormDataContentType(), nil
}

// jsonEachRowArray frames ClickHouse's newline-delimited JSONEachRow output as
// the JSON array the read endpoints return, copying the rows through
// untouched. A JSONEachRow row is one JSON object on one line — a newline in
// a string is escaped — so splitting on '\n' cannot cut a row in half. No
// rows is an empty body and becomes `[]`, never `null`: SDK consumers do
// `data.length` on every response. From the first line that does not open an
// object, the rest of the body is returned as tail.
func jsonEachRowArray(body []byte) (rows, tail []byte) {
	out := make([]byte, 0, len(body)+2)
	out = append(out, '[')
	first := true
	for len(body) > 0 {
		line, rest, _ := bytes.Cut(body, []byte{'\n'})
		if len(line) == 0 {
			body = rest
			continue
		}
		if line[0] != '{' {
			return nil, body
		}
		if !first {
			out = append(out, ',')
		}
		out = append(out, line...)
		first = false
		body = rest
	}
	return append(out, ']'), nil
}

// checkRequestSize refuses a bound query the HTTP interface would refuse,
// at ClickHouse's own limits, so the caller gets a 400 naming the limit
// rather than code 1000 classed as an outage. A scalar value rides on the
// query string, each one capped and all of them together bounded by the
// request line; with an `in` list the statement rides in one form field.
// An `in` list itself has no cap: its table is read whole.
func checkRequestSize(b *query.Bound) error {
	total := 0
	for _, p := range b.Params {
		n := len(url.QueryEscape(p.Value))
		if n > chMaxFieldBytes {
			return fmt.Errorf("filter value too large: %d bytes once encoded, over the %d ClickHouse's HTTP interface takes for one value; an in list has no such limit", n, chMaxFieldBytes)
		}
		total += n
	}
	if total > chMaxURIBytes-chURIHeadroom {
		return fmt.Errorf("filter values too large: %d bytes once encoded, over the %d ClickHouse's HTTP interface takes for all of them; an in list has no such limit", total, chMaxURIBytes-chURIHeadroom)
	}
	if len(b.Tables) > 0 && len(b.SQL) > chMaxFieldBytes {
		return fmt.Errorf("query too large: %d bytes of SQL, over the %d ClickHouse's HTTP interface takes in the form field a query with an in list travels in; use fewer filters", len(b.SQL), chMaxFieldBytes)
	}
	return nil
}

// cacheValues are b's bound values as the cache key takes them: each
// parameter's value, then each table's bytes, exactly as they go on the wire.
// The statement, hashed alongside, names every parameter and table, so the
// split between the two is never ambiguous.
func cacheValues(b *query.Bound) []string {
	out := make([]string, 0, len(b.Params)+len(b.Tables))
	for _, p := range b.Params {
		out = append(out, p.Value)
	}
	for _, t := range b.Tables {
		out = append(out, string(t.Data))
	}
	return out
}

// targetOf is target's answer for store — the tenant's HTTP wiring — and the
// zero Target, a tenant on no pool, for an unwired source.
func targetOf(target func(*settings.Store) chconn.Target, store *settings.Store) chconn.Target {
	if target == nil {
		return chconn.Target{}
	}
	return target(store)
}

// connsOf is conns's answer for store, zero (the default cap) for an unwired
// source.
func connsOf(conns func(*settings.Store) int, store *settings.Store) int {
	if conns == nil {
		return 0
	}
	return conns(store)
}

// timeoutOf is timeout's answer for store, zero for an unwired source.
func timeoutOf(timeout func(*settings.Store) time.Duration, store *settings.Store) time.Duration {
	if timeout == nil {
		return 0
	}
	return timeout(store)
}
