package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
)

// chReader runs the cached read paths — the structured query and named pipes
// — against ClickHouse's HTTP interface and hands back ClickHouse's own JSON
// rendering of the rows.
//
// Asking ClickHouse for JSON rather than scanning rows through the native
// driver is what keeps the response correct across server versions: the
// spelling of a Decimal, a DateTime64's scale, an Enum's name and an IPv6's
// compression are the server's to decide, and they change between versions.
// It is also why the SELECT-vs-mutation classifier this file replaced is
// gone: the classifier existed because clickhouse-go's Query() errors on a
// statement with no result set, and a structured query or a pipe is a read by
// construction. The raw-SQL escape hatch (/v1/ops/query) has proxied to the
// same interface for the same reasons since it was written; see query.go.
type chReader struct {
	client *http.Client
	// target resolves the ClickHouse HTTP wiring per request (chconn.Manager
	// in production) so a settings reload applies to the next read.
	target func() chconn.Target
	// maxResponseBytes optionally overrides maxCHResponseBytes. Test-only
	// seam for the cap-overflow path; not a production knob.
	maxResponseBytes int64
}

// newCHReader builds the shared reader. The client has no Timeout — every
// read carries a context deadline derived from the inbound request, which
// bounds the whole exchange including the body read.
func newCHReader(target func() chconn.Target) *chReader {
	return &chReader{
		client: &http.Client{
			// The target is operator config, not user input, and ClickHouse
			// does not redirect in normal operation: surface a 3xx as itself
			// rather than chase it.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		target: target,
	}
}

// chOutputFormat is the format and the rendering knobs that make up the
// response contract. JSONEachRow rather than JSON because the framing is one
// row per line and the array wrapper costs a single pass; the rows themselves
// are byte-identical between the two.
//
// date_time_output_format is deliberately absent: ClickHouse's default is the
// `YYYY-MM-DD HH:MM:SS[.fff]` spelling the SSE wire already carries, and the
// two surfaces must agree byte for byte (#372). /v1/ops/query sets `iso`
// because an admin reading raw SQL output is a different audience.
//
// 64-bit integers are pinned UNQUOTED because that is what the endpoint has
// always returned; it is lossy in a JavaScript consumer past 2^53, but that
// was already true and quoting them now would break every consumer that does
// arithmetic on one. The setting is sent explicitly rather than inherited so
// a server-side profile cannot silently change the contract.
//
// Denormal floats are deliberately NOT quoted: ClickHouse's default renders a
// NaN or an Inf as JSON `null`, which is valid JSON, where quoting them puts
// the string "nan" in a numeric field. (Go's marshaller used to fail the whole
// response with "unsupported value: NaN", so anything is an improvement.)
var chOutputFormat = map[string]string{
	"default_format": "JSONEachRow",
	"output_format_json_quote_64bit_integers": "0",
}

// query executes sql and returns the result rows as a JSON array — the
// response body both cached read paths serve.
//
// params supply `param_p0` … `param_pN-1` positionally, for the `{pN:String}`
// / `{pN:Array(String)}` placeholders query.BuildResult.NamedParams emitted;
// they are already encoded for ClickHouse's parameter reader. settings are the
// role's resource caps from chReadSettings.
func (c *chReader) query(ctx context.Context, sql string, params []string, settings map[string]string) ([]byte, error) {
	if c == nil || c.client == nil || c.target == nil {
		return nil, fmt.Errorf("clickhouse reader not configured")
	}
	target := c.target()
	u, err := url.Parse(target.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid clickhouse endpoint: %w", err)
	}
	q := u.Query()
	for k, v := range chOutputFormat {
		q.Set(k, v)
	}
	if target.Database != "" {
		q.Set("database", target.Database)
	}
	for k, v := range settings {
		q.Set(k, v)
	}
	for i, p := range params {
		q.Set("param_p"+strconv.Itoa(i), p)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if target.Username != "" {
		req.Header.Set("X-ClickHouse-User", target.Username)
	}
	if target.Password != "" {
		req.Header.Set("X-ClickHouse-Key", target.Password)
	}

	// #nosec G704 -- the destination is operator config, not caller input: the
	// scheme, host and path come from chconn.Target and only RawQuery is
	// replaced, with url.Values.Encode() percent-encoding every key and value.
	// A bound value therefore cannot introduce a host, a path or a fragment.
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respCap := int64(maxCHResponseBytes)
	if c.maxResponseBytes > 0 {
		respCap = c.maxResponseBytes
	}
	// Read one byte past the cap so "exactly cap or more" is detectable
	// without a second read.
	body, err := io.ReadAll(io.LimitReader(resp.Body, respCap+1))
	if err != nil {
		return nil, fmt.Errorf("read clickhouse response: %w", err)
	}
	if int64(len(body)) > respCap {
		return nil, fmt.Errorf("clickhouse response exceeded %d bytes; narrow the query", respCap)
	}
	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("clickhouse returned status %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("clickhouse query: %s", msg)
	}
	return jsonEachRowArray(body)
}

// jsonEachRowArray frames ClickHouse's newline-delimited JSONEachRow output as
// the JSON array this endpoint has always returned. A JSONEachRow row is one
// complete JSON object on one line — a newline inside a string is escaped —
// so splitting on '\n' cannot cut a row in half, and the rows themselves are
// copied through untouched.
//
// A zero-row result is an empty body and becomes `[]`, not `null`: SDK
// consumers do `data.length` on every response.
//
// A line that does not open an object is ClickHouse's exception text appended
// after the stream had already started (it cannot revise the 200 it sent), so
// it becomes the error it would have been had it arrived in time.
func jsonEachRowArray(body []byte) ([]byte, error) {
	out := make([]byte, 0, len(body)+2)
	out = append(out, '[')
	first := true
	for len(body) > 0 {
		line := body
		if i := bytes.IndexByte(body, '\n'); i >= 0 {
			line, body = body[:i], body[i+1:]
		} else {
			body = nil
		}
		if len(line) == 0 {
			continue
		}
		if line[0] != '{' {
			return nil, fmt.Errorf("clickhouse query: %s", strings.TrimSpace(string(line)))
		}
		if !first {
			out = append(out, ',')
		}
		out = append(out, line...)
		first = false
	}
	return append(out, ']'), nil
}
