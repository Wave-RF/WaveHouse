package api

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// fakeCHURL is the address a fakeCH target names; nothing listens on it.
const fakeCHURL = "http://clickhouse.test:8123"

// fakeCH is ClickHouse's HTTP interface in memory: an http.RoundTripper that
// records every request and hands it to answer, which writes the response (a
// 200 with no rows when nil). In memory rather than on a socket so a test
// under synctest can see a request held inside it.
type fakeCH struct {
	answer func(w http.ResponseWriter, req *chSeen)
	// err, when set, is the transport failure every request meets instead.
	err error

	mu   sync.Mutex
	seen []*chSeen
	// reads and writes count requests sent with and without readonly=2.
	reads, writes atomic.Int32
}

// chSeen is one request a fakeCH received.
type chSeen struct {
	sql    string
	query  url.Values
	header http.Header
}

func (f *fakeCH) RoundTrip(r *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	seen := &chSeen{sql: string(body), query: r.URL.Query(), header: r.Header.Clone()}
	f.mu.Lock()
	f.seen = append(f.seen, seen)
	f.mu.Unlock()
	if seen.query.Get("readonly") == "2" {
		f.reads.Add(1)
	} else {
		f.writes.Add(1)
	}
	if f.err != nil {
		return nil, f.err
	}
	rec := httptest.NewRecorder()
	if f.answer != nil {
		f.answer(rec, seen)
	}
	resp := rec.Result()
	resp.Request = r
	return resp, nil
}

// reader is a chReader whose every client goes through f.
func (f *fakeCH) reader() *chReader {
	return newCHReader(func(*tls.Config, int) *http.Client { return &http.Client{Transport: f} })
}

// target is a Target getter naming f, whatever the tenant.
func (f *fakeCH) target(*settings.Store) chconn.Target { return chconn.Target{URL: fakeCHURL} }

// last is the most recent request f received, nil before the first.
func (f *fakeCH) last() *chSeen {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.seen) == 0 {
		return nil
	}
	return f.seen[len(f.seen)-1]
}

// sql is the statement of the most recent request, "" before the first —
// which is itself the assertion for a request refused before ClickHouse.
func (f *fakeCH) sql() string {
	if s := f.last(); s != nil {
		return s.sql
	}
	return ""
}

// params are the most recent request's bound values in placeholder order:
// what {p0:…}, {p1:…}, … received.
func (f *fakeCH) params() []string {
	s := f.last()
	if s == nil {
		return nil
	}
	var out []string
	for i := 0; ; i++ {
		v, ok := s.query["param_p"+strconv.Itoa(i)]
		if !ok {
			return out
		}
		out = append(out, v[0])
	}
}

// setting is one query-string setting of the most recent request.
func (f *fakeCH) setting(name string) string {
	if s := f.last(); s != nil {
		return s.query.Get(name)
	}
	return ""
}

// answerRows answers with body as ClickHouse's JSONEachRow output.
func answerRows(body string) func(http.ResponseWriter, *chSeen) {
	return func(w http.ResponseWriter, _ *chSeen) { _, _ = io.WriteString(w, body) }
}

// answerException answers as ClickHouse refusing a statement: status, the
// exception code header (none for 0), and body.
func answerException(status int, code int32, body string) func(http.ResponseWriter, *chSeen) {
	return func(w http.ResponseWriter, _ *chSeen) {
		if code > 0 {
			w.Header().Set("X-ClickHouse-Exception-Code", strconv.Itoa(int(code)))
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// read is a read of sql against f's target with no params or settings.
func read(t *testing.T, r *chReader, sql string) ([]byte, error) {
	t.Helper()
	return r.do(t.Context(), chconn.Target{URL: fakeCHURL}, 0, chRequest{sql: sql})
}

// TestCHReader_ResponseShape pins the body the cached read paths serve.
// ClickHouse's JSONEachRow output is newline-delimited, the endpoints'
// contract is a JSON array, and a zero-row result must be `[]` and never
// `null` — every SDK consumer does `data.length` on it.
func TestCHReader_ResponseShape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want string
	}{
		{"no rows is an empty body", "", "[]"},
		{"one row", "{\"a\":1}\n", `[{"a":1}]`},
		{"several rows", "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n", `[{"a":1},{"a":2},{"a":3}]`},
		{"no trailing newline", "{\"a\":1}", `[{"a":1}]`},
		{"blank lines are skipped", "\n{\"a\":1}\n\n", `[{"a":1}]`},
		{
			// A newline inside a value is escaped by JSONEachRow, so splitting
			// on '\n' cannot cut a row in half.
			name: "an escaped newline inside a value is not a row boundary",
			body: "{\"a\":\"x\\ny\"}\n",
			want: `[{"a":"x\ny"}]`,
		},
		{
			// The bytes are ClickHouse's: key order, a decimal's digits and a
			// timestamp's spelling all pass through untouched.
			name: "rows are copied, not re-encoded",
			body: "{\"z\":1,\"a\":12.50,\"ts\":\"2026-01-15 10:30:00.120\"}\n",
			want: `[{"z":1,"a":12.50,"ts":"2026-01-15 10:30:00.120"}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := read(t, (&fakeCH{answer: answerRows(tt.body)}).reader(), "SELECT 1")
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestCHReader_Request pins what reaches ClickHouse: the SQL as the POST body,
// the fixed rendering and failure settings, readonly=2 on a read and not on a
// write, the database, the bound values as param_pN in placeholder order, the
// per-query settings, and the credentials as ClickHouse's own headers — over
// the target's configured headers, which ride along.
func TestCHReader_Request(t *testing.T) {
	t.Parallel()
	ch := &fakeCH{}
	target := chconn.Target{
		URL: fakeCHURL, Username: "u", Password: "p", Database: "warehouse",
		Headers: map[string]string{"X-Proxy-Token": "t", "X-ClickHouse-User": "spoofed"},
	}
	_, err := ch.reader().do(t.Context(), target, 0, chRequest{
		sql:      "SELECT * FROM `t` WHERE `a` = {p0:String} AND `b` IN {p1:Array(String)}",
		params:   []string{"/home", "['x','y']"},
		settings: map[string]string{"max_rows_to_read": "1", "read_overflow_mode": "throw"},
	})
	require.NoError(t, err)

	got := ch.last()
	assert.Equal(t, "SELECT * FROM `t` WHERE `a` = {p0:String} AND `b` IN {p1:Array(String)}", got.sql)
	for name, want := range chReadSettingsFixed {
		assert.Equal(t, want, got.query.Get(name), name)
	}
	assert.Equal(t, "2", got.query.Get("readonly"))
	assert.Equal(t, "warehouse", got.query.Get("database"))
	assert.Equal(t, []string{"/home", "['x','y']"}, ch.params())
	assert.Equal(t, "1", got.query.Get("max_rows_to_read"))
	assert.Equal(t, "throw", got.query.Get("read_overflow_mode"))
	assert.Equal(t, "u", got.header.Get("X-ClickHouse-User"))
	assert.Equal(t, "p", got.header.Get("X-ClickHouse-Key"))
	assert.Equal(t, "t", got.header.Get("X-Proxy-Token"))

	_, err = ch.reader().do(t.Context(), target, 0, chRequest{sql: "INSERT INTO t VALUES (1)", write: true})
	require.NoError(t, err)
	assert.False(t, ch.last().query.Has("readonly"), "a write must not be sent read-only")
	assert.Equal(t, int32(1), ch.reads.Load())
	assert.Equal(t, int32(1), ch.writes.Load())
}

// TestCHReader_Errors covers every way a read fails, each typed so
// chconn.Classify and chFailureOf can answer it by class, and each carrying
// ClickHouse's own message — the only diagnostic an operator gets.
func TestCHReader_Errors(t *testing.T) {
	t.Parallel()

	t.Run("a refusal carries its code from the header", func(t *testing.T) {
		t.Parallel()
		_, err := read(t, (&fakeCH{answer: answerException(500, 158, "Code: 158. DB::Exception: Limit for rows exceeded. (TOO_MANY_ROWS)\n")}).reader(), "SELECT 1")
		he, ok := errors.AsType[*chconn.HTTPError](err)
		require.True(t, ok, "%v", err)
		assert.Equal(t, int32(158), he.Code)
		assert.Equal(t, 500, he.StatusCode)
		assert.Contains(t, chErrorMessage(err), "TOO_MANY_ROWS")
		assert.NotContains(t, chErrorMessage(err), "HTTP 500", "the caller reads ClickHouse's text, not the wrapper's")
	})

	t.Run("or from the body when the header is missing", func(t *testing.T) {
		t.Parallel()
		_, err := read(t, (&fakeCH{answer: answerException(500, 0, "Code: 62. DB::Exception: Syntax error")}).reader(), "SELEC 1")
		code, ok := chconn.ExceptionCode(err)
		require.True(t, ok, "%v", err)
		assert.Equal(t, int32(62), code)
	})

	t.Run("an empty refusal still names the status", func(t *testing.T) {
		t.Parallel()
		_, err := read(t, (&fakeCH{answer: answerException(502, 0, "")}).reader(), "SELECT 1")
		status, ok := chconn.HTTPStatus(err)
		require.True(t, ok, "%v", err)
		assert.Equal(t, 502, status)
		assert.Contains(t, chErrorMessage(err), "502")
	})

	t.Run("an exception appended after the rows is an error, not a row", func(t *testing.T) {
		t.Parallel()
		// What ClickHouse sends when a query fails after the 200 went out —
		// which wait_end_of_query exists to prevent, unless something on the
		// way overrides it. Without the check the text would be spliced into
		// the array as if it were data.
		body := "{\"a\":1}\nCode: 241. DB::Exception: Memory limit (total) exceeded. (MEMORY_LIMIT_EXCEEDED)\n"
		_, err := read(t, (&fakeCH{answer: answerRows(body)}).reader(), "SELECT 1")
		code, ok := chconn.ExceptionCode(err)
		require.True(t, ok, "%v", err)
		assert.Equal(t, int32(241), code)
		assert.True(t, strings.HasPrefix(chErrorMessage(err), "Code: 241."), chErrorMessage(err))
	})

	t.Run("trailing text with no code has no verdict", func(t *testing.T) {
		t.Parallel()
		_, err := read(t, (&fakeCH{answer: answerRows("{\"a\":1}\nsomething odd\n")}).reader(), "SELECT 1")
		require.Error(t, err)
		_, hasCode := chconn.ExceptionCode(err)
		assert.False(t, hasCode)
		assert.Equal(t, chconn.Unknown, chconn.Classify(err))
	})

	t.Run("an oversized response is refused, not buffered", func(t *testing.T) {
		t.Parallel()
		r := (&fakeCH{answer: answerRows(strings.Repeat("{\"a\":1}\n", 100))}).reader()
		r.maxResponseBytes = 32
		_, err := read(t, r, "SELECT 1")
		tooLarge, ok := errors.AsType[*chResponseTooLargeError](err)
		require.True(t, ok, "%v", err)
		assert.Equal(t, int64(32), tooLarge.limit)
		assert.Contains(t, err.Error(), "exceeded 32 bytes")
	})

	t.Run("an unreachable endpoint is unavailable", func(t *testing.T) {
		t.Parallel()
		r := newCHReader(readerHTTPClient)
		_, err := r.do(t.Context(), chconn.Target{URL: "http://" + closedAddr(t)}, 0, chRequest{sql: "SELECT 1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "clickhouse request failed")
		assert.Equal(t, chconn.Unavailable, chconn.Classify(err))
	})

	t.Run("an endpoint that does not parse", func(t *testing.T) {
		t.Parallel()
		_, err := (&fakeCH{}).reader().do(t.Context(), chconn.Target{URL: "http://[::1"}, 0, chRequest{sql: "SELECT 1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid clickhouse endpoint")
	})
}

// TestCHReader_ClientsPerCap: a tenant's reads share one transport per
// connection cap, capped at that many connections per server; no cap is
// defaultReadConns, and two caps never share a transport.
func TestCHReader_ClientsPerCap(t *testing.T) {
	t.Parallel()
	r := newCHReader(readerHTTPClient)
	target := chconn.Target{URL: fakeCHURL}
	maxConns := func(c *http.Client) int { return c.Transport.(*http.Transport).MaxConnsPerHost }

	assert.Equal(t, defaultReadConns, maxConns(r.client(target, 0)))
	assert.Same(t, r.client(target, 0), r.client(target, defaultReadConns))
	assert.Equal(t, 7, maxConns(r.client(target, 7)))
	assert.Same(t, r.client(target, 7), r.client(target, 7))
	assert.NotSame(t, r.client(target, 7), r.client(target, 8))
	assert.Equal(t, 7, r.client(target, 7).Transport.(*http.Transport).MaxIdleConnsPerHost)
}

// TestCHReader_ConnectionCapHolds: past the cap, a read waits for a
// connection rather than opening another. The second read is in flight while
// the first holds the only connection; it must not reach the server until the
// first is done.
func TestCHReader_ConnectionCapHolds(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	active, peak := 0, 0
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		mu.Lock()
		active++
		peak = max(peak, active)
		mu.Unlock()
		entered <- struct{}{}
		<-release
		mu.Lock()
		active--
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)

	r := newCHReader(readerHTTPClient)
	target := chconn.Target{URL: srv.URL}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			_, err := r.do(context.Background(), target, 1, chRequest{sql: "SELECT 1"})
			assert.NoError(t, err)
		})
	}
	<-entered
	select {
	case <-entered:
		t.Fatal("a second connection opened past a cap of 1")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, peak)
}

// TestCheckParamSizes: a bound value is refused when its percent-encoded form
// passes ClickHouse's per-field limit — measured to the byte, 131072 read and
// 132096 refused — and all of them together when they pass what the request
// line holds.
func TestCheckParamSizes(t *testing.T) {
	t.Parallel()
	assert.NoError(t, checkParamSizes(nil))
	assert.NoError(t, checkParamSizes([]string{strings.Repeat("a", chMaxFieldBytes)}))
	err := checkParamSizes([]string{strings.Repeat("a", chMaxFieldBytes+1)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "in list")
	// The encoded size counts: a quote is three bytes on the wire.
	require.Error(t, checkParamSizes([]string{strings.Repeat("'", chMaxFieldBytes/3+1)}))

	under := strings.Repeat("a", chMaxFieldBytes)
	var many []string
	for range (chMaxURIBytes - chURIHeadroom) / chMaxFieldBytes {
		many = append(many, under)
	}
	assert.NoError(t, checkParamSizes(many))
	require.Error(t, checkParamSizes(append(many, under)))
}

// chExceptionBody is ClickHouse's own refusal text for code.
func chExceptionBody(code int32, msg string) string {
	return fmt.Sprintf("Code: %d. DB::Exception: %s. (version 26.6.3.62 (official build))\n", code, msg)
}
