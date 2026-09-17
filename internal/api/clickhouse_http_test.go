package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
)

// chStub is a ClickHouse HTTP interface that answers with whatever the test
// wants and records what it was asked.
type chStub struct {
	server *httptest.Server
	status int
	body   string

	gotSQL    string
	gotQuery  url.Values
	gotHeader http.Header
}

func newCHStub(t testing.TB) *chStub {
	t.Helper()
	s := &chStub{status: http.StatusOK}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.gotSQL = string(body)
		s.gotQuery = r.URL.Query()
		s.gotHeader = r.Header.Clone()
		w.WriteHeader(s.status)
		_, _ = io.WriteString(w, s.body)
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *chStub) reader(target chconn.Target) *chReader {
	if target.URL == "" {
		target.URL = s.server.URL
	}
	return newCHReader(func() chconn.Target { return target })
}

// TestCHReader_ResponseShape pins the body the cached read paths serve.
// ClickHouse's JSONEachRow output is newline-delimited, the endpoint's
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
		{
			// A newline inside a value is escaped by JSONEachRow, so splitting
			// on '\n' cannot cut a row in half.
			name: "an escaped newline inside a value is not a row boundary",
			body: "{\"a\":\"x\\ny\"}\n",
			want: `[{"a":"x\ny"}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := newCHStub(t)
			stub.body = tt.body
			got, err := stub.reader(chconn.Target{}).query(context.Background(), "SELECT 1", nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestCHReader_Request pins what reaches ClickHouse: the SQL as the POST body,
// the output-format contract and the database on the query string, the bound
// values as param_pN in placeholder order, the role's caps as settings, and
// the credentials as ClickHouse's own headers.
func TestCHReader_Request(t *testing.T) {
	t.Parallel()
	stub := newCHStub(t)
	r := stub.reader(chconn.Target{Username: "u", Password: "p", Database: "warehouse"})

	_, err := r.query(context.Background(),
		"SELECT * FROM `t` WHERE `a` = {p0:String} AND `b` IN {p1:Array(String)}",
		[]string{"/home", "['x','y']"},
		map[string]string{"max_rows_to_read": "1", "read_overflow_mode": "throw"},
	)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM `t` WHERE `a` = {p0:String} AND `b` IN {p1:Array(String)}", stub.gotSQL)
	assert.Equal(t, "JSONEachRow", stub.gotQuery.Get("default_format"))
	// Unquoted 64-bit integers are the contract this endpoint has always
	// returned; pinned explicitly so a server-side profile cannot change it.
	assert.Equal(t, "0", stub.gotQuery.Get("output_format_json_quote_64bit_integers"))
	// DateTime rendering is deliberately NOT overridden: ClickHouse's default
	// is the same spelling the SSE wire carries (#372).
	assert.Empty(t, stub.gotQuery.Get("date_time_output_format"))
	assert.Equal(t, "warehouse", stub.gotQuery.Get("database"))
	assert.Equal(t, "/home", stub.gotQuery.Get("param_p0"))
	assert.Equal(t, "['x','y']", stub.gotQuery.Get("param_p1"))
	assert.Equal(t, "1", stub.gotQuery.Get("max_rows_to_read"))
	assert.Equal(t, "throw", stub.gotQuery.Get("read_overflow_mode"))
	assert.Equal(t, "u", stub.gotHeader.Get("X-ClickHouse-User"))
	assert.Equal(t, "p", stub.gotHeader.Get("X-ClickHouse-Key"))
}

// TestCHReader_Errors covers every way a read fails. ClickHouse's own message
// is carried through verbatim in each case — it is the only diagnostic an
// operator gets.
func TestCHReader_Errors(t *testing.T) {
	t.Parallel()

	t.Run("non-200 surfaces ClickHouse's message", func(t *testing.T) {
		t.Parallel()
		stub := newCHStub(t)
		stub.status = http.StatusInternalServerError
		stub.body = "Code: 158. DB::Exception: Limit for rows exceeded. (TOO_MANY_ROWS)\n"
		_, err := stub.reader(chconn.Target{}).query(context.Background(), "SELECT 1", nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Code: 158")
		assert.Contains(t, err.Error(), "TOO_MANY_ROWS")
	})

	t.Run("an empty non-200 body still names the status", func(t *testing.T) {
		t.Parallel()
		stub := newCHStub(t)
		stub.status = http.StatusBadGateway
		_, err := stub.reader(chconn.Target{}).query(context.Background(), "SELECT 1", nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "502")
	})

	t.Run("an exception appended mid-stream is an error, not a row", func(t *testing.T) {
		t.Parallel()
		// ClickHouse had already sent 200 and some rows when the query failed,
		// so it appends the exception to the body. Without this check the text
		// would be spliced into the JSON array as if it were data.
		stub := newCHStub(t)
		stub.body = "{\"a\":1}\nCode: 241. DB::Exception: Memory limit exceeded. (MEMORY_LIMIT_EXCEEDED)\n"
		_, err := stub.reader(chconn.Target{}).query(context.Background(), "SELECT 1", nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Code: 241")
	})

	t.Run("an oversized response is refused, not buffered", func(t *testing.T) {
		t.Parallel()
		stub := newCHStub(t)
		stub.body = strings.Repeat("{\"a\":1}\n", 100)
		r := stub.reader(chconn.Target{})
		r.maxResponseBytes = 32
		_, err := r.query(context.Background(), "SELECT 1", nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeded 32 bytes")
	})

	t.Run("an unconfigured reader reports itself", func(t *testing.T) {
		t.Parallel()
		// A zero-value handler (routing-only tests) must surface a diagnostic
		// rather than panic on a nil target.
		_, err := newCHReader(nil).query(context.Background(), "SELECT 1", nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not configured")
	})

	t.Run("an unreachable endpoint is an error", func(t *testing.T) {
		t.Parallel()
		r := newCHReader(func() chconn.Target { return chconn.Target{URL: "http://127.0.0.1:1"} })
		_, err := r.query(context.Background(), "SELECT 1", nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "clickhouse request failed")
	})
}
