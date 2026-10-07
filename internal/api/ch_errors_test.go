package api

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// failingConn answers every query with err, as the driver would — schema
// discovery's connection, which still speaks the native protocol.
type failingConn struct {
	driver.Conn
	err error
}

func (c *failingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, c.err
}

func chException(code int32, name, msg string) error {
	return &clickhouse.Exception{Code: code, Name: name, Message: msg}
}

// refusedDial is the error a dial to a closed port really returns.
func refusedDial(t *testing.T) error {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	var d net.Dialer
	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if conn != nil {
		_ = conn.Close()
	}
	require.Error(t, err)
	return err
}

// chErrorCase is one way ClickHouse, or the way to it, fails a query: the
// answer the fake gives (or the transport failure it meets), and the
// response that must come of it.
type chErrorCase struct {
	name string
	// answer is what ClickHouse sends; transport, when set, is the failure
	// the request meets instead.
	answer    func(http.ResponseWriter, *chSeen)
	transport error
	// maxResponse, when set, caps the response buffer.
	maxResponse int64
	caps        policy.SelectPermissions
	// readOnly marks a case that only a read meets: ClickHouse refusing
	// readonly=2, which a write is never sent under.
	readOnly      bool
	wantStatus    int
	wantCode      string
	wantRetryable bool
}

// refused is ClickHouse refusing a statement with code at status.
func refused(status int, code int32, msg string) func(http.ResponseWriter, *chSeen) {
	return answerException(status, code, chExceptionBody(code, msg))
}

func chErrorCases(t *testing.T) []chErrorCase {
	t.Helper()
	return []chErrorCase{
		{name: "syntax error", answer: refused(400, 62, "Syntax error"), wantStatus: 400, wantCode: codeCHRejected},
		{name: "unknown identifier (#271)", answer: refused(404, 47, "Unknown expression identifier `received_timestamp`"), wantStatus: 400, wantCode: codeCHRejected},
		{name: "type mismatch", answer: refused(400, 53, "Cannot convert string '2026-01-15T10:30:00Z' to type DateTime"), wantStatus: 400, wantCode: codeCHRejected},
		{name: "unknown table", answer: refused(404, 60, "Table default.gone does not exist"), wantStatus: 400, wantCode: codeCHRejected},
		{name: "rows read cap", answer: refused(500, 158, "Limit for rows exceeded"), wantStatus: 400, wantCode: codeCHLimitExceeded},
		{name: "time cap of the role", answer: refused(408, 159, "Timeout exceeded"), caps: policy.SelectPermissions{MaxExecutionTime: 1}, wantStatus: 400, wantCode: codeCHLimitExceeded},
		// A dropped connection under a capped role is not the cap: ClickHouse
		// reports the cap itself as 159.
		{name: "deadline under a time cap", transport: context.DeadlineExceeded, caps: policy.SelectPermissions{MaxExecutionTime: 1}, wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "cancel under a time cap", transport: context.Canceled, caps: policy.SelectPermissions{MaxExecutionTime: 1}, wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		// A cap longer than the handler's 5s query_timeout is not what
		// stopped the query: the timeout reads as it does with no cap.
		{name: "query_timeout under a longer time cap", answer: refused(408, 159, "Timeout exceeded"), caps: policy.SelectPermissions{MaxExecutionTime: 10000}, wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "memory cap of the role", answer: refused(500, 241, "Memory limit (for query) exceeded"), caps: policy.SelectPermissions{MaxMemoryUsage: 1}, wantStatus: 400, wantCode: codeCHLimitExceeded},
		{name: "server timeout, no role cap", answer: refused(408, 159, "Timeout exceeded"), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "server memory, no role cap", answer: refused(500, 241, "Memory limit (total) exceeded"), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "missing grant", answer: refused(403, 497, "default: Not enough privileges"), wantStatus: 403, wantCode: codeCHAccessDenied},
		{name: "wrong password", answer: refused(403, 516, "Authentication failed"), wantStatus: 502, wantCode: codeCHMisconfigured},
		{name: "overloaded", answer: refused(500, 202, "Too many simultaneous queries"), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		// A statement that writes, refused by the readonly=2 every read
		// carries, or a readonly=1 profile refusing the settings every read
		// sends: configuration, the same on a retry.
		{name: "read-only refusal of a read", answer: refused(500, 164, "default: Cannot execute query in readonly mode"), readOnly: true, wantStatus: 502, wantCode: codeCHMisconfigured},
		{name: "connection refused", transport: refusedDial(t), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "codeless 404 on the way", answer: answerException(404, 0, "There is no handle /nope"), wantStatus: 502, wantCode: codeCHMisconfigured},
		{name: "codeless 500 on the way", answer: answerException(500, 0, "upstream exploded"), wantStatus: 500, wantCode: codeCHUnknown, wantRetryable: true},
		{name: "exception after the rows", answer: answerRows("{\"page\":\"/\"}\n" + chExceptionBody(241, "Memory limit (total) exceeded")), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "no verdict", answer: answerRows("{\"page\":\"/\"}\nsomething odd\n"), wantStatus: 500, wantCode: codeCHUnknown, wantRetryable: true},
		{name: "response too large", answer: answerRows(strings.Repeat("{\"page\":\"/\"}\n", 10)), maxResponse: 16, wantStatus: 502, wantCode: codeCHResponseTooLarge},
	}
}

// fake is the fakeCH tc answers through, and its reader.
func (tc chErrorCase) fake() (*fakeCH, *chReader) {
	f := &fakeCH{answer: tc.answer, err: tc.transport}
	r := f.reader()
	r.maxResponseBytes = tc.maxResponse
	return f, r
}

func assertCHError(t *testing.T, w *httptest.ResponseRecorder, tc chErrorCase) {
	t.Helper()
	require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
	var got errorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, tc.wantCode, got.Code)
	require.NotNil(t, got.Retryable)
	assert.Equal(t, tc.wantRetryable, *got.Retryable)
	assert.NotEmpty(t, got.Error)
	if tc.wantStatus == http.StatusServiceUnavailable {
		assert.Equal(t, retryAfterClickHouse, w.Header().Get("Retry-After"))
	} else {
		assert.Empty(t, w.Header().Get("Retry-After"))
	}
}

// TestStructuredQuery_ClickHouseErrors: a failed ClickHouse read on
// /v1/query answers by the error's class, not a flat 500 (#271).
func TestStructuredQuery_ClickHouseErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range chErrorCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			perms := tc.caps
			perms.AllowColumns = []string{"*"}
			f, r := tc.fake()
			h := newCapturingHandler(t, f, policyWithViewer(perms))
			h.ch = r
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(viewerRequest(t, query.StructuredQuery{Columns: []string{"page"}})))
			assertCHError(t, w, tc)
		})
	}
}

// TestPipes_ClickHouseErrors: the same mapping on a pipe. A pipe carries no
// role caps, so the rows it is given here carry none either.
func TestPipes_ClickHouseErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range chErrorCases(t) {
		if tc.caps.MaxExecutionTime > 0 || tc.caps.MaxMemoryUsage > 0 {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, r := tc.fake()
			store := staticPipes(&pipes.NamedQuery{Name: "p", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}})
			h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), f.target, nil, func(*settings.Store) time.Duration { return time.Second })
			h.ch = r
			req := pipesRequest(t, http.MethodGet, "/v1/pipes/p", "p", nil)
			req = req.WithContext(auth.WithRole(req.Context(), "viewer"))
			w := httptest.NewRecorder()
			h.Execute(w, withTenant(req))
			assertCHError(t, w, tc)
		})
	}
}

// TestPipes_WriteClickHouseErrors: a failed write pipe answers with its
// class's status and code, but never as retryable and with no Retry-After:
// the statement may have run, so a client that retried would run it again.
func TestPipes_WriteClickHouseErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range chErrorCases(t) {
		if tc.caps.MaxExecutionTime > 0 || tc.caps.MaxMemoryUsage > 0 || tc.readOnly {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, r := tc.fake()
			h := writerPipesHandler(t, f, nil, &pipes.NamedQuery{Name: "log", SQL: "INSERT INTO audit_log VALUES ({{msg}}, now())"})
			h.ch = r
			w := pipeCallAs(t, h, "log")
			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			var got errorBody
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			assert.Equal(t, tc.wantCode, got.Code)
			require.NotNil(t, got.Retryable)
			assert.False(t, *got.Retryable)
			assert.Empty(t, w.Header().Get("Retry-After"))
			assert.Equal(t, int32(1), f.writes.Load())
		})
	}
}

// TestCHErrors_MessageIsClickHousesOwn: the caller reads ClickHouse's own
// refusal text, verbatim, as the proxy forwards it — not a wrapper around it.
func TestCHErrors_MessageIsClickHousesOwn(t *testing.T) {
	t.Parallel()
	f := &fakeCH{answer: refused(400, 62, "Syntax error")}
	h := newCapturingHandler(t, f, policyWithViewer(policy.SelectPermissions{AllowColumns: []string{"*"}}))
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(viewerRequest(t, query.StructuredQuery{Columns: []string{"page"}})))
	var got errorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, strings.TrimSpace(chExceptionBody(62, "Syntax error")), got.Error)
}

type errRow struct{ err error }

func (r errRow) Err() error           { return r.err }
func (r errRow) Scan(...any) error    { return r.err }
func (r errRow) ScanStruct(any) error { return r.err }

func (c *failingConn) QueryRow(context.Context, string, ...any) driver.Row { return errRow{c.err} }

// TestSchemaRefresh_ClickHouseDown: a refresh that cannot reach ClickHouse is
// a 503 with Retry-After; any other failure stays the 500 it was.
func TestSchemaRefresh_ClickHouseDown(t *testing.T) {
	t.Parallel()
	refresh := func(err error) *httptest.ResponseRecorder {
		conn := &failingConn{err: err}
		reg := discovery.NewSchemaRegistry(func() (driver.Conn, string) { return conn, "test" }, tenant.Default, func(tenant.ID) time.Duration { return time.Hour })
		h := NewSchemaHandler(fixedRegistry(reg))
		h.Tenants = testTenants()
		w := httptest.NewRecorder()
		h.Refresh(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ops/schema/refresh", nil))
		return w
	}
	assertUnavailable(t, refresh(refusedDial(t)), "refresh failed", retryAfterClickHouse)
	w := refresh(chException(62, "DB::Exception", "Syntax error"))
	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
}
