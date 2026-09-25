package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
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

// failingConn answers every query with err, as the driver would.
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

type chErrorCase struct {
	name          string
	err           error
	caps          policy.SelectPermissions
	wantStatus    int
	wantCode      string
	wantRetryable bool
}

func chErrorCases(t *testing.T) []chErrorCase {
	t.Helper()
	return []chErrorCase{
		{name: "syntax error", err: chException(62, "DB::Exception", "Syntax error"), wantStatus: 400, wantCode: codeCHRejected},
		{name: "unknown identifier (#271)", err: chException(47, "DB::Exception", "Unknown expression identifier `received_timestamp`"), wantStatus: 400, wantCode: codeCHRejected},
		{name: "type mismatch", err: chException(53, "DB::Exception", "Type mismatch"), wantStatus: 400, wantCode: codeCHRejected},
		{name: "unknown table", err: chException(60, "DB::Exception", "Table default.gone does not exist"), wantStatus: 400, wantCode: codeCHRejected},
		{name: "rows read cap", err: chException(158, "DB::Exception", "Limit for rows exceeded"), wantStatus: 400, wantCode: codeCHLimitExceeded},
		{name: "time cap of the role", err: chException(159, "DB::Exception", "Timeout exceeded"), caps: policy.SelectPermissions{MaxExecutionTime: 1}, wantStatus: 400, wantCode: codeCHLimitExceeded},
		{name: "deadline of the role's time cap", err: fmt.Errorf("clickhouse query: %w", context.DeadlineExceeded), caps: policy.SelectPermissions{MaxExecutionTime: 1}, wantStatus: 400, wantCode: codeCHLimitExceeded},
		{name: "memory cap of the role", err: chException(241, "DB::Exception", "Memory limit (for query) exceeded"), caps: policy.SelectPermissions{MaxMemoryUsage: 1}, wantStatus: 400, wantCode: codeCHLimitExceeded},
		{name: "server timeout, no role cap", err: chException(159, "DB::Exception", "Timeout exceeded"), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "server memory, no role cap", err: chException(241, "DB::Exception", "Memory limit (total) exceeded"), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "missing grant", err: chException(497, "DB::Exception", "default: Not enough privileges"), wantStatus: 403, wantCode: codeCHAccessDenied},
		{name: "wrong password", err: chException(516, "DB::Exception", "Authentication failed"), wantStatus: 502, wantCode: codeCHMisconfigured},
		{name: "overloaded", err: chException(202, "DB::Exception", "Too many simultaneous queries"), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "connection refused", err: fmt.Errorf("clickhouse query: %w", refusedDial(t)), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "pool exhausted", err: fmt.Errorf("clickhouse query: %w", clickhouse.ErrAcquireConnTimeout), wantStatus: 503, wantCode: codeCHUnavailable, wantRetryable: true},
		{name: "no verdict", err: errors.New("scan clickhouse row: something odd"), wantStatus: 500, wantCode: codeCHUnknown, wantRetryable: true},
	}
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
			h := newCapturingHandler(t, &failingConn{err: tc.err}, policyWithViewer(perms))
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
			store := staticPipes(&pipes.NamedQuery{Name: "p", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}})
			h := NewPipesHandler(store, staticPolicy(&policy.Policy{}), fixedConn(&failingConn{err: tc.err}), nil, func(*settings.Store) time.Duration { return time.Second })
			r := pipesRequest(t, http.MethodGet, "/v1/pipes/p", "p", nil)
			r = r.WithContext(auth.WithRole(r.Context(), "viewer"))
			w := httptest.NewRecorder()
			h.Execute(w, withTenant(r))
			assertCHError(t, w, tc)
		})
	}
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
