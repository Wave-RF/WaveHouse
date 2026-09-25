package chconn

import (
	"bytes"
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refusedDialErr is what net/http really returns for a ClickHouse that is not
// listening: a *url.Error around a *net.OpError around ECONNREFUSED.
func refusedDialErr(t *testing.T) error {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+addr+"/", strings.NewReader("[1]"))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	return err
}

// clientTimeoutErr is net/http's own timeout: a ClickHouse that accepted the
// connection and never answered.
func clientTimeoutErr(t *testing.T) error {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(func() { close(release); srv.Close() })
	c := &http.Client{Timeout: 20 * time.Millisecond}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("[1]"))
	require.NoError(t, err)
	resp, err := c.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	return err
}

func httpResp(status int, header http.Header, body string) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewBufferString(body))}
}

// readHTTPError is NewHTTPError over a canned response, closed afterwards as
// the worker closes its own.
func readHTTPError(status int, header http.Header, body string) *HTTPError {
	resp := httpResp(status, header, body)
	defer func() { _ = resp.Body.Close() }()
	return NewHTTPError(resp)
}

func codeHeader(code string) http.Header {
	h := http.Header{}
	h.Set("X-ClickHouse-Exception-Code", code)
	return h
}

func TestClassify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  func(t *testing.T) error
		want Class
	}{
		{"nil", func(*testing.T) error { return nil }, Unknown},
		{"plain error", func(*testing.T) error { return errors.New("boom") }, Unknown},

		// Transport: the request never reached a verdict.
		{"connection refused (real dial)", refusedDialErr, Unavailable},
		{"client timeout (real)", clientTimeoutErr, Unavailable},
		{"context deadline", func(*testing.T) error { return fmt.Errorf("insert: %w", context.DeadlineExceeded) }, Unavailable},
		{"context canceled", func(*testing.T) error { return context.Canceled }, Unavailable},
		{"connection reset", func(*testing.T) error {
			return &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
		}, Unavailable},
		{"dns", func(*testing.T) error { return &net.DNSError{Err: "no such host", Name: "ch"} }, Unavailable},
		{"unexpected eof", func(*testing.T) error { return fmt.Errorf("read: %w", io.ErrUnexpectedEOF) }, Unavailable},
		{"driver acquire timeout", func(*testing.T) error { return clickhouse.ErrAcquireConnTimeout }, Unavailable},
		{"driver connection closed", func(*testing.T) error { return clickhouse.ErrConnectionClosed }, Unavailable},
		{"driver bad conn", func(*testing.T) error { return sqldriver.ErrBadConn }, Unavailable},

		// Native driver exceptions.
		{"native TIMEOUT_EXCEEDED", func(*testing.T) error { return &clickhouse.Exception{Code: 159} }, Unavailable},
		{"native TOO_MANY_SIMULTANEOUS_QUERIES", func(*testing.T) error { return &clickhouse.Exception{Code: 202} }, Unavailable},
		{"native MEMORY_LIMIT_EXCEEDED wrapped", func(*testing.T) error {
			return fmt.Errorf("query: %w", &clickhouse.Exception{Code: 241})
		}, Unavailable},
		{"native KEEPER_EXCEPTION", func(*testing.T) error { return &clickhouse.Exception{Code: 999} }, Unavailable},
		{"native ACCESS_DENIED", func(*testing.T) error { return &clickhouse.Exception{Code: 497} }, Denied},
		{"native UNKNOWN_TABLE", func(*testing.T) error { return &clickhouse.Exception{Code: 60} }, Rejected},
		{"native CANNOT_PARSE_TEXT", func(*testing.T) error { return &clickhouse.Exception{Code: 6} }, Rejected},
		{"driver HTTPError around AUTHENTICATION_FAILED", func(*testing.T) error {
			return &clickhouse.HTTPError{StatusCode: 403, Err: &clickhouse.Exception{Code: 516}}
		}, Denied},
		{"driver HTTPError, proxy 503 without code", func(*testing.T) error {
			return &clickhouse.HTTPError{StatusCode: 503, Err: errors.New(`response body: "upstream down"`)}
		}, Unavailable},

		// The worker's HTTP interface answers.
		{"http READONLY by header", func(*testing.T) error {
			return readHTTPError(500, codeHeader("164"), "Code: 164. DB::Exception: readonly")
		}, Unavailable},
		{"http TOO_MANY_PARTS by body", func(*testing.T) error {
			return readHTTPError(500, nil, "Code: 252. DB::Exception: Too many parts (TOO_MANY_PARTS)")
		}, Unavailable},
		{"http TABLE_IS_READ_ONLY", func(*testing.T) error {
			return readHTTPError(500, codeHeader("242"), "Code: 242. DB::Exception: Table is in readonly mode")
		}, Unavailable},
		{"http CANNOT_PARSE_NUMBER", func(*testing.T) error {
			return readHTTPError(400, codeHeader("72"), "Code: 72. DB::Exception: Cannot parse number")
		}, Rejected},
		{"http TYPE_MISMATCH wrapped", func(*testing.T) error {
			return fmt.Errorf("insert: %w", readHTTPError(500, codeHeader("53"), "Code: 53. type mismatch"))
		}, Rejected},
		{"http AUTHENTICATION_FAILED", func(*testing.T) error {
			return readHTTPError(403, codeHeader("516"), "Code: 516. DB::Exception: default: Authentication failed")
		}, Denied},
		{"http unlisted code", func(*testing.T) error {
			return readHTTPError(500, codeHeader("117"), "Code: 117. INCORRECT_DATA")
		}, Rejected},
		{"http 502 from a proxy", func(*testing.T) error { return readHTTPError(502, nil, "Bad Gateway") }, Unavailable},
		{"http 429 from a proxy", func(*testing.T) error { return readHTTPError(429, nil, "slow down") }, Unavailable},
		{"http 401 from a proxy", func(*testing.T) error { return readHTTPError(401, nil, "who are you") }, Denied},
		{"http 413 from a proxy", func(*testing.T) error { return readHTTPError(413, nil, "too large") }, Rejected},
		{"http 500 without a code", func(*testing.T) error { return readHTTPError(500, nil, "internal error") }, Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, Classify(tt.err(t)))
		})
	}
}

func TestClassOfCode(t *testing.T) {
	t.Parallel()
	for code := range unavailableCodes {
		assert.Equal(t, Unavailable, ClassOfCode(code), "code %d", code)
		_, alsoDenied := deniedCodes[code]
		assert.False(t, alsoDenied, "code %d is on both lists", code)
	}
	for code := range deniedCodes {
		assert.Equal(t, Denied, ClassOfCode(code), "code %d", code)
	}
	// Unlisted codes are the server's verdict on the request.
	for _, code := range []int32{6, 16, 27, 38, 41, 47, 53, 60, 62, 72, 81, 117, 1002} {
		assert.Equal(t, Rejected, ClassOfCode(code), "code %d", code)
	}
}

func TestClassString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "unknown", Unknown.String())
	assert.Equal(t, "unavailable", Unavailable.String())
	assert.Equal(t, "denied", Denied.String())
	assert.Equal(t, "rejected", Rejected.String())
}

func TestNewHTTPError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		status   int
		header   http.Header
		body     string
		wantCode int32
	}{
		{"header wins over body", 500, codeHeader("241"), "Code: 60. something", 241},
		{"body when no header", 500, nil, "Code: 60. DB::Exception: Unknown table", 60},
		{"garbage header falls back to body", 500, codeHeader("x"), "Code: 81. db", 81},
		{"no code anywhere", 502, nil, "Bad Gateway", 0},
		{"code not at the start is not a code", 500, nil, "proxy says Code: 60. nope", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := readHTTPError(tt.status, tt.header, tt.body)
			assert.Equal(t, tt.wantCode, e.Code)
			assert.Equal(t, tt.status, e.StatusCode)
			code, ok := ExceptionCode(e)
			assert.Equal(t, tt.wantCode != 0, ok)
			assert.Equal(t, tt.wantCode, code)
		})
	}

	// The message keeps the body verbatim (the DLQ headers carry it), capped.
	e := readHTTPError(400, nil, "Code: 60. DB::Exception: Unknown table.")
	assert.Equal(t, "HTTP 400: Code: 60. DB::Exception: Unknown table.", e.Error())
	long := readHTTPError(500, nil, strings.Repeat("x", 10*maxErrorBody))
	assert.Len(t, long.Body, maxErrorBody)
}
