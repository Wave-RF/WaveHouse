package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/chconn/chconntest"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/pkg/pipes"
	"github.com/Wave-RF/WaveHouse/pkg/policy"
)

// recordCHSpans installs a span recorder as the global tracer provider until
// the test ends. The caller must not run in parallel.
func recordCHSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		_ = tp.Shutdown(context.Background())
	})
	return rec
}

// chSpan is the one ended clickhouse.query span rec holds, checked to be a
// client span under server that names no statement text.
func chSpan(t *testing.T, rec *tracetest.SpanRecorder, server trace.Span, sql string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "clickhouse.query" {
			found = append(found, s)
		}
	}
	require.Len(t, found, 1)
	s := found[0]
	assert.Equal(t, trace.SpanKindClient, s.SpanKind())
	assert.Equal(t, server.SpanContext().SpanID(), s.Parent().SpanID(), "a child of the server span")
	for _, kv := range s.Attributes() {
		assert.NotEqual(t, "db.statement", string(kv.Key))
		assert.NotContains(t, kv.Value.String(), "hunter2", "%s carries a parameter", kv.Key)
		if sql != "" {
			assert.NotContains(t, kv.Value.String(), sql, "%s carries the statement", kv.Key)
		}
	}
	for _, e := range s.Events() {
		for _, kv := range e.Attributes {
			assert.NotContains(t, kv.Value.String(), "hunter2", "event %s carries a parameter", e.Name)
		}
	}
	return s
}

// A structured query is one clickhouse.query span under the server span,
// failed and named by its result alone when ClickHouse refuses it: the
// exception's message, which can quote the statement, stays off the span.
func TestStructuredQuery_CHSpan(t *testing.T) {
	rec := recordCHSpans(t)
	_, server := otel.Tracer("test").Start(context.Background(), "server")
	defer server.End()
	h := newCapturingHandler(t, &failingConn{err: chException(62, "DB::Exception", "Syntax error near 'hunter2'")},
		policyWithViewer(policy.SelectPermissions{AllowColumns: []string{"*"}}))
	h.CHTarget = staticTarget("http://ch.internal:8123", "default", "secret", "events")

	r := viewerRequest(t, query.StructuredQuery{Columns: []string{"page"}, Filters: []query.Filter{{Column: "page", Op: "eq", Value: "hunter2"}}})
	h.Handle(httptest.NewRecorder(), withTenant(r.WithContext(trace.ContextWithSpan(r.Context(), server))))

	s := chSpan(t, rec, server, "")
	attrs := s.Attributes()
	assert.Contains(t, attrs, attribute.String("db.system", "clickhouse"))
	assert.Contains(t, attrs, attribute.String("db.operation", "SELECT"))
	assert.Contains(t, attrs, attribute.String("db.sql.table", "clicks"))
	assert.Contains(t, attrs, attribute.String("db.name", "events"))
	assert.Contains(t, attrs, attribute.String("server.address", "ch.internal"))
	assert.Contains(t, attrs, attribute.String("operation", opStructuredQuery))
	assert.Contains(t, attrs, attribute.String("result", chResult(codeCHRejected)))
	assert.Contains(t, attrs, attribute.String("error.type", chResult(codeCHRejected)))
	for _, kv := range attrs {
		assert.NotEqual(t, "server.port", string(kv.Key), "the native port is not the URL's")
	}
	assert.Equal(t, codes.Error, s.Status().Code)
	assert.Equal(t, chResult(codeCHRejected), s.Status().Description)
}

// A pipe's call is a span naming the pipe; a read that fails is failed, a
// write that succeeds is not.
func TestPipes_CHSpan(t *testing.T) {
	rec := recordCHSpans(t)
	_, server := otel.Tracer("test").Start(context.Background(), "server")
	defer server.End()

	store := staticPipes(&pipes.NamedQuery{Name: "p", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}})
	p := NewPipesHandler(store, staticPolicy(&policy.Policy{}), fixedConn(&failingConn{err: chException(159, "DB::Exception", "Timeout exceeded")}), nil,
		func(*settings.Store) time.Duration { return time.Second })
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/p", "p", nil)
	p.Execute(httptest.NewRecorder(), withTenant(r.WithContext(auth.WithRole(trace.ContextWithSpan(r.Context(), server), "viewer"))))
	s := chSpan(t, rec, server, "SELECT 1")
	assert.Contains(t, s.Attributes(), attribute.String("pipe", "p"))
	assert.Contains(t, s.Attributes(), attribute.String("operation", opPipe))
	assert.Contains(t, s.Attributes(), attribute.String("db.operation", "SELECT"))
	assert.Equal(t, codes.Error, s.Status().Code)

	rec.Reset()
	const insert = "INSERT INTO audit_log VALUES ({{msg}}, now())"
	wp := writerPipesHandler(t, &writeConn{}, nil, &pipes.NamedQuery{Name: "log", SQL: insert})
	w := httptest.NewRecorder()
	r = pipesRequest(t, http.MethodPost, "/v1/pipes/log", "log", map[string]any{"msg": "hunter2"})
	wp.Execute(w, withTenant(r.WithContext(auth.WithRole(trace.ContextWithSpan(r.Context(), server), "writer"))))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	s = chSpan(t, rec, server, "INSERT INTO audit_log")
	assert.Contains(t, s.Attributes(), attribute.String("pipe", "log"))
	assert.Contains(t, s.Attributes(), attribute.String("operation", opPipeWrite))
	assert.Contains(t, s.Attributes(), attribute.String("db.operation", "INSERT"))
	assert.Contains(t, s.Attributes(), attribute.String("result", resultOK))
	assert.Equal(t, codes.Unset, s.Status().Code)
}

// The raw-SQL proxy's call is a span with the server's address and port,
// failed when ClickHouse refuses the statement or its answer is over the cap.
func TestOpsQuery_CHSpan(t *testing.T) {
	rec := recordCHSpans(t)
	ctx, server := otel.Tracer("test").Start(context.Background(), "server")
	defer server.End()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sql, _ := io.ReadAll(r.Body)
		switch {
		case strings.HasPrefix(string(sql), "SELECT 'hunter2'"):
			_, _ = w.Write([]byte(`{"data":[{"x":1}]}`))
		case strings.HasPrefix(string(sql), "SELECT big"):
			_, _ = w.Write([]byte(`{"data":[` + strings.Repeat(`{"x":1},`, 64) + `{"x":1}]}`))
		default:
			w.Header().Set("X-ClickHouse-Exception-Code", "62")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Code: 62. DB::Exception: Syntax error near 'hunter2'. (SYNTAX_ERROR)\n"))
		}
	}))
	t.Cleanup(srv.Close)
	h := newTestQueryHandler(staticTarget(srv.URL, "default", "secret", "default"), func(*settings.Store) time.Duration { return 30 * time.Second })
	h.maxResponseBytes = 256
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)

	for _, tc := range []struct {
		name, sql, result string
		code              codes.Code
	}{
		{"select", "SELECT 'hunter2'", resultOK, codes.Unset},
		{"syntax error", "SELEC 'hunter2'", chResult(codeCHRejected), codes.Error},
		{"response too large", "SELECT big", chResult(codeCHResponseTooLarge), codes.Error},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			body, err := json.Marshal(queryRequest{SQL: tc.sql})
			require.NoError(t, err)
			r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/ops/query", bytes.NewReader(body))
			h.Handle(httptest.NewRecorder(), r)

			s := chSpan(t, rec, server, tc.sql)
			attrs := s.Attributes()
			assert.Contains(t, attrs, attribute.String("operation", opOpsQuery))
			assert.Contains(t, attrs, attribute.String("db.name", "default"))
			assert.Contains(t, attrs, attribute.String("server.address", u.Hostname()))
			assert.Contains(t, attrs, attribute.Int("server.port", port))
			assert.Contains(t, attrs, attribute.String("result", tc.result))
			assert.Equal(t, tc.code, s.Status().Code)
		})
	}
}

// ctxConn records the context each statement reached the driver with.
type ctxConn struct {
	driver.Conn
	mu   sync.Mutex
	ctxs []context.Context
}

func (c *ctxConn) keep(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ctxs = append(c.ctxs, ctx)
}

func (c *ctxConn) Query(ctx context.Context, _ string, _ ...any) (driver.Rows, error) {
	c.keep(ctx)
	return &chainEmptyRows{}, nil
}

func (c *ctxConn) Exec(ctx context.Context, _ string, _ ...any) error {
	c.keep(ctx)
	return nil
}

// last is the context of the last statement, checked to be the only one.
func (c *ctxConn) last(t *testing.T) context.Context {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Len(t, c.ctxs, 1)
	ctx := c.ctxs[0]
	c.ctxs = nil
	return ctx
}

// A structured query sends its clickhouse.query span to ClickHouse as the
// query's trace context, alongside the role's caps.
func TestStructuredQuery_SendsTraceContext(t *testing.T) {
	rec := recordCHSpans(t)
	_, server := otel.Tracer("test").Start(context.Background(), "server")
	defer server.End()
	conn := &ctxConn{}
	h := newCapturingHandler(t, conn, policyWithViewer(policy.SelectPermissions{AllowColumns: []string{"*"}, MaxRows: 5}))

	r := viewerRequest(t, query.StructuredQuery{Columns: []string{"page"}})
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(r.WithContext(trace.ContextWithSpan(r.Context(), server))))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	span, settings := chconntest.QueryOptions(conn.last(t), t)
	assert.Equal(t, chSpan(t, rec, server, "").SpanContext(), span)
	assert.Equal(t, 5, settings["max_result_rows"], "the role's caps still ride on the query")
}

// A pipe's read and write each send their clickhouse.query span.
func TestPipes_SendTraceContext(t *testing.T) {
	rec := recordCHSpans(t)
	_, server := otel.Tracer("test").Start(context.Background(), "server")
	defer server.End()

	conn := &ctxConn{}
	store := staticPipes(&pipes.NamedQuery{Name: "p", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}})
	p := NewPipesHandler(store, staticPolicy(&policy.Policy{}), fixedConn(conn), nil,
		func(*settings.Store) time.Duration { return time.Second })
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/p", "p", nil)
	w := httptest.NewRecorder()
	p.Execute(w, withTenant(r.WithContext(auth.WithRole(trace.ContextWithSpan(r.Context(), server), "viewer"))))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	span, _ := chconntest.QueryOptions(conn.last(t), t)
	assert.Equal(t, chSpan(t, rec, server, "SELECT 1").SpanContext(), span)

	rec.Reset()
	const insert = "INSERT INTO audit_log VALUES ({{msg}}, now())"
	wp := writerPipesHandler(t, conn, nil, &pipes.NamedQuery{Name: "log", SQL: insert})
	w = httptest.NewRecorder()
	r = pipesRequest(t, http.MethodPost, "/v1/pipes/log", "log", map[string]any{"msg": "hunter2"})
	wp.Execute(w, withTenant(r.WithContext(auth.WithRole(trace.ContextWithSpan(r.Context(), server), "writer"))))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	span, _ = chconntest.QueryOptions(conn.last(t), t)
	assert.Equal(t, chSpan(t, rec, server, "INSERT INTO audit_log").SpanContext(), span)
}

// The raw-SQL proxy sends its clickhouse.query span as a traceparent header,
// over a configured header of the same name.
func TestOpsQuery_SendsTraceContext(t *testing.T) {
	rec := recordCHSpans(t)
	ctx, server := otel.Tracer("test").Start(context.Background(), "server")
	defer server.End()
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("traceparent")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	t.Cleanup(srv.Close)
	target := staticTarget(srv.URL, "default", "secret", "default")
	h := newTestQueryHandler(func(s *settings.Store) chconn.Target {
		tg := target(s)
		tg.Headers = map[string]string{"traceparent": "configured"}
		return tg
	}, func(*settings.Store) time.Duration { return 30 * time.Second })

	body, err := json.Marshal(queryRequest{SQL: "SELECT 1"})
	require.NoError(t, err)
	w := httptest.NewRecorder()
	h.Handle(w, httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/ops/query", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	sc := chSpan(t, rec, server, "SELECT 1").SpanContext()
	assert.Equal(t, "00-"+sc.TraceID().String()+"-"+sc.SpanID().String()+"-01", <-got)
}

func TestSQLVerb(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, sql, want string
	}{
		{"plain select", "SELECT 1", "SELECT"},
		{"leading whitespace", "  select 1", "SELECT"},
		{"comments before insert", "-- note\n/* more */ insert INTO t", "INSERT"},
		{"execute as prefix", "EXECUTE AS bob SELECT 1", "SELECT"},
		{"cte select", "WITH a AS (SELECT 1) SELECT * FROM a", "SELECT"},
		{"cte insert", "WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a", "INSERT"},
		{"alter", "ALTER TABLE t DELETE WHERE 1", "ALTER"},
		{"empty", "", "OTHER"},
		{"not a keyword", "hunter2", "OTHER"},
		{"parenthesized", "(SELECT 1)", "OTHER"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, sqlVerb(tc.sql))
		})
	}
}
