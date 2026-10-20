package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/pkg/pipes"
	"github.com/Wave-RF/WaveHouse/pkg/policy"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// histogramCounts collects reader and returns, for the histogram name, how
// many values each label set recorded, keyed by the labels as key=value
// pairs joined by "," in key order.
func histogramCounts(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != name {
				continue
			}
			hist, ok := md.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "%s is a float64 histogram", name)
			for _, dp := range hist.DataPoints {
				var pairs []string
				for _, kv := range dp.Attributes.ToSlice() {
					pairs = append(pairs, string(kv.Key)+"="+kv.Value.String())
				}
				out[strings.Join(pairs, ",")] += dp.Count
			}
		}
	}
	return out
}

// testMeterReader is a meter provider of the test's own, so a test reads
// only what it recorded and runs in parallel.
func testMeterReader(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp, reader
}

// TestHTTPMetrics_LabelsByRoutePattern: a request is recorded under the
// route it matched, its pattern and not its path, so a path parameter adds
// no series (the mount's own pattern when it was answered inside the mount
// without reaching a route, unmatched when it reached no mount); under the
// method, a closed set; and under its status class, a panic counting as the
// 500 the recoverer answers.
func TestHTTPMetrics_LabelsByRoutePattern(t *testing.T) {
	t.Parallel()
	mp, reader := testMeterReader(t)
	m := httpMetricsOn(mp.Meter(meterName))

	r := chi.NewRouter()
	r.Use(m.middleware)
	r.Use(jsonRecoverer)
	r.Route("/v1", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Use(func(next http.Handler) http.Handler { return next })
			r.Get("/pipes/{name}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
			r.Post("/ingest", func(w http.ResponseWriter, _ *http.Request) { writeJSONError(w, http.StatusBadRequest, "bad") })
			r.Get("/stream", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		})
	})
	r.Get("/boom", func(http.ResponseWriter, *http.Request) { panic("boom") })

	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/v1/pipes/a"},
		{http.MethodGet, "/v1/pipes/b"},
		{http.MethodPost, "/v1/ingest"},
		{http.MethodGet, "/v1/stream"},
		{"PURGE", "/v1/pipes/a"},
		{http.MethodDelete, "/v1/pipes/a"},
		{http.MethodGet, "/v1/nope"},
		{http.MethodGet, "/nope"},
		{http.MethodGet, "/boom"},
	} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), req.method, req.path, nil))
	}

	assert.Equal(t, map[string]uint64{
		"method=GET,route=/v1/pipes/{name},status_class=2xx": 2,
		"method=POST,route=/v1/ingest,status_class=4xx":      1,
		"method=other,route=unmatched,status_class=4xx":      1,
		"method=DELETE,route=/v1/*,status_class=4xx":         1,
		"method=GET,route=/v1/*,status_class=4xx":            1,
		"method=GET,route=unmatched,status_class=4xx":        1,
		"method=GET,route=/boom,status_class=5xx":            1,
	}, histogramCounts(t, reader, "wavehouse_http_request_duration_seconds"),
		"/v1/stream is not timed: its lifetime is wavehouse_sse_stream_duration_seconds")
}

// TestHTTPMetrics_RouterRecords: the routers every listener serves time
// their requests, the ops-only one included.
func TestHTTPMetrics_RouterRecords(t *testing.T) {
	// No t.Parallel(): newHTTPMetrics reads the global meter provider, which
	// this test swaps.
	mp, reader := testMeterReader(t)
	saved := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { otel.SetMeterProvider(saved) })

	h := NewOpsRouter(OpsDependencies{Health: NewHealthHandler(), Version: NewVersionHandler("v", "c", "b")})
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/version", nil))

	assert.Equal(t, map[string]uint64{"method=GET,route=/version,status_class=2xx": 1},
		histogramCounts(t, reader, "wavehouse_http_request_duration_seconds"))
}

func TestStatusClass(t *testing.T) {
	t.Parallel()
	for status, want := range map[int]string{0: "2xx", 101: "1xx", 200: "2xx", 204: "2xx", 304: "3xx", 404: "4xx", 499: "4xx", 500: "5xx", 503: "5xx"} {
		assert.Equal(t, want, statusClass(status), "status %d", status)
	}
}

// TestQueryMetrics_ClickHouseErrors: each failed ClickHouse call on the
// native query paths is recorded once, under the code its response carries.
func TestQueryMetrics_ClickHouseErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range chErrorCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := chResult(tc.wantCode)

			mp, reader := testMeterReader(t)
			perms := tc.caps
			perms.AllowColumns = []string{"*"}
			sq := newCapturingHandler(t, &failingConn{err: tc.err}, policyWithViewer(perms))
			sq.metrics = queryMetricsOn(mp.Meter(meterName))
			sq.Handle(httptest.NewRecorder(), withTenant(viewerRequest(t, query.StructuredQuery{Columns: []string{"page"}})))
			assert.Equal(t, map[string]uint64{"operation=query,result=" + want: 1},
				histogramCounts(t, reader, "wavehouse_clickhouse_query_duration_seconds"))

			if tc.caps.MaxExecutionTime > 0 || tc.caps.MaxMemoryUsage > 0 {
				return // a pipe carries no role caps
			}
			mp, reader = testMeterReader(t)
			store := staticPipes(&pipes.NamedQuery{Name: "p", SQL: "SELECT 1", AllowedRoles: []string{"viewer"}})
			p := NewPipesHandler(store, staticPolicy(&policy.Policy{}), fixedConn(&failingConn{err: tc.err}), nil, func(*settings.Store) time.Duration { return time.Second })
			p.metrics = queryMetricsOn(mp.Meter(meterName))
			r := pipesRequest(t, http.MethodGet, "/v1/pipes/p", "p", nil)
			p.Execute(httptest.NewRecorder(), withTenant(r.WithContext(auth.WithRole(r.Context(), "viewer"))))
			assert.Equal(t, map[string]uint64{"operation=pipe,result=" + want: 1},
				histogramCounts(t, reader, "wavehouse_clickhouse_query_duration_seconds"))

			mp, reader = testMeterReader(t)
			wp := writerPipesHandler(t, &writeConn{err: tc.err}, nil, &pipes.NamedQuery{Name: "log", SQL: "INSERT INTO audit_log VALUES ({{msg}}, now())"})
			wp.metrics = queryMetricsOn(mp.Meter(meterName))
			pipeCallAs(t, wp, "log")
			assert.Equal(t, map[string]uint64{"operation=pipe_write,result=" + want: 1},
				histogramCounts(t, reader, "wavehouse_clickhouse_query_duration_seconds"))
		})
	}
}

// TestQueryMetrics_Proxy: the raw-SQL proxy records each call it makes, ok
// or under the code its response carries, the response cap included; a
// request refused before the call records nothing.
func TestQueryMetrics_Proxy(t *testing.T) {
	t.Parallel()
	mp, reader := testMeterReader(t)
	h := newProxyHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sql, _ := io.ReadAll(r.Body)
		switch string(sql) {
		case "SELECT 1":
			_, _ = w.Write([]byte(`{"data":[{"1":1}]}`))
		case "SELECT big":
			_, _ = w.Write([]byte(`{"data":[` + strings.Repeat(`{"x":1},`, 64) + `{"x":1}]}`))
		default:
			w.Header().Set("X-ClickHouse-Exception-Code", "62")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Code: 62. DB::Exception: Syntax error. (SYNTAX_ERROR)\n"))
		}
	}))
	h.metrics = queryMetricsOn(mp.Meter(meterName))
	h.maxResponseBytes = 256

	for _, sql := range []string{"SELECT 1", "SELECT 1", "SELEC", "SELECT big", ""} {
		body, err := json.Marshal(queryRequest{SQL: sql})
		require.NoError(t, err)
		postQuery(h, body)
	}

	assert.Equal(t, map[string]uint64{
		"operation=ops_query,result=ok":                 2,
		"operation=ops_query,result=rejected":           1,
		"operation=ops_query,result=response_too_large": 1,
	}, histogramCounts(t, reader, "wavehouse_clickhouse_query_duration_seconds"))
}

// TestQueryMetrics_NilIsNoop: a handler built without its constructor
// records nothing and does not panic.
func TestQueryMetrics_NilIsNoop(t *testing.T) {
	t.Parallel()
	var m *queryMetrics
	assert.NotPanics(t, func() {
		m.observe(context.Background(), opPipe, time.Second, nil, queryCaps{})
		m.record(opOpsQuery, time.Second, resultOK)
	})
	var hm *httpMetrics
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	assert.NotNil(t, hm.middleware(next))
}

// TestQueryMetrics_ClientGone: a call cut short because the client went away
// is canceled, not unavailable, which would read as ClickHouse failing; a
// cancel of WaveHouse's own (the time-cap backstop) with the client still
// there stays unavailable.
func TestQueryMetrics_ClientGone(t *testing.T) {
	t.Parallel()
	mp, reader := testMeterReader(t)
	m := queryMetricsOn(mp.Meter(meterName))
	gone, cancel := context.WithCancel(t.Context())
	cancel()
	err := fmt.Errorf("clickhouse query: %w", context.Canceled)
	m.observe(gone, opStructuredQuery, time.Millisecond, err, queryCaps{})
	m.observe(t.Context(), opStructuredQuery, time.Millisecond, err, queryCaps{time: true})

	assert.Equal(t, map[string]uint64{
		"operation=query,result=canceled":    1,
		"operation=query,result=unavailable": 1,
	}, histogramCounts(t, reader, "wavehouse_clickhouse_query_duration_seconds"))
}
