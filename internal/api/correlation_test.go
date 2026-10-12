package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

func TestRequestID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		header []string // every X-Request-Id line sent
		keep   bool
	}{
		{name: "none sent"},
		{name: "empty", header: []string{""}},
		{name: "kept", header: []string{"proxy-7f3a/42"}, keep: true},
		{name: "longest kept", header: []string{strings.Repeat("a", maxRequestIDLen)}, keep: true},
		{name: "too long", header: []string{strings.Repeat("a", maxRequestIDLen+1)}},
		{name: "space", header: []string{"a b"}},
		{name: "control character", header: []string{"a\tb"}},
		{name: "non-ASCII", header: []string{"idé"}},
		{name: "sent twice", header: []string{"one", "two"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var seen string
			h := requestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = observability.RequestID(r.Context())
			}))
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			for _, v := range tt.header {
				req.Header.Add(requestIDHeader, v)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			got := w.Header().Get(requestIDHeader)
			assert.Equal(t, seen, got, "the response names the id the request was logged under")
			if tt.keep {
				assert.Equal(t, tt.header[0], got)
			} else {
				assert.Regexp(t, `^[0-9a-f]{32}$`, got)
			}
		})
	}
}

func TestRequestID_FreshPerRequest(t *testing.T) {
	t.Parallel()
	h := requestID(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	ids := map[string]bool{}
	for range 3 {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
		ids[w.Header().Get(requestIDHeader)] = true
	}
	assert.Len(t, ids, 3)
}

func TestAnnotateSpanMiddleware(t *testing.T) {
	t.Parallel()
	rec := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")

	h := requestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "server")
		annotateSpan(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(w, r.WithContext(ctx))
		span.End()
	}))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set(requestIDHeader, "req-1")
	h.ServeHTTP(httptest.NewRecorder(), req)

	require.Len(t, rec.Ended(), 1)
	assert.Contains(t, rec.Ended()[0].Attributes(), attribute.String(observability.RequestIDKey, "req-1"))
}

// captureCorrelated routes the default logger through the production
// TraceHandler into a buffer until the test ends. Like logtest.Capture, the
// caller must not run in parallel.
func captureCorrelated(t *testing.T) *logtest.Buffer {
	t.Helper()
	buf := &logtest.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(&observability.TraceHandler{Handler: slog.NewJSONHandler(buf, nil)}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// records decodes the captured JSON lines whose msg is msg.
func records(t *testing.T, buf *logtest.Buffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range bytes.SplitSeq(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal(line, &rec))
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// A tenant route's log lines, the authentication middleware's and the
// handler's authorization denial alike, carry the request's id and tenant.
func TestNewRouter_TenantRouteLogsCarryRequestAndTenant(t *testing.T) {
	buf := captureCorrelated(t)
	reg := testRegistry(t)
	ingest := NewIngestHandler(fixedRegistry(reg), &testutil.MockPublisher{})
	ingest.PolicySource = staticPolicy(&policy.Policy{}) // grants nothing
	router := NewRouter(Dependencies{
		Tenants: testTenants(),
		Ingest:  ingest,
		Query:   &QueryHandler{},
		SSE:     NewStreamHandler(stream.NewHub(nil, nil, nil), nil),
		Health:  &HealthHandler{},
		Version: NewVersionHandler("test", "test", "test"),
		Schema:  schemaHandlerOver(reg, testTenants()),
		AuthMW: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				slog.InfoContext(r.Context(), "auth ran")
				next.ServeHTTP(w, r)
			})
		},
		PolicySource: policy.Static(&policy.Policy{}),
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/ingest?table=clicks", strings.NewReader(`{"page":"/"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(tenant.Header, tenant.Default.String())
	req.Header.Set(requestIDHeader, "req-ingest")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, "req-ingest", w.Header().Get(requestIDHeader))

	for _, msg := range []string{"auth ran", "authorization denied"} {
		recs := records(t, buf, msg)
		require.Len(t, recs, 1, msg)
		assert.Equal(t, "req-ingest", recs[0][observability.RequestIDKey], msg)
		assert.Equal(t, tenant.Default.String(), recs[0][observability.TenantKey], msg)
	}
}

// An ops route names its tenant in ?tenant=, or serves tenant 0 without one;
// either way its lines carry it from the handler's read on.
func TestOpsStore_StampsTheTenant(t *testing.T) {
	for _, target := range []string{"/v1/ops/schema?tenant=0", "/v1/ops/schema"} {
		t.Run(target, func(t *testing.T) {
			buf := captureCorrelated(t)
			req := httptest.NewRequestWithContext(observability.WithRequest(t.Context(), "req-ops"), http.MethodGet, target, nil)
			_, ok := opsStore(httptest.NewRecorder(), req, testTenants())
			require.True(t, ok)
			slog.InfoContext(req.Context(), "ops read")

			recs := records(t, buf, "ops read")
			require.Len(t, recs, 1)
			assert.Equal(t, tenant.Default.String(), recs[0][observability.TenantKey])
		})
	}
}

// Every response names its request id, the probes' included.
func TestNewRouter_EchoesRequestID(t *testing.T) {
	t.Parallel()
	var sawStore []bool
	router := tenantProbeRouter(t, &sawStore)
	for _, path := range []string{"/livez", "/v1/health", "/no-such-route"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		assert.Regexp(t, `^[0-9a-f]{32}$`, w.Header().Get(requestIDHeader), path)
	}
}

// The dead-letter read serves tenant 0 when ?tenant= names none, and its
// lines say so.
func TestDLQStats_StampsTheDefaultTenant(t *testing.T) {
	buf := captureCorrelated(t)
	req := httptest.NewRequestWithContext(observability.WithRequest(t.Context(), "req-dlq"), http.MethodGet, "/v1/ops/dlq/stats", nil)
	NewDLQHandler(&testutil.MockDeadLetterStats{}).Stats(httptest.NewRecorder(), req)
	slog.InfoContext(req.Context(), "dlq read")

	recs := records(t, buf, "dlq read")
	require.Len(t, recs, 1)
	assert.Equal(t, tenant.Default.String(), recs[0][observability.TenantKey])
}

// An ops request's tenant is on its lines from authentication on, so the
// operator-key audit line carries it: the one ?tenant= names, tenant 0 for a
// read naming none, and none for a reload naming none (it acts on every
// tenant) or for a query opsTenant refuses.
func TestNewRouter_OpsLogsCarryTheTenant(t *testing.T) {
	tests := []struct {
		query      string
		wantRead   any
		wantReload any
	}{
		{query: "?tenant=acme", wantRead: "acme", wantReload: "acme"},
		{query: "", wantRead: tenant.Default.String(), wantReload: nil},
		{query: "?tenant=acme&tenant=globex", wantRead: nil, wantReload: nil},
		{query: "?tenant=..%2Fetc", wantRead: nil, wantReload: nil},
		{query: "?tenant=acme;x=1", wantRead: nil, wantReload: nil},
	}
	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			slog.InfoContext(r.Context(), "auth ran", "path", r.URL.Path)
			next.ServeHTTP(w, r)
		})
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			buf := captureCorrelated(t)
			reg := testRegistry(t)
			api := NewRouter(Dependencies{
				Tenants:      testTenants(),
				Ingest:       NewIngestHandler(fixedRegistry(reg), &testutil.MockPublisher{}),
				Query:        &QueryHandler{},
				SSE:          NewStreamHandler(stream.NewHub(nil, nil, nil), nil),
				Health:       &HealthHandler{},
				Version:      NewVersionHandler("test", "test", "test"),
				Schema:       schemaHandlerOver(reg, testTenants()),
				Settings:     &SettingsHandler{},
				AuthMW:       auth,
				PolicySource: policy.Static(&policy.Policy{}),
			})
			ops := NewOpsRouter(OpsDependencies{
				Health:   &HealthHandler{},
				Version:  NewVersionHandler("test", "test", "test"),
				Settings: &SettingsHandler{},
				AuthMW:   auth,
			})
			serve := func(h http.Handler, method, path string) {
				h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), method, path+tt.query, nil))
			}
			serve(api, http.MethodGet, "/v1/ops/schema")
			serve(api, http.MethodPost, opsReloadPath)
			serve(ops, http.MethodPost, opsReloadPath)

			recs := records(t, buf, "auth ran")
			require.Len(t, recs, 3)
			for _, rec := range recs {
				want := tt.wantRead
				if rec["path"] == opsReloadPath {
					want = tt.wantReload
				}
				assert.Equal(t, want, rec[observability.TenantKey], rec["path"])
				assert.NotEmpty(t, rec[observability.RequestIDKey])
			}
		})
	}
}
