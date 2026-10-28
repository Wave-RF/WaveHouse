package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// pingFailConn satisfies driver.Conn via the embedded nil interface — the
// Readiness handler only calls Ping, so nil embedding is safe; the
// overridden Ping below returns the simulated failure that drives the 503
// branch.
type pingFailConn struct {
	driver.Conn
	err error
}

func (c pingFailConn) Ping(_ context.Context) error { return c.err }

func TestHealth_Liveness(t *testing.T) {
	t.Parallel()
	h := NewHealthHandler()

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/livez", nil)
	h.Liveness(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "ok", resp["status"])
	assert.NotContains(t, resp, "error", "liveness success body must not carry an error field (api.md contract)")
}

func TestHealth_Readiness_NilConn(t *testing.T) {
	t.Parallel()
	h := NewHealthHandler()

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	h.Readiness(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "ready", resp["status"])
	assert.NotContains(t, resp, "error", "readiness success body must not carry an error field (api.md contract)")
}

func TestHealth_Readiness_PingFails(t *testing.T) {
	t.Parallel()
	// Pin the contract on the 503 branch: the Content-Type and nosniff
	// headers are set unconditionally at the top of Readiness today, but
	// without a test for the failure path a future refactor that moves
	// header setup into the success branch would silently drop them on
	// 503 responses.
	h := NewHealthHandler(Check{Name: "clickhouse", Run: pingFailConn{err: errors.New("ch ping failed")}.Ping})

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	h.Readiness(w, r)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "not ready", resp["status"])
	assert.Equal(t, "clickhouse: ch ping failed", resp["error"])
}

// Every check runs on every call, and the 503 names each one that failed —
// an operator reading the body sees the whole picture, not the first
// failure — while a check that passed is not in it.
func TestHealth_Readiness_NamesEveryFailedCheck(t *testing.T) {
	t.Parallel()
	ran := map[string]int{}
	check := func(name string, err error) Check {
		return Check{Name: name, Run: func(context.Context) error { ran[name]++; return err }}
	}
	h := NewHealthHandler(
		check("clickhouse", errors.New("no ClickHouse pool is open")),
		check("mq", errors.New("not connected to nats")),
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	h.Readiness(w, r)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "not ready", resp["status"])
	assert.Equal(t, "clickhouse: no ClickHouse pool is open\nmq: not connected to nats", resp["error"])
	assert.Equal(t, map[string]int{"clickhouse": 1, "mq": 1}, ran, "a failure does not stop the checks after it")

	h.Checks = []Check{check("clickhouse", nil), check("mq", errors.New("not connected to nats"))}
	w = httptest.NewRecorder()
	h.Readiness(w, r)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "mq: not connected to nats", resp["error"], "a check that passed is not named")

	h.Checks = []Check{check("clickhouse", nil), check("mq", nil)}
	w = httptest.NewRecorder()
	h.Readiness(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"status":"ready"}`, w.Body.String())
}

func TestBootState_DefaultIsReady(t *testing.T) {
	t.Parallel()
	bs := NewBootState(nil)
	assert.NoError(t, bs.Err())
}

func TestBootState_SetAndGet(t *testing.T) {
	t.Parallel()
	bs := NewBootState(errors.New("starting"))
	require.Error(t, bs.Err())
	assert.Equal(t, "starting", bs.Err().Error())

	bs.Set(errors.New("still starting"))
	assert.Equal(t, "still starting", bs.Err().Error())

	bs.Set(nil)
	assert.NoError(t, bs.Err())
}

func TestHealth_Liveness_BootDegraded(t *testing.T) {
	t.Parallel()
	// When BootState reports a non-nil error, Liveness must return 503 with
	// the diagnostic in the JSON body. This is the behavior the gateway
	// relies on so an operator can `curl /livez` during a boot-time
	// ClickHouse outage instead of grepping a restart-loop log.
	h := NewHealthHandler()
	h.Boot = NewBootState(errors.New("schema discovery: dial tcp 127.0.0.1:9000: connect: connection refused"))

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/livez", nil)
	h.Liveness(w, r)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "degraded", resp["status"])
	assert.Contains(t, resp["error"], "connection refused")
}

func TestHealth_Liveness_BootReadyFlipsTo200(t *testing.T) {
	t.Parallel()
	// Once the retry loop succeeds, BootState.Set(nil) flips Liveness back
	// to 200 — the "normal serving begins" half of the issue's contract.
	bs := NewBootState(errors.New("schema discovery: still failing"))
	h := NewHealthHandler()
	h.Boot = bs

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/livez", nil)
	h.Liveness(w, r)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	bs.Set(nil)

	w = httptest.NewRecorder()
	r = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/livez", nil)
	h.Liveness(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "ok", resp["status"])
}

func TestHealth_Readiness_BootDegradedReports503(t *testing.T) {
	t.Parallel()
	// Readiness should also surface boot degradation: a kubelet readiness
	// probe needs to see "not ready" while schema discovery is still
	// failing, even if the ClickHouse ping path would otherwise succeed.
	h := NewHealthHandler()
	h.Boot = NewBootState(errors.New("schema discovery: code: 81, Database wavehouse does not exist"))

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	h.Readiness(w, r)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "not ready", resp["status"])
	assert.Contains(t, resp["error"], "Database wavehouse does not exist")
}

func TestHealth_Online(t *testing.T) {
	t.Parallel()
	// /v1/health is the content-free SDK liveness ping: 200 with an empty body
	// once boot has completed, 503 (also empty) while boot is still degraded.
	// Pin both the status mapping AND the empty-body contract — the whole point
	// is that nothing is JSON-encoded or cached per request.
	t.Run("past boot -> 200 empty", func(t *testing.T) {
		t.Parallel()
		h := NewHealthHandler() // nil Boot = boot completed
		w := httptest.NewRecorder()
		h.Online(w, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/health", nil))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, w.Body.String(), "Online must write no body")
	})
	t.Run("boot-degraded -> 503 empty", func(t *testing.T) {
		t.Parallel()
		h := NewHealthHandler()
		h.Boot = NewBootState(errors.New("schema discovery: connection refused"))
		w := httptest.NewRecorder()
		h.Online(w, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/health", nil))
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Empty(t, w.Body.String(), "Online must write no body even on 503")
	})
}

// readinessReading is what the readiness gauges report on one collection.
type readinessReading struct {
	ok   map[string]int64
	last int64
}

// readReadiness collects reader's readiness gauges.
func readReadiness(t *testing.T, reader *sdkmetric.ManualReader) readinessReading {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := readinessReading{ok: map[string]int64{}, last: -1}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			g := md.Data.(metricdata.Gauge[int64])
			switch md.Name {
			case "wavehouse_readiness_check_ok":
				for _, dp := range g.DataPoints {
					check, _ := dp.Attributes.Value(attribute.Key("check"))
					out.ok[check.AsString()] = dp.Value
				}
			case "wavehouse_readiness_last_check_timestamp_seconds":
				require.Len(t, g.DataPoints, 1)
				out.last = g.DataPoints[0].Value
			}
		}
	}
	return out
}

// TestHealth_ReadinessMetrics: every check, the boot diagnostic as
// check="boot", is exported at 0 from registration, before any probe; each
// probe records each check's outcome and when it ran (#901).
func TestHealth_ReadinessMetrics(t *testing.T) {
	t.Parallel()
	var chErr atomic.Pointer[error]
	ok := error(nil)
	chErr.Store(&ok)
	h := NewHealthHandler(
		Check{Name: "clickhouse", Run: func(context.Context) error { return *chErr.Load() }},
		Check{Name: "mq", Run: func(context.Context) error { return nil }},
	)
	h.Boot = NewBootState(nil)
	reader := sdkmetric.NewManualReader()
	reg, err := h.RegisterReadinessMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Unregister() })

	probe := func() int {
		w := httptest.NewRecorder()
		h.Readiness(w, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
		return w.Code
	}

	assert.Equal(t, readinessReading{ok: map[string]int64{"boot": 0, "clickhouse": 0, "mq": 0}, last: 0}, readReadiness(t, reader),
		"before any probe each check is present at 0 and nothing has run")

	require.Equal(t, http.StatusOK, probe())
	got := readReadiness(t, reader)
	assert.Equal(t, map[string]int64{"boot": 1, "clickhouse": 1, "mq": 1}, got.ok)
	assert.Positive(t, got.last)

	down := errors.New("no pool answered")
	chErr.Store(&down)
	require.Equal(t, http.StatusServiceUnavailable, probe())
	assert.Equal(t, map[string]int64{"boot": 1, "clickhouse": 0, "mq": 1}, readReadiness(t, reader).ok,
		"only the failing check reads 0")

	chErr.Store(&ok)
	require.Equal(t, http.StatusOK, probe())
	assert.Equal(t, map[string]int64{"boot": 1, "clickhouse": 1, "mq": 1}, readReadiness(t, reader).ok)

	h.Boot.Set(errors.New("schema discovery: connection refused"))
	require.Equal(t, http.StatusServiceUnavailable, probe())
	assert.Equal(t, map[string]int64{"boot": 0, "clickhouse": 1, "mq": 1}, readReadiness(t, reader).ok,
		"a failing boot diagnostic reads 0; the checks it skips keep their last outcome")
}

// TestHealth_ReadinessMetrics_NoBoot: a handler without a boot diagnostic (a
// process without the api role) exports no boot series.
func TestHealth_ReadinessMetrics_NoBoot(t *testing.T) {
	t.Parallel()
	h := NewHealthHandler(Check{Name: "mq", Run: func(context.Context) error { return nil }})
	reader := sdkmetric.NewManualReader()
	reg, err := h.RegisterReadinessMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Unregister() })
	assert.Equal(t, map[string]int64{"mq": 0}, readReadiness(t, reader).ok)
}

// TestHealth_Readiness_LogsOnlyOnChange: a check that starts failing logs one
// WARN naming it and the error, repeated failing probes log nothing more, and
// recovery logs one INFO with how long it failed; a passing check that never
// failed logs nothing (#901). Not parallel: it captures the default logger.
func TestHealth_Readiness_LogsOnlyOnChange(t *testing.T) {
	buf := logtest.Capture(t, slog.LevelInfo)
	var failing atomic.Bool
	h := NewHealthHandler(
		Check{Name: "clickhouse", Run: func(context.Context) error {
			if failing.Load() {
				return errors.New("no pool answered")
			}
			return nil
		}},
		Check{Name: "mq", Run: func(context.Context) error { return nil }},
	)
	probe := func() {
		h.Readiness(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
	}

	probe()
	assert.Empty(t, buf.String(), "a first pass logs nothing")

	failing.Store(true)
	for range 3 {
		probe()
	}
	out := buf.String()
	assert.Equal(t, 1, strings.Count(out, `"msg":"readiness check failing"`), out)
	assert.Contains(t, out, `"level":"WARN"`)
	assert.Contains(t, out, `"check":"clickhouse"`)
	assert.Contains(t, out, `"error":"no pool answered"`)
	assert.NotContains(t, out, `"check":"mq"`)

	failing.Store(false)
	probe()
	probe()
	out = buf.String()
	assert.Equal(t, 1, strings.Count(out, `"msg":"readiness check passing again"`), out)
	assert.Contains(t, out, `"failed_for":`)
	assert.Equal(t, 1, strings.Count(out, `"msg":"readiness check failing"`), "recovery adds no WARN")
}

// TestHealth_Readiness_FirstFailureLogs: a check that fails on its first
// probe logs the WARN, so a process that boots not ready says why.
func TestHealth_Readiness_FirstFailureLogs(t *testing.T) {
	buf := logtest.Capture(t, slog.LevelInfo)
	h := NewHealthHandler(Check{Name: "mq", Run: func(context.Context) error { return errors.New("not connected") }})
	h.Readiness(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
	out := buf.String()
	assert.Equal(t, 1, strings.Count(out, `"msg":"readiness check failing"`), out)
	assert.Contains(t, out, `"check":"mq"`)
}

// TestHealth_Readiness_DropsOlderOutcome: an outcome taken before the one
// recorded, from a slower concurrent probe, changes neither the gauge nor the
// logs, and an earlier call does not move the last-call time back (#901). Not
// parallel: it captures the default logger.
func TestHealth_Readiness_DropsOlderOutcome(t *testing.T) {
	buf := logtest.Capture(t, slog.LevelInfo)
	h := NewHealthHandler(Check{Name: "clickhouse", Run: func(context.Context) error { return nil }})
	reader := sdkmetric.NewManualReader()
	reg, err := h.RegisterReadinessMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Unregister() })

	t2 := time.Unix(2_000_000, 0)
	t1 := t2.Add(-time.Second)
	h.results.called(t2)
	h.results.record(context.Background(), "clickhouse", t2, nil)
	h.results.called(t1)
	h.results.record(context.Background(), "clickhouse", t1, errors.New("no pool answered"))

	got := readReadiness(t, reader)
	assert.Equal(t, map[string]int64{"clickhouse": 1}, got.ok, "the older failure is dropped")
	assert.Equal(t, t2.Unix(), got.last, "the last-call time does not move back")
	assert.Empty(t, buf.String(), "a dropped outcome logs nothing")
}
