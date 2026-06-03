package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	h := NewHealthHandler(nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
	h.Liveness(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "ok", resp["status"])
}

func TestHealth_Readiness_NilConn(t *testing.T) {
	t.Parallel()
	h := NewHealthHandler(nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	h.Readiness(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "ready", resp["status"])
}

func TestHealth_Readiness_PingFails(t *testing.T) {
	t.Parallel()
	// Pin the contract on the 503 branch: the Content-Type and nosniff
	// headers are set unconditionally at the top of Readiness today, but
	// without a test for the failure path a future refactor that moves
	// header setup into the success branch would silently drop them on
	// 503 responses.
	h := NewHealthHandler(pingFailConn{err: errors.New("ch ping failed")})

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	h.Readiness(w, r)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "not ready", resp["status"])
	assert.Equal(t, "ch ping failed", resp["error"])
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
	// relies on so an operator can `curl /healthz` during a boot-time
	// ClickHouse outage instead of grepping a restart-loop log.
	h := NewHealthHandler(nil)
	h.Boot = NewBootState(errors.New("schema discovery: dial tcp 127.0.0.1:9000: connect: connection refused"))

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
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
	h := NewHealthHandler(nil)
	h.Boot = bs

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
	h.Liveness(w, r)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	bs.Set(nil)

	w = httptest.NewRecorder()
	r = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
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
	h := NewHealthHandler(nil)
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
