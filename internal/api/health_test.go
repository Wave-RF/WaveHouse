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
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
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
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ready", nil)
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
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ready", nil)
	h.Readiness(w, r)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "not ready", resp["status"])
	assert.Equal(t, "ch ping failed", resp["error"])
}
