//go:build integration

package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/api"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// TestBootResilience_StickyHealthVsConditionalReady exercises the full
// post-#95 contract matrix against a real ClickHouse testcontainer that we
// stop/start mid-test:
//
//	| State                    | /health | /ready |
//	|--------------------------|---------|--------|
//	| Boot, CH down            | 503     | 503    |
//	| Boot, CH up after retry  | 200     | 200    |
//	| Post-boot, CH dies       | 200 ★   | 503    |
//	| Post-boot, CH back       | 200     | 200    |
//
// ★ — the sticky-/health invariant the PR adds, where /health stays at
// "boot completed once" rather than reflecting current ClickHouse state.
// The unit chain test (internal/api/boot_chain_test.go) pins the same
// wiring with a fake conn; this test additionally pins it against a real
// CH driver dial AND covers the sticky-vs-conditional dichotomy that the
// unit test can't exercise (it has no concept of "CH dies post-boot").
//
// Uses its own per-test CH testcontainer rather than sharedEnv — the
// shared env assumes CH stays up for the duration of every test in this
// package, which is exactly the assumption this test needs to violate.
func TestBootResilience_StickyHealthVsConditionalReady(t *testing.T) {
	ctx := context.Background()
	logger := testutil.NopLogger()

	ch, err := startClickHouse(ctx)
	require.NoError(t, err, "starting initial CH container")
	t.Cleanup(func() {
		if ch.conn != nil {
			_ = ch.conn.Close()
		}
		// Always terminate, even after intermediate Stop()s — the test
		// may have stopped the container as part of the matrix below.
		_ = ch.container.Terminate(context.Background())
	})

	// Phase 0 — drop CH to its stopped state so the first Refresh sees a
	// real conn-refused, not a half-ready boot.
	stopTimeout := 10 * time.Second
	require.NoError(t, ch.container.Stop(ctx, &stopTimeout), "stop CH before boot phase")

	// Close + reopen the driver. The original conn was Open'd while CH
	// was up so it has a healthy idle pool; using it now would race
	// against the driver discarding the dead pool entries. Fresh conn
	// gives us a clean conn-refused on the first Refresh.
	_ = ch.conn.Close()
	ch.conn, err = openDriver(ch.nativeAddr())
	require.NoError(t, err, "reopen driver against stopped CH")

	bootState := api.NewBootState(nil)
	registry := discovery.NewSchemaRegistry(ch.conn, testCHDatabase, time.Minute, logger)

	// === Row 1: Boot, CH down ===
	err = registry.Refresh(ctx)
	require.Error(t, err, "Refresh against stopped CH must fail")
	bootState.Set(fmt.Errorf("schema discovery: %w", err))

	h := api.NewHealthHandler(ch.conn)
	h.Boot = bootState

	assertHealth(t, "/health", h.Liveness, http.StatusServiceUnavailable, `"status":"degraded"`)
	assertHealth(t, "/ready", h.Readiness, http.StatusServiceUnavailable, `"status":"not ready"`)

	// Restart CH and refresh the cached mapped port — Docker may reassign
	// the host port on Stop+Start, so the chInstance.nativePort cached at
	// initial startClickHouse() can be stale. Rebuild the driver against
	// the live port before waiting for readiness.
	require.NoError(t, ch.container.Start(ctx), "restart CH for recovery phase")
	require.NoError(t, refreshChAddr(ctx, ch), "refresh mapped port after restart")
	_ = ch.conn.Close()
	ch.conn, err = openDriver(ch.nativeAddr())
	require.NoError(t, err, "reopen driver against restarted CH")
	registry = discovery.NewSchemaRegistry(ch.conn, testCHDatabase, time.Minute, logger)
	h.CHConn = ch.conn
	require.NoError(t, waitForNativeReady(ctx, ch.conn, 30*time.Second), "CH native should be ready after restart")

	retryCtx, retryCancel := context.WithTimeout(ctx, 30*time.Second)
	defer retryCancel()
	require.NoError(t, registry.RetryRefresh(retryCtx, 250*time.Millisecond, 2*time.Second, func(e error) {
		bootState.Set(fmt.Errorf("schema discovery: %w", e))
	}), "RetryRefresh should succeed once CH is back")
	bootState.Set(nil)

	// === Row 2: Boot, CH up after retry ===
	assertHealth(t, "/health", h.Liveness, http.StatusOK, `"status":"ok"`)
	assertHealth(t, "/ready", h.Readiness, http.StatusOK, `"status":"ready"`)

	// Now simulate "CH dies post-boot" — the runtime side of the contract.
	require.NoError(t, ch.container.Stop(ctx, &stopTimeout), "stop CH for post-boot outage")

	// === Row 3: Post-boot, CH dies ===
	// /health must stay 200 — BootState is nil and never gets touched
	// again, that's the sticky invariant. /ready must drop to 503 because
	// the readiness handler still pings CH on every call.
	assertHealth(t, "/health", h.Liveness, http.StatusOK, `"status":"ok"`)
	assertHealth(t, "/ready", h.Readiness, http.StatusServiceUnavailable, `"status":"not ready"`)

	// Restart CH a final time. Same port-may-have-changed dance — refresh
	// the mapped port, rebuild the driver, point the health handler at
	// the new conn. /ready is the only assertion here, so registry is
	// not re-wired (it has cached schemas from the earlier success and
	// the StartAutoRefresh ticker isn't running in this test).
	require.NoError(t, ch.container.Start(ctx), "restart CH for final recovery")
	require.NoError(t, refreshChAddr(ctx, ch), "refresh mapped port after final restart")
	_ = ch.conn.Close()
	ch.conn, err = openDriver(ch.nativeAddr())
	require.NoError(t, err, "reopen driver against final restart")
	h.CHConn = ch.conn
	require.NoError(t, waitForNativeReady(ctx, ch.conn, 30*time.Second), "CH native should be ready after second restart")

	// === Row 4: Post-boot, CH back ===
	assertHealth(t, "/health", h.Liveness, http.StatusOK, `"status":"ok"`)
	assertHealth(t, "/ready", h.Readiness, http.StatusOK, `"status":"ready"`)
}

// assertHealth invokes the given handler with a GET to path and asserts the
// status code + body substring. Wraps the boilerplate four lines that show
// up at every row of the contract matrix above.
func assertHealth(t *testing.T, path string, fn http.HandlerFunc, wantStatus int, wantSubstr string) {
	t.Helper()
	rec := httptest.NewRecorder()
	fn(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	assert.Equal(t, wantStatus, rec.Code, "%s status", path)
	assert.Contains(t, rec.Body.String(), wantSubstr, "%s body", path)
}

// openDriver mirrors the chConn construction inside startClickHouse so the
// boot test can rebuild the driver after a Stop without duplicating the
// option block.
func openDriver(addr string) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: testCHDatabase,
			Username: testCHUser,
			Password: testCHPassword,
		},
	})
}

// refreshChAddr re-queries Docker for the container's currently-mapped
// 9000/tcp host port and updates ch.nativePort in place. Necessary after
// Stop+Start cycles: Docker preserves the container ID but reassigns the
// host port (the binding was released when the container stopped, picked
// up by something else, etc.). The cached value from initial startup
// becomes stale.
func refreshChAddr(ctx context.Context, ch *chInstance) error {
	p, err := ch.container.MappedPort(ctx, "9000")
	if err != nil {
		return fmt.Errorf("mapped port: %w", err)
	}
	ch.nativePort = p.Port()
	return nil
}
