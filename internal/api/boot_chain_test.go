package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// errsThenSuccessConn drives the boot Refresh/RetryRefresh chain in tests.
// Each Query returns the next entry of errs; once exhausted, queries return
// an empty rowset so Refresh succeeds with zero tables. driver.Conn is
// embedded nil — none of its other methods are reached on the discovery
// path, so this minimal stub is sufficient and immune to interface drift.
type errsThenSuccessConn struct {
	driver.Conn
	errs  []error
	calls atomic.Int32
}

func (c *errsThenSuccessConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	n := c.calls.Add(1)
	if int(n) <= len(c.errs) {
		return nil, c.errs[n-1]
	}
	return &chainEmptyRows{}, nil
}

type chainEmptyRows struct{ driver.Rows }

func (*chainEmptyRows) Next() bool                       { return false }
func (*chainEmptyRows) Close() error                     { return nil }
func (*chainEmptyRows) Err() error                       { return nil }
func (*chainEmptyRows) ColumnTypes() []driver.ColumnType { return nil }

// TestBoot_Chain_DegradedThenRecovers nails down the full sequence the PR
// adds: the initial Refresh on a stopped/unreachable ClickHouse fails →
// BootState carries the diagnostic → /healthz returns 503 with the wrapped
// error → RetryRefresh keeps trying with backoff → success clears BootState
// → /healthz flips to 200.
//
// The pieces are unit-tested in isolation (BootState set/get in this file,
// RetryRefresh happy/retry/cancel paths in discovery_test.go, Liveness 503
// branch in TestHealth_Liveness_BootDegraded). This test pins them as a
// working pipeline using the production types — a refactor that breaks the
// wiring between Refresh failure → BootState → Liveness 503 → RetryRefresh
// success → BootState.Set(nil) → Liveness 200 fails here even if every
// individual unit test still passes.
func TestBoot_Chain_DegradedThenRecovers(t *testing.T) {
	t.Parallel()

	connRefused := errors.New("dial tcp 127.0.0.1:9000: connect: connection refused")
	dbMissing := errors.New("code: 81, message: Database wavehouse does not exist")
	// Three failures then success — matches the realistic case where CH
	// comes up partway through the retry backoff.
	conn := &errsThenSuccessConn{errs: []error{connRefused, connRefused, dbMissing}}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	registry := discovery.NewSchemaRegistry(conn, "test", time.Hour, logger)

	// Phase 0 — synchronous boot Refresh fails. main.go records the
	// diagnostic in BootState and proceeds with the retry loop in a
	// goroutine; we drive both inline here for determinism.
	bootState := NewBootState(nil)
	err := registry.Refresh(context.Background())
	require.Error(t, err, "Refresh against unreachable CH must fail")
	bootState.Set(fmt.Errorf("schema discovery: %w", err))

	handler := NewHealthHandler(nil)
	handler.Boot = bootState

	// Phase 1 — /healthz surfaces the diagnostic verbatim with 503. This is
	// the operator-facing contract: `curl /healthz` answers "why can't this
	// process serve traffic" instead of the operator having to grep a
	// restart-loop log.
	rec := httptest.NewRecorder()
	handler.Liveness(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), `"status":"degraded"`)
	assert.Contains(t, rec.Body.String(), "connection refused")

	// Phase 2 — background retry loop drives Refresh to eventual success.
	// Tight bounds keep the test fast; the retry-loop unit tests cover the
	// actual backoff math.
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer retryCancel()
	retryErr := registry.RetryRefresh(retryCtx, time.Millisecond, 10*time.Millisecond, func(attemptErr error) {
		bootState.Set(fmt.Errorf("schema discovery: %w", attemptErr))
	})
	require.NoError(t, retryErr, "retry loop should succeed on the 4th attempt")
	bootState.Set(nil)

	// Phase 3 — /healthz flips to 200 once BootState is cleared. This is
	// the sticky-from-here invariant the CHANGELOG calls out: BootState
	// won't be touched again for the rest of the process lifetime, so
	// /healthz remains 200 even if ClickHouse goes unreachable later
	// (that case is reflected in /readyz, not /healthz — covered by the
	// integration test in tests/integration/boot_resilience_test.go).
	rec = httptest.NewRecorder()
	handler.Liveness(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"status":"ok"`)

	// Sanity: exactly four Query calls — the initial sync Refresh (1) plus
	// the retry loop's three attempts (2,3 fail; 4 succeeds). An off-by-one
	// in the loop's success short-circuit would show up here.
	assert.Equal(t, int32(4), conn.calls.Load(), "expected 4 Query calls total")
}
