package discovery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_ValidData(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "amount", Type: "Float64"},
			{Name: "clicked_at", Type: "DateTime64(3, 'UTC')"},
		},
	}

	data := map[string]any{
		"user_id":    "alice",
		"amount":     42.5,
		"clicked_at": "2025-01-01T00:00:00Z",
	}

	err := Validate(schema, data)
	assert.NoError(t, err)
}

func TestValidate_UnknownField(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
		},
	}

	data := map[string]any{
		"user_id": "alice",
		"unknown": "value",
	}

	err := Validate(schema, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestValidate_TypeMismatch(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
		},
	}

	data := map[string]any{
		"user_id": 123.0, // number, not string
	}

	err := Validate(schema, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

func TestValidate_MissingRequiredColumn(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "amount", Type: "Float64"},
		},
	}

	data := map[string]any{
		"user_id": "alice",
		// amount is missing and has no default
	}

	err := Validate(schema, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column")
}

func TestValidate_NullableColumnCanBeOmitted(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "notes", Type: "Nullable(String)", IsNullable: true},
		},
	}

	data := map[string]any{
		"user_id": "alice",
	}

	assert.NoError(t, Validate(schema, data))
}

func TestValidate_DefaultColumnCanBeOmitted(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "created_at", Type: "DateTime64(3, 'UTC')", HasDefault: true},
		},
	}

	data := map[string]any{
		"user_id": "alice",
	}

	assert.NoError(t, Validate(schema, data))
}

func TestValidate_NullForNonNullable(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
		},
	}

	data := map[string]any{
		"user_id": nil,
	}

	err := Validate(schema, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "null value for non-nullable")
}

func TestValidate_NullForNullable(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "notes", Type: "Nullable(String)", IsNullable: true},
		},
	}

	data := map[string]any{
		"notes": nil,
	}

	assert.NoError(t, Validate(schema, data))
}

func TestValidate_NullForNonNullableWithDefault(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "clicks",
		Columns: []Column{
			{Name: "user_id", Type: "String"},
			{Name: "created_at", Type: "DateTime64(3, 'UTC')", HasDefault: true},
		},
	}

	data := map[string]any{
		"user_id":    "alice",
		"created_at": nil, // non-nullable but has a default — should be accepted
	}

	assert.NoError(t, Validate(schema, data))
}

func TestIsTypeCompatible_StringTypes(t *testing.T) {
	t.Parallel()
	stringTypes := []string{
		"String", "FixedString(32)", "UUID",
		"DateTime64(3, 'UTC')", "Date", "Date32",
		"Enum8('a'=1)", "IPv4", "IPv6",
	}
	for _, ct := range stringTypes {
		assert.True(t, isTypeCompatible(ct, "hello"), "expected %s to accept string", ct)
		assert.False(t, isTypeCompatible(ct, 42.0), "expected %s to reject float64", ct)
	}
}

func TestIsTypeCompatible_NumericTypes(t *testing.T) {
	t.Parallel()
	numTypes := []string{
		"UInt8", "UInt16", "UInt32", "UInt64",
		"Int8", "Int16", "Int32", "Int64",
		"Float32", "Float64",
		"Decimal(18, 4)",
	}
	for _, ct := range numTypes {
		assert.True(t, isTypeCompatible(ct, 42.0), "expected %s to accept float64", ct)
		assert.False(t, isTypeCompatible(ct, "hello"), "expected %s to reject string", ct)
	}
}

func TestIsTypeCompatible_Bool(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Bool", true))
	assert.True(t, isTypeCompatible("Bool", false))
	assert.True(t, isTypeCompatible("Bool", 1.0)) // JSON number 1/0
	assert.False(t, isTypeCompatible("Bool", "true"))
}

func TestIsTypeCompatible_Array(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Array(String)", []any{"a", "b"}))
	assert.False(t, isTypeCompatible("Array(String)", "not-an-array"))
}

func TestIsTypeCompatible_Map(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Map(String, String)", map[string]any{"k": "v"}))
	assert.False(t, isTypeCompatible("Map(String, String)", "not-a-map"))
}

func TestIsTypeCompatible_LowCardinality(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("LowCardinality(String)", "hello"))
	assert.False(t, isTypeCompatible("LowCardinality(String)", 42.0))
}

func TestIsTypeCompatible_NullableUnwrap(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Nullable(String)", "hello"))
	assert.False(t, isTypeCompatible("Nullable(String)", 42.0))
	assert.True(t, isTypeCompatible("Nullable(Float64)", 42.0))
}

func TestIsNullable(t *testing.T) {
	t.Parallel()
	assert.True(t, isNullable("Nullable(String)"))
	assert.False(t, isNullable("String"))
	assert.False(t, isNullable("LowCardinality(String)"))
}

func TestIsTypeCompatible_Tuple(t *testing.T) {
	t.Parallel()
	// Tuple accepts arrays or objects.
	assert.True(t, isTypeCompatible("Tuple(String, Int32)", []any{"a", 1.0}))
	assert.True(t, isTypeCompatible("Tuple(a String, b Int32)", map[string]any{"a": "x", "b": 1.0}))
	assert.False(t, isTypeCompatible("Tuple(String, Int32)", "not-a-tuple"))
	assert.False(t, isTypeCompatible("Tuple(String)", 42.0))
}

func TestIsTypeCompatible_Enum16(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Enum16('a'=1,'b'=2)", "a"))
	assert.False(t, isTypeCompatible("Enum16('a'=1)", 42.0))
}

func TestIsTypeCompatible_UnknownType(t *testing.T) {
	t.Parallel()
	// Unknown types accept any value (let ClickHouse validate).
	assert.True(t, isTypeCompatible("SomeFutureType", "anything"))
	assert.True(t, isTypeCompatible("SomeFutureType", 42.0))
	assert.True(t, isTypeCompatible("SomeFutureType", true))
}

func TestIsTypeCompatible_Decimal(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("Decimal(10,2)", 42.5))
	assert.True(t, isTypeCompatible("Decimal128(5)", 99.0))
	assert.False(t, isTypeCompatible("Decimal(10,2)", "not-a-number"))
}

func TestIsTypeCompatible_IPv4IPv6(t *testing.T) {
	t.Parallel()
	assert.True(t, isTypeCompatible("IPv4", "192.168.1.1"))
	assert.True(t, isTypeCompatible("IPv6", "::1"))
	assert.False(t, isTypeCompatible("IPv4", 42.0))
	assert.False(t, isTypeCompatible("IPv6", true))
}

func TestValidate_NilData(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "test",
		Columns: []Column{
			{Name: "id", Type: "UInt64"},
		},
	}
	err := Validate(schema, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column")
}

func TestValidate_EmptyData_AllDefaults(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{
		Name: "test",
		Columns: []Column{
			{Name: "id", Type: "UInt64", HasDefault: true},
			{Name: "name", Type: "String", IsNullable: true},
		},
	}
	assert.NoError(t, Validate(schema, map[string]any{}))
}

func TestIsNumericType(t *testing.T) {
	t.Parallel()
	for _, typ := range []string{
		"UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
		"Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
		"Float32", "Float64",
	} {
		assert.True(t, isNumericType(typ), "expected %q to be numeric", typ)
	}
	assert.False(t, isNumericType("String"))
	assert.False(t, isNumericType("Bool"))
}

func TestNewSchemaRegistry_ConstructorDefaults(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sr := NewSchemaRegistry(nil, "wavehouse", 30*time.Second, logger)
	require.NotNil(t, sr)
	assert.Equal(t, "wavehouse", sr.database)
	assert.Equal(t, 30*time.Second, sr.refreshInterval)
	assert.Same(t, logger, sr.logger)
	assert.NotNil(t, sr.tables)
	assert.Empty(t, sr.List())
	assert.Nil(t, sr.Get("anything"))
}

func TestNewSchemaRegistryFromMap_PopulatesAndLookups(t *testing.T) {
	t.Parallel()
	clicks := &TableSchema{Name: "clicks", Columns: []Column{{Name: "id", Type: "String"}}}
	users := &TableSchema{Name: "users", Columns: []Column{{Name: "id", Type: "UInt64"}}}

	sr := NewSchemaRegistryFromMap([]*TableSchema{clicks, users})
	require.NotNil(t, sr)

	assert.Same(t, clicks, sr.Get("clicks"))
	assert.Same(t, users, sr.Get("users"))
	assert.Nil(t, sr.Get("missing"))

	got := sr.List()
	assert.Len(t, got, 2)
	names := map[string]bool{}
	for _, s := range got {
		names[s.Name] = true
	}
	assert.True(t, names["clicks"])
	assert.True(t, names["users"])
}

func TestNewSchemaRegistryFromMap_Empty(t *testing.T) {
	t.Parallel()
	sr := NewSchemaRegistryFromMap(nil)
	require.NotNil(t, sr)
	assert.Empty(t, sr.List())
	assert.Nil(t, sr.Get("x"))
}

// fakeConn implements just enough of driver.Conn to drive RetryRefresh. It
// returns successive errors from errsThenSuccess, and once that slice is
// exhausted returns emptyRows (which makes Refresh succeed with zero tables).
// The nil-embedded interface trick handles every other method — none of them
// are reached by Refresh.
type fakeConn struct {
	driver.Conn
	errsThenSuccess []error
	calls           atomic.Int32
}

func (c *fakeConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	n := c.calls.Add(1)
	if int(n) <= len(c.errsThenSuccess) {
		return nil, c.errsThenSuccess[n-1]
	}
	return &emptyRows{}, nil
}

type emptyRows struct {
	driver.Rows
}

func (*emptyRows) Next() bool   { return false }
func (*emptyRows) Close() error { return nil }
func (*emptyRows) Err() error   { return nil }
func (*emptyRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func newFakeRegistry(t *testing.T, errs []error) (*SchemaRegistry, *fakeConn) {
	t.Helper()
	conn := &fakeConn{errsThenSuccess: errs}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewSchemaRegistry(conn, "test", time.Hour, logger), conn
}

// TestRetryRefresh_SucceedsOnFirstAttempt is the happy path: Refresh returns
// nil immediately and no backoff is observed.
func TestRetryRefresh_SucceedsOnFirstAttempt(t *testing.T) {
	t.Parallel()
	sr, conn := newFakeRegistry(t, nil)

	// 2s timeout context bounds a hypothetical regression where RetryRefresh
	// starts sleeping before checking success — initialBackoff=time.Hour means
	// such a regression would otherwise hang the test until the Go test
	// framework's default timeout (10m). With WithTimeout, the test fails fast
	// with a context error instead.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var attempts int32
	start := time.Now()
	err := sr.RetryRefresh(ctx, time.Hour, time.Hour, func(error) {
		atomic.AddInt32(&attempts, 1)
	})
	require.NoError(t, err)

	// Same 250ms headroom as TestRetryRefresh_BackoffIsBounded — the
	// expected wall-clock budget here is ~0 (no sleep at all), but a
	// scheduler stall on a contended CI runner can drag a no-sleep test
	// past 100ms. 250ms is still orders of magnitude under any real-sleep
	// regression (the misbehaviour would sleep `initialBackoff` = 1h).
	assert.Less(t, time.Since(start), 250*time.Millisecond, "should not have slept")
	assert.Equal(t, int32(0), atomic.LoadInt32(&attempts), "onAttempt should only fire on failure")
	assert.Equal(t, int32(1), conn.calls.Load(), "exactly one Query call expected")
}

// TestRetryRefresh_RetriesUntilSuccess verifies the loop keeps trying through
// transient errors and surfaces each failure via onAttempt with the most
// recent error becoming the diagnostic.
func TestRetryRefresh_RetriesUntilSuccess(t *testing.T) {
	t.Parallel()
	errFirst := errors.New("dial tcp: connect: connection refused")
	errSecond := errors.New("code: 81, Database wavehouse does not exist")
	sr, conn := newFakeRegistry(t, []error{errFirst, errSecond})

	var captured []error
	err := sr.RetryRefresh(context.Background(), time.Millisecond, 10*time.Millisecond, func(e error) {
		captured = append(captured, e)
	})
	require.NoError(t, err)

	assert.Equal(t, int32(3), conn.calls.Load(), "two failures + one success")
	require.Len(t, captured, 2)
	assert.ErrorIs(t, captured[0], errFirst)
	assert.ErrorIs(t, captured[1], errSecond)
}

// TestRetryRefresh_ReturnsOnContextCancel verifies the loop exits with
// ctx.Err() when the context is cancelled mid-backoff, rather than spinning
// forever or panicking.
func TestRetryRefresh_ReturnsOnContextCancel(t *testing.T) {
	t.Parallel()
	// Long-running failure stream so success never happens.
	errs := make([]error, 1000)
	for i := range errs {
		errs[i] = errors.New("still failing")
	}
	sr, _ := newFakeRegistry(t, errs)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// Long backoff so the loop is parked in the sleep when we cancel.
		done <- sr.RetryRefresh(ctx, time.Second, time.Second, nil)
	}()

	// `done` provides the only synchronisation we need. Whether cancel
	// fires before or after the goroutine enters the select, the loop
	// must observe ctx.Done() and return — no sleep-based scheduling
	// assumption required.
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("RetryRefresh did not return after ctx cancel")
	}
}

// TestRetryRefresh_DoesNotFireOnAttemptDuringCancel pins the shutdown-noise
// guard added per Gemini's review on f854274: when the context is already
// cancelled going into the loop, the failed Refresh's error is a downstream
// reflection of cancellation (or arrives before the select can catch
// ctx.Done() — either way), and surfacing it via onAttempt would write
// "context canceled" into BootState as a spurious /health diagnostic during
// the normal shutdown window. The guard is a single `ctx.Err() == nil`
// check on the callback site; this test pins it.
func TestRetryRefresh_DoesNotFireOnAttemptDuringCancel(t *testing.T) {
	t.Parallel()
	// fakeConn doesn't inspect ctx, so the first Refresh against a
	// pre-cancelled ctx returns "transient" rather than ctx.Canceled.
	// That's exactly the case the guard needs to handle — the error
	// looks real, but ctx.Err() reveals we're shutting down anyway.
	conn := &fakeConn{errsThenSuccess: []error{errors.New("transient")}}
	sr, _ := newFakeRegistry(t, nil)
	sr.conn = conn // override the no-error conn from newFakeRegistry

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before RetryRefresh starts

	var attempts atomic.Int32
	err := sr.RetryRefresh(ctx, time.Second, time.Second, func(_ error) {
		attempts.Add(1)
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(0), attempts.Load(),
		"onAttempt must not fire when ctx is already cancelled — that would surface shutdown as a /health diagnostic")
	// Sanity: Refresh actually ran at least once (so we exercised the
	// callback site, not a short-circuit that skipped Refresh entirely).
	assert.Equal(t, int32(1), conn.calls.Load(), "Refresh should have been called exactly once before the select caught ctx.Done()")
}

// TestRetryRefresh_BackoffIsBounded verifies that maxBackoff caps the
// exponential growth. We use small bounds so the test stays fast.
func TestRetryRefresh_BackoffIsBounded(t *testing.T) {
	t.Parallel()
	// Five failures then success; with initial 1ms and max 4ms backoff,
	// sleeps are 1, 2, 4, 4, 4 = 15ms total. The unbounded-doubling worst
	// case would be 1+2+4+8+16 = 31ms. We leave generous headroom on the
	// upper bound because shared CI runners can stall the scheduler enough
	// to drag a 15ms sleep budget past 100ms; 250ms still catches a real
	// unbounded backoff regression (which would balloon by orders of
	// magnitude) without flaking on noisy hosts.
	errs := make([]error, 5)
	for i := range errs {
		errs[i] = errors.New("transient")
	}
	sr, _ := newFakeRegistry(t, errs)

	start := time.Now()
	err := sr.RetryRefresh(context.Background(), time.Millisecond, 4*time.Millisecond, nil)
	require.NoError(t, err)

	elapsed := time.Since(start)
	// Lower bound proves we actually slept; upper bound proves capping.
	assert.GreaterOrEqual(t, elapsed, 10*time.Millisecond)
	assert.Less(t, elapsed, 250*time.Millisecond)
}

// TestRetryRefresh_NilOnAttemptIsSafe verifies the loop tolerates a nil
// onAttempt callback (we want callers to be able to skip the diagnostic
// surface when they don't need it).
func TestRetryRefresh_NilOnAttemptIsSafe(t *testing.T) {
	t.Parallel()
	sr, _ := newFakeRegistry(t, []error{errors.New("once")})

	err := sr.RetryRefresh(context.Background(), time.Millisecond, time.Millisecond, nil)
	require.NoError(t, err)
}

// TestClampBackoff exercises the busy-loop guard directly without observing
// real wall-clock sleeps. RetryRefresh delegates to this helper, so the
// behavioural guarantee ("zero/negative bounds fall back to a 1s default
// rather than spinning the CPU") is locked in here instead of by sleeping
// 1s in a unit test.
func TestClampBackoff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		initial, maxIn time.Duration
		wantI, wantM   time.Duration
	}{
		{"zero initial clamps to 1s", 0, 5 * time.Second, time.Second, 5 * time.Second},
		{"negative initial clamps to 1s", -1, 5 * time.Second, time.Second, 5 * time.Second},
		{"zero max widens to initial", 100 * time.Millisecond, 0, 100 * time.Millisecond, 100 * time.Millisecond},
		{"max smaller than initial widens to initial", 2 * time.Second, time.Second, 2 * time.Second, 2 * time.Second},
		{"positive bounds pass through", 100 * time.Millisecond, time.Second, 100 * time.Millisecond, time.Second},
		{"both non-positive falls back to default", 0, -1, time.Second, time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			i, m := clampBackoff(tt.initial, tt.maxIn)
			assert.Equal(t, tt.wantI, i)
			assert.Equal(t, tt.wantM, m)
		})
	}
}

// TestStartAutoRefresh_ExitsOnContextCancel verifies the ticker loop exits
// cleanly when the context is cancelled, without calling Refresh (nil conn
// would otherwise panic).
func TestStartAutoRefresh_ExitsOnContextCancel(t *testing.T) {
	t.Parallel()
	sr := NewSchemaRegistryFromMap(nil)
	// Long interval so the ticker never fires before cancel.
	sr.refreshInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sr.StartAutoRefresh(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("StartAutoRefresh did not return after ctx cancel")
	}
}

// concurrentBuffer wraps bytes.Buffer with a mutex so the test goroutine can
// read the buffer while slog's JSON handler writes to it from another. slog
// serialises its own writes via an internal mutex, but the test's reads sit
// outside that mutex — without external synchronisation, `go test -race`
// reports the buf access as a data race.
type concurrentBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (c *concurrentBuffer) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.Write(p)
}

func (c *concurrentBuffer) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.String()
}

// TestStartAutoRefresh_LogsAndContinuesOnError covers the error branch in
// StartAutoRefresh's ticker loop: a failed Refresh logs an ERROR line and
// the loop keeps going. Operators rely on this so transient ClickHouse
// blips after boot don't kill the auto-refresh goroutine — the schema
// cache stays warm until the next successful tick.
//
// Uses a perpetual-error fake conn + a 5ms refresh interval to trigger the
// branch in milliseconds, then asserts the log line is present via
// assert.Eventually (no time.Sleep-based scheduling assumption).
func TestStartAutoRefresh_LogsAndContinuesOnError(t *testing.T) {
	t.Parallel()
	errs := make([]error, 50)
	for i := range errs {
		errs[i] = errors.New("transient")
	}
	conn := &fakeConn{errsThenSuccess: errs}

	var buf concurrentBuffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sr := NewSchemaRegistry(conn, "test", 5*time.Millisecond, logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sr.StartAutoRefresh(ctx)
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "schema auto-refresh failed")
	}, 2*time.Second, 5*time.Millisecond, "expected auto-refresh failure log line within 2s")

	cancel()
	select {
	case <-done:
		// expected — ticker observed ctx.Done() and exited
	case <-time.After(2 * time.Second):
		t.Fatal("StartAutoRefresh did not return after ctx cancel")
	}

	// Sanity: the ticker actually fired Refresh at least once. Without
	// this, an off-by-one that skipped the first tick would silently let
	// the test pass off a stale buffer assertion on a never-run loop.
	assert.Greater(t, conn.calls.Load(), int32(0), "ticker never fired Refresh")
}

func TestClassifyValidationError(t *testing.T) {
	t.Parallel()
	schema := &TableSchema{Name: "events", Columns: []Column{
		{Name: "id", Type: "UInt64", IsNullable: false},
		{Name: "name", Type: "String", IsNullable: false},
		{Name: "ts", Type: "DateTime", IsNullable: false, HasDefault: true},
	}}
	cases := []struct {
		name string
		data map[string]any
		want string
	}{
		{"valid", map[string]any{"id": float64(1), "name": "a"}, ""},
		{"unknown column", map[string]any{"id": float64(1), "name": "a", "bogus": 1}, "unknown_field"},
		{"missing required", map[string]any{"id": float64(1)}, "missing_required"},
		{"null on non-nullable", map[string]any{"id": float64(1), "name": nil}, "null_violation"},
		{"type mismatch", map[string]any{"id": "not a number", "name": "a"}, "type_mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := Validate(schema, tc.data)
			if tc.want == "" {
				require.NoError(t, err)
				assert.Equal(t, "", ClassifyValidationError(err))
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.want, ClassifyValidationError(err))
		})
	}
}

func TestClassifyValidationError_Nil(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", ClassifyValidationError(nil))
}

func TestClassifyValidationError_Other(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "other", ClassifyValidationError(errors.New("something else")))
}
