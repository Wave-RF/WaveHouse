package discovery

import (
	"context"
	"errors"
	"io"
	"log/slog"
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewSchemaRegistry(conn, "test", time.Hour, logger), conn
}

// TestRetryRefresh_SucceedsOnFirstAttempt is the happy path: Refresh returns
// nil immediately and no backoff is observed.
func TestRetryRefresh_SucceedsOnFirstAttempt(t *testing.T) {
	t.Parallel()
	sr, conn := newFakeRegistry(t, nil)

	var attempts int32
	start := time.Now()
	err := sr.RetryRefresh(context.Background(), time.Hour, time.Hour, func(error) {
		atomic.AddInt32(&attempts, 1)
	})
	require.NoError(t, err)

	assert.Less(t, time.Since(start), 100*time.Millisecond, "should not have slept")
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

	// Give the goroutine a moment to enter its first sleep.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("RetryRefresh did not return after ctx cancel")
	}
}

// TestRetryRefresh_BackoffIsBounded verifies that maxBackoff caps the
// exponential growth. We use small bounds so the test stays fast.
func TestRetryRefresh_BackoffIsBounded(t *testing.T) {
	t.Parallel()
	// Five failures then success; with initial 1ms and max 4ms backoff,
	// sleeps are 1, 2, 4, 4, 4 = 15ms total. We assert it stays well under
	// the unbounded-doubling worst case (1+2+4+8+16 = 31ms).
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
	assert.Less(t, elapsed, 100*time.Millisecond)
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

// TestRetryRefresh_ClampsInvalidBackoffs verifies zero or negative bounds
// fall back to defaults instead of busy-looping (a 0-duration time.After
// would otherwise fire immediately and spin the CPU).
func TestRetryRefresh_ClampsInvalidBackoffs(t *testing.T) {
	t.Parallel()
	sr, _ := newFakeRegistry(t, []error{errors.New("once")})

	// Cancel via context after a short window so we have a hard ceiling on
	// how long this test can run, but expect success well before that.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use 0 / -1 — should clamp to defaults (1s initial), but with only one
	// failure the loop succeeds on the second attempt after ~1s.
	start := time.Now()
	err := sr.RetryRefresh(ctx, 0, -1, nil)
	require.NoError(t, err)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "should sleep about 1s with clamped default")
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
