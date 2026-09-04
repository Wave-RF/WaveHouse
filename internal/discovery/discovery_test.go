package discovery

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestTableSchema_ColumnNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		schema *TableSchema
		want   []string
	}{
		{
			name: "preserves discovered order",
			schema: &TableSchema{Name: "clicks", Columns: []Column{
				{Name: "page"}, {Name: "user_id"}, {Name: "ts"},
			}},
			want: []string{"page", "user_id", "ts"},
		},
		{
			name:   "no columns yields empty non-nil slice",
			schema: &TableSchema{Name: "empty"},
			want:   []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.schema.ColumnNames()
			assert.Equal(t, tt.want, got)
			assert.NotNil(t, got)
		})
	}
}

func TestNewSchemaRegistry_ConstructorDefaults(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sr := NewSchemaRegistry(nil, func() string { return "wavehouse" }, func() time.Duration { return 30 * time.Second }, logger)
	require.NotNil(t, sr)
	assert.Equal(t, "wavehouse", sr.database())
	assert.Equal(t, 30*time.Second, sr.refreshInterval())
	assert.Same(t, logger, sr.logger)
	assert.NotNil(t, sr.tables)
	assert.Empty(t, sr.List())
	assert.Nil(t, sr.Get("anything"))
}

// TestRefresh_PopulatesAndLookups: Refresh turns the system.columns scan into
// name-keyed schemas — Get finds them, List returns them all.
func TestRefresh_PopulatesAndLookups(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{
		version: "25.3.1.1",
		columns: []fakeColumn{
			{table: "clicks", name: "id", chType: "String", position: 1},
			{table: "clicks", name: "page", chType: "String", defaultKind: "DEFAULT", defaultExpr: "'/'", position: 2},
			{table: "users", name: "id", chType: "UInt64", position: 1},
		},
		tables: [][2]string{
			{"clicks", "CREATE TABLE test.clicks (`id` String) ENGINE = MergeTree"},
			{"users", "CREATE TABLE test.users (`id` UInt64) ENGINE = MergeTree"},
			// Listed in system.tables but with no system.columns rows — skipped,
			// never published as a column-less schema.
			{"ghost", "CREATE TABLE test.ghost (`x` String) ENGINE = MergeTree"},
		},
	}
	sr := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())
	require.NoError(t, sr.Refresh(context.Background()))

	clicks := sr.Get("clicks")
	require.NotNil(t, clicks)
	assert.Equal(t, []string{"id", "page"}, clicks.ColumnNames())
	assert.False(t, clicks.Columns[0].HasDefault)
	assert.True(t, clicks.Columns[1].HasDefault, "non-empty default_kind ⇒ HasDefault")
	assert.Empty(t, clicks.Columns[0].DefaultExpression)
	assert.Equal(t, "'/'", clicks.Columns[1].DefaultExpression)
	assert.Equal(t, uint64(1), clicks.Columns[0].Position)
	assert.Equal(t, uint64(2), clicks.Columns[1].Position)
	assert.Equal(t, "CREATE TABLE test.clicks (`id` String) ENGINE = MergeTree", clicks.DDL)
	require.NotNil(t, sr.Get("users"))
	assert.Nil(t, sr.Get("ghost"), "a table with no columns is not a schema")
	assert.Nil(t, sr.Get("missing"))
	assert.Len(t, sr.List(), 2)
	assert.Equal(t, "25.3.1.1", sr.ServerVersion())
}

// TestRefresh_DDLIsNotSerialized: the schema endpoint marshals TableSchema
// straight to the client, and an external-engine table renders its wiring in
// create_table_query — endpoint, bucket, username, access key id. ClickHouse
// masks the password as `[HIDDEN]` from ~23.9, so the topology is the exposure
// — and DDL must never appear in the JSON either way.
func TestRefresh_DDLIsNotSerialized(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{
		columns: []fakeColumn{{table: "clicks", name: "id", chType: "String", position: 1}},
		// As a modern server actually renders it: the password is masked, the
		// topology is not. The field is withheld for the topology.
		tables: [][2]string{{"clicks", "CREATE TABLE test.clicks (`id` String) ENGINE = S3('https://acme-private.s3.amazonaws.com/events.csv', 'AKIAEXAMPLEKEY', '[HIDDEN]', 'CSV')"}},
	}
	sr := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())
	require.NoError(t, sr.Refresh(context.Background()))

	encoded, err := json.Marshal(sr.Get("clicks"))
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "acme-private", "the bucket must not reach the client")
	assert.NotContains(t, string(encoded), "AKIAEXAMPLEKEY", "nor the access key id")
	assert.NotContains(t, string(encoded), "CREATE TABLE")
	assert.Contains(t, string(encoded), `"position":1`)
}

// TestRefresh_ServerVersionQueryFails: version() is probed on the same
// connection as timezone(), so its failure fails the refresh rather than
// publishing a registry with no idea what server produced it.
func TestRefresh_ServerVersionQueryFails(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{
		version: "25.3.2.2",
		columns: []fakeColumn{{table: "clicks", name: "id", chType: "String", position: 1}},
	}
	sr := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())
	require.NoError(t, sr.Refresh(context.Background()))
	require.Equal(t, "25.3.2.2", sr.ServerVersion())

	// Now fail the probe. Asserting emptiness after a single failed refresh would
	// pass even if the failure had cleared an already-published registry, which
	// is the opposite of the documented behavior — callers keep the prior cache.
	conn.versionErr = errors.New("code: 497, not enough privileges")
	require.ErrorContains(t, sr.Refresh(context.Background()), "query server version")
	assert.Equal(t, "25.3.2.2", sr.ServerVersion(), "a failed refresh must not clear the prior server version")
	assert.NotNil(t, sr.Get("clicks"), "a failed refresh must not clear the prior schemas")
}

// TestRefresh_TablesQueryFails: system.tables is part of the same refresh, so a
// failure there keeps the prior cache instead of publishing DDL-less schemas.
func TestRefresh_TablesQueryFails(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{
		columns: []fakeColumn{{table: "clicks", name: "id", chType: "String", position: 1}},
		tables:  [][2]string{{"clicks", "CREATE TABLE test.clicks (id String) ENGINE = MergeTree"}},
	}
	sr := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())
	require.NoError(t, sr.Refresh(context.Background()))
	require.NotNil(t, sr.Get("clicks"))
	require.NotEmpty(t, sr.Get("clicks").DDL)

	// Seeded first on purpose: the claim is that a failed DDL scan KEEPS the
	// prior cache, and asserting Nil after one failed refresh cannot tell that
	// apart from a refresh that wiped it.
	conn.tablesErr = errors.New("connection reset")
	require.ErrorContains(t, sr.Refresh(context.Background()), "query system.tables")
	if got := sr.Get("clicks"); assert.NotNil(t, got, "a failed DDL scan must keep the prior registry") {
		assert.NotEmpty(t, got.DDL, "and its prior DDL")
	}
}

// TestRefresh_EmptyDatabase: zero system.columns rows yield an empty, usable
// registry — nil lookups, empty List, no error.
func TestRefresh_EmptyDatabase(t *testing.T) {
	t.Parallel()
	sr, _ := newFakeRegistry(t, nil)
	require.NoError(t, sr.Refresh(context.Background()))
	assert.Empty(t, sr.List())
	assert.Nil(t, sr.Get("x"))
}

// fakeConn implements just enough of driver.Conn to drive Refresh and
// RetryRefresh: successive errors from errsThenSuccess, then columns as the
// system.columns result set (emptyRows when nil). The nil-embedded interface
// covers every other method — none are reached by Refresh.
type fakeConn struct {
	driver.Conn
	errsThenSuccess []error
	calls           atomic.Int32
	tz              string       // SELECT timezone() answer; "" ⇒ "UTC"
	version         string       // SELECT version() answer; "" ⇒ "24.1.1.1"
	versionErr      error        // non-nil ⇒ the version() probe fails
	columns         []fakeColumn // system.columns rows served on success; nil ⇒ none
	iterErr         error        // rows.Err() after the served rows; nil ⇒ clean iteration
	tables          [][2]string  // system.tables rows: {name, create_table_query}
	tablesErr       error        // non-nil ⇒ the system.tables query fails
	onQueryArgs     func([]any)  // non-nil ⇒ observe each query's bound args
}

// Query serves both of Refresh's result sets. Only the system.columns call
// advances errsThenSuccess and the calls counter, so the retry tests still count
// refresh attempts rather than statements.
func (c *fakeConn) Query(_ context.Context, q string, args ...any) (driver.Rows, error) {
	if c.onQueryArgs != nil {
		c.onQueryArgs(args)
	}
	if strings.Contains(q, "system.tables") {
		if c.tablesErr != nil {
			return nil, c.tablesErr
		}
		if c.tables == nil {
			return &emptyRows{}, nil
		}
		return &fakeTableRows{rows: c.tables}, nil
	}
	n := c.calls.Add(1)
	if int(n) <= len(c.errsThenSuccess) {
		return nil, c.errsThenSuccess[n-1]
	}
	if c.columns != nil || c.iterErr != nil {
		return &fakeRows{rows: c.columns, err: c.iterErr}, nil
	}
	return &emptyRows{}, nil
}

// QueryRow answers the SELECT timezone() (#372) and SELECT version() probes
// Refresh issues before the system.columns query; errsThenSuccess sequencing
// stays keyed on Query.
func (c *fakeConn) QueryRow(_ context.Context, q string, _ ...any) driver.Row {
	if strings.Contains(q, "version()") {
		if c.versionErr != nil {
			return scalarRow{err: c.versionErr}
		}
		if c.version != "" {
			return scalarRow{val: c.version}
		}
		return scalarRow{val: "24.1.1.1"}
	}
	if c.tz != "" {
		return scalarRow{val: c.tz}
	}
	return scalarRow{val: "UTC"}
}

// scalarRow answers a one-column probe (timezone(), version()) with a string,
// or with a canned error.
type scalarRow struct {
	driver.Row
	val string
	err error
}

func (r scalarRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) == 1 {
		if s, ok := dest[0].(*string); ok {
			*s = r.val
			return nil
		}
	}
	return errors.New("unexpected scalar probe scan")
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

// fakeColumn is one system.columns row the fake serves, in the select order
// Refresh scans: table, name, type, default_kind, default_expression, position.
type fakeColumn struct {
	table       string
	name        string
	chType      string
	defaultKind string
	defaultExpr string
	position    uint64
}

// fakeRows plays a system.columns result set, in the given order.
type fakeRows struct {
	driver.Rows
	rows []fakeColumn
	next int
	err  error // returned from Err() once iteration ends
}

func (r *fakeRows) Next() bool {
	r.next++
	return r.next <= len(r.rows)
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.next-1]
	if len(dest) != 6 {
		return errors.New("unexpected system.columns scan shape")
	}
	strs := []string{row.table, row.name, row.chType, row.defaultKind, row.defaultExpr}
	for i, want := range strs {
		s, ok := dest[i].(*string)
		if !ok {
			return errors.New("system.columns scan dest must be *string")
		}
		*s = want
	}
	pos, ok := dest[5].(*uint64)
	if !ok {
		return errors.New("system.columns position dest must be *uint64")
	}
	*pos = row.position
	return nil
}

func (*fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error { return r.err }

// fakeTableRows plays a system.tables result set: one {name,
// create_table_query} pair per table.
type fakeTableRows struct {
	driver.Rows
	rows [][2]string
	next int
}

func (r *fakeTableRows) Next() bool {
	r.next++
	return r.next <= len(r.rows)
}

func (r *fakeTableRows) Scan(dest ...any) error {
	row := r.rows[r.next-1]
	if len(dest) != len(row) {
		return errors.New("unexpected system.tables scan shape")
	}
	for i, d := range dest {
		s, ok := d.(*string)
		if !ok {
			return errors.New("system.tables scan dest must be *string")
		}
		*s = row[i]
	}
	return nil
}

func (*fakeTableRows) Close() error { return nil }
func (*fakeTableRows) Err() error   { return nil }

func newFakeRegistry(t *testing.T, errs []error) (*SchemaRegistry, *fakeConn) {
	t.Helper()
	conn := &fakeConn{errsThenSuccess: errs}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, logger), conn
}

// TestRefresh_UnresolvableServerTimezone_NotFatal: an unresolvable server zone
// degrades to pass-through canonicalization (#372), never a failed refresh.
func TestRefresh_UnresolvableServerTimezone_NotFatal(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{tz: "Not/AZone"}
	sr := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())
	require.NoError(t, sr.Refresh(context.Background()))
}

// TestRefresh_RowsIterationError_Fails: rows.Next() returns false on a
// mid-stream driver error too, so Refresh must consult rows.Err() and fail —
// keeping the prior cached schema — rather than publish a silently truncated
// registry.
func TestRefresh_RowsIterationError_Fails(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{
		columns: []fakeColumn{{table: "events", name: "id", chType: "String", position: 1}},
		iterErr: errors.New("network drop mid-stream"),
	}
	sr := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())
	err := sr.Refresh(context.Background())
	require.ErrorContains(t, err, "network drop mid-stream")
	require.Nil(t, sr.Get("events"), "truncated scan must not be published")
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
// "context canceled" into BootState as a spurious /livez diagnostic during
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
		"onAttempt must not fire when ctx is already cancelled — that would surface shutdown as a /livez diagnostic")
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
	// Long interval so the ticker never fires before cancel.
	sr := NewSchemaRegistry(nil, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())

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
	sr := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return 5 * time.Millisecond }, logger)

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

// TestRefresh_DatabaseSnapshottedForWholeRefresh: `database` is a live getter so
// a ClickHouse reconfigure is honored on the NEXT refresh. Reading it once per
// query instead of once per refresh let a reconfigure land between the
// system.columns and system.tables scans, attaching DDL from the new database to
// same-named schemas discovered from the old one — a silent cross-database mix.
func TestRefresh_DatabaseSnapshottedForWholeRefresh(t *testing.T) {
	t.Parallel()
	var seen []string
	var n atomic.Int32
	conn := &fakeConn{
		columns: []fakeColumn{{table: "clicks", name: "id", chType: "String", position: 1}},
		tables:  [][2]string{{"clicks", "CREATE TABLE old.clicks (id String) ENGINE = MergeTree"}},
		onQueryArgs: func(args []any) {
			if len(args) == 1 {
				if db, ok := args[0].(string); ok {
					seen = append(seen, db)
				}
			}
		},
	}
	// Flips after the first query, i.e. between system.columns and system.tables.
	db := func() string {
		if n.Add(1) > 1 {
			return "new"
		}
		return "old"
	}
	sr := NewSchemaRegistry(conn, db, func() time.Duration { return time.Hour }, discardLogger())
	require.NoError(t, sr.Refresh(context.Background()))

	require.Len(t, seen, 2, "both scans should be parameterised by a database")
	assert.Equal(t, seen[0], seen[1],
		"both queries in one refresh must use the same database; a reconfigure applies to the next refresh")
	assert.Equal(t, "old", seen[0], "the snapshot is taken once, at the start of the refresh")
}

// TestColumn_IsInsertable pins the rule against what ClickHouse actually
// refuses (verified on 26.7.3): a MATERIALIZED column is code 44
// ILLEGAL_COLUMN and an ALIAS column is code 16 NO_SUCH_COLUMN_IN_TABLE, while
// a plain, DEFAULT or EPHEMERAL column takes a value. EPHEMERAL is the one
// that reads backwards at a glance — it is insert-ONLY, never stored.
func TestColumn_IsInsertable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		kind string
		want bool
	}{
		{"", true},
		{"DEFAULT", true},
		{"EPHEMERAL", true},
		{"MATERIALIZED", false},
		{"ALIAS", false},
	}
	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, Column{Name: "c", DefaultKind: tt.kind}.IsInsertable())
		})
	}
}

// TestTableSchema_InsertableColumns: computed columns are dropped from the
// ingest contract but stay in Columns — the schema endpoint and the query path
// still see the whole table.
func TestTableSchema_InsertableColumns(t *testing.T) {
	t.Parallel()
	ts := &TableSchema{Name: "t", Columns: []Column{
		{Name: "id", Position: 1},
		{Name: "mat", DefaultKind: "MATERIALIZED", Position: 2},
		{Name: "page", DefaultKind: "DEFAULT", Position: 3},
		{Name: "ali", DefaultKind: "ALIAS", Position: 4},
	}}
	assert.Equal(t, []string{"id", "page"}, ts.InsertableColumnNames())
	assert.Equal(t, []string{"id", "mat", "page", "ali"}, ts.ColumnNames(),
		"the full column list is untouched")

	got := ts.InsertableColumns()
	require.Len(t, got, 2)
	assert.Equal(t, uint64(3), got[1].Position, "declaration order and ordinals survive the filter")
}

// TestTableSchema_InsertableColumnNames_NoneQualify: a table of nothing but
// computed columns yields an empty, non-nil list rather than nil.
func TestTableSchema_InsertableColumnNames_NoneQualify(t *testing.T) {
	t.Parallel()
	ts := &TableSchema{Name: "t", Columns: []Column{{Name: "ali", DefaultKind: "ALIAS"}}}
	got := ts.InsertableColumnNames()
	assert.Empty(t, got)
	assert.NotNil(t, got)
}

// TestRefresh_CapturesDefaultKind: the kind is what decides insertability, so
// it must survive the scan verbatim rather than being flattened to HasDefault.
func TestRefresh_CapturesDefaultKind(t *testing.T) {
	t.Parallel()
	conn := &fakeConn{columns: []fakeColumn{
		{table: "t", name: "id", chType: "UInt64", position: 1},
		{table: "t", name: "mat", chType: "String", defaultKind: "MATERIALIZED", defaultExpr: "concat('m', id)", position: 2},
		{table: "t", name: "page", chType: "String", defaultKind: "DEFAULT", defaultExpr: "'/'", position: 3},
	}}
	sr := NewSchemaRegistry(conn, func() string { return "test" }, func() time.Duration { return time.Hour }, discardLogger())
	require.NoError(t, sr.Refresh(context.Background()))

	ts := sr.Get("t")
	require.NotNil(t, ts)
	assert.Equal(t, "", ts.Columns[0].DefaultKind)
	assert.Equal(t, "MATERIALIZED", ts.Columns[1].DefaultKind)
	assert.True(t, ts.Columns[1].HasDefault, "a computed column still reads as having a default")
	assert.Equal(t, "DEFAULT", ts.Columns[2].DefaultKind)
	assert.Equal(t, []string{"id", "page"}, ts.InsertableColumnNames())
}

func TestInsertableColumns_CachedAndUncachedAgree(t *testing.T) {
	t.Parallel()
	// A literal-built TableSchema (what tests construct) leaves the memo nil and
	// takes the compute path; cacheInsertable is what the registry calls after a
	// refresh. Both must answer identically, or a discovered table and a
	// hand-built one disagree about what may be inserted.
	ts := &TableSchema{Name: "clicks", Columns: []Column{
		{Name: "id", Type: "UInt64"},
		{Name: "day", Type: "Date", DefaultKind: "MATERIALIZED"},
		{Name: "page", Type: "String"},
		{Name: "alias", Type: "String", DefaultKind: "ALIAS"},
		{Name: "note", Type: "String", DefaultKind: "DEFAULT"},
	}}

	uncached := ts.InsertableColumns()
	uncachedNames := ts.InsertableColumnNames()
	require.Nil(t, ts.insertable, "reading must not populate the memo")

	ts.cacheInsertable()
	assert.Equal(t, uncached, ts.InsertableColumns())
	assert.Equal(t, uncachedNames, ts.InsertableColumnNames())
	assert.Equal(t, []string{"id", "page", "note"}, ts.InsertableColumnNames(),
		"MATERIALIZED and ALIAS are excluded; DEFAULT and plain are not")

	// The memo is returned by reference, so every caller shares one slice. That
	// is safe only while no caller writes to it — pin the contract here, since a
	// caller that appended would corrupt every later request for this table.
	first := ts.InsertableColumns()
	second := ts.InsertableColumns()
	require.NotEmpty(t, first)
	assert.Same(t, &first[0], &second[0], "callers share the memoized backing array")
}
