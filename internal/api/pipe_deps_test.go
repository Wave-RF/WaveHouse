package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pipeDepNames(deps []cache.Namespace) []string {
	out := make([]string, len(deps))
	for i, d := range deps {
		out[i], _ = chsql.SafeDecodeNATS(d.Table)
	}
	sort.Strings(out)
	return out
}

// resolveDeps asks ClickHouse (EXPLAIN QUERY TREE) which tables a pipe reads —
// mocked here via resolveTablesFn so the unit test needs no server. It covers the
// fold, the per-pipe memo (EXPLAIN runs once), the database-version fallback when
// ClickHouse can't analyze the query, the pruned TTL cap, and ClearResolvedDeps
// re-resolution.
func TestResolveDeps_Explain(t *testing.T) {
	ctx := context.Background()
	reg := discovery.NewSchemaRegistryForTest(
		[]*discovery.TableSchema{{Name: "events"}, {Name: "users"}, {Name: "orders"}}, nil)
	lc, _ := cache.NewLocal(1 << 20)
	defer func() { _ = lc.Close() }()

	calls := 0
	h := &PipesHandler{registry: reg, Cache: lc, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}
	h.resolveTablesFn = func(_ context.Context, sql string) (discovery.Resolution, error) {
		calls++
		switch sql {
		case "SELECT * FROM events":
			return discovery.Resolution{Tables: []string{"events"}}, nil
		case "SELECT * FROM events JOIN users ON 1":
			return discovery.Resolution{Tables: []string{"events", "users"}}, nil
		case "INSERT INTO events VALUES (1)": // a write → EXPLAIN errors
			return discovery.Resolution{}, errors.New("syntax error")
		case "SELECT * FROM mystery": // resolves to a name the registry doesn't know
			return discovery.Resolution{Tables: []string{"mystery_view"}}, nil
		case "SELECT * FROM events WHERE 'w'='m'": // a dead-branch-pruned arm
			return discovery.Resolution{Tables: []string{"events"}, Pruned: true}, nil
		case "SELECT e.id, s3data.v FROM events e JOIN s3('http://x/y') s3data ON 1": // an external read
			return discovery.Resolution{Tables: []string{"events"}, External: true}, nil
		default:
			return discovery.Resolution{}, nil
		}
	}

	// resolves to the reported tables; all known → not TTL-capped
	deps, unresolved := h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Equal(t, []string{"events"}, pipeDepNames(deps))
	assert.False(t, unresolved, "a known base table must not cap the TTL")

	// cached: a second call with the same bound query doesn't re-run EXPLAIN
	before := calls
	_, _ = h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Equal(t, before, calls, "resolveTablesFn must be cached per bound query")

	// multi-table
	deps, _ = h.resolveDeps(ctx, "SELECT * FROM events JOIN users ON 1")
	assert.Equal(t, []string{"events", "users"}, pipeDepNames(deps))

	// fallback: an un-analyzable query folds the single database-version namespace
	// (any write bumps it, so it is version-maintained and NOT TTL-capped)
	deps, unresolved = h.resolveDeps(ctx, "INSERT INTO events VALUES (1)")
	assert.Equal(t, []cache.Namespace{cache.DatabaseNamespace()}, deps)
	assert.False(t, unresolved, "the database-version fallback must not cap the TTL")

	// a resolved dep the registry doesn't know (an unfoldable view) → TTL-capped
	deps, unresolved = h.resolveDeps(ctx, "SELECT * FROM mystery")
	assert.Equal(t, []string{"mystery_view"}, pipeDepNames(deps))
	assert.True(t, unresolved, "an unknown/unfoldable dep must cap the TTL")

	// a dead-branch-pruned set keeps its tables but caps the TTL
	// (belt-and-suspenders against a hypothetical wrong prune)
	deps, unresolved = h.resolveDeps(ctx, "SELECT * FROM events WHERE 'w'='m'")
	assert.Equal(t, []string{"events"}, pipeDepNames(deps))
	assert.True(t, unresolved, "a pruned dependency set must cap the TTL")

	// an external read (a table function, a cross-database table) keeps its local
	// tables but caps the TTL — writes to the external source are invisible
	deps, unresolved = h.resolveDeps(ctx, "SELECT e.id, s3data.v FROM events e JOIN s3('http://x/y') s3data ON 1")
	assert.Equal(t, []string{"events"}, pipeDepNames(deps))
	assert.True(t, unresolved, "an external read must cap the TTL")

	// ClearResolvedDeps forces re-resolution
	callsBefore := calls
	h.ClearResolvedDeps()
	_, _ = h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Greater(t, calls, callsBefore, "ClearResolvedDeps must force re-resolution")

	// no registry → tracking disabled (TTL-only), no deps
	hNil := &PipesHandler{Cache: lc, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}
	nilDeps, nilUnresolved := hNil.resolveDeps(ctx, "SELECT 1")
	assert.Empty(t, nilDeps)
	assert.False(t, nilUnresolved)
}

// gatedConn signals when its first Query begins and holds every Query until
// released, so a test can land an invalidation while a pipe query is in flight.
// The embedded nil driver.Conn panics on any other method (loud test failure).
type gatedConn struct {
	driver.Conn
	entered chan struct{} // closed when the first Query begins
	release chan struct{} // closed to let queries return
	once    sync.Once
}

func (c *gatedConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return &chainEmptyRows{}, nil
}

// End-to-end through Execute: a write that lands while the pipe query is
// executing must not let the pre-write result be served to post-write readers.
// The versioned key is snapshotted before the query runs (Cache.Key contract),
// so the mid-flight bump orphans the Set; re-folding the key at Set time instead
// would cache the pre-write rows under the post-bump key — a stale HIT for the
// full TTL that no write could ever evict.
func TestPipesHandler_Execute_MidFlightWriteIsNotServedStale(t *testing.T) {
	t.Parallel()
	reg := discovery.NewSchemaRegistryForTest([]*discovery.TableSchema{{Name: "events"}}, nil)
	lc, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()
	lc.SetDependents(reg.Dependents())

	conn := &gatedConn{entered: make(chan struct{}), release: make(chan struct{})}
	store := pipes.NewMemoryStore(&pipes.NamedQuery{Name: "recent", SQL: "SELECT * FROM events"})
	h := NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), conn, lc, reg, 5*time.Second, testutil.NopLogger())
	h.resolveTablesFn = func(_ context.Context, _ string) (discovery.Resolution, error) {
		return discovery.Resolution{Tables: []string{"events"}}, nil
	}

	exec := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := pipesRequest(t, http.MethodGet, "/v1/pipes/recent/execute", "recent", nil)
		r = r.WithContext(auth.WithRole(r.Context(), "admin"))
		h.Execute(w, r)
		return w
	}

	// Request 1 misses and starts executing...
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- exec() }()
	<-conn.entered

	// ...an ingest write invalidates the table while the query is in flight...
	_, err = lc.Invalidate(context.Background(), []cache.Namespace{{Table: "events"}})
	require.NoError(t, err)
	close(conn.release)

	w1 := <-done
	require.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, "MISS", w1.Header().Get("X-Cache"))
	lc.Wait() // let request 1's (orphaned) Set settle before probing

	// ...so a post-write reader must re-execute, not HIT the pre-write result.
	w2 := exec()
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "MISS", w2.Header().Get("X-Cache"),
		"a result computed before a mid-flight write must not be served after it")
}

// A fallback marker is memoized only when ClickHouse actually ANSWERED that the
// query can't be analyzed (*clickhouse.Exception) — a property of the query
// against the current schema, dropped on refresh via ClearResolvedDeps. A failure
// to ASK — an unreachable server, a canceled request — must not be memoized, or
// one transient blip would pin the pipe to the database-version fallback (evicted
// by every write) until the next schema CHANGE (on a stable schema: never); the
// next execution retries instead.
func TestResolveDeps_FailureMemoization(t *testing.T) {
	ctx := context.Background()
	reg := discovery.NewSchemaRegistryForTest([]*discovery.TableSchema{{Name: "events"}}, nil)

	// Transient failure: falls back for this execution, retried on the next.
	h := &PipesHandler{registry: reg, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}
	calls := 0
	h.resolveTablesFn = func(_ context.Context, _ string) (discovery.Resolution, error) {
		calls++
		if calls == 1 {
			return discovery.Resolution{}, context.Canceled // transient: the caller's request went away
		}
		return discovery.Resolution{Tables: []string{"events"}}, nil
	}
	deps, _ := h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Equal(t, []cache.Namespace{cache.DatabaseNamespace()}, deps, "the transient failure still falls back for this execution")
	deps, _ = h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Equal(t, 2, calls, "a transient failure must not be memoized — the next execution retries")
	assert.Equal(t, []string{"events"}, pipeDepNames(deps), "the retry resolves precisely")
	_, _ = h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Equal(t, 2, calls, "the successful resolution is memoized")

	// ClickHouse-answered failure: memoized like a success.
	h = &PipesHandler{registry: reg, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}
	calls = 0
	h.resolveTablesFn = func(_ context.Context, _ string) (discovery.Resolution, error) {
		calls++
		return discovery.Resolution{}, fmt.Errorf("explain query tree: %w", &clickhouse.Exception{Code: 62, Message: "Syntax error"})
	}
	deps, _ = h.resolveDeps(ctx, "INSERT INTO events VALUES (1)")
	assert.Equal(t, []cache.Namespace{cache.DatabaseNamespace()}, deps, "falls back to the database-version namespace")
	_, _ = h.resolveDeps(ctx, "INSERT INTO events VALUES (1)")
	assert.Equal(t, 1, calls, "a ClickHouse-answered failure is memoized until the schema changes")
}

// The memo is bounded: at maxPipeDeps entries, inserting a new resolution evicts
// ONE arbitrary existing entry (random replacement) rather than dropping the
// whole map — a high-cardinality parameter can't wipe every working pipe's
// resolution, and the memo can't grow without limit between refreshes.
func TestResolveDeps_MemoCapEvictsOneEntry(t *testing.T) {
	ctx := context.Background()
	reg := discovery.NewSchemaRegistryForTest([]*discovery.TableSchema{{Name: "events"}}, nil)
	h := &PipesHandler{registry: reg, logger: testutil.NopLogger(), pipeDeps: make(map[string]*resolvedDeps, maxPipeDeps)}
	h.resolveTablesFn = func(_ context.Context, _ string) (discovery.Resolution, error) {
		return discovery.Resolution{Tables: []string{"events"}}, nil
	}

	// Fill the memo to its cap (same-package shortcut; going through resolveDeps
	// 50k times would just be slower).
	h.pipeDepsMu.Lock()
	for i := range maxPipeDeps {
		h.pipeDeps[fmt.Sprintf("SELECT %d", i)] = &resolvedDeps{tables: []string{"events"}}
	}
	h.pipeDepsMu.Unlock()

	_, _ = h.resolveDeps(ctx, "SELECT * FROM events")

	h.pipeDepsMu.RLock()
	defer h.pipeDepsMu.RUnlock()
	assert.Len(t, h.pipeDeps, maxPipeDeps, "one eviction + one insert keeps the memo at its cap")
	_, ok := h.pipeDeps["SELECT * FROM events"]
	assert.True(t, ok, "the new resolution is memoized after evicting one arbitrary entry")
}

// slowConn answers every Query with empty rows after a fixed delay, so
// QueryTimeToTTL derives an uncapped TTL well above UnresolvedDepsTTLCap.
type slowConn struct {
	driver.Conn
	delay time.Duration
}

func (c *slowConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	time.Sleep(c.delay)
	return &chainEmptyRows{}, nil
}

// ttlRecordingCache records the TTL of the last Set so a test can assert the
// cap was applied at the Set site, not just decided by resolveDeps.
type ttlRecordingCache struct {
	cache.Cache
	lastSetTTL time.Duration
}

func (c *ttlRecordingCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	c.lastSetTTL = ttl
	return c.Cache.Set(ctx, key, value, ttl)
}

// A result whose dependency set can't be trusted to version invalidation (here:
// dead-branch pruned) must be STORED with the capped TTL. The cap decision is
// pinned in TestResolveDeps_Explain; this pins its application at the pipe Set.
// The fake query takes 25ms, so the uncapped TTL would be ~25s (QueryTimeToTTL's
// 1000x curve) — the ≤10s assertion genuinely distinguishes capped from not.
func TestPipesHandler_Execute_UnresolvedDepsTTLCapApplied(t *testing.T) {
	t.Parallel()
	reg := discovery.NewSchemaRegistryForTest([]*discovery.TableSchema{{Name: "events"}}, nil)
	lc, err := cache.NewLocal(1 << 20)
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()
	rec := &ttlRecordingCache{Cache: lc}

	store := pipes.NewMemoryStore(&pipes.NamedQuery{Name: "recent", SQL: "SELECT * FROM events"})
	h := NewPipesHandler(store, policy.NewMemoryStore(&policy.Policy{}), &slowConn{delay: 25 * time.Millisecond}, rec, reg, 5*time.Second, testutil.NopLogger())
	h.resolveTablesFn = func(_ context.Context, _ string) (discovery.Resolution, error) {
		return discovery.Resolution{Tables: []string{"events"}, Pruned: true}, nil
	}

	w := httptest.NewRecorder()
	r := pipesRequest(t, http.MethodGet, "/v1/pipes/recent/execute", "recent", nil)
	r = r.WithContext(auth.WithRole(r.Context(), "admin"))
	h.Execute(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	assert.Positive(t, rec.lastSetTTL, "the miss must reach Set")
	assert.LessOrEqual(t, rec.lastSetTTL, cache.UnresolvedDepsTTLCap,
		"a pruned dependency set must cap the stored TTL")
}

// A ClearResolvedDeps landing while a resolution is in flight must win: the
// in-flight resolution was computed against the pre-refresh schema, so inserting
// it after the clear would silently re-populate the memo with a stale dependency
// set that persists until the NEXT refresh. The generation gate drops it instead,
// and the following execution re-resolves.
func TestResolveDeps_ClearDuringResolveIsNotUndone(t *testing.T) {
	ctx := context.Background()
	reg := discovery.NewSchemaRegistryForTest([]*discovery.TableSchema{{Name: "events"}}, nil)
	h := &PipesHandler{registry: reg, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}

	calls := 0
	h.resolveTablesFn = func(_ context.Context, _ string) (discovery.Resolution, error) {
		calls++
		if calls == 1 {
			// The schema refresh fires while this resolution is in flight.
			h.ClearResolvedDeps()
		}
		return discovery.Resolution{Tables: []string{"events"}}, nil
	}

	_, _ = h.resolveDeps(ctx, "SELECT * FROM events")

	h.pipeDepsMu.RLock()
	_, memoized := h.pipeDeps["SELECT * FROM events"]
	h.pipeDepsMu.RUnlock()
	assert.False(t, memoized, "a resolution overlapped by ClearResolvedDeps must not be memoized")

	_, _ = h.resolveDeps(ctx, "SELECT * FROM events")
	assert.Equal(t, 2, calls, "the next execution re-resolves against the refreshed schema")
	h.pipeDepsMu.RLock()
	_, memoized = h.pipeDeps["SELECT * FROM events"]
	h.pipeDepsMu.RUnlock()
	assert.True(t, memoized, "the post-refresh resolution is memoized normally")
}

// Concurrent cold misses for the same bound query coalesce onto ONE EXPLAIN via
// the resolve singleflight; every waiter shares the outcome.
func TestResolveDeps_ConcurrentColdMissesCoalesce(t *testing.T) {
	ctx := context.Background()
	reg := discovery.NewSchemaRegistryForTest([]*discovery.TableSchema{{Name: "events"}}, nil)
	h := &PipesHandler{registry: reg, logger: testutil.NopLogger(), pipeDeps: map[string]*resolvedDeps{}}

	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	h.resolveTablesFn = func(_ context.Context, _ string) (discovery.Resolution, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return discovery.Resolution{Tables: []string{"events"}}, nil
	}

	const waiters = 8
	var wg sync.WaitGroup
	results := make([][]cache.Namespace, waiters)
	for i := range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], _ = h.resolveDeps(ctx, "SELECT * FROM events")
		}()
	}
	<-started
	// The first resolution is now blocked in flight; give the remaining waiters
	// time to reach the singleflight and join it. A straggler that misses even
	// this window would still be served by the memo after the flight completes,
	// so the assertion below stays stable.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load(), "concurrent cold misses must coalesce onto one EXPLAIN")
	for i := range waiters {
		assert.Equal(t, []string{"events"}, pipeDepNames(results[i]))
	}
}
