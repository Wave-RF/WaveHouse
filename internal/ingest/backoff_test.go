package ingest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
)

// noJitter makes every spread 0, so a delay is exactly half its step.
func noJitter(int64) int64 { return 0 }

// testCap is the seed's ingest.retry_backoff_max.
const testCap = 30 * time.Second

func TestBackoff_ClosedAllowsEveryFlush(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	now := time.Unix(0, 0)
	for range 3 {
		wait, ok := b.allow(now, testCap)
		assert.True(t, ok)
		assert.Zero(t, wait)
	}
	recovered, _ := b.succeed(now)
	assert.False(t, recovered, "a closed backoff has nothing to recover from")
}

func TestBackoff_EscalatesToTheCap(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	now := time.Unix(0, 0)
	var got []time.Duration
	for range 8 {
		_, ok := b.allow(now, testCap)
		require.True(t, ok, "the backoff has elapsed, so a probe may try")
		wait, _, _ := b.fail(now, testCap)
		got = append(got, wait)
		now = now.Add(wait)
	}
	// Half of 1s, 2s, 4s, 8s, 16s, then the 30s cap.
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second, 15 * time.Second}
	assert.Equal(t, want, got)
}

// The cap is the tenant's, read per failure: a longer one lets the doubling
// run on, and one under the base — a value the settings refuse, or a tenant
// with none — floors at the base rather than retrying in a tight loop.
func TestBackoff_CapIsTheTenants(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	now := time.Unix(0, 0)
	var got []time.Duration
	for range 8 {
		wait, _, _ := b.fail(now, 2*time.Minute)
		got = append(got, wait)
		now = now.Add(wait)
	}
	// Half of 1s, 2s, 4s, 8s, 16s, 32s, 64s, then the 2m cap.
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute}
	assert.Equal(t, want, got)

	floor := &backoff{jitter: noJitter}
	wait, _, _ := floor.fail(now, 0)
	assert.Equal(t, retryBase/2, wait)
	now = now.Add(wait)
	wait, _, _ = floor.fail(now, 0)
	assert.Equal(t, retryBase/2, wait, "capped at the base")
}

// Tenants on one ClickHouse tuple share its pool's backoff. Each window
// opens under the cap of the tenant whose flush met the failure, and any
// tenant whose rows meet the window meanwhile cuts it to its own cap from
// then on, so no tenant waits longer than its own cap and the one with the
// larger cap retries sooner than it asked; the next window opens under its
// prober's cap again, so a reload that raises a cap applies at the next
// failure, and a tenant that left stops counting.
func TestBackoff_SharedPoolCutsAWindowToTheSmallestCap(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	now := time.Unix(0, 0)
	var wait time.Duration
	for range 8 { // a 2m tenant escalates: half of 1s, 2s, …, 64s, then half its cap
		wait, _, _ = b.fail(now, 2*time.Minute)
		now = now.Add(wait)
	}
	wait, _, _ = b.fail(now, 2*time.Minute)
	require.Equal(t, time.Minute, wait)

	wait, ok := b.waiting(now.Add(time.Second), 5*time.Second)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, wait, "a 5s tenant's rows wait its own cap at most: the window is cut from now")
	now = now.Add(6 * time.Second)
	_, ok = b.allow(now, 2*time.Minute)
	require.True(t, ok, "the cut window has elapsed, so a probe goes out")
	wait, _, _ = b.fail(now, 2*time.Minute)
	assert.Equal(t, time.Minute, wait, "the next window opens under the 2m tenant's own cap again")
	wait, ok = b.waiting(now, 5*time.Second)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, wait, "and the 5s tenant's next row cuts it again")

	now = now.Add(6 * time.Second)
	_, ok = b.allow(now, 5*time.Minute)
	require.True(t, ok)
	wait, _, _ = b.fail(now, 5*time.Minute)
	assert.Equal(t, 150*time.Second, wait, "a reload that raised the cap to 5m applies at the next failure")
}

// A failure that lands while the window is running — a flush that started
// before the first one reported — does not escalate the window, but still
// folds its tenant's cap in: under another tenant's window its rows are
// handed back for at most its own cap.
func TestBackoff_ANonEscalatingFailureCutsTheWindow(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	now := time.Unix(0, 0)
	var wait time.Duration
	for range 9 { // a 2m tenant's window of a minute
		wait, _, _ = b.fail(now, 2*time.Minute)
		now = now.Add(wait)
	}
	now = now.Add(-wait) // inside the last window
	wait, first, _ := b.fail(now.Add(time.Second), 5*time.Second)
	assert.False(t, first)
	assert.Equal(t, 5*time.Second, wait)
}

func TestBackoff_JitterStaysWithinTheStep(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: func(n int64) int64 { return n - 1 }}
	wait, first, log := b.fail(time.Unix(0, 0), testCap)
	assert.True(t, first)
	assert.True(t, log)
	assert.Less(t, wait, retryBase)
	assert.GreaterOrEqual(t, wait, retryBase/2)
}

func TestBackoff_OpenTurnsFlushesAwayUntilItElapses(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	t0 := time.Unix(100, 0)
	wait, first, _ := b.fail(t0, testCap)
	require.True(t, first)

	// Inside the window: turned away for what is left of it.
	got, ok := b.allow(t0.Add(100*time.Millisecond), testCap)
	assert.False(t, ok)
	assert.Equal(t, wait-100*time.Millisecond, got)

	// A failure reported late, from a flush that started before the first
	// one reported, does not escalate the running backoff.
	late, lateFirst, lateLog := b.fail(t0.Add(200*time.Millisecond), testCap)
	assert.False(t, lateFirst)
	assert.False(t, lateLog)
	assert.Equal(t, wait-200*time.Millisecond, late)
	assert.Equal(t, 1, b.failures)

	// Elapsed: one probe goes; the rest wait for its answer.
	after := t0.Add(wait)
	_, ok = b.allow(after, testCap)
	assert.True(t, ok, "first flush after the window is the probe")
	got, ok = b.allow(after, testCap)
	assert.False(t, ok, "a second flush waits while the probe is out")
	assert.Equal(t, retryBase/2, got, "floored, so rows do not cycle while a slow probe is out")
	got, ok = b.waiting(after, testCap)
	assert.True(t, ok, "arriving rows are handed back while the probe is out too")
	assert.Equal(t, retryBase/2, got)

	// The probe succeeds: closed, every flush tries again.
	recovered, lasted := b.succeed(after.Add(time.Second))
	assert.True(t, recovered)
	assert.Equal(t, wait+time.Second, lasted)
	_, ok = b.allow(after.Add(time.Second), testCap)
	assert.True(t, ok)
}

func TestBackoff_LogsAnOngoingOutageAtABoundedRate(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	now := time.Unix(0, 0)
	var logged int
	for range 20 { // ~2.5 minutes of failed probes at the capped backoff
		_, _, log := b.fail(now, testCap)
		if log {
			logged++
		}
		now = now.Add(b.until.Sub(now))
	}
	assert.Greater(t, logged, 1, "an ongoing outage keeps logging")
	assert.Less(t, logged, 10, "but not once per probe")
}

func TestBackoff_ReleaseReturnsAnUnusedProbe(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	wait, _, _ := b.fail(time.Unix(0, 0), testCap)
	after := time.Unix(0, 0).Add(wait)
	_, ok := b.allow(after, testCap)
	require.True(t, ok)
	b.release()
	_, ok = b.allow(after, testCap)
	assert.True(t, ok, "the released probe can be claimed again")
}

func TestBackoffs_TableAndPoolAreSeparate(t *testing.T) {
	t.Parallel()
	var bs backoffs
	tgt := chconn.Target{URL: "http://a:8123", Username: "u", Database: "d"}
	now := time.Unix(0, 0)
	bs.forTable(tgt, "ro").fail(now, testCap)

	_, ok := bs.waiting(func() chconn.Target { return tgt }, "ro", now, func() time.Duration { return testCap })
	assert.True(t, ok, "the failing table waits")
	_, ok = bs.waiting(func() chconn.Target { return tgt }, "healthy", now, func() time.Duration { return testCap })
	assert.False(t, ok, "its neighbour on the pool does not")
	assert.NotSame(t, bs.forTarget(tgt), bs.forTable(tgt, "ro"))
}

func TestBackoffs_OnePerPool(t *testing.T) {
	t.Parallel()
	var bs backoffs
	a := chconn.Target{URL: "http://a:8123", Username: "u", Database: "d"}
	aOtherTenantSamePool := chconn.Target{URL: "http://a:8123", Username: "u", Database: "d", Password: "p"}
	aOtherUser := chconn.Target{URL: "http://a:8123", Username: "v", Database: "d"}
	assert.Same(t, bs.forTarget(a), bs.forTarget(aOtherTenantSamePool))
	assert.NotSame(t, bs.forTarget(a), bs.forTarget(aOtherUser))
}
