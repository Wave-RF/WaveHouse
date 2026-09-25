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

func TestBackoff_ClosedAllowsEveryFlush(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	now := time.Unix(0, 0)
	for range 3 {
		wait, ok := b.allow(now)
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
		_, ok := b.allow(now)
		require.True(t, ok, "the backoff has elapsed, so a probe may try")
		wait, _, _ := b.fail(now)
		got = append(got, wait)
		now = now.Add(wait)
	}
	// Half of 1s, 2s, 4s, 8s, 16s, then the 30s cap.
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second, 15 * time.Second}
	assert.Equal(t, want, got)
}

func TestBackoff_JitterStaysWithinTheStep(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: func(n int64) int64 { return n - 1 }}
	wait, first, log := b.fail(time.Unix(0, 0))
	assert.True(t, first)
	assert.True(t, log)
	assert.Less(t, wait, retryBase)
	assert.GreaterOrEqual(t, wait, retryBase/2)
}

func TestBackoff_OpenTurnsFlushesAwayUntilItElapses(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	t0 := time.Unix(100, 0)
	wait, first, _ := b.fail(t0)
	require.True(t, first)

	// Inside the window: turned away for what is left of it.
	got, ok := b.allow(t0.Add(100 * time.Millisecond))
	assert.False(t, ok)
	assert.Equal(t, wait-100*time.Millisecond, got)

	// A failure reported late, from a flush that started before the first
	// one reported, does not escalate the running backoff.
	late, lateFirst, lateLog := b.fail(t0.Add(200 * time.Millisecond))
	assert.False(t, lateFirst)
	assert.False(t, lateLog)
	assert.Equal(t, wait-200*time.Millisecond, late)
	assert.Equal(t, 1, b.failures)

	// Elapsed: one probe goes; the rest wait for its answer.
	after := t0.Add(wait)
	_, ok = b.allow(after)
	assert.True(t, ok, "first flush after the window is the probe")
	got, ok = b.allow(after)
	assert.False(t, ok, "a second flush waits while the probe is out")
	assert.Equal(t, retryBase/2, got, "floored, so rows do not cycle while a slow probe is out")
	got, ok = b.waiting(after)
	assert.True(t, ok, "arriving rows are handed back while the probe is out too")
	assert.Equal(t, retryBase/2, got)

	// The probe succeeds: closed, every flush tries again.
	recovered, lasted := b.succeed(after.Add(time.Second))
	assert.True(t, recovered)
	assert.Equal(t, wait+time.Second, lasted)
	_, ok = b.allow(after.Add(time.Second))
	assert.True(t, ok)
}

func TestBackoff_LogsAnOngoingOutageAtABoundedRate(t *testing.T) {
	t.Parallel()
	b := &backoff{jitter: noJitter}
	now := time.Unix(0, 0)
	var logged int
	for range 20 { // ~2.5 minutes of failed probes at the capped backoff
		_, _, log := b.fail(now)
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
	wait, _, _ := b.fail(time.Unix(0, 0))
	after := time.Unix(0, 0).Add(wait)
	_, ok := b.allow(after)
	require.True(t, ok)
	b.release()
	_, ok = b.allow(after)
	assert.True(t, ok, "the released probe can be claimed again")
}

func TestBackoffs_TableAndPoolAreSeparate(t *testing.T) {
	t.Parallel()
	var bs backoffs
	tgt := chconn.Target{URL: "http://a:8123", Username: "u", Database: "d"}
	now := time.Unix(0, 0)
	bs.forTable(tgt, "ro").fail(now)

	_, ok := bs.waiting(tgt, "ro", now)
	assert.True(t, ok, "the failing table waits")
	_, ok = bs.waiting(tgt, "healthy", now)
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
