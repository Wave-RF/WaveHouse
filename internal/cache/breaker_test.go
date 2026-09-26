package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func TestBreaker(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newBreaker(3, 5*time.Second, clock.now)
	allow := func() (bool, bool) { return b.allow() }

	ok, probe := allow()
	assert.True(t, ok)
	assert.False(t, probe)

	b.failure()
	b.failure()
	b.success() // a success resets the run
	b.failure()
	b.failure()
	assert.False(t, b.isOpen(), "two in a row is below the threshold")
	b.failure()
	assert.True(t, b.isOpen())

	ok, probe = allow()
	assert.False(t, ok, "open: skip the server")
	assert.False(t, probe, "not due for a probe yet")

	clock.t = clock.t.Add(5 * time.Second)
	ok, probe = allow()
	assert.False(t, ok)
	assert.True(t, probe, "due: this caller probes")
	ok, probe = allow()
	assert.False(t, ok)
	assert.False(t, probe, "one probe at a time")

	b.failure() // the probe failed: open for another period
	assert.True(t, b.isOpen())
	clock.t = clock.t.Add(4 * time.Second)
	_, probe = allow()
	assert.False(t, probe)
	clock.t = clock.t.Add(time.Second)
	_, probe = allow()
	assert.True(t, probe)

	b.success()
	assert.False(t, b.isOpen())
	ok, probe = allow()
	assert.True(t, ok)
	assert.False(t, probe)
}

// A reply refusing the work opens the breaker at once, and only the probe's
// success closes it: a call that set out before it opened proves nothing.
func TestBreaker_TripAndProbeSchedule(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newBreaker(100, 5*time.Second, clock.now)
	_, open := b.untilProbe()
	assert.False(t, open)

	assert.True(t, b.trip(), "it opened")
	assert.True(t, b.isOpen(), "no threshold for a refusal")
	assert.False(t, b.trip(), "already open")
	b.success()
	assert.True(t, b.isOpen(), "a success that is not the probe's leaves it open")
	d, open := b.untilProbe()
	assert.True(t, open)
	assert.Equal(t, 5*time.Second, d)

	clock.t = clock.t.Add(2 * time.Second)
	d, _ = b.untilProbe()
	assert.Equal(t, 3*time.Second, d)

	clock.t = clock.t.Add(3 * time.Second)
	_, probe := b.allow()
	require.True(t, probe)
	d, _ = b.untilProbe()
	assert.Equal(t, 5*time.Second, d, "while the probe runs, wait out a whole period")
	assert.True(t, b.trip(), "the probe was refused too")
	d, _ = b.untilProbe()
	assert.Equal(t, 5*time.Second, d)

	clock.t = clock.t.Add(5 * time.Second)
	_, probe = b.allow()
	require.True(t, probe)
	b.success()
	assert.False(t, b.isOpen())
}
