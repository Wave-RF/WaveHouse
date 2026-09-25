package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
