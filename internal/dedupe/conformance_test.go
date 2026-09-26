package dedupe_test

import (
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/dedupe/dedupetest"
)

// fakeClock is a clock tests move by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestEmbedded_Conformance(t *testing.T) {
	t.Parallel()
	dedupetest.Run(t, func(t *testing.T) dedupetest.Harness {
		e := dedupe.NewEmbedded(t.TempDir())
		clock := &fakeClock{now: time.Now()}
		dedupe.SetClock(e, clock.Now)
		return dedupetest.Harness{
			Factory:         e.Tenant,
			Advance:         clock.Advance,
			FailNextReserve: func(n int) { dedupe.FailNextReserve(e, n) },
		}
	})
}

// The suite's sleeping path, which a backend without an injectable clock
// takes, on the real clock.
func TestEmbedded_ConformanceRealClock(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("sleeps past leases")
	}
	dedupetest.Run(t, func(t *testing.T) dedupetest.Harness {
		return dedupetest.Harness{Factory: dedupe.NewEmbedded(t.TempDir()).Tenant}
	})
}
