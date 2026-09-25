package cache

import (
	"sync"
	"time"
)

// breaker stops a failing cache server from costing every request its full
// timeout: after threshold consecutive failures it opens, and while open
// callers skip the server entirely. Once openFor has passed, one caller is
// told to probe; the probe's outcome closes the breaker or reopens it.
type breaker struct {
	threshold int
	openFor   time.Duration
	now       func() time.Time

	mu       sync.Mutex
	failures int
	open     bool
	openedAt time.Time
	probing  bool
}

func newBreaker(threshold int, openFor time.Duration, now func() time.Time) *breaker {
	return &breaker{threshold: threshold, openFor: openFor, now: now}
}

// allow reports whether a call may go to the server, and whether the caller
// should start the one probe that decides whether an open breaker closes.
func (b *breaker) allow() (ok, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return true, false
	}
	if b.probing || b.now().Sub(b.openedAt) < b.openFor {
		return false, false
	}
	b.probing = true
	return false, true
}

// success records a call the server answered, and closes the breaker.
func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures, b.open, b.probing = 0, false, false
}

// failure records a call the server did not answer in time, and opens the
// breaker at the threshold — or at once, for a failed probe.
func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.probing || b.failures >= b.threshold {
		b.open, b.openedAt, b.probing = true, b.now(), false
	}
}

func (b *breaker) isOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}
