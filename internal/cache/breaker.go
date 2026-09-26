package cache

import (
	"sync"
	"time"
)

// breaker stops a failing cache server from costing every request its full
// timeout: after threshold consecutive failures it opens — at once, for a
// reply refusing the work — and while open callers skip the server
// entirely. Once openFor has passed, one caller is told to probe; the
// probe's outcome closes the breaker or reopens it.
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

// success records a call the server answered. Only a probe's closes an
// open breaker: any other set out before it opened, and a server that
// answers reads may still be refusing writes.
func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.open && !b.probing {
		return
	}
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

// trip opens the breaker at once, for a reply that says the server cannot
// do the work: one is as conclusive as any number.
func (b *breaker) trip() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.open, b.openedAt, b.probing = true, b.now(), false
}

func (b *breaker) isOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

// untilProbe reports how long until an open breaker is due its probe, and
// whether it is open. While a probe runs it reports openFor: the probe ends
// by closing the breaker or by opening it afresh.
func (b *breaker) untilProbe() (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case !b.open:
		return 0, false
	case b.probing:
		return b.openFor, true
	}
	return b.openedAt.Add(b.openFor).Sub(b.now()), true
}
