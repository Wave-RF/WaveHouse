package ingest

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
)

// Retry backoff for a ClickHouse that cannot take inserts. The delay doubles
// per consecutive failure from retryBase to retryCap; each is jittered down
// to half so the tables and replicas of one outage do not return in step.
const (
	retryBase = time.Second
	retryCap  = 30 * time.Second
	// outageLogEvery bounds the log lines one ongoing outage writes.
	outageLogEvery = 30 * time.Second
)

// poolKey names what one backoff covers: the ClickHouse a batch is inserted
// into and the identity it goes in as — a chconn tuple's HTTP half. Every
// table of every tenant on it backs off together, so an outage costs one
// probe per backoff, not one per table loop.
type poolKey struct {
	url, user, database string
}

func keyOf(t chconn.Target) poolKey {
	return poolKey{url: t.URL, user: t.Username, database: t.Database}
}

// backoffs holds one backoff per pool, created on first use and kept for
// the process: a key is a (URL, user, database) the settings named, so the
// set is bounded by the tuples ever configured.
type backoffs struct {
	mu sync.Mutex
	m  map[poolKey]*backoff
	// open counts the pools in an outage, so the per-row check (waiting) costs
	// one atomic load while every pool is healthy.
	open atomic.Int32
}

// waiting reports whether t's pool is inside a backoff window, and for how
// long, without claiming the probe that allow hands out once it elapses.
func (b *backoffs) waiting(t chconn.Target, now time.Time) (time.Duration, bool) {
	if b.open.Load() == 0 {
		return 0, false
	}
	return b.forTarget(t).waiting(now)
}

func (b *backoffs) forTarget(t chconn.Target) *backoff {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.m == nil {
		b.m = make(map[poolKey]*backoff)
	}
	k := keyOf(t)
	bo, ok := b.m[k]
	if !ok {
		bo = &backoff{jitter: rand.Int64N, open: &b.open}
		b.m[k] = bo
	}
	return bo
}

// backoff is a small circuit breaker over one pool. Closed (no failures):
// every flush tries. Open: flushes are turned away until the backoff
// elapses; then one flush — the probe — tries, and the rest keep waiting
// until it reports. Any answer from ClickHouse that is not an availability
// failure (a success, or a rejected row) closes it.
type backoff struct {
	mu       sync.Mutex
	failures int       // consecutive, 0 = closed
	until    time.Time // no try before this while open
	probing  bool      // a probe is out
	since    time.Time // when the outage began
	logged   time.Time // last outage log line
	jitter   func(int64) int64
	open     *atomic.Int32 // the set's outage count; nil in tests of one backoff
}

// allow reports whether a flush may try ClickHouse now; when not, wait is
// how long its rows should stay away.
func (b *backoff) allow(now time.Time) (wait time.Duration, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures == 0 {
		return 0, true
	}
	if now.Before(b.until) {
		return b.until.Sub(now) + b.spread(retryBase), false
	}
	if b.probing {
		return b.spread(retryBase), false
	}
	b.probing = true
	return 0, true
}

// waiting reports whether the backoff window is still running, and how long
// is left of it.
func (b *backoff) waiting(now time.Time) (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures == 0 || !now.Before(b.until) {
		return 0, false
	}
	return b.until.Sub(now) + b.spread(retryBase), true
}

// fail records an availability failure and returns how long the failed rows
// should stay away. Failures that land while the backoff is already running
// (flushes that started before the first one reported) do not escalate it.
// log is true when this failure should be logged: the first of an outage,
// then at most once per outageLogEvery.
func (b *backoff) fail(now time.Time) (wait time.Duration, first, log bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures > 0 && now.Before(b.until) {
		return b.until.Sub(now) + b.spread(retryBase), false, false
	}
	b.probing = false
	b.failures++
	first = b.failures == 1
	if first {
		b.since = now
		if b.open != nil {
			b.open.Add(1)
		}
	}
	d := retryCap
	if shift := b.failures - 1; shift < 5 { // 1s·2^5 already passes the cap
		d = min(retryBase<<shift, retryCap)
	}
	d = d/2 + b.spread(d/2)
	b.until = now.Add(d)
	if first || now.Sub(b.logged) >= outageLogEvery {
		b.logged = now
		log = true
	}
	return d, first, log
}

// succeed closes the backoff. recovered is true when it was open, with how
// long the outage lasted.
func (b *backoff) succeed(now time.Time) (recovered bool, lasted time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures == 0 {
		return false, 0
	}
	lasted = now.Sub(b.since)
	b.failures, b.probing, b.until = 0, false, time.Time{}
	if b.open != nil {
		b.open.Add(-1)
	}
	return true, lasted
}

// spread is a random duration in [0, d).
func (b *backoff) spread(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(b.jitter(int64(d)))
}
