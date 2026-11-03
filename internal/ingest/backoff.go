package ingest

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
)

// Retry backoff for a ClickHouse that cannot take inserts. The delay doubles
// per consecutive failure from retryBase to the tenant's cap
// (ingest.retry_backoff_max, Limits.RetryBackoffMax, read per flush) — on a
// pool tenants share, each window opens under the cap of the tenant whose
// flush met the failure and is cut to a smaller cap by any tenant whose rows
// meet it meanwhile (adopt); each is jittered down to half so the tables and
// replicas of one outage do not return in step.
const (
	retryBase = time.Second
	// outageLogEvery bounds the log lines one ongoing outage writes.
	outageLogEvery = 30 * time.Second
)

// poolKey names what one backoff covers: the ClickHouse a batch is inserted
// into and the identity it goes in as — a chconn tuple's HTTP half. With no
// table, every table of every tenant on it backs off together, so an outage
// costs one probe per backoff, not one per table loop. With a table, it is
// that table's own backoff, for the failures of one table
// (chconn.TableScoped) — a read-only table must not hold back, or be
// reopened by, the healthy tables beside it.
type poolKey struct {
	url, user, database, table string
}

func keyOf(t chconn.Target, table string) poolKey {
	return poolKey{url: t.URL, user: t.Username, database: t.Database, table: table}
}

// backoffs holds one backoff per pool and per (pool, table), created on
// first use and kept for the process: a key is a (URL, user, database) the
// settings named, with a table name that passed the ingest handler's schema
// check, so the set is bounded by the tuples ever configured times the tables
// ever flushed.
type backoffs struct {
	mu sync.RWMutex
	m  map[poolKey]*backoff
	// open counts the pools and tables with a failure not yet followed by a
	// success, so the per-row check (waiting) costs one atomic load, and no
	// target resolution, while none has failed. A table that fails and is
	// never written again keeps it above zero; past that, a row costs two
	// read-locked lookups.
	open atomic.Int32
}

// waiting reports whether table's rows on its pool should stay away — the
// pool or the table backing off — and for how long, without claiming the
// probe that allow hands out once a window elapses. target and ceiling
// (the rows' tenant's cap, backoff.adopt) are called only when some
// backoff is open.
func (b *backoffs) waiting(target func() chconn.Target, table string, now time.Time, ceiling func() time.Duration) (time.Duration, bool) {
	if b.open.Load() == 0 {
		return 0, false
	}
	t, c := target(), ceiling()
	if wait, ok := b.forTarget(t).waiting(now, c); ok {
		return wait, true
	}
	return b.forTable(t, table).waiting(now, c)
}

func (b *backoffs) forTarget(t chconn.Target) *backoff { return b.get(keyOf(t, "")) }

func (b *backoffs) forTable(t chconn.Target, table string) *backoff {
	return b.get(keyOf(t, table))
}

func (b *backoffs) get(k poolKey) *backoff {
	b.mu.RLock()
	bo, ok := b.m[k]
	b.mu.RUnlock()
	if ok {
		return bo
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.m == nil {
		b.m = make(map[poolKey]*backoff)
	}
	bo, ok = b.m[k]
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
	// ceiling is the window's cap: the failing tenant's, lowered by any
	// tenant that touches the window (adopt); 0 while closed.
	ceiling time.Duration
	jitter  func(int64) int64
	open    *atomic.Int32 // the set's outage count; nil in tests of one backoff
}

// adopt folds a tenant's cap into an open backoff. Tenants on one ClickHouse
// tuple share its pool's backoff: each window opens under the cap of the
// tenant whose flush met the failure (fail), and any tenant that touches it
// meanwhile — rows turned away, a flush asking to try, a failure that does
// not escalate — lowers it to its own, cutting the window to now plus that
// cap, so no tenant waits longer than its own cap and the one with the
// larger cap retries sooner than it asked. The next window opens under its
// prober's cap again, so a reload that raises a cap applies at the next
// failure, and a tenant that left stops counting. A ceiling of 0 is none
// known (a Limits the validator did not bound) and folds nothing. Called
// with b.mu held.
func (b *backoff) adopt(now time.Time, ceiling time.Duration) {
	if ceiling <= 0 {
		return
	}
	ceiling = max(ceiling, retryBase)
	if b.ceiling == 0 || ceiling < b.ceiling {
		b.ceiling = ceiling
	}
	if limit := now.Add(b.ceiling); b.until.After(limit) {
		b.until = limit
	}
}

// allow reports whether a flush may try ClickHouse now; when not, wait is
// how long its rows should stay away. ceiling is the flushing tenant's cap
// (adopt).
func (b *backoff) allow(now time.Time, ceiling time.Duration) (wait time.Duration, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures == 0 {
		return 0, true
	}
	b.adopt(now, ceiling)
	if now.Before(b.until) {
		return b.until.Sub(now) + b.spread(retryBase), false
	}
	if b.probing {
		return b.probeOut(), false
	}
	b.probing = true
	return 0, true
}

// release hands back a probe allow gave out but the caller did not use.
func (b *backoff) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
}

// probeOut is how long rows stay away while a probe is out: floored, since a
// probe to a server dropping packets can take the whole insert timeout, and a
// near-zero delay would cycle the backlog through the worker meanwhile.
func (b *backoff) probeOut() time.Duration { return retryBase/2 + b.spread(retryBase/2) }

// waiting reports whether the backoff window is still running, or its probe
// is still out, and how long rows should stay away. ceiling is the rows'
// tenant's cap (adopt).
func (b *backoff) waiting(now time.Time, ceiling time.Duration) (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures == 0 {
		return 0, false
	}
	b.adopt(now, ceiling)
	switch {
	case now.Before(b.until):
		return b.until.Sub(now) + b.spread(retryBase), true
	case b.probing:
		return b.probeOut(), true
	}
	return 0, false
}

// fail records an availability failure and returns how long the failed rows
// should stay away: the doubling delay, no longer than the failing tenant's
// cap (never under retryBase, the floor the doubling starts from), which
// adopt may lower while the window runs. Failures that land while
// the backoff is already running (flushes that started before the first one
// reported) do not escalate it. log is true when this failure should be
// logged: the first of an outage, then at most once per outageLogEvery.
func (b *backoff) fail(now time.Time, ceiling time.Duration) (wait time.Duration, first, log bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures > 0 && now.Before(b.until) {
		b.adopt(now, ceiling)
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
	b.ceiling = 0 // each window opens under the failing tenant's cap
	b.adopt(now, ceiling)
	d := max(b.ceiling, retryBase)
	if shift := b.failures - 1; shift < 30 { // past that the doubling passes any cap, and would overflow
		d = min(retryBase<<shift, d)
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
	b.failures, b.probing, b.until, b.ceiling = 0, false, time.Time{}, 0
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
