package stream

import (
	"context"
	"sync"
	"time"
)

const (
	// defaultKeepaliveInterval is the effective per-connection keepalive period:
	// the longest a quiet stream goes unwritten. 30s sits under the common 55–60s
	// proxy/LB idle timeouts (nginx, ingress-nginx, ALB, Heroku) with ~2× margin.
	// The settings directory (stream.keepalive_interval) is the operative value
	// and validation requires it >= 1; these are the library's last-resort
	// fallbacks for non-positive inputs, not compiled defaults.
	defaultKeepaliveInterval = 30 * time.Second
	defaultKeepaliveBuckets  = 3
)

// Heartbeater is a timing wheel that keepalives every live subscriber from one
// timer: subscribers spread across a ring of buckets, one bucket fired per tick,
// so each is nudged once per period while only ~1/buckets are written per tick —
// spreading the load instead of writing every connection at the same instant.
//
// The wheel is reconfigurable while it runs (Reconfigure): a settings reload
// that changes the period or bucket count rebuilds the ring in place, carrying
// every live subscriber over, and Run picks up the new tick on its next
// select. Every access to a subscriber's bucket goes through mu, so a Remove
// racing a rebuild lands in whichever ring is current — never in a discarded
// one, which would leak the subscriber into the new ring.
type Heartbeater struct {
	comment []byte

	mu           sync.Mutex // guards everything below
	tickInterval time.Duration
	buckets      []Bucket
	hand         int
	// retick wakes Run when tickInterval changes; buffered so Reconfigure
	// never blocks on a Run that is mid-tick (or not running, in tests).
	retick chan struct{}
}

// NewHeartbeater builds the keepalive wheel. period is the effective
// per-connection interval (stream.keepalive_interval); buckets spreads that work
// across it, so the per-tick interval is period/buckets and one rotation spans
// the period. Non-positive inputs fall back to the package defaults.
func NewHeartbeater(period time.Duration, buckets int) *Heartbeater {
	tick, n := wheelShape(period, buckets)
	hb := &Heartbeater{
		comment:      []byte(":\n\n"),
		tickInterval: tick,
		buckets:      newRing(n),
		retick:       make(chan struct{}, 1),
	}
	return hb
}

// wheelShape resolves the (tick, bucket count) pair for a period and bucket
// request, applying the fallbacks and the sub-nanosecond clamp.
func wheelShape(period time.Duration, buckets int) (time.Duration, int) {
	if buckets < 1 {
		buckets = defaultKeepaliveBuckets
	}
	if period <= 0 {
		period = defaultKeepaliveInterval
	}
	if period < time.Duration(buckets) {
		// Sub-nanosecond-per-tick: integer division would round the tick to zero.
		// Collapse the ring to one bucket so the effective period stays ≈ period
		// instead of ballooning to period × buckets.
		buckets = 1
	}
	tick := period / time.Duration(buckets)
	if tick <= 0 {
		// Defensive: NewTicker must never see a non-positive duration.
		tick = period
	}
	return tick, buckets
}

func newRing(n int) []Bucket {
	ring := make([]Bucket, n)
	for i := range ring {
		ring[i] = newSubscriberSet()
	}
	return ring
}

// Reconfigure applies a new period and bucket count to a running wheel. A
// call that changes nothing is a no-op; otherwise the ring is rebuilt with
// every live subscriber redistributed round-robin (the hand resets, so each
// gets up to one full new period before its next keepalive) and Run's ticker
// is reset to the new tick. Safe to call whether or not Run is running.
func (hb *Heartbeater) Reconfigure(period time.Duration, buckets int) {
	tick, n := wheelShape(period, buckets)

	hb.mu.Lock()
	defer hb.mu.Unlock()
	if tick == hb.tickInterval && n == len(hb.buckets) {
		return
	}
	hb.tickInterval = tick
	if n != len(hb.buckets) {
		ring := newRing(n)
		i := 0
		for _, old := range hb.buckets {
			for _, sub := range old.Snapshot() {
				ring[i%n].Add(sub)
				sub.bucket = ring[i%n]
				i++
			}
		}
		hb.buckets = ring
		hb.hand = 0
	}
	select {
	case hb.retick <- struct{}{}:
	default: // a wake-up is already pending; Run re-reads tickInterval when it fires
	}
}

// Add registers sub in the bucket that fires last (just behind the hand), giving
// it a full period before its first keepalive.
func (hb *Heartbeater) Add(sub *Subscriber) {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	last := hb.buckets[(hb.hand+len(hb.buckets)-1)%len(hb.buckets)]
	sub.bucket = last
	last.Add(sub)
}

// Remove deregisters sub from its bucket. A no-op if it was never added, so the
// handler's deferred Remove is always safe.
func (hb *Heartbeater) Remove(sub *Subscriber) {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	if sub.bucket != nil {
		sub.bucket.Remove(sub)
	}
}

// Len reports the live subscriber count across the ring (for tests and metrics).
func (hb *Heartbeater) Len() int {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	n := 0
	for _, b := range hb.buckets {
		n += b.Len()
	}
	return n
}

// Run drives the wheel until ctx is cancelled. Run it in its own goroutine for
// the lifetime of the server.
func (hb *Heartbeater) Run(ctx context.Context) {
	hb.mu.Lock()
	tick := hb.tickInterval
	hb.mu.Unlock()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hb.tick()
		case <-hb.retick:
			hb.mu.Lock()
			tick = hb.tickInterval
			hb.mu.Unlock()
			ticker.Reset(tick)
		}
	}
}

func (hb *Heartbeater) tick() {
	hb.mu.Lock()
	front := hb.buckets[hb.hand]
	hb.hand = (hb.hand + 1) % len(hb.buckets)
	hb.mu.Unlock()

	front.Push(Frame{Kind: KindKeepalive, Data: hb.comment})
}
