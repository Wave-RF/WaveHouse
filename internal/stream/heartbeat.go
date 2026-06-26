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
	defaultKeepaliveInterval = 30 * time.Second
	defaultKeepaliveBuckets  = 3
)

// Heartbeater is a timing wheel that keepalives every live subscriber from one
// timer: subscribers spread across a ring of buckets, one bucket fired per tick,
// so each is nudged once per period while only ~1/buckets are written per tick —
// spreading the load instead of writing every connection at the same instant.
type Heartbeater struct {
	tickInterval time.Duration // one bucket fires per tick; period = tickInterval × buckets
	comment      []byte
	buckets      []Bucket
	mu           sync.Mutex // guards hand
	hand         int
}

// NewHeartbeater builds the keepalive wheel. period is the effective
// per-connection interval (stream.keepalive_interval); buckets spreads that work
// across it, so the per-tick interval is period/buckets and one rotation spans
// the period. Non-positive inputs fall back to the package defaults (config
// rejects negatives, so 0 — "unset" — is the only live fallback path).
func NewHeartbeater(period time.Duration, buckets int) *Heartbeater {
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
	ring := make([]Bucket, buckets)
	for i := range ring {
		ring[i] = newSubscriberSet()
	}
	return &Heartbeater{
		tickInterval: tick,
		comment:      []byte(":\n\n"),
		buckets:      ring,
	}
}

// Add registers sub in the bucket that fires last (just behind the hand), giving
// it a full period before its first keepalive.
func (hb *Heartbeater) Add(sub *Subscriber) {
	hb.mu.Lock()
	last := hb.buckets[(hb.hand+len(hb.buckets)-1)%len(hb.buckets)]
	hb.mu.Unlock()

	sub.bucket = last
	last.Add(sub)
}

// Remove deregisters sub from its bucket. A no-op if it was never added, so the
// handler's deferred Remove is always safe.
func (hb *Heartbeater) Remove(sub *Subscriber) {
	if sub.bucket != nil {
		sub.bucket.Remove(sub)
	}
}

// Len reports the live subscriber count across the ring (for tests and metrics).
func (hb *Heartbeater) Len() int {
	n := 0
	for _, b := range hb.buckets {
		n += b.Len()
	}
	return n
}

// Run drives the wheel until ctx is cancelled. Run it in its own goroutine for
// the lifetime of the server.
func (hb *Heartbeater) Run(ctx context.Context) {
	ticker := time.NewTicker(hb.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hb.tick()
		}
	}
}

func (hb *Heartbeater) tick() {
	hb.mu.Lock()
	front := hb.buckets[hb.hand]
	hb.hand = (hb.hand + 1) % len(hb.buckets)
	hb.mu.Unlock()

	front.Push(hb.comment)
}
