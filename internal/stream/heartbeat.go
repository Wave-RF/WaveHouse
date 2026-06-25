package stream

import (
	"context"
	"sync"
	"time"
)

const (
	// defaultKeepaliveInterval is the effective per-connection keepalive period —
	// the longest a quiet stream goes without a write before the wheel nudges it.
	// 30s sits safely under the common 55–60s proxy/LB idle timeouts (nginx and
	// ingress-nginx proxy_read_timeout, AWS ALB, Heroku) with ~2× margin.
	defaultKeepaliveInterval = 30 * time.Second
	defaultKeepaliveBuckets  = 3
)

// Heartbeater drives keepalives for every live subscriber from a single timer. It
// is a timing wheel: subscribers are spread across a ring of buckets and one
// bucket is pushed a keepalive per tick, so a subscriber is nudged once every
// effective period (interval) while only ~1/buckets of subscribers are written on
// any given tick — the keepalive write load is spread out instead of hitting every
// connection at the same instant.
type Heartbeater struct {
	tickInterval time.Duration // ticker period; exactly one bucket fires per tick
	comment      []byte
	buckets      []Bucket
	mu           sync.Mutex // guards hand
	hand         int
}

// NewHeartbeater builds the keepalive wheel. period is the effective
// per-connection keepalive interval (stream.keepalive_interval): every subscriber
// is written at most once per period. buckets spreads that work across the period
// so the wheel nudges ~1/buckets of subscribers per tick instead of all at once —
// the per-tick interval is period/buckets, so one full rotation spans the period.
// Non-positive inputs fall back to the package defaults (config validation already
// rejects negatives, so 0 — meaning "unset" — is the only live fallback path).
func NewHeartbeater(period time.Duration, buckets int) *Heartbeater {
	if buckets < 1 {
		buckets = defaultKeepaliveBuckets
	}
	if period <= 0 {
		period = defaultKeepaliveInterval
	}
	tick := period / time.Duration(buckets)
	if tick <= 0 {
		// Pathological: period smaller than the bucket count (sub-nanosecond per
		// tick). Collapse to one write of the whole ring per period so NewTicker
		// never panics on a zero duration.
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

// Add registers a subscriber in the bucket that will be pushed last, giving it a
// full rotation before its first keepalive.
func (hb *Heartbeater) Add(sub *Subscriber) {
	hb.mu.Lock()
	// buckets[hand] fires next; the bucket just behind the hand fires last, so a
	// subscriber placed there waits a full period for its first keepalive.
	last := hb.buckets[(hb.hand+len(hb.buckets)-1)%len(hb.buckets)]
	hb.mu.Unlock()

	sub.bucket = last
	last.Add(sub)
}

// Remove deregisters a subscriber from its bucket. Safe to call once per
// subscriber after Add; a no-op if the subscriber was never added.
func (hb *Heartbeater) Remove(sub *Subscriber) {
	if sub.bucket != nil {
		sub.bucket.Remove(sub)
	}
}

// Len reports the number of subscribers currently registered across the ring —
// i.e. live keepalive-tracked SSE streams. Useful for tests and metrics.
func (hb *Heartbeater) Len() int {
	n := 0
	for _, b := range hb.buckets {
		n += b.Len()
	}
	return n
}

// Run drives the wheel until ctx is cancelled, pushing a keepalive to the front
// bucket and advancing the hand on every tick. Intended to run in its own
// goroutine for the lifetime of the server.
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
