package api

import (
	"context"
	"sync"
	"time"
)

const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultHeartbeatBuckets  = 3
)

type conn struct {
	// This will buffer the heartbeat and have the handler goroutine do the write
	hbCh chan []byte
	// set is the bucket this conn was added to, recorded so Remove is O(1).
	set Bucket
}

type Bucket interface {
	Add(c *conn)
	Remove(c *conn)
	Len() int
	Push(b []byte)
}

// connSet is the concrete Bucket: a concurrency-safe set of connections.
type connSet struct {
	mu    sync.RWMutex
	conns map[*conn]struct{}
}

var _ Bucket = (*connSet)(nil)

func newConnSet() *connSet {
	return &connSet{conns: make(map[*conn]struct{})}
}

func (s *connSet) Add(c *conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[c] = struct{}{}
}

func (s *connSet) Remove(c *conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
}

func (s *connSet) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conns)
}

func (s *connSet) Push(b []byte) {
	// Snapshot under the read lock, then send outside it: a non-blocking channel
	// send is fast, but holding the lock across the whole fan-out would serialize
	// it against Add/Remove for no reason.
	s.mu.RLock()
	targets := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		targets = append(targets, c)
	}
	s.mu.RUnlock()

	for _, c := range targets {
		select {
		case c.hbCh <- b:
		default:
		}
	}
}

type Heartbeater struct {
	interval time.Duration
	comment  []byte
	buckets  []Bucket
	mu       sync.Mutex // guards hand
	hand     int
}

func NewHeartbeater(buckets int, interval time.Duration) *Heartbeater {
	if buckets < 1 {
		buckets = defaultHeartbeatBuckets
	}
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	ring := make([]Bucket, buckets)
	for i := range ring {
		ring[i] = newConnSet()
	}
	return &Heartbeater{
		interval: interval,
		comment:  []byte(": heartbeat\n\n"),
		buckets:  ring,
	}
}

// Add registers a connection in the bucket that will be pushed last, giving it a
// full rotation before its first heartbeat.
func (hb *Heartbeater) Add(c *conn) {
	hb.mu.Lock()
	// buckets[hand] fires next; the bucket just behind the hand fires last, so a
	// connection placed there waits a full period for its first heartbeat.
	last := hb.buckets[(hb.hand+len(hb.buckets)-1)%len(hb.buckets)]
	hb.mu.Unlock()

	c.set = last
	last.Add(c)
}

// Remove deregisters a connection from its bucket. Safe to call once per conn
// after Add; a no-op if the conn was never added.
func (hb *Heartbeater) Remove(c *conn) {
	if c.set != nil {
		c.set.Remove(c)
	}
}

// Run drives the wheel until ctx is cancelled, pushing a heartbeat to the front
// bucket and advancing the hand on every tick. Intended to run in its own
// goroutine for the lifetime of the server.
func (hb *Heartbeater) Run(ctx context.Context) {
	ticker := time.NewTicker(hb.interval)
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
