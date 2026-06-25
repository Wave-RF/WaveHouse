package stream

import "sync"

// Bucket is a concurrency-safe set of subscribers that a single Push fans a
// shared byte slice out to. It is the reusable fan-out primitive: the keepalive
// wheel holds a ring of Buckets for load-spreading, and #294's delivery path will
// hold one Bucket per (role, table) column-set so a projected frame is built once
// and Push'd to every member.
type Bucket interface {
	Add(sub *Subscriber)
	Remove(sub *Subscriber)
	Len() int
	Push(b []byte)
}

// subscriberSet is the concrete Bucket: a concurrency-safe set of subscribers.
type subscriberSet struct {
	mu   sync.RWMutex
	subs map[*Subscriber]struct{}
}

var _ Bucket = (*subscriberSet)(nil)

func newSubscriberSet() *subscriberSet {
	return &subscriberSet{subs: make(map[*Subscriber]struct{})}
}

func (s *subscriberSet) Add(sub *Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[sub] = struct{}{}
}

func (s *subscriberSet) Remove(sub *Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, sub)
}

func (s *subscriberSet) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subs)
}

func (s *subscriberSet) Push(b []byte) {
	// Snapshot under the read lock, then send outside it: holding the lock across
	// the whole fan-out would serialize it against Add/Remove for no reason. A
	// subscriber removed between the snapshot and the Send still receives on its
	// never-closed, buffered queue — no panic, Send just buffers-or-drops.
	s.mu.RLock()
	targets := make([]*Subscriber, 0, len(s.subs))
	for sub := range s.subs {
		targets = append(targets, sub)
	}
	s.mu.RUnlock()

	for _, sub := range targets {
		sub.Send(b)
	}
}
