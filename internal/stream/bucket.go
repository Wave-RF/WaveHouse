package stream

import "sync"

// Bucket is a concurrency-safe set of subscribers that a single Push fans one
// byte slice out to. It is the reusable fan-out primitive: the keepalive wheel
// holds a ring of them for load-spreading; #294 will hold one per (role, table)
// column-set so a projected frame is built once and Push'd to every member.
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
	// Snapshot under the read lock, then Send outside it — holding the lock across
	// the fan-out would serialize it against Add/Remove for no reason. A subscriber
	// removed mid-fan-out still has a live buffered queue, so its Send just
	// buffers-or-drops; no panic.
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
