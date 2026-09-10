package stream

import "sync"

// Bucket is a concurrency-safe set of subscribers. Push fans one Frame out to
// every member fire-and-forget — the keepalive wheel is its only caller now
// that both Hub paths iterate Snapshot, since the schema announcement is per
// connection even where the projection is shared per role (Send itself counts
// any queue-full drop); Snapshot exposes the members so the event Hub can
// evaluate row visibility per subscriber before sending (and, later, evict).
type Bucket interface {
	Add(sub *Subscriber)
	Remove(sub *Subscriber)
	Len() int
	Push(f Frame)
	Snapshot() []*Subscriber
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

// Snapshot copies the current members under the read lock so callers can fan out
// without holding it — a subscriber removed mid-fan-out still has a live buffered
// queue, so its Send just buffers-or-drops; no panic.
func (s *subscriberSet) Snapshot() []*Subscriber {
	s.mu.RLock()
	defer s.mu.RUnlock()
	targets := make([]*Subscriber, 0, len(s.subs))
	for sub := range s.subs {
		targets = append(targets, sub)
	}
	return targets
}

// Push fans one frame out to every member fire-and-forget. Snapshot-then-Send so
// the fan-out never holds the lock against Add/Remove.
func (s *subscriberSet) Push(f Frame) {
	for _, sub := range s.Snapshot() {
		sub.Send(f)
	}
}
