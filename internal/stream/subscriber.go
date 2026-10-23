package stream

import (
	"sync"
	"sync/atomic"
)

// defaultSubscriberQueue is the per-connection outbound buffer of a
// Subscriber built without a size: NewSubscriber, and a NewTableSubscriber
// handed none, which is a harness that wires no settings. The SSE handler
// sizes each connection's from its tenant's stream.subscriber_queue, whose
// seed is this value (#152; measuring the right size is still open there).
// A cap >1 (the keepalive-only queue was cap 1) lets live event frames queue
// while the handler is mid-write instead of being dropped on the spot. A
// keepalive Send still coalesces harmlessly — it only drops when the queue
// is already full of events, which means the stream is actively writing
// anyway.
const defaultSubscriberQueue = 64

// Frame is one ready-to-write SSE byte frame tagged with its kind, so the handler
// can label the write (frames_sent_total{kind=…}) at the moment it happens — a
// frame dropped by a full queue is never counted as sent. Producers (the keepalive
// wheel, the event Hub) fan frames in with Send; the handler writes Data verbatim.
type Frame struct {
	Kind string // a Kind* constant: keepalive, schema, event, or replay
	Data []byte // the exact bytes written to the client
}

// Subscriber is one SSE connection's outbound side: a queue of ready-to-write
// frames the HTTP handler drains to the client. Producers fan frames in via Send.
type Subscriber struct {
	out    chan Frame // ready-to-write frames; see defaultSubscriberQueue
	bucket Bucket     // set on Add so the keepalive wheel's Remove is O(1); nil until added

	// claims are this connection's JWT claims, evaluated against a role's row-level
	// security filter so the Hub can decide, per subscriber, whether each event row is
	// visible (see Hub.Broadcast). A private deep-copied snapshot, fixed at
	// construction — claims are a property of the authenticated connection, and there
	// is deliberately no setter — so the fan-out goroutine's unsynchronized read is
	// race-free structurally: publication happens under the bucket mutex in Hub.Add,
	// and no caller retains a reference that could mutate a nested claim (and with it
	// an authorization decision) mid-stream. A policy reload needs no claims update:
	// the Hub re-reads the policy store on every event, and claims only feed Evaluate.
	// nil ⇒ no claims (a tokenless subscriber), which fails any claim-scoped
	// row-filter closed.
	claims map[string]any

	// evict is closed, once, by Evict to ask the owning handler to end the stream;
	// the handler selects on Evicted() and tears the stream down, after which the
	// client reconnects. The Hub evicts every subscriber of a tenant no longer
	// served (Hub.Prune); disconnecting a wedged slow consumer is the other
	// closer it is meant for (#294 / #94).
	evict     chan struct{}
	evictOnce sync.Once

	// metric counts queue-full drops inside Send itself (by frame kind), so every
	// producer — the event fan-out, replay, the keepalive wheel — is covered without
	// each call site remembering to count. Nil-safe (nil in tests).
	metric *Metrics
	// table is the drop counter's table label: the streamed table when the
	// Hub knows it (Hub.LabelTable), else "".
	table string

	// full counts this connection's buffer_full drops and peak the deepest its
	// queue has been, for the handler's slow-consumer log line and the
	// queue-peak gauge; dropped signals the handler (see Dropped).
	full    atomic.Int64
	peak    atomic.Int64
	dropped chan struct{}

	// closed holds the drop reason once Close has run: a frame sent after it,
	// by a fan-out that took its snapshot before the handler deregistered, is
	// counted under that reason instead of queuing where nothing reads it.
	closed atomic.Pointer[string]

	// schemaMu guards lastSchema against the connection's own handler goroutine,
	// which reads nothing here but may run alongside the fan-out.
	//
	// It does NOT make Hub.deliver's check→send→record sequence atomic, and
	// deliver does not need it to be: the hub bridge registered in
	// internal/app calls Broadcast inline per message, on one delivery
	// goroutine per tenant (mq.Subscriber), and a connection subscribes to one
	// tenant's topic — so all of its events come from that one goroutine, and
	// no two events race to announce the same connection's columns. A future
	// change that fans one tenant's Broadcasts out across goroutines must hold
	// a lock across that whole sequence, or two events will both send an
	// announcement (harmless) while a third slips a row between a check and its
	// record (not harmless: the client zips it against the previous list).
	// Replay does not touch this field at all — it tracks drift in its own
	// closure; see Hub.ReplayProjector.
	schemaMu sync.Mutex
	// lastSchema is the signature of the column list most recently announced to
	// this connection ("" ⇒ none yet). Rows travel positionally, so a client that
	// has not been told the column list cannot read one.
	lastSchema string
}

// needsSchema reports whether sig differs from the column list last announced to
// this connection. It deliberately does NOT record: the caller records only once
// the frame is actually queued (see recordSchema), so an announcement dropped by
// a full queue leaves the connection still due one rather than silently
// unsynchronized for every row until the list next changes.
func (s *Subscriber) needsSchema(sig string) bool {
	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()
	return s.lastSchema != sig
}

// recordSchema notes that sig has been announced to this connection.
func (s *Subscriber) recordSchema(sig string) {
	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()
	s.lastSchema = sig
}

// NewSubscriber returns a Subscriber ready to register with a Heartbeater and the
// event Hub, carrying the connection's JWT claims (nil for a tokenless caller),
// deep-copied — see the claims field for the snapshot rationale — and the shared
// stream metrics (nil-safe) that Send counts drops on, with the default queue.
func NewSubscriber(claims map[string]any, m *Metrics) *Subscriber {
	return NewTableSubscriber("", claims, m, 0)
}

// NewTableSubscriber is NewSubscriber whose dropped frames carry table as
// their label — pass Hub.LabelTable's, never the request's raw value — and
// whose outbound queue holds queue frames: the tenant's
// stream.subscriber_queue, read when the connection opens; 0 or less takes
// the default.
func NewTableSubscriber(table string, claims map[string]any, m *Metrics, queue int) *Subscriber {
	if queue <= 0 {
		queue = defaultSubscriberQueue
	}
	s := newSubscriber(queue, m)
	s.table = table
	s.claims = cloneClaimsMap(claims)
	return s
}

// cloneClaimsMap deep-copies a decoded-JSON claims tree (nested objects and
// arrays; scalars — string, bool, float64, json.Number, nil — are immutable and
// shared). nil in, nil out, preserving the tokenless-subscriber signal.
func cloneClaimsMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneClaimValue(v)
	}
	return out
}

func cloneClaimValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneClaimsMap(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneClaimValue(e)
		}
		return out
	default:
		return v
	}
}

// newSubscriber builds a Subscriber with an explicit queue size — the seam tests
// use to exercise the full-queue/drop path without enqueuing 64 frames. It is
// the one place the drop-counting metric is wired, so the production
// constructor and the drop tests share it.
func newSubscriber(size int, m *Metrics) *Subscriber {
	return &Subscriber{out: make(chan Frame, size), evict: make(chan struct{}), dropped: make(chan struct{}, 1), metric: m}
}

// Frames is the queue of ready-to-write frames; the handler writes whatever
// arrives here to the client verbatim.
func (s *Subscriber) Frames() <-chan Frame {
	return s.out
}

// Send enqueues one frame without blocking, returning false if the queue is full
// (a slow consumer). The drop is counted here, labeled by the frame's kind, so no
// producer can forget to; callers may ignore the result (a keepalive that drops
// coalesces harmlessly — the full queue keeps the stream alive anyway).
func (s *Subscriber) Send(f Frame) bool {
	if s.closed.Load() != nil {
		s.drop(f.Kind)
		return false
	}
	select {
	case s.out <- f:
		s.notePeak(int64(len(s.out)))
		// Close may have drained the queue between the check above and this
		// send; draining again counts the frame rather than stranding it.
		if r := s.closed.Load(); r != nil {
			s.discard(*r)
		}
		return true
	default:
		s.drop(f.Kind)
		return false
	}
}

// drop counts one frame of kind that never reached the queue: under the
// reason Close recorded once the stream has ended, else as a buffer_full drop.
func (s *Subscriber) drop(kind string) {
	if r := s.closed.Load(); r != nil {
		s.metric.FrameDropped(s.table, kind, *r)
		return
	}
	s.metric.FrameDropped(s.table, kind, DropBufferFull)
	s.full.Add(1)
	s.notePeak(int64(cap(s.out)))
	select {
	case s.dropped <- struct{}{}:
	default:
	}
}

func (s *Subscriber) notePeak(depth int64) {
	for {
		p := s.peak.Load()
		if depth <= p || s.peak.CompareAndSwap(p, depth) {
			return
		}
	}
}

// discard empties the queue, counting each frame under reason, and reports
// how many it took.
func (s *Subscriber) discard(reason string) int {
	n := 0
	for {
		select {
		case f := <-s.out:
			s.metric.FrameDropped(s.table, f.Kind, reason)
			n++
		default:
			return n
		}
	}
}

// Close ends the subscriber's delivery once its handler has stopped writing:
// every frame still queued, and any sent from here on, is counted as dropped
// for reason (a Drop* constant). It reports how many were queued. Call it after
// deregistering from the Hub and the keepalive wheel.
func (s *Subscriber) Close(reason string) int {
	s.closed.Store(&reason)
	return s.discard(reason)
}

// Dropped receives, coalesced, after a buffer_full drop, so the handler can log
// the drops without polling.
func (s *Subscriber) Dropped() <-chan struct{} {
	return s.dropped
}

// QueueStats reports the connection's buffer_full drops so far, its queue's
// depth now, the deepest it has been, and its capacity.
func (s *Subscriber) QueueStats() (dropped int64, depth, peak, capacity int) {
	return s.full.Load(), len(s.out), int(s.peak.Load()), cap(s.out)
}

// Evict marks the subscriber for disconnection: its handler ends the stream.
// Safe to call more than once, from any goroutine.
func (s *Subscriber) Evict() {
	s.evictOnce.Do(func() { close(s.evict) })
}

// Evicted is closed once the subscriber has been marked for disconnection. The
// handler selects on it to tear the connection down.
func (s *Subscriber) Evicted() <-chan struct{} {
	return s.evict
}
