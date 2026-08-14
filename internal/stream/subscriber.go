package stream

// defaultSubscriberQueue is the per-connection outbound buffer. A cap >1 (the
// keepalive-only queue was cap 1) lets live event frames queue while the handler
// is mid-write instead of being dropped on the spot; 64 matches the per-subscriber
// hub channel this replaces. A keepalive Send still coalesces harmlessly — it only
// drops when the queue is already full of events, which means the stream is
// actively writing anyway. Right-sizing this into a config knob is tracked in #152.
const defaultSubscriberQueue = 64

// Frame is one ready-to-write SSE byte frame tagged with its kind, so the handler
// can label the write (frames_sent_total{kind=…}) at the moment it happens — a
// frame dropped by a full queue is never counted as sent. Producers (the keepalive
// wheel, the event Hub) fan frames in with Send; the handler writes Data verbatim.
type Frame struct {
	Kind string // a Kind* constant: keepalive, event, or replay
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

	// evict is closed (once) to ask the owning handler to disconnect a wedged slow
	// consumer; the handler selects on Evicted() and tears the stream down, after
	// which the client reconnects and gap-fills via Last-Event-ID. The seam is wired
	// here and consumed by the handler; the policy that *closes* it (consecutive-drop
	// threshold) lands with the slow-consumer follow-up (#294 / #94).
	evict chan struct{}

	// metric counts queue-full drops inside Send itself (by frame kind), so every
	// producer — the event fan-out, replay, the keepalive wheel — is covered without
	// each call site remembering to count. Nil-safe (nil in tests).
	metric *Metrics
}

// NewSubscriber returns a Subscriber ready to register with a Heartbeater and the
// event Hub, carrying the connection's JWT claims (nil for a tokenless caller),
// deep-copied — see the claims field for the snapshot rationale — and the shared
// stream metrics (nil-safe) that Send counts drops on.
func NewSubscriber(claims map[string]any, m *Metrics) *Subscriber {
	s := newSubscriber(defaultSubscriberQueue, m)
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
	return &Subscriber{out: make(chan Frame, size), evict: make(chan struct{}), metric: m}
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
	select {
	case s.out <- f:
		return true
	default:
		s.metric.FrameDropped(f.Kind)
		return false
	}
}

// Evicted is closed when the subscriber has been marked for disconnection. The
// handler selects on it to tear the connection down. Inert until the slow-consumer
// follow-up wires the threshold that closes it.
func (s *Subscriber) Evicted() <-chan struct{} {
	return s.evict
}
