package stream

// Subscriber is one SSE connection's handle in the streaming subsystem. The HTTP
// handler creates a Subscriber, registers it with a Heartbeater (and, in #294,
// with a delivery Bucket too), and writes whatever arrives on Frames() verbatim to
// the client. Producers — the keepalive wheel today, the projected-event delivery
// path in #294 — fan ready-to-write bytes in via Send(). The Subscriber owns its
// queue, so it's the single place delivery and backpressure (and, in #294,
// per-subscriber drop metrics) live, independent of what's producing the bytes.
type Subscriber struct {
	// out is the ready-to-write frame queue. cap 1 is right while it carries only
	// keepalives — a queued keepalive makes the next redundant, so coalescing is
	// correct — and #294 sizes it for event throughput when projected event frames
	// start flowing through the same queue.
	out chan []byte
	// bucket is the Bucket this Subscriber was added to, recorded so Remove is O(1).
	bucket Bucket
}

// NewSubscriber returns a Subscriber ready to register with a Heartbeater.
func NewSubscriber() *Subscriber {
	return &Subscriber{out: make(chan []byte, 1)}
}

// Frames is the stream of ready-to-write SSE byte frames for the handler to write
// to the client verbatim — keepalive comments today, projected event frames once
// #294 routes delivery through the same queue. Keeping the handler's write path
// payload-agnostic is what lets the keepalive and event cases converge.
func (s *Subscriber) Frames() <-chan []byte {
	return s.out
}

// Send fans one ready-to-write frame to this subscriber without blocking: it
// enqueues and reports true if there's room, or drops and reports false when the
// queue is full (a slow consumer). Callers may ignore the result (keepalive drops
// are benign — a pending frame already keeps the stream alive); the bool is the
// hook #294 uses for per-subscriber drop metrics and slow-consumer eviction.
func (s *Subscriber) Send(b []byte) bool {
	select {
	case s.out <- b:
		return true
	default:
		return false
	}
}
