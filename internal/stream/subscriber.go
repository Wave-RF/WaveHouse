package stream

// Subscriber is one SSE connection's outbound side: a queue of ready-to-write
// byte frames the HTTP handler drains to the client. Producers — the keepalive
// wheel today, projected-event delivery in #294 — fan frames in via Send.
type Subscriber struct {
	out    chan []byte // ready-to-write frames; cap 1 coalesces redundant keepalives
	bucket Bucket      // set on Add so Remove is O(1); nil until added
}

// NewSubscriber returns a Subscriber ready to register with a Heartbeater.
func NewSubscriber() *Subscriber {
	return &Subscriber{out: make(chan []byte, 1)}
}

// Frames is the stream of ready-to-write byte frames for the handler to write to
// the client verbatim — keepalive comments today, projected events in #294.
func (s *Subscriber) Frames() <-chan []byte {
	return s.out
}

// Send enqueues one frame without blocking, returning false if the queue is full
// (a slow consumer). The bool is #294's hook for drop metrics and slow-consumer
// eviction; keepalive callers ignore it, since a full queue already has a frame
// pending that keeps the stream alive.
func (s *Subscriber) Send(b []byte) bool {
	select {
	case s.out <- b:
		return true
	default:
		return false
	}
}
