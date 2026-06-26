package stream

// Subscriber is one SSE connection's outbound side: a queue of ready-to-write
// byte frames the HTTP handler drains to the client. Producers fan frames in
// via Send.
type Subscriber struct {
	out    chan []byte // ready-to-write frames; cap 1 coalesces redundant keepalives
	bucket Bucket      // set on Add so Remove is O(1); nil until added
}

// NewSubscriber returns a Subscriber ready to register with a Heartbeater.
func NewSubscriber() *Subscriber {
	return &Subscriber{out: make(chan []byte, 1)}
}

// Frames is the queue of ready-to-write byte frames; the handler writes whatever
// arrives here to the client verbatim.
func (s *Subscriber) Frames() <-chan []byte {
	return s.out
}

// Send enqueues one frame without blocking, returning false if the queue is full
// (a slow consumer). Keepalive callers ignore the result — a full queue already
// has a frame pending that keeps the stream alive.
func (s *Subscriber) Send(b []byte) bool {
	select {
	case s.out <- b:
		return true
	default:
		return false
	}
}
