package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubscriber_SendDeliversThenDropsWhenFull(t *testing.T) {
	t.Parallel()
	sub := newSubscriber(1) // cap-1 so the second Send has nowhere to go
	frame := Frame{Kind: KindKeepalive, Data: []byte(":\n\n")}

	// The cap-1 queue takes the first frame; the second has nowhere to go, so it's
	// dropped rather than blocking — Send reports which happened.
	assert.True(t, sub.Send(frame), "first Send enqueues into the empty queue")
	assert.False(t, sub.Send(frame), "second Send drops while the queue is full")

	assert.Equal(t, frame, <-sub.Frames(), "Frames yields what was Sent")
	select {
	case <-sub.Frames():
		t.Fatal("only one frame should have been enqueued")
	default:
	}

	// Draining the queue frees the slot, so the next Send enqueues again.
	assert.True(t, sub.Send(frame), "Send enqueues again once the queue drained")
}

func TestSubscriber_EvictedIsOpenUntilClosed(t *testing.T) {
	t.Parallel()
	sub := NewSubscriber()

	// The eviction seam is inert until the slow-consumer follow-up closes it: the
	// channel stays open, so a non-blocking read finds nothing.
	select {
	case <-sub.Evicted():
		t.Fatal("Evicted must not fire until the subscriber is marked for eviction")
	default:
	}
}
