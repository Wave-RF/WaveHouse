package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubscriber_SendDeliversThenDropsWhenFull(t *testing.T) {
	t.Parallel()
	sub := newSubscriber(1, nil) // cap-1 so the second Send has nowhere to go
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

func TestSubscriber_EvictedIsOpenUntilEvicted(t *testing.T) {
	t.Parallel()
	sub := NewSubscriber(nil, nil)

	select {
	case <-sub.Evicted():
		t.Fatal("Evicted must not fire until the subscriber is marked for eviction")
	default:
	}

	// Evicting twice is safe: the Hub's Prune and the slow-consumer threshold
	// may both reach the same subscriber.
	sub.Evict()
	sub.Evict()
	select {
	case <-sub.Evicted():
	default:
		t.Fatal("Evicted must fire once the subscriber is evicted")
	}
}

// An evicted subscriber takes no further frame, whatever room its queue has:
// its handler may still write what is queued, and a frame queued after the
// eviction could be one past events the client was never sent. What was
// queued before stays.
func TestSubscriber_SendAfterEvictIsRefused(t *testing.T) {
	t.Parallel()
	sub := newSubscriber(4, nil)
	before := Frame{Kind: KindEvent, Data: []byte("id: 1\ndata: {}\n\n")}
	assert.True(t, sub.Send(before))

	sub.Evict()
	assert.False(t, sub.Send(Frame{Kind: KindEvent, Data: []byte("id: 3\ndata: {}\n\n")}))
	assert.False(t, sub.Send(Frame{Kind: KindKeepalive, Data: []byte(":\n\n")}))

	assert.Equal(t, before, <-sub.Frames())
	select {
	case f := <-sub.Frames():
		t.Fatalf("a frame was queued after the eviction: %q", f.Data)
	default:
	}
	dropped, _, _, _ := sub.QueueStats()
	assert.Zero(t, dropped, "a refused frame is not a slow consumer's drop")
}

// The queue is the size the handler passes — the tenant's
// stream.subscriber_queue — and the package default for a caller that
// passes none.
func TestSubscriber_QueueIsTheSizeAsked(t *testing.T) {
	t.Parallel()
	_, _, _, capacity := NewTableSubscriber("clicks", nil, nil, 7).QueueStats()
	assert.Equal(t, 7, capacity)
	_, _, _, capacity = NewTableSubscriber("clicks", nil, nil, 0).QueueStats()
	assert.Equal(t, defaultSubscriberQueue, capacity, "none asked: the default")
	_, _, _, capacity = NewSubscriber(nil, nil).QueueStats()
	assert.Equal(t, defaultSubscriberQueue, capacity)
}
