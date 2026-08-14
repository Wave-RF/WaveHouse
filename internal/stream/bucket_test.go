package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSubscriberSet_AddRemoveLen(t *testing.T) {
	t.Parallel()
	s := newSubscriberSet()
	assert.Equal(t, 0, s.Len())

	s1 := NewSubscriber(nil, nil)
	s2 := NewSubscriber(nil, nil)
	s.Add(s1)
	s.Add(s2)
	assert.Equal(t, 2, s.Len())

	s.Remove(s1)
	assert.Equal(t, 1, s.Len())
	s.Remove(s2)
	assert.Equal(t, 0, s.Len())

	// Removing an absent subscriber is a no-op.
	s.Remove(s1)
	assert.Equal(t, 0, s.Len())
}

func TestSubscriberSet_PushIsNonBlockingAndDropsWhenFull(t *testing.T) {
	t.Parallel()
	s := newSubscriberSet()
	sub := newSubscriber(1, nil) // cap-1 so the second push has nowhere to go
	s.Add(sub)
	payload := Frame{Kind: KindEvent, Data: []byte("payload")}

	// First push fills the 1-slot buffer; the second must drop rather than block.
	s.Push(payload)
	s.Push(payload)

	assert.Equal(t, payload, <-sub.out)
	select {
	case <-sub.out:
		t.Fatal("second push should have been dropped while the buffer was full")
	default:
	}
}

func TestSubscriberSet_PushAfterRemove_NoPanicNoBlock(t *testing.T) {
	t.Parallel()
	s := newSubscriberSet()
	sub := NewSubscriber(nil, nil)
	s.Add(sub)
	s.Remove(sub)
	frame := Frame{Kind: KindKeepalive, Data: []byte(":\n\n")}

	// Push after the subscriber is gone: it isn't in the snapshot, so nothing is
	// sent and nothing panics (mirrors the wheel pushing a bucket a departed
	// subscriber just left).
	assert.NotPanics(t, func() { s.Push(frame) })
	select {
	case <-sub.out:
		t.Fatal("a removed subscriber must not receive a push")
	default:
	}

	// The snapshot race: Remove lands after Push already snapshotted the
	// subscriber, so the send still targets its never-closed, reader-less queue.
	// That is bounded (the buffer fills, the rest hit default) and panic-free —
	// never a send-on-closed-channel, because the queue is never closed.
	assert.NotPanics(t, func() {
		for range 5 {
			sub.Send(frame)
		}
	})
}

func TestSubscriberSet_PushToStoppedReader_DropsAndDoesNotStallWheel(t *testing.T) {
	t.Parallel()
	s := newSubscriberSet()
	// stuck never drains, so its cap-1 buffer fills and stays full; live keeps room.
	stuck := newSubscriber(1, nil)
	live := newSubscriber(4, nil)
	s.Add(stuck)
	s.Add(live)
	frame := Frame{Kind: KindKeepalive, Data: []byte(":\n\n")}

	// Many pushes against a wedged consumer must each return promptly — one stuck
	// subscriber cannot stall the fan-out for a live peer (non-blocking sends).
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			s.Push(frame)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Push stalled on a non-draining subscriber")
	}

	assert.Equal(t, 1, len(stuck.out), "stuck subscriber buffers one and drops the rest")
	assert.GreaterOrEqual(t, len(live.out), 1, "a live subscriber still receives despite a stuck peer")
}
