package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConnSet_AddRemoveLen(t *testing.T) {
	t.Parallel()
	s := newConnSet()
	assert.Equal(t, 0, s.Len())

	c1 := &conn{hbCh: make(chan []byte, 1)}
	c2 := &conn{hbCh: make(chan []byte, 1)}
	s.Add(c1)
	s.Add(c2)
	assert.Equal(t, 2, s.Len())

	s.Remove(c1)
	assert.Equal(t, 1, s.Len())
	s.Remove(c2)
	assert.Equal(t, 0, s.Len())

	// Removing an absent conn is a no-op.
	s.Remove(c1)
	assert.Equal(t, 0, s.Len())
}

func TestConnSet_PushIsNonBlockingAndDropsWhenFull(t *testing.T) {
	t.Parallel()
	s := newConnSet()
	c := &conn{hbCh: make(chan []byte, 1)}
	s.Add(c)

	// First push fills the 1-slot buffer; the second must drop rather than block.
	s.Push(heartbeatComment)
	s.Push(heartbeatComment)

	assert.Equal(t, heartbeatComment, <-c.hbCh)
	select {
	case <-c.hbCh:
		t.Fatal("second push should have been dropped while the buffer was full")
	default:
	}
}

func TestNewHeartbeater_ClampsInvalidValues(t *testing.T) {
	t.Parallel()
	hb := NewHeartbeater(0, 0)
	assert.Len(t, hb.buckets, defaultHeartbeatBuckets, "non-positive buckets falls back to default")
	assert.Equal(t, defaultHeartbeatInterval, hb.interval, "non-positive interval falls back to default")

	hb = NewHeartbeater(-3, -time.Second)
	assert.Len(t, hb.buckets, defaultHeartbeatBuckets)
	assert.Equal(t, defaultHeartbeatInterval, hb.interval)
}

func TestNewHeartbeater_UsesProvidedValues(t *testing.T) {
	t.Parallel()
	// Valid (positive) values — i.e. what config passes in — are used as-is. The
	// default consts are the fallback for invalid input only, never an override:
	// these assertions would fail if the consts (5s, 3) were used instead.
	hb := NewHeartbeater(4, 10*time.Second)
	assert.Len(t, hb.buckets, 4, "bucket count must come from the provided (config) value")
	assert.Equal(t, 10*time.Second, hb.interval, "interval must come from the provided (config) value")
}

func TestHeartbeater_AddPlacesConnInLastToFireBucket(t *testing.T) {
	t.Parallel()
	// A long interval means the ticker never fires during the test, so bucket
	// membership is deterministic.
	hb := NewHeartbeater(3, time.Hour)
	c := &conn{hbCh: make(chan []byte, 1)}
	hb.Add(c)

	// hand starts at 0; buckets[0] fires next, so a new conn lands in buckets[2]
	// (fired last → a full rotation before its first heartbeat).
	assert.Equal(t, 0, hb.buckets[0].Len())
	assert.Equal(t, 0, hb.buckets[1].Len())
	assert.Equal(t, 1, hb.buckets[2].Len())

	hb.Remove(c)
	assert.Equal(t, 0, hb.buckets[2].Len(), "Remove takes the conn back out of its bucket")
}

func TestHeartbeater_PushesHeartbeatToIdleConnection(t *testing.T) {
	t.Parallel()
	// One bucket + tiny interval: the lone connection is pushed on every tick.
	hb := NewHeartbeater(1, 5*time.Millisecond)
	go hb.Run(t.Context())

	c := &conn{hbCh: make(chan []byte, 1)}
	hb.Add(c)
	defer hb.Remove(c)

	select {
	case got := <-c.hbCh:
		assert.Equal(t, heartbeatComment, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected a heartbeat within a rotation")
	}
}

func TestHeartbeater_RunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	hb := NewHeartbeater(2, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		hb.Run(ctx)
	}()

	cancel()
	select {
	case <-done:
		// Run returned promptly after cancellation.
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
