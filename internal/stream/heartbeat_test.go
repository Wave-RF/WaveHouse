package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewHeartbeater(t *testing.T) {
	t.Parallel()
	// period/buckets is the per-tick interval; non-positive inputs fall back to
	// the package defaults. See NewHeartbeater.
	defaultTick := defaultKeepaliveInterval / defaultKeepaliveBuckets
	tests := []struct {
		name        string
		period      time.Duration
		buckets     int
		wantBuckets int
		wantTick    time.Duration
	}{
		{"derives tick from period and buckets", 30 * time.Second, 3, 3, 10 * time.Second},
		{"single bucket: tick equals period", 9 * time.Second, 1, 1, 9 * time.Second},
		{"sub-nanosecond period clamps ring to one bucket", 2 * time.Nanosecond, 3, 1, 2 * time.Nanosecond},
		{"zero falls back to defaults", 0, 0, defaultKeepaliveBuckets, defaultTick},
		{"negative falls back to defaults", -time.Second, -3, defaultKeepaliveBuckets, defaultTick},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hb := NewHeartbeater(tt.period, tt.buckets)
			assert.Len(t, hb.buckets, tt.wantBuckets)
			assert.Equal(t, tt.wantTick, hb.tickInterval)
		})
	}
}

func TestHeartbeater_AddPlacesSubscriberInLastToFireBucket(t *testing.T) {
	t.Parallel()
	// A long period means the ticker never fires during the test, so bucket
	// membership is deterministic.
	hb := NewHeartbeater(time.Hour, 3)
	sub := NewSubscriber(nil)
	hb.Add(sub)

	// hand starts at 0; buckets[0] fires next, so a new subscriber lands in
	// buckets[2] (fired last → a full rotation before its first keepalive).
	assert.Equal(t, 0, hb.buckets[0].Len())
	assert.Equal(t, 0, hb.buckets[1].Len())
	assert.Equal(t, 1, hb.buckets[2].Len())

	hb.Remove(sub)
	assert.Equal(t, 0, hb.buckets[2].Len(), "Remove takes the subscriber back out of its bucket")
}

func TestHeartbeater_RotatesOneBucketPerTick(t *testing.T) {
	t.Parallel()
	// A long period keeps Run's ticker quiet; we step the wheel by hand so the
	// rotation is deterministic.
	const buckets = 3
	hb := NewHeartbeater(time.Hour, buckets)

	// Place one subscriber directly in each bucket so every bucket is identifiable.
	// A roomy buffer means a (buggy) double-push would be observed, not dropped.
	subs := make([]*Subscriber, buckets)
	for i := range subs {
		subs[i] = newSubscriber(4)
		hb.buckets[i].Add(subs[i])
	}

	// Each tick pushes exactly the bucket under the hand, then advances it, so one
	// full rotation pings every subscriber exactly once — the keepalive load is
	// spread across ticks instead of hitting every connection at the same instant.
	for range buckets {
		before := make([]int, buckets)
		for i, sub := range subs {
			before[i] = len(sub.out)
		}

		hb.tick()

		gained := 0
		for i, sub := range subs {
			delta := len(sub.out) - before[i]
			assert.LessOrEqual(t, delta, 1, "bucket %d got more than one keepalive in one tick", i)
			gained += delta
		}
		assert.Equal(t, 1, gained, "exactly one bucket is pushed per tick")
	}

	for i, sub := range subs {
		assert.Equal(t, 1, len(sub.out), "bucket %d is pinged exactly once per rotation", i)
	}
}

func TestHeartbeater_PushesKeepaliveToIdleSubscriber(t *testing.T) {
	t.Parallel()
	// One bucket + tiny period: the lone subscriber is pushed on every tick.
	hb := NewHeartbeater(5*time.Millisecond, 1)
	go hb.Run(t.Context())

	sub := NewSubscriber(nil)
	hb.Add(sub)
	defer hb.Remove(sub)

	select {
	case got := <-sub.Frames():
		assert.Equal(t, Frame{Kind: KindKeepalive, Data: hb.comment}, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected a keepalive within a rotation")
	}
}

func TestHeartbeater_RunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	hb := NewHeartbeater(10*time.Millisecond, 2)
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

func TestHeartbeater_RemoveIdempotentAndWithoutAdd(t *testing.T) {
	t.Parallel()
	hb := NewHeartbeater(time.Hour, 3)

	// Never added: sub.bucket is nil, so Remove is a no-op. The stream handler's
	// deferred Remove must be safe even when Add was skipped (e.g. the wheel is
	// wired but the request bailed before registering).
	never := NewSubscriber(nil)
	assert.NotPanics(t, func() { hb.Remove(never) })
	assert.Equal(t, 0, hb.Len())

	// Added once, removed twice: the second Remove is a no-op, not a panic or a
	// negative count.
	sub := NewSubscriber(nil)
	hb.Add(sub)
	hb.Remove(sub)
	assert.NotPanics(t, func() { hb.Remove(sub) })
	assert.Equal(t, 0, hb.Len())
}

// TestHeartbeater_ConcurrentAddRemovePush_Race churns subscribers in and out of
// the wheel while it ticks, so `go test -race` exercises subscriberSet.Push's
// snapshot-then-send-outside-lock window against concurrent Add/Remove and the
// hand advancing under tick. The race detector is the assertion; a clean run
// (and a fully-drained ring) is the pass.
func TestHeartbeater_ConcurrentAddRemovePush_Race(t *testing.T) {
	t.Parallel()
	hb := NewHeartbeater(15*time.Millisecond, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hb.Run(ctx)

	const workers = 8
	const iterations = 2000
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				sub := NewSubscriber(nil)
				hb.Add(sub)
				select { // drain whatever the wheel delivered, then leave
				case <-sub.Frames():
				default:
				}
				hb.Remove(sub)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 0, hb.Len(), "every added subscriber is removed; the ring drains")
}
