package api

import (
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHub_SubscribeAndBroadcast(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	hub.Subscribe("clicks", ch)
	defer hub.Unsubscribe("clicks", ch)

	hub.Broadcast("clicks", ingest.EventMessage{
		TableName:         "clicks",
		ReceivedTimestamp: "2024-01-01T00:00:00Z",
		Data:              map[string]any{"page": "/home"},
	})

	select {
	case msg := <-ch:
		assert.Contains(t, string(msg), "clicks")
		assert.Contains(t, string(msg), "/home")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for broadcast")
	}
}

func TestHub_TopicIsolation(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	chClicks := make(chan []byte, 10)
	chUsers := make(chan []byte, 10)
	hub.Subscribe("clicks", chClicks)
	hub.Subscribe("users", chUsers)
	defer hub.Unsubscribe("clicks", chClicks)
	defer hub.Unsubscribe("users", chUsers)

	hub.Broadcast("clicks", ingest.EventMessage{TableName: "clicks"})

	select {
	case <-chClicks:
		// expected
	case <-time.After(time.Second):
		t.Fatal("clicks channel should have received")
	}

	select {
	case <-chUsers:
		t.Fatal("users channel should NOT have received")
	case <-time.After(50 * time.Millisecond):
		// expected — no message
	}
}

func TestHub_Unsubscribe(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	hub.Subscribe("t", ch)
	hub.Unsubscribe("t", ch)

	// After unsubscribe, the channel is closed.
	// Verify Broadcast doesn't panic and the channel is indeed closed.
	hub.Broadcast("t", ingest.EventMessage{TableName: "t"})

	_, open := <-ch
	assert.False(t, open, "channel should be closed after unsubscribe")
}

func TestHub_MultipleSubscribers(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	const n = 5
	chs := make([]chan []byte, n)
	for i := range n {
		chs[i] = make(chan []byte, 10)
		hub.Subscribe("topic", chs[i])
	}

	hub.Broadcast("topic", ingest.EventMessage{TableName: "topic"})

	for i, ch := range chs {
		select {
		case msg := <-ch:
			assert.Contains(t, string(msg), "topic", "subscriber %d", i)
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive", i)
		}
	}
}

func TestHub_SlowConsumerDropped(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	// Unbuffered channel — will always block.
	ch := make(chan []byte)
	hub.Subscribe("t", ch)
	defer hub.Unsubscribe("t", ch)

	// Broadcast should not block (drops message for slow consumer).
	done := make(chan struct{})
	go func() {
		hub.Broadcast("t", ingest.EventMessage{TableName: "t"})
		close(done)
	}()

	select {
	case <-done:
		// Good — broadcast didn't block.
	case <-time.After(time.Second):
		t.Fatal("broadcast blocked on slow consumer")
	}
}

func TestHub_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := make(chan []byte, 1)
			hub.Subscribe("t", ch)
			hub.Broadcast("t", ingest.EventMessage{TableName: "t"})
			hub.Unsubscribe("t", ch)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// passed
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock in concurrent hub operations")
	}
}

func TestHub_UnsubscribeCleansEmptyTopic(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 1)
	hub.Subscribe("clean", ch)

	// Verify internal state.
	hub.mu.RLock()
	require.Contains(t, hub.subscribers, "clean")
	hub.mu.RUnlock()

	hub.Unsubscribe("clean", ch)

	hub.mu.RLock()
	_, exists := hub.subscribers["clean"]
	hub.mu.RUnlock()
	assert.False(t, exists, "topic should be removed when last subscriber leaves")
}

func TestHub_WildcardGreaterThan(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	// Subscribe with "ingest.>" wildcard.
	ch := make(chan []byte, 10)
	hub.Subscribe("ingest.>", ch)
	defer hub.Unsubscribe("ingest.>", ch)

	hub.Broadcast("ingest.clicks", ingest.EventMessage{TableName: "clicks"})

	select {
	case msg := <-ch:
		assert.Contains(t, string(msg), "clicks")
	case <-time.After(time.Second):
		t.Fatal("wildcard subscriber should have received ingest.clicks")
	}
}

func TestHub_WildcardStar(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	hub.Subscribe("ingest.*", ch)
	defer hub.Unsubscribe("ingest.*", ch)

	hub.Broadcast("ingest.clicks", ingest.EventMessage{TableName: "clicks"})

	select {
	case msg := <-ch:
		assert.Contains(t, string(msg), "clicks")
	case <-time.After(time.Second):
		t.Fatal("star wildcard subscriber should have received")
	}
}

func TestHub_WildcardStarNoMultiToken(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	hub.Subscribe("ingest.*", ch)
	defer hub.Unsubscribe("ingest.*", ch)

	// "ingest.*" should NOT match "ingest.clicks.subpath" (star = one token).
	hub.Broadcast("ingest.clicks.subpath", ingest.EventMessage{TableName: "clicks"})

	select {
	case <-ch:
		t.Fatal("star wildcard should NOT match multi-token subject")
	case <-time.After(50 * time.Millisecond):
		// expected — no message
	}
}

func TestHub_WildcardGreaterThanMultiToken(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	hub.Subscribe("ingest.>", ch)
	defer hub.Unsubscribe("ingest.>", ch)

	// "ingest.>" should match multi-token subjects.
	hub.Broadcast("ingest.clicks.subpath", ingest.EventMessage{TableName: "clicks"})

	select {
	case msg := <-ch:
		assert.Contains(t, string(msg), "clicks")
	case <-time.After(time.Second):
		t.Fatal("> wildcard should match multi-token subjects")
	}
}

func TestHub_WildcardDoesNotMatchExact(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	hub.Subscribe("ingest.>", ch)
	defer hub.Unsubscribe("ingest.>", ch)

	// "ingest.>" should NOT match "ingest" alone (> requires 1+ tokens after).
	hub.Broadcast("ingest", ingest.EventMessage{TableName: "ingest"})

	select {
	case <-ch:
		t.Fatal("> should not match bare prefix")
	case <-time.After(50 * time.Millisecond):
		// good
	}
}

func TestHub_BareGreaterThanMatchesAll(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	hub.Subscribe(">", ch)
	defer hub.Unsubscribe(">", ch)

	hub.Broadcast("anything.here", ingest.EventMessage{TableName: "t"})

	select {
	case <-ch:
		// expected
	case <-time.After(time.Second):
		t.Fatal("bare > should match everything")
	}
}

func TestHub_WildcardNoDuplicateDelivery(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	// Subscribe with both exact and wildcard that would match.
	hub.Subscribe("ingest.clicks", ch)
	hub.Subscribe("ingest.>", ch)
	defer hub.Unsubscribe("ingest.clicks", ch)
	defer hub.Unsubscribe("ingest.>", ch)

	hub.Broadcast("ingest.clicks", ingest.EventMessage{TableName: "clicks"})

	// Should receive exactly one message, not two.
	select {
	case <-ch:
		// got first
	case <-time.After(time.Second):
		t.Fatal("should have received at least one message")
	}

	select {
	case <-ch:
		t.Fatal("should NOT receive a duplicate")
	case <-time.After(50 * time.Millisecond):
		// good
	}
}

func TestMatchTopic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern string
		subject string
		want    bool
	}{
		{"ingest.clicks", "ingest.clicks", true},
		{"ingest.clicks", "ingest.users", false},
		{"ingest.*", "ingest.clicks", true},
		{"ingest.*", "ingest.clicks.sub", false},
		{"ingest.>", "ingest.clicks", true},
		{"ingest.>", "ingest.clicks.sub", true},
		{"ingest.>", "ingest", false},
		{">", "anything", true},
		{">", "a.b.c", true},
		{"*.*", "ingest.clicks", true},
		{"*.*", "ingest", false},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.d", false},
	}
	for _, tt := range tests {
		t.Run(tt.pattern+"_vs_"+tt.subject, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, matchTopic(tt.pattern, tt.subject))
		})
	}
}
