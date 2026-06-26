package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHub_SubscribeAndBroadcast(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	ch := make(chan []byte, 10)
	hub.Subscribe("clicks", ch)
	defer hub.Unsubscribe("clicks", ch)

	payload := []byte(`{"table_name":"clicks","page":"/home"}`)
	hub.Broadcast("clicks", &mq.Message{
		Ctx:     context.Background(),
		Subject: "clicks",
		Data:    payload,
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

	hub.Broadcast("clicks", &mq.Message{
		Ctx:     context.Background(),
		Subject: "clicks",
		Data:    []byte(`{"table_name":"clicks"}`),
	})

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
	hub.Broadcast("t", &mq.Message{
		Ctx:     context.Background(),
		Subject: "t",
		Data:    []byte(`{"table_name":"t"}`),
	})

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

	hub.Broadcast("topic", &mq.Message{
		Ctx:     context.Background(),
		Subject: "topic",
		Data:    []byte(`{"table_name":"topic"}`),
	})

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
		hub.Broadcast("t", &mq.Message{
			Ctx:     context.Background(),
			Subject: "t",
			Data:    []byte(`{"table_name":"t"}`),
		})
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
			hub.Broadcast("t", &mq.Message{
				Ctx:     context.Background(),
				Subject: "t",
				Data:    []byte(`{"table_name":"t"}`),
			})
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
