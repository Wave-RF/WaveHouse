package api

import (
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/mq"
)

// Hub manages broadcast fan-out from a single MQ subscription to N clients.
type Hub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan []byte]struct{} // topic -> set of channels
}

// NewHub creates a broadcast hub.
func NewHub() *Hub {
	return &Hub{
		subscribers: make(map[string]map[chan []byte]struct{}),
	}
}

// Subscribe registers a channel to receive messages for a topic.
func (h *Hub) Subscribe(topic string, ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subscribers[topic] == nil {
		h.subscribers[topic] = make(map[chan []byte]struct{})
	}
	h.subscribers[topic][ch] = struct{}{}
}

// Unsubscribe removes a channel from a topic and closes it.
// The caller must not send to or read from ch after calling Unsubscribe.
func (h *Hub) Unsubscribe(topic string, ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	subs, ok := h.subscribers[topic]
	if !ok {
		return
	}
	if _, found := subs[ch]; !found {
		return
	}
	delete(subs, ch)
	if len(subs) == 0 {
		delete(h.subscribers, topic)
	}
	// Only close the channel if it is no longer registered under any topic.
	for _, s := range h.subscribers {
		if _, exists := s[ch]; exists {
			return
		}
	}
	close(ch)
}

// Broadcast sends an event's raw ingest.EventMessage JSON to every subscriber of
// a topic. A slow consumer (full channel) is dropped, never blocked.
func (h *Hub) Broadcast(topic string, msg *mq.Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for ch := range h.subscribers[topic] {
		select {
		case ch <- msg.Data:
		default:
		}
	}
}
