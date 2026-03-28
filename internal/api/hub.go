package api

import (
	"encoding/json"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
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
	if subs, ok := h.subscribers[topic]; ok {
		delete(subs, ch)
		close(ch)
		if len(subs) == 0 {
			delete(h.subscribers, topic)
		}
	}
}

// Broadcast sends an event to all subscribers of a topic.
func (h *Hub) Broadcast(topic string, evt ingest.EventMessage) {
	data, err := json.Marshal(evt)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.subscribers[topic] {
		select {
		case ch <- data:
		default:
			// Drop if client is slow.
		}
	}
}
