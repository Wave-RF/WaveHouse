package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Wave-RF/BeachHouse/internal/mq"
	"github.com/nats-io/nats.go/jetstream"
)

// SSEHandler handles GET /v1/stream/sse.
type SSEHandler struct {
	Hub *Hub
	JS  jetstream.JetStream
}

func NewSSEHandler(hub *Hub, js jetstream.JetStream) *SSEHandler {
	return &SSEHandler{Hub: hub, JS: js}
}

func (h *SSEHandler) Handle(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		topic = "ingest.events"
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Subscribe for live events.
	ch := make(chan []byte, 64)
	h.Hub.Subscribe(topic, ch)
	defer h.Hub.Unsubscribe(topic, ch)

	// Gap fill from NATS using DeliverByStartTime.
	if since := r.URL.Query().Get("since"); since != "" {
		if ts, err := time.Parse(time.RFC3339, since); err == nil && h.JS != nil {
			h.replayFromNATS(r.Context(), ts, topic, func(data []byte) bool {
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
				return true
			})
		}
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// replayFromNATS creates an ephemeral NATS consumer starting at the given time
// and sends all available messages to the callback until caught up.
func (h *SSEHandler) replayFromNATS(ctx context.Context, since time.Time, subject string, send func([]byte) bool) {
	cons, err := h.JS.CreateOrUpdateConsumer(ctx, mq.StreamName(), jetstream.ConsumerConfig{
		FilterSubject:     subject,
		DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
		OptStartTime:      &since,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 5 * time.Second,
	})
	if err != nil {
		return
	}

	for {
		msg, err := cons.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			return // No more messages or timeout — done with gap-fill.
		}
		if !send(msg.Data()) {
			return
		}
	}
}
