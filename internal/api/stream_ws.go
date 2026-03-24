package api

import (
	"context"
	"net/http"
	"time"

	"github.com/Wave-RF/BeachHouse/internal/mq"
	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"
)

// WSHandler handles GET /v1/stream/ws.
type WSHandler struct {
	Hub *Hub
	JS  jetstream.JetStream
}

func NewWSHandler(hub *Hub, js jetstream.JetStream) *WSHandler {
	return &WSHandler{Hub: hub, JS: js}
}

func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		topic = "ingest.events"
	}

	// Subscribe for live events.
	ch := make(chan []byte, 64)
	h.Hub.Subscribe(topic, ch)
	defer h.Hub.Unsubscribe(topic, ch)

	// Gap fill from NATS using DeliverByStartTime.
	if since := r.URL.Query().Get("since"); since != "" {
		if ts, err := time.Parse(time.RFC3339, since); err == nil && h.JS != nil {
			h.replayFromNATS(r.Context(), ts, topic, func(data []byte) bool {
				return conn.Write(r.Context(), websocket.MessageText, data) == nil
			})
		}
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			if err := conn.Write(r.Context(), websocket.MessageText, data); err != nil {
				return
			}
		}
	}
}

// replayFromNATS creates an ephemeral NATS consumer starting at the given time.
func (h *WSHandler) replayFromNATS(ctx context.Context, since time.Time, subject string, send func([]byte) bool) {
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
			return
		}
		if !send(msg.Data()) {
			return
		}
	}
}
