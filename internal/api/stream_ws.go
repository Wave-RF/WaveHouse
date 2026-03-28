package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"
)

// WSHandler handles GET /v1/stream/ws.
type WSHandler struct {
	Hub            *Hub
	JS             jetstream.JetStream
	PolicyStore    *policy.Store
	AllowedOrigins []string
}

func NewWSHandler(hub *Hub, js jetstream.JetStream, allowedOrigins []string) *WSHandler {
	return &WSHandler{Hub: hub, JS: js, AllowedOrigins: allowedOrigins}
}

func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {
	origins := h.AllowedOrigins
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: origins,
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		topic = "ingest.>"
	}

	// Resolve stream permissions for this request.
	role := RoleFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())

	// Subscribe for live events.
	ch := make(chan []byte, 64)
	h.Hub.Subscribe(topic, ch)
	defer h.Hub.Unsubscribe(topic, ch)

	// Gap fill from NATS using DeliverByStartTime.
	if since := r.URL.Query().Get("since"); since != "" {
		if ts, err := time.Parse(time.RFC3339, since); err == nil && h.JS != nil {
			h.replayFromNATS(r.Context(), ts, topic, func(data []byte) bool {
				out := h.applyStreamPolicy(data, role, claims)
				if out == nil {
					return true // skip this message
				}
				return conn.Write(r.Context(), websocket.MessageText, out) == nil
			})
		}
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			out := h.applyStreamPolicy(data, role, claims)
			if out == nil {
				continue
			}
			if err := conn.Write(r.Context(), websocket.MessageText, out); err != nil {
				return
			}
		}
	}
}

// applyStreamPolicy transforms raw event data for the client, filtering columns
// based on the caller's policy permissions. Returns nil if the event should be skipped.
func (h *WSHandler) applyStreamPolicy(raw []byte, role string, claims map[string]any) []byte {
	var evt ingest.EventMessage
	if err := json.Unmarshal(raw, &evt); err != nil || evt.TableName == "" {
		// Not an EventMessage — pass through if valid JSON, skip otherwise.
		if !json.Valid(raw) {
			return nil
		}
		return raw
	}

	if h.PolicyStore != nil {
		p := h.PolicyStore.Get()
		perms := policy.Evaluate(p, role, evt.TableName, "select", claims)
		if !perms.Allowed {
			return nil
		}
		evt.Data = filterEventColumns(evt.Data, perms)
	}

	out := map[string]any{
		"table_name":         evt.TableName,
		"received_timestamp": evt.ReceivedTimestamp,
		"data":               evt.Data,
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	return data
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
