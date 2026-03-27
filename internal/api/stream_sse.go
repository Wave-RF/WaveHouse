package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/nats-io/nats.go/jetstream"
)

// SSEHandler handles GET /v1/stream/sse.
type SSEHandler struct {
	Hub         *Hub
	JS          jetstream.JetStream
	PolicyStore *policy.Store
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
		topic = "ingest.>"
	}

	// Resolve stream permissions for this request.
	role := RoleFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())

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
				out := h.applyStreamPolicy(data, role, claims)
				if out == nil {
					return true // skip this message
				}
				fmt.Fprintf(w, "data: %s\n\n", out)
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
			out := h.applyStreamPolicy(data, role, claims)
			if out == nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", out)
			flusher.Flush()
		}
	}
}

// applyStreamPolicy transforms raw event data for the client, filtering columns
// based on the caller's policy permissions. Returns nil if the event should be skipped.
func (h *SSEHandler) applyStreamPolicy(raw []byte, role string, claims map[string]any) []byte {
	var evt ingest.EventMessage
	if err := json.Unmarshal(raw, &evt); err != nil {
		// Not an EventMessage — pass through transformed.
		out, err := transformForClient(raw)
		if err != nil {
			return nil
		}
		return out
	}

	// Apply policy-based column filtering.
	if h.PolicyStore != nil {
		p := h.PolicyStore.Get()
		perms := policy.Evaluate(p, role, evt.TableName, "select", claims)
		if !perms.Allowed {
			return nil // role has no access to this table
		}
		filterEventColumns(evt.Data, perms)
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
