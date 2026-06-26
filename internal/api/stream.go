package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/chsql"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamHandler handles GET /v1/stream
type StreamHandler struct {
	Hub         *Hub
	JS          jetstream.JetStream
	PolicyStore *policy.Store
	Heartbeater *stream.Heartbeater
	Metrics     *stream.Metrics
}

func NewStreamHandler(hub *Hub, js jetstream.JetStream) *StreamHandler {
	return &StreamHandler{Hub: hub, JS: js}
}

func (h *StreamHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// CORS (including the Last-Event-ID resumption header read below) is
	// handled by the router-level corsMiddleware, not here. See NewRouter.
	// TODO: for servers or clients that don't support SSE or are having issues, should we use this path and build in the full mechanisms for a fallback, like long-polling (probably bad idea) or just periodic fetch queries, or have them fallback to structured queries or a pipe or something? Or no fallback at all?
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	table := r.URL.Query().Get("table")
	if table == "" {
		writeJSONError(w, http.StatusBadRequest, "missing required query parameter: table")
		return
	}

	// Resolve stream permissions for this request. Evaluate maps an empty role
	// to the policy default_role per event (applyStreamPolicy), so the raw role
	// from context is what we keep here.
	role := auth.RoleFromContext(r.Context())
	claims, _ := auth.ClaimsFromContext(r.Context())

	// TODO: impl scope
	scope := ""
	topic := "ingest." + chsql.SafeEncodeNATS(table)
	if scope != "" {
		topic += "." + chsql.SafeEncodeNATS(scope)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Tell nginx-class proxies not to buffer this response, so events reach the
	// client as they're flushed instead of being held until a buffer fills.
	// nginx strips X-Accel-Buffering before forwarding (the browser never sees
	// it); Caddy/Cloudflare ignore it harmlessly. Saves operators a per-location
	// `proxy_buffering off` — see docs: Behind a reverse proxy → Server-Sent Events.
	w.Header().Set("X-Accel-Buffering", "no")

	// Flush headers immediately so the client's EventSource.onopen fires right
	// away instead of waiting for the first data event.
	if _, err := fmt.Fprintf(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()

	h.Metrics.ConnOpened()
	connectedAt := time.Now()
	defer func() { h.Metrics.ConnClosed(time.Since(connectedAt)) }()

	// Subscribe for live events.
	ch := make(chan []byte, 64) // TODO: need to test how many are actually needed, as this is ~1.6KB per subscriber channel...
	h.Hub.Subscribe(topic, ch)
	defer h.Hub.Unsubscribe(topic, ch)

	// Gap fill from NATS using DeliverByStartTime.
	// Prefer Last-Event-ID header (set automatically by EventSource on reconnect)
	// over the "since" query parameter.
	// TODO: this breaks I think if we multiplex SSE? Need to test further...
	sinceStr := r.Header.Get("Last-Event-ID")
	if sinceStr == "" {
		sinceStr = r.URL.Query().Get("since")
	}
	if sinceStr != "" {
		// One send path for both timestamp formats: project, write, and count the
		// replayed frame. A write error means the client is gone, so stop the
		// gap-fill and let the deferred cleanup unwind.
		sendReplay := func(data []byte) bool {
			out := h.applyStreamPolicy(data, role, claims)
			if out == nil {
				return true // filtered for this role — skip
			}
			id := extractEventTimestamp(out)
			n, err := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", id, out)
			if err != nil {
				return false
			}
			flusher.Flush()
			h.Metrics.FrameSent(stream.KindReplay, n)
			return true
		}
		if ts, err := time.Parse(time.RFC3339Nano, sinceStr); err == nil && h.JS != nil {
			h.replayFromNATS(r.Context(), ts, topic, sendReplay)
		} else if err != nil {
			// Fall back to RFC3339 without nanos.
			if ts, err := time.Parse(time.RFC3339, sinceStr); err == nil && h.JS != nil {
				h.replayFromNATS(r.Context(), ts, topic, sendReplay)
			}
		}
	}

	// Register with the shared keepalive wheel so a quiet stream isn't idle-closed
	// by a proxy/tunnel between events.
	sub := stream.NewSubscriber()
	if h.Heartbeater != nil {
		h.Heartbeater.Add(sub)
		defer h.Heartbeater.Remove(sub)
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case raw := <-sub.Frames():
			// Pre-serialized bytes written verbatim — the generic byte-pump. Today
			// only the keepalive wheel feeds this. A write error means the client is
			// gone (also a liveness probe on idle streams).
			n, err := w.Write(raw)
			if err != nil {
				return
			}
			flusher.Flush()
			h.Metrics.FrameSent(stream.KindKeepalive, n)
		case data := <-ch:
			// Live hub event: unmarshal, policy-project, and serialize per subscriber.
			out := h.applyStreamPolicy(data, role, claims)
			if out == nil {
				continue
			}
			id := extractEventTimestamp(out)
			n, err := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", id, out)
			if err != nil {
				return
			}
			flusher.Flush()
			h.Metrics.FrameSent(stream.KindEvent, n)
		}
	}
}

// extractEventTimestamp extracts the received_timestamp field from a JSON event
// payload for use as the SSE id: field. Returns empty string on failure.
func extractEventTimestamp(data []byte) string {
	var envelope struct {
		ReceivedTimestamp string `json:"received_timestamp"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil && envelope.ReceivedTimestamp != "" {
		return envelope.ReceivedTimestamp
	}
	return ""
}

// applyStreamPolicy transforms raw event data for the client, filtering columns
// based on the caller's policy permissions. Returns nil if the event should be skipped.
func (h *StreamHandler) applyStreamPolicy(raw []byte, role string, claims map[string]any) []byte {
	// Scope should be applied before getting here, so we ignore it here
	var evt ingest.EventMessage
	if err := json.Unmarshal(raw, &evt); err != nil || evt.TableName == "" {
		// Not an EventMessage — pass through if valid JSON, skip otherwise.
		if !json.Valid(raw) {
			return nil
		}
		return raw
	}

	// Apply policy-based column filtering.
	if h.PolicyStore != nil {
		p := h.PolicyStore.Get()
		perms := policy.Evaluate(p, role, evt.TableName, "select", claims)
		if !perms.Allowed {
			return nil // role has no access to this table
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

// replayFromNATS creates an ephemeral NATS consumer starting at the given time
// and sends all available messages to the callback until caught up.
func (h *StreamHandler) replayFromNATS(ctx context.Context, since time.Time, subject string, send func([]byte) bool) {
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
