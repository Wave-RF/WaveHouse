package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamHandler handles GET /v1/stream
type StreamHandler struct {
	Hub         *stream.Hub
	JS          jetstream.JetStream
	Heartbeater *stream.Heartbeater
	Metrics     *stream.Metrics
}

func NewStreamHandler(hub *stream.Hub, js jetstream.JetStream) *StreamHandler {
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

	// Resolve stream permissions for this request. The raw role from context is the
	// bucket key: the Hub serializes the column projection once per (topic, role),
	// since column visibility derives only from the role+table policy entry. Claims
	// are separate — they drive the row-level-security filter, which the Hub evaluates
	// per subscriber (see stream.Hub), so they ride on the Subscriber rather than the
	// bucket key. Evaluate maps an empty role to the policy default_role.
	role := auth.RoleFromContext(r.Context())
	claims, _ := auth.ClaimsFromContext(r.Context())

	// TODO: impl scope
	scope := ""
	topic := "ingest." + query.SafeEncodeNATS(table)
	if scope != "" {
		topic += "." + query.SafeEncodeNATS(scope)
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

	// Register for live events before gap-fill so events arriving during replay
	// buffer in the subscriber queue instead of being missed (an overlap with
	// replay yields duplicates, deduped client-side by id: — at-least-once).
	sub := stream.NewSubscriber(claims, h.Metrics)
	h.Hub.Add(topic, role, sub)
	defer h.Hub.Remove(topic, role, sub)

	// Gap fill from NATS using DeliverByStartTime.
	// Prefer Last-Event-ID header (set automatically by EventSource on reconnect)
	// over the "since" query parameter.
	// TODO: this breaks I think if we multiplex SSE? Need to test further...
	sinceStr := r.Header.Get("Last-Event-ID")
	if sinceStr == "" {
		sinceStr = r.URL.Query().Get("since")
	}
	if sinceStr != "" {
		// One send path for both timestamp formats: project per-connection (replay is
		// low-volume and one-time, unlike the per-role live fan-out), write, and count
		// the replayed frame. A write error means the client is gone, so stop the
		// gap-fill and let the deferred cleanup unwind.
		project := h.Hub.ReplayProjector(role, claims)
		sendReplay := func(data []byte) bool {
			f, ok := project(data)
			if !ok {
				return true // filtered for this role — skip
			}
			n, err := w.Write(f.Data)
			if err != nil {
				return false
			}
			flusher.Flush()
			h.Metrics.FrameSent(f.Kind, n)
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
	if h.Heartbeater != nil {
		h.Heartbeater.Add(sub)
		defer h.Heartbeater.Remove(sub)
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-sub.Evicted():
			// Marked for disconnection (slow consumer). The client reconnects and
			// gap-fills via Last-Event-ID. Inert until the slow-consumer follow-up.
			return
		case f := <-sub.Frames():
			// One byte-pump for every frame kind: keepalive comments from the wheel and
			// projected event frames from the Hub, already serialized. A write error
			// means the client is gone (also a liveness probe on idle streams).
			n, err := w.Write(f.Data)
			if err != nil {
				return
			}
			flusher.Flush()
			h.Metrics.FrameSent(f.Kind, n)
		}
	}
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
