package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/stream"
)

// StreamHandler handles GET /v1/stream
type StreamHandler struct {
	Hub         *stream.Hub
	Replayer    mq.Replayer // gap-fill source; nil disables replay
	Heartbeater *stream.Heartbeater
	Metrics     *stream.Metrics
	// Closing, when set, is closed as the server begins shutting down, and
	// every open stream ends at once — mid-replay too: a stream is a
	// connection to close, not in-flight work for the drain to wait on, and
	// the client reconnects and gap-fills via Last-Event-ID. A nil channel
	// never fires (a harness that serves the handler itself).
	Closing <-chan struct{}
}

func NewStreamHandler(hub *stream.Hub, replayer mq.Replayer) *StreamHandler {
	return &StreamHandler{Hub: hub, Replayer: replayer}
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
	topic := mq.Topic{Table: table, Scope: scope}

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
	// replay yields duplicates — at-least-once). The TS SDK does *not* dedupe live
	// frames, and liveQuery's single seam pass does not help here: it compares
	// live frames against the REST backfill's rows, not against each other, so a
	// replayed frame and its live twin share a received_timestamp and are kept or
	// dropped together. A consumer that cares keys on timestamp plus its own row
	// identity.
	sub := stream.NewSubscriber(claims, h.Metrics)

	// Announce the column list BEFORE registering for live events: rows travel
	// positionally, so a client that hasn't been told the columns can't read one.
	// Sending it here rather than after Add means a live event arriving during
	// setup finds the announcement already recorded and doesn't repeat it; if the
	// registry can't supply the columns yet, the event path announces them before
	// the first data frame instead.
	if f, ok := h.Hub.SubscribeSchemaFrame(table, role, sub); ok {
		n, err := w.Write(f.Data)
		if err != nil {
			return
		}
		flusher.Flush()
		h.Metrics.FrameSent(f.Kind, n)
	}

	topicKey := topic.Key()
	h.Hub.Add(topicKey, role, sub)
	defer h.Hub.Remove(topicKey, role, sub)

	// Gap fill from the MQ's retained messages (DeliverByStartTime, see
	// mq.Replayer).
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
		project := h.Hub.ReplayProjector(role, sub)
		sendReplay := func(data []byte) bool {
			// Zero frames means the event is filtered out for this role; two means
			// the column list changed and is announced before the row.
			for _, f := range project(data) {
				n, err := w.Write(f.Data)
				if err != nil {
					return false
				}
				flusher.Flush()
				h.Metrics.FrameSent(f.Kind, n)
			}
			return true
		}
		replayCtx, cancelReplay := h.replayContext(r)
		if ts, err := time.Parse(time.RFC3339Nano, sinceStr); err == nil && h.Replayer != nil {
			h.replay(replayCtx, ts, topic, sendReplay)
		} else if err != nil {
			// Fall back to RFC3339 without nanos.
			if ts, err := time.Parse(time.RFC3339, sinceStr); err == nil && h.Replayer != nil {
				h.replay(replayCtx, ts, topic, sendReplay)
			}
		}
		cancelReplay()
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
		case <-h.Closing:
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

// replayContext is the gap-fill's context: the request's, cancelled early
// when the server begins shutting down. Shutdown never cancels a request
// context itself, so without this the consumer creation — an MQ round trip
// made before the replay loop's first check — could hold the drain.
func (h *StreamHandler) replayContext(r *http.Request) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(r.Context())
	if h.Closing != nil {
		go func() {
			select {
			case <-h.Closing:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancel
}

// replay sends every message retained on topic since the given time to the
// callback until caught up or ctx is done (the client went away, or the
// server is shutting down — a long gap-fill must not hold the drain any more
// than a live stream would). A replay that cannot start, or that fails before
// catching up, is not fatal to the stream — the client still gets live events
// from here on — so the error is logged rather than ending the connection. A
// done ctx is the connection ending, not a failure, and is not logged.
func (h *StreamHandler) replay(ctx context.Context, since time.Time, topic mq.Topic, send func([]byte) bool) {
	if err := h.Replayer.ReplaySince(ctx, topic, since, send); err != nil && ctx.Err() == nil {
		slog.Default().WarnContext(ctx, "gap-fill replay ended early; the client continues with live events only",
			"component", "stream", "table", topic.Table, "scope", topic.Scope, "since", since, "error", err)
	}
}
