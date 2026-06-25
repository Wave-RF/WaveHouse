package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// StreamHandler handles GET /v1/stream
type StreamHandler struct {
	Hub         *Hub
	JS          jetstream.JetStream
	PolicyStore *policy.Store
	Heartbeater *stream.Heartbeater
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
		if ts, err := time.Parse(time.RFC3339Nano, sinceStr); err == nil && h.JS != nil {
			h.replayFromNATS(r.Context(), ts, topic, func(data []byte) bool {
				out := h.applyStreamPolicy(data, role, claims)
				if out == nil {
					return true // skip this message
				}
				id := extractEventTimestamp(out)
				_, _ = fmt.Fprintf(w, "id: %s\ndata: %s\n\n", id, out)
				flusher.Flush()
				return true
			})
		} else if parseErr := err; parseErr != nil {
			// Try RFC3339 without nanos as fallback.
			if ts, err := time.Parse(time.RFC3339, sinceStr); err == nil && h.JS != nil {
				h.replayFromNATS(r.Context(), ts, topic, func(data []byte) bool {
					out := h.applyStreamPolicy(data, role, claims)
					if out == nil {
						return true
					}
					id := extractEventTimestamp(out)
					_, _ = fmt.Fprintf(w, "id: %s\ndata: %s\n\n", id, out)
					flusher.Flush()
					return true
				})
			}
		}
	}

	tracer := otel.Tracer("wavehouse-api")

	// Register with the shared keepalive wheel: a single goroutine periodically
	// pushes a minimal `:` comment to this subscriber so a quiet stream isn't
	// idle-closed by a proxy/tunnel between events.
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
			// Ready-to-write bytes off the subscriber's outbound queue, written
			// verbatim — the generic byte-pump. Today the keepalive wheel is the
			// only producer (`:` comments); once #294 projects + serializes events
			// upstream they flow through this same queue and the `ch` case below
			// folds into here. A write error means the connection is already gone
			// (this doubles as a liveness probe on otherwise-quiet streams); stop.
			if _, err := w.Write(raw); err != nil {
				return
			}
			flusher.Flush()
		case data := <-ch:
			// Bespoke per-subscriber transform path: raw MQ envelopes still need
			// unmarshal + policy projection + serialization here, which is why this
			// can't yet share the byte-pump above. Collapsing it into Frames() by
			// projecting once per (role, table) upstream is the core of #294.
			var envelope struct {
				TraceHeaders map[string]string `json:"trace_headers"`
				Payload      []byte            `json:"payload"`
			}
			if err := json.Unmarshal(data, &envelope); err != nil {
				continue
			}

			parentCtx := otel.GetTextMapPropagator().Extract(
				context.Background(),
				propagation.MapCarrier(envelope.TraceHeaders),
			)

			_, pushSpan := tracer.Start(parentCtx, "SSE.PushEvent")

			out := h.applyStreamPolicy(envelope.Payload, role, claims)
			if out == nil {
				pushSpan.End()
				continue
			}
			id := extractEventTimestamp(out)
			_, _ = fmt.Fprintf(w, "id: %s\ndata: %s\n\n", id, out)
			flusher.Flush()
			pushSpan.End()
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
