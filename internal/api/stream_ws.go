package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/observability"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// WSHandler handles GET /v1/stream/ws.
// Supports multiplexed subscriptions via in-band JSON commands:
//
//	{"action":"subscribe","topic":"ingest.clicks"}
//	{"action":"unsubscribe","topic":"ingest.clicks"}
//
// Outbound messages are wrapped in a topic envelope:
//
//	{"topic":"ingest.clicks","data":{...event...}}
//
// For backward compatibility, the ?topic= query parameter auto-subscribes
// on connect. If no ?topic= is set, the connection starts with no
// subscriptions and waits for in-band subscribe commands.
type WSHandler struct {
	Hub            *Hub
	JS             jetstream.JetStream
	PolicyStore    *policy.Store
	AllowedOrigins []string
}

func NewWSHandler(hub *Hub, js jetstream.JetStream, allowedOrigins []string) *WSHandler {
	return &WSHandler{Hub: hub, JS: js, AllowedOrigins: allowedOrigins}
}

// wsCommand represents an in-band subscribe/unsubscribe command.
type wsCommand struct {
	Action string `json:"action"`
	Topic  string `json:"topic"`
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

	// Resolve stream permissions for this request.
	role := RoleFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())

	ctx := r.Context()

	// Merged channel receives messages from all subscribed topics.
	merged := make(chan []byte, 64)

	// Track active topic subscriptions and their per-topic channels.
	var mu sync.Mutex
	subs := make(map[string]chan []byte) // topic → per-topic channel

	subscribeTopic := func(topic string) {
		mu.Lock()
		defer mu.Unlock()
		if _, exists := subs[topic]; exists {
			return 
		}
		ch := make(chan []byte, 64)
		subs[topic] = ch
		h.Hub.Subscribe(topic, ch)
		
		// Pump per-topic channel into merged channel
		go func() {
			for msg := range ch {
				select {
				case merged <- msg:
				default: 
				}
			}
		}()
	}

	unsubscribeTopic := func(topic string) {
		mu.Lock()
		ch, exists := subs[topic]
		if !exists {
			mu.Unlock()
			return
		}
		delete(subs, topic)
		mu.Unlock()
		h.Hub.Unsubscribe(topic, ch) // closes ch, which stops the pump goroutine
	}

	unsubscribeAll := func() {
		mu.Lock()
		topics := make([]string, 0, len(subs))
		for t := range subs {
			topics = append(topics, t)
		}
		mu.Unlock()
		for _, t := range topics {
			unsubscribeTopic(t)
		}
	}
	defer unsubscribeAll()

	// Backward compat: auto-subscribe if ?topic= is set.
	if topic := r.URL.Query().Get("topic"); topic != "" {
		subscribeTopic(topic)

		// Gap fill from NATS.
		if since := r.URL.Query().Get("since"); since != "" {
			if ts, parseErr := time.Parse(time.RFC3339, since); parseErr == nil && h.JS != nil {
				h.replayFromNATS(ctx, ts, topic, func(data []byte) bool {
					out := h.applyStreamPolicy(data, role, map[string]any(claims), topic)
					if out == nil {
						return true
					}
					return conn.Write(ctx, websocket.MessageText, out) == nil
				})
			}
		}
	}

	// Read loop: process in-band commands from the client.
	go func() {
		for {
			_, data, readErr := conn.Read(ctx)
			if readErr != nil {
				return 
			}
			var cmd wsCommand
			if json.Unmarshal(data, &cmd) != nil || cmd.Topic == "" {
				continue 
			}
			switch cmd.Action {
			case "subscribe":
				subscribeTopic(cmd.Topic)
			case "unsubscribe":
				unsubscribeTopic(cmd.Topic)
			}
		}
	}()

	tracer := otel.Tracer("wavehouse-api")

	// Write loop: send events from merged channel.
	for {
		select {
		case <-ctx.Done():
			return
		// Determine the event's table_name to use as the topic in the envelope.
		case data := <-merged:
			var envelope struct {
				TraceHeaders map[string]string `json:"trace_headers"`
				Payload      []byte            `json:"payload"`
			}
			if err := json.Unmarshal(data, &envelope); err != nil {
				continue 
			}

			// 2. OLD LOGIC RESTORED: We lost the topic string, so we must peek 
			// into the nested Payload JSON to extract the table_name
			var rawEvt struct {
				TableName string `json:"table_name"`
			}
			evtTopic := ""
			if json.Unmarshal(envelope.Payload, &rawEvt) == nil && rawEvt.TableName != "" {
				evtTopic = "ingest." + rawEvt.TableName
			}

			// 3. Continue with tracing and policy logic
			parentCtx := otel.GetTextMapPropagator().Extract(
				context.Background(),
				propagation.MapCarrier(envelope.TraceHeaders),
			)

			_, pushSpan := tracer.Start(parentCtx, "WS.PushEvent")

			out := h.applyStreamPolicy(envelope.Payload, role, map[string]any(claims), evtTopic)
			if out == nil {
				pushSpan.End()
				continue
			}
			
			if err := conn.Write(r.Context(), websocket.MessageText, out); err != nil {
				pushSpan.End()
				return
			}
			pushSpan.End()
		}
	}
}

// applyStreamPolicy transforms raw event data for the client, filtering columns
// based on the caller's policy permissions. Returns nil if the event should be skipped.
// The result is wrapped in a topic envelope: {"topic":"...","data":{...}}.
func (h *WSHandler) applyStreamPolicy(raw []byte, role string, claims map[string]any, topic string) []byte {
	var evt ingest.EventMessage
	if err := json.Unmarshal(raw, &evt); err != nil || evt.TableName == "" {
		if !json.Valid(raw) {
			return nil
		}
		envelope := map[string]any{"topic": topic, "data": json.RawMessage(raw)}
		data, err := json.Marshal(envelope)
		if err != nil {
			return nil
		}
		return data
	}

	if h.PolicyStore != nil {
		p := h.PolicyStore.Get()
		perms := policy.Evaluate(p, role, evt.TableName, "select", claims)
		if !perms.Allowed {
			return nil
		}
		evt.Data = filterEventColumns(evt.Data, perms)
	}

	inner := map[string]any{
		"table_name":         evt.TableName,
		"received_timestamp": evt.ReceivedTimestamp,
		"data":               evt.Data,
	}
	envelope := map[string]any{
		"topic": "ingest." + evt.TableName,
		"data":  inner,
	}
	data, err := json.Marshal(envelope)
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

	tracer := otel.Tracer("wavehouse-api")

	for {
		msg, err := cons.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			return
		}
		msgCtx := observability.ExtractNATS(context.Background(), msg)
		_, pushSpan := tracer.Start(msgCtx, "WS.ReplayEvent")

		if !send(msg.Data()) {
			pushSpan.End()
			return
		}

		pushSpan.End()
	}
}