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
	"github.com/Wave-RF/WaveHouse/internal/query"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// WSHandler handles GET /v1/stream/ws.
// Supports multiplexed subscriptions via in-band JSON commands:
//
//	{"action":"subscribe","table":"clicks"}
//	{"action":"unsubscribe","table":"clicks"}
//
// Outbound messages are wrapped in a table envelope:
//
//	{"table":"clicks","data":{...event...}}
//
// The optional ?table= query parameter auto-subscribes on connect. If no
// ?table= is set, the connection starts with no subscriptions and waits
// for in-band subscribe commands.
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
	Table  string `json:"table"`
}

func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Validate ?table= before upgrading. Empty is OK — the client can
	// still subscribe via in-band commands after connect.
	if t := r.URL.Query().Get("table"); t == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid table name")
		return
	}

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
	defer func() { _ = conn.CloseNow() }()

	// Resolve stream permissions for this request.
	role := RoleFromContext(r.Context())
	claims, _ := ClaimsFromContext(r.Context())

	ctx := r.Context()

	// Merged channel receives messages from all subscribed tables. Each message
	// carries the subscribing table name so the dispatcher doesn't need to
	// re-unmarshal the payload to label the outbound envelope — and so the
	// envelope's "table" field is correct even when the payload isn't an
	// ingest.EventMessage (e.g. raw JSON pass-through).
	merged := make(chan wsOutbound, 64)

	// Track active subscriptions by table name, with their per-table channels.
	var mu sync.Mutex
	subs := make(map[string]chan []byte) // table → per-table channel

	subscribeTable := func(table string) {
		mu.Lock()
		defer mu.Unlock()
		if _, exists := subs[table]; exists {
			return
		}
		ch := make(chan []byte, 64)
		subs[table] = ch
		h.Hub.Subscribe("ingest."+query.EncodeTable(table), ch)

		// Pump per-table channel into merged channel, tagging each message with
		// the subscribing table so the writer keeps the subscription context.
		go func() {
			for msg := range ch {
				select {
				case merged <- wsOutbound{table: table, data: msg}:
				default:
				}
			}
		}()
	}

	unsubscribeTable := func(table string) {
		mu.Lock()
		ch, exists := subs[table]
		if !exists {
			mu.Unlock()
			return
		}
		delete(subs, table)
		mu.Unlock()
		h.Hub.Unsubscribe("ingest."+query.EncodeTable(table), ch) // closes ch, which stops the pump goroutine
	}

	unsubscribeAll := func() {
		mu.Lock()
		tables := make([]string, 0, len(subs))
		for t := range subs {
			tables = append(tables, t)
		}
		mu.Unlock()
		for _, t := range tables {
			unsubscribeTable(t)
		}
	}
	defer unsubscribeAll()

	// Auto-subscribe if ?table= is set.
	if table := r.URL.Query().Get("table"); table != "" {
		subscribeTable(table)

		// Gap fill from NATS. RFC3339Nano subsumes RFC3339 (the fractional
		// component is optional), matching the SSE handler's acceptance of
		// high-precision client timestamps.
		if since := r.URL.Query().Get("since"); since != "" {
			if ts, parseErr := time.Parse(time.RFC3339Nano, since); parseErr == nil && h.JS != nil {
				h.replayFromNATS(ctx, ts, "ingest."+query.EncodeTable(table), func(data []byte) bool {
					out := h.applyStreamPolicy(data, role, map[string]any(claims), table)
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
			if json.Unmarshal(data, &cmd) != nil || cmd.Table == "" {
				continue
			}
			switch cmd.Action {
			case "subscribe":
				subscribeTable(cmd.Table)
			case "unsubscribe":
				unsubscribeTable(cmd.Table)
			}
		}
	}()

	tracer := otel.Tracer("wavehouse-api")

	// Write loop: send events from merged channel.
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-merged:
			var envelope struct {
				TraceHeaders map[string]string `json:"trace_headers"`
				Payload      []byte            `json:"payload"`
			}
			if err := json.Unmarshal(m.data, &envelope); err != nil {
				continue
			}

			parentCtx := otel.GetTextMapPropagator().Extract(
				context.Background(),
				propagation.MapCarrier(envelope.TraceHeaders),
			)

			_, pushSpan := tracer.Start(parentCtx, "WS.PushEvent")

			out := h.applyStreamPolicy(envelope.Payload, role, map[string]any(claims), m.table)
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

// wsOutbound pairs a message with the subscribing table name so the WS write
// loop can label the outbound envelope without re-unmarshalling the payload.
type wsOutbound struct {
	table string
	data  []byte
}

// applyStreamPolicy transforms raw event data for the client, filtering columns
// based on the caller's policy permissions. Returns nil if the event should be skipped.
// The result is wrapped in a table envelope: {"table":"...","data":{...}}.
func (h *WSHandler) applyStreamPolicy(raw []byte, role string, claims map[string]any, table string) []byte {
	var evt ingest.EventMessage
	if err := json.Unmarshal(raw, &evt); err != nil || evt.TableName == "" {
		if !json.Valid(raw) {
			return nil
		}
		envelope := map[string]any{"table": table, "data": json.RawMessage(raw)}
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
		"table": evt.TableName,
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
