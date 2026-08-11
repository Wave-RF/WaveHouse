package stream

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// Hub fans live events out to SSE subscribers. Column projection is serialized ONCE
// per (topic, role) instead of once per subscriber — the #294/#353 lever — because
// column visibility depends solely on the role+table policy entry, never on JWT
// claims, so the projected frame is identical for every subscriber of a role.
//
// Row-level security is the exception: a role's row-filter (RLS predicate) is
// resolved against each subscriber's claims, so two subscribers of the same role can
// be entitled to different rows. For a role that carries a row-filter, Broadcast
// therefore keeps the shared column projection but evaluates row visibility PER
// subscriber (ResolvedPermissions.RowVisible) before delivering — closing the
// query/stream RLS drift in #319. Roles without a row-filter keep the pure
// once-per-role fast path unchanged. See projectColumns.
type Hub struct {
	mu       sync.RWMutex
	topics   map[string]*topicRoutes
	policy   *policy.Store             // nil ⇒ policy filtering not configured (legacy passthrough)
	registry *discovery.SchemaRegistry // nil ⇒ no column types; row-filter comparison degrades fail-closed (see columnKinds)
	metric   *Metrics                  // nil-safe
}

// topicRoutes holds the per-role buckets subscribed to one topic.
type topicRoutes struct {
	roles map[string]Bucket // role -> Bucket of Subscribers
}

// NewHub builds an event hub. A nil policy store passes every event through
// unfiltered (the unwired-tests case); a non-nil store whose Get returns nil is a
// total lockout (a deleted/absent policy denies everyone). A nil registry leaves
// every column's type unknown, so row-filter comparison degrades FAIL-CLOSED:
// equality/set predicates admit only a byte-identical value and ordering/!= admit
// nothing (see policy.ColumnKind); metric may be nil.
func NewHub(policyStore *policy.Store, registry *discovery.SchemaRegistry, metric *Metrics) *Hub {
	return &Hub{topics: make(map[string]*topicRoutes), policy: policyStore, registry: registry, metric: metric}
}

// Add registers sub to receive events for (topic, role), creating the role bucket
// (and topic) on first use.
func (h *Hub) Add(topic, role string, sub *Subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	tr := h.topics[topic]
	if tr == nil {
		tr = &topicRoutes{roles: make(map[string]Bucket)}
		h.topics[topic] = tr
	}
	b := tr.roles[role]
	if b == nil {
		b = newSubscriberSet()
		tr.roles[role] = b
	}
	b.Add(sub)
}

// Remove deregisters sub from (topic, role), garbage-collecting the bucket and the
// topic once empty. A no-op if the registration is already gone, so the handler's
// deferred Remove is always safe.
func (h *Hub) Remove(topic, role string, sub *Subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	tr := h.topics[topic]
	if tr == nil {
		return
	}
	b := tr.roles[role]
	if b == nil {
		return
	}
	b.Remove(sub)
	if b.Len() == 0 {
		delete(tr.roles, role)
	}
	if len(tr.roles) == 0 {
		delete(h.topics, topic)
	}
}

// Len reports the live subscriber count across one topic (for tests and metrics).
func (h *Hub) Len(topic string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	tr := h.topics[topic]
	if tr == nil {
		return 0
	}
	n := 0
	for _, b := range tr.roles {
		n += b.Len()
	}
	return n
}

// roleBucket pairs a subscribed role with its bucket for the lock-free fan-out.
type roleBucket struct {
	role   string
	bucket Bucket
}

// Broadcast projects raw — a published EventMessage JSON delivered on topic — and
// fans the finished SSE frame to each subscribed role's bucket. The column
// projection (decode, evaluate, marshal) happens once per distinct role. For a role
// that carries a row-level-security filter, that shared frame is still delivered only
// to the subscribers whose claims admit this row (evaluated per subscriber); a role
// without a filter takes the pure once-per-role fast path.
func (h *Hub) Broadcast(topic string, raw []byte) {
	// Snapshot the role->bucket set under the read lock; do the unmarshal / project
	// / serialize outside it. Skip the decode entirely when nobody is listening.
	h.mu.RLock()
	tr := h.topics[topic]
	var roleBuckets []roleBucket
	if tr != nil {
		roleBuckets = make([]roleBucket, 0, len(tr.roles))
		for role, b := range tr.roles {
			roleBuckets = append(roleBuckets, roleBucket{role: role, bucket: b})
		}
	}
	h.mu.RUnlock()
	if len(roleBuckets) == 0 {
		return
	}

	var evt ingest.EventMessage
	decoded := decodeEvent(raw, &evt)
	p, filter := h.snapshotPolicy()

	// Column kinds for type-aware row-filter comparison — resolved lazily at most
	// once per event, only when some role actually carries a row-filter, and reused
	// across every filtered role and subscriber.
	var colKinds map[string]policy.ColumnKind
	kindsResolved := false

	for _, rb := range roleBuckets {
		wire, perms, ok := projectColumns(p, filter, rb.role, &evt, raw, decoded)
		if !ok {
			continue // denied table / invalid payload for this role
		}
		frame := Frame{Kind: KindEvent, Data: wire}

		if !perms.HasRowFilter() {
			// No row-level security for this role: one projection serves the whole
			// bucket, regardless of per-subscriber claims (the #294/#353 fast path).
			for _, sub := range rb.bucket.Snapshot() {
				if !sub.Send(frame) {
					h.metric.FrameDropped(KindEvent)
				}
			}
			continue
		}

		// Row-level security applies. The column projection is claims-independent and
		// shared, but whether each subscriber may see THIS row depends on its claims, so
		// evaluate visibility per subscriber. Predicates read the full event data (a
		// filter may key on a column the role can't SELECT), not the projected columns.
		if !kindsResolved {
			colKinds = h.columnKinds(evt.TableName)
			kindsResolved = true
		}
		for _, sub := range rb.bucket.Snapshot() {
			subPerms := policy.Evaluate(p, rb.role, evt.TableName, "select", sub.claims)
			if !subPerms.RowVisible(evt.Data, colKinds) {
				h.metric.RowWithheld(evt.TableName, rb.role)
				continue // this row is filtered out for this subscriber
			}
			if !sub.Send(frame) {
				h.metric.FrameDropped(KindEvent)
			}
		}
	}
}

// columnKinds classifies each of the table's columns for the row-filter evaluator:
// numeric types compare numerically (9 < 100, matching ClickHouse), String compares
// bytewise (exactly ClickHouse's String collation), and any other type is omitted —
// policy.ColumnOpaque, the map's zero value — admitting byte-equality only. nil when
// no schema is available (unknown table, or a Hub built without a registry), which
// reads as every column Opaque: the fail-closed floor, never a lexicographic
// fallback that could admit rows the query path excludes ("9" > "100" as text).
func (h *Hub) columnKinds(table string) map[string]policy.ColumnKind {
	if h.registry == nil {
		return nil
	}
	schema := h.registry.Get(table)
	if schema == nil {
		return nil
	}
	m := make(map[string]policy.ColumnKind, len(schema.Columns))
	for _, c := range schema.Columns {
		switch {
		case discovery.IsNumericType(c.Type):
			m[c.Name] = policy.ColumnNumeric
		case discovery.IsStringType(c.Type):
			m[c.Name] = policy.ColumnText
		}
	}
	return m
}

// decodeEvent parses raw as a published EventMessage, reporting whether it is one
// (a decode error, trailing garbage, or a missing table name all read as "not an
// EventMessage", which projectColumns then fails closed under a policy). Numbers
// decode as json.Number — exact digit strings, not float64 — because the row-filter
// comparison must see the same value ClickHouse stores: ingest decodes with
// UseNumber and forwards the raw JSON to ClickHouse verbatim, so a bare 64-bit ID
// past 2^53 keeps its exact digits on the query path, and a lossy float64 decode
// here would collapse neighboring IDs into one value and deliver another tenant's
// row (the same reason ingest uses UseNumber). It also keeps the re-serialized wire
// frame byte-faithful for big integers.
func decodeEvent(raw []byte, evt *ingest.EventMessage) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	// !More() rejects trailing content after the object, matching the strictness of
	// the json.Unmarshal this replaces.
	return dec.Decode(evt) == nil && !dec.More() && evt.TableName != ""
}

// snapshotPolicy returns the current policy and whether filtering is configured.
// filter is false only when no store is wired (legacy passthrough); a wired store
// returning a nil policy is a deliberate lockout that Evaluate denies.
func (h *Hub) snapshotPolicy() (p *policy.Policy, filter bool) {
	if h.policy == nil {
		return nil, false
	}
	return h.policy.Get(), true
}

// ReplayProjector returns the projection function for one connection's gap-fill:
// each call projects a single replayed event for the connection's role+claims into
// a ready-to-write replay frame, or ok=false to skip it (denied table, invalid
// payload, or a row the claims aren't entitled to see). It is a Hub method so
// replay shares the Hub's policy store and schema registry with the live fan-out —
// the handler can't accidentally project replay against a different (or nil)
// policy. Replay is already per-connection, so row-level security evaluates against
// this connection's claims directly; the returned closure caches the per-table
// column-kind lookup across the replay loop (the same hoist Broadcast does per
// event), so a large Last-Event-ID gap-fill doesn't pay one registry lookup and map
// build per event. The closure is for a single goroutine — each connection makes
// its own. The live path uses Broadcast.
func (h *Hub) ReplayProjector(role string, claims map[string]any) func(raw []byte) (Frame, bool) {
	var colKinds map[string]policy.ColumnKind
	kindsFor := "" // table name colKinds was resolved for ("" ⇒ not yet resolved)
	return func(raw []byte) (Frame, bool) {
		var evt ingest.EventMessage
		decoded := decodeEvent(raw, &evt)
		p, filter := h.snapshotPolicy()
		wire, perms, ok := projectColumns(p, filter, role, &evt, raw, decoded)
		if !ok {
			return Frame{}, false
		}
		if perms.HasRowFilter() {
			// One topic ⇒ one table, so this resolves once per replay in practice; the
			// guard re-resolves if a stream ever mixes tables rather than going stale.
			if kindsFor != evt.TableName {
				colKinds = h.columnKinds(evt.TableName)
				kindsFor = evt.TableName
			}
			subPerms := policy.Evaluate(p, role, evt.TableName, "select", claims)
			if !subPerms.RowVisible(evt.Data, colKinds) {
				h.metric.RowWithheld(evt.TableName, role)
				return Frame{}, false // this row is filtered out for these claims
			}
		}
		return Frame{Kind: KindReplay, Data: wire}, true
	}
}

// projectColumns applies role/table COLUMN policy to a decoded EventMessage (or
// passes a non-EventMessage JSON payload through untouched), returning the SSE wire
// frame and the resolved permissions. Column visibility derives only from the
// role+table policy entry (AllowColumns/DenyColumns), never from claims, so this
// frame is byte-identical for every subscriber of a (role, table) — the shared
// projection that lets one serialization serve a whole bucket. Row-level security is
// NOT applied here; the caller checks perms.RowVisible per subscriber (with that
// subscriber's claims) when perms.HasRowFilter(). ok=false means skip: the role can't
// read the table, or the payload is unusable. perms is nil for the legacy no-policy
// passthrough (which has no row-filter).
func projectColumns(p *policy.Policy, filter bool, role string, evt *ingest.EventMessage, raw []byte, decoded bool) (wire []byte, perms *policy.ResolvedPermissions, ok bool) {
	if !decoded {
		// Without a decoded EventMessage there's no table to evaluate policy against,
		// so fail closed whenever policy is configured: a malformed-but-valid-JSON
		// payload on ingest.<table> must not bypass column filtering. Pass through
		// only when no policy store is wired at all (the legacy/test passthrough).
		if filter || !json.Valid(raw) {
			return nil, nil, false
		}
		return wireFrame("", raw), nil, true
	}

	data := evt.Data
	if filter {
		// Column allow/deny is claims-independent, so evaluate it once with nil claims;
		// the caller re-evaluates the row-filter per subscriber against real claims.
		perms = policy.Evaluate(p, role, evt.TableName, "select", nil)
		if !perms.Allowed {
			return nil, nil, false // role has no access to this table
		}
		data = filterColumns(evt.Data, perms)
	}

	payload, err := json.Marshal(map[string]any{
		"table_name":         evt.TableName,
		"received_timestamp": evt.ReceivedTimestamp,
		"data":               data,
	})
	if err != nil {
		return nil, nil, false
	}
	return wireFrame(evt.ReceivedTimestamp, payload), perms, true
}

// filterColumns returns a copy of data containing only columns the role may see.
// The single per-column decision is policy.IsColumnAllowed, shared with the query
// path so the two read surfaces can never drift.
func filterColumns(data map[string]any, perms *policy.ResolvedPermissions) map[string]any {
	if perms == nil || data == nil {
		return data
	}
	filtered := make(map[string]any, len(data))
	for col, val := range data {
		if perms.IsColumnAllowed(col) {
			filtered[col] = val
		}
	}
	return filtered
}

// wireFrame assembles one SSE event frame: an "id:" line carrying the event's
// received_timestamp (blank when unknown — e.g. a passthrough payload) so the
// client can resume via Last-Event-ID, then the payload as one or more "data:"
// lines, terminated by a blank line. Each newline in the payload starts a fresh
// "data:" line, per the SSE spec — a compact JSON event (the normal case) has none,
// so this stays "id: <ts>\ndata: <json>\n\n", but a multi-line passthrough payload
// can't emit a bare continuation line.
func wireFrame(id string, payload []byte) []byte {
	b := append([]byte("id: "), id...)
	for line := range bytes.SplitSeq(payload, []byte{'\n'}) {
		b = append(b, "\ndata: "...)
		b = append(b, line...)
	}
	return append(b, "\n\n"...)
}
