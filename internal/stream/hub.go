package stream

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// Hub fans live events out to SSE subscribers, projecting and serializing each
// event ONCE per (topic, role) instead of once per subscriber — the #294 lever.
// Subscribers register under (topic, role); Broadcast decodes the event once,
// applies each subscribed role's column policy once, builds one SSE frame per
// role, and fans it to every member of that role's Bucket.
//
// The (topic, role) key is sufficient because the live path's only per-subscriber
// transform is column filtering, which depends solely on the role+table policy
// entry — never on JWT claims (claims feed only row-level WHERE/CHECK, which the
// stream path does not apply). See projectData.
type Hub struct {
	mu     sync.RWMutex
	topics map[string]*topicRoutes
	policy *policy.Store // nil ⇒ policy filtering not configured (legacy passthrough)
	metric *Metrics      // nil-safe
}

// topicRoutes holds the per-role buckets subscribed to one topic.
type topicRoutes struct {
	roles map[string]Bucket // role -> Bucket of Subscribers
}

// NewHub builds an event hub. A nil policy store passes every event through
// unfiltered (the unwired-tests case); a non-nil store whose Get returns nil is a
// total lockout (a deleted/absent policy denies everyone). metric may be nil.
func NewHub(policyStore *policy.Store, metric *Metrics) *Hub {
	return &Hub{topics: make(map[string]*topicRoutes), policy: policyStore, metric: metric}
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

// Broadcast projects raw — a published EventMessage JSON delivered on topic —
// once per subscribed role and fans the finished SSE frame to that role's bucket.
// The expensive work (decode, evaluate, marshal) happens once per distinct role,
// not once per subscriber.
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
	decoded := json.Unmarshal(raw, &evt) == nil && evt.TableName != ""
	p, filter := h.snapshotPolicy()

	for _, rb := range roleBuckets {
		wire, ok := project(p, filter, rb.role, &evt, raw, decoded)
		if !ok {
			continue // denied table / invalid payload for this role
		}
		frame := Frame{Kind: KindEvent, Data: wire}
		for _, sub := range rb.bucket.Snapshot() {
			if !sub.Send(frame) {
				h.metric.FrameDropped(KindEvent)
			}
		}
	}
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

// ReplayFrame projects a single gap-fill event for one role into a ready-to-write
// replay frame, or ok=false to skip it (denied table / invalid payload). It is a
// method so replay shares the Hub's policy store with the live fan-out — the
// handler can't accidentally project replay against a different (or nil) policy.
// The per-connection replay path uses this; the live path uses Broadcast, which
// projects once per role instead of once per connection.
func (h *Hub) ReplayFrame(role string, raw []byte) (Frame, bool) {
	var evt ingest.EventMessage
	decoded := json.Unmarshal(raw, &evt) == nil && evt.TableName != ""
	p, filter := h.snapshotPolicy()
	wire, ok := project(p, filter, role, &evt, raw, decoded)
	if !ok {
		return Frame{}, false
	}
	return Frame{Kind: KindReplay, Data: wire}, true
}

// project applies role/table column policy to a decoded EventMessage (or passes a
// non-EventMessage JSON payload through untouched), returning the SSE wire frame.
// ok=false means skip: the role can't read the table, or the payload is unusable.
//
// claims are intentionally not consulted: column visibility derives only from the
// role+table policy entry (AllowColumns/DenyColumns), so the projection is
// byte-identical for every subscriber of a (role, table) regardless of claims —
// which is exactly what lets one serialization serve the whole bucket. If row-level
// filtering is ever added to the stream path, this key (and projection) must take
// claims into account.
func project(p *policy.Policy, filter bool, role string, evt *ingest.EventMessage, raw []byte, decoded bool) (wire []byte, ok bool) {
	if !decoded {
		// Without a decoded EventMessage there's no table to evaluate policy against,
		// so fail closed whenever policy is configured: a malformed-but-valid-JSON
		// payload on ingest.<table> must not bypass column filtering. Pass through
		// only when no policy store is wired at all (the legacy/test passthrough).
		if filter || !json.Valid(raw) {
			return nil, false
		}
		return wireFrame("", raw), true
	}

	data := evt.Data
	if filter {
		perms := policy.Evaluate(p, role, evt.TableName, "select", nil)
		if !perms.Allowed {
			return nil, false // role has no access to this table
		}
		data = filterColumns(evt.Data, perms)
	}

	payload, err := json.Marshal(map[string]any{
		"table_name":         evt.TableName,
		"received_timestamp": evt.ReceivedTimestamp,
		"data":               data,
	})
	if err != nil {
		return nil, false
	}
	return wireFrame(evt.ReceivedTimestamp, payload), true
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
