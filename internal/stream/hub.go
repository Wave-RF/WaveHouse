package stream

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
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
// subscriber (RowView.Visible over the event parsed once) before delivering —
// closing the query/stream RLS drift in #319. Roles without a row-filter keep the
// pure once-per-role fast path unchanged, and never reach the type layer at all.
// See projectColumns.
type Hub struct {
	mu       sync.RWMutex
	topics   map[string]*topicRoutes
	policy   policy.Source             // nil ⇒ policy filtering not configured (legacy passthrough)
	registry *discovery.SchemaRegistry // nil ⇒ no schema frame on subscribe; row filtering is unaffected (the type layer owns it)
	metric   *Metrics                  // nil-safe

	// RowEvaluator decides a row's visibility under a role's row-filter — the
	// one place that decision is taken, for live fan-out and replay alike.
	// Production wires NewRowEvaluator (the type layer); nil means the
	// fail-closed default below, which withholds every row of a row-filtered
	// role, because an unwired evaluator must never read as "everything
	// visible". Wired once before the Hub serves traffic and not safe to mutate
	// afterwards: rowAdmitted reads it from the consumer goroutine and from SSE
	// handler goroutines without holding h.mu. Every delivery path reaches it
	// through Prepare + rowAdmitted, never directly.
	RowEvaluator RowEvaluator

	// unwired is that fail-closed default: an engineEvaluator with no Engine,
	// which withholds and logs once. It is a field rather than a fresh value per
	// call so the "no engine" report really is once per Hub.
	unwired engineEvaluator
}

// rowEvaluator returns the Hub's RowEvaluator, or the fail-closed default when
// none is wired. Nil-safe rather than constructor-enforced, so a zero Hub still
// enforces row-level security instead of panicking past it — or, worse, reading
// an absent evaluator as an absent restriction.
func (h *Hub) rowEvaluator() RowEvaluator {
	if h.RowEvaluator != nil {
		return h.RowEvaluator
	}
	return &h.unwired
}

// topicRoutes holds the per-role buckets subscribed to one topic.
type topicRoutes struct {
	roles map[string]Bucket // role -> Bucket of Subscribers
}

// NewHub builds an event hub. A nil policy store passes every event through
// unfiltered (the unwired-tests case); a non-nil store whose Get returns nil is a
// total lockout (a deleted/absent policy denies everyone). A nil registry only
// costs the on-subscribe schema frame — row filtering reads its types from the
// type layer behind RowEvaluator, which the caller wires separately; metric may
// be nil.
func NewHub(policyStore policy.Source, registry *discovery.SchemaRegistry, metric *Metrics) *Hub {
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

	ev := newEventView(raw)
	p, filter := h.snapshotPolicy()

	// The row is parsed at most ONCE per event — only when some role actually
	// carries a row-filter — and the parsed view is reused by every filtered role
	// and subscriber. Parsing is the expensive half; evaluating a compiled
	// predicate over an already-parsed row is not.
	var view RowView
	var prepErr error
	prepared := false
	defer func() {
		if view != nil {
			view.Close()
		}
	}()

	for _, rb := range roleBuckets {
		plan, ok := planForRole(p, filter, rb.role, ev, KindEvent)
		if !ok {
			continue // denied table / unusable payload for this role
		}

		if !plan.perms.HasRowFilter() {
			// No row-level security for this role: one projection serves the whole
			// bucket, regardless of per-subscriber claims (the #294/#353 fast path).
			// The loop is the one Bucket.Push runs internally — it is here rather
			// than behind Push only because the schema frame is per CONNECTION
			// (whether this subscriber has already been told this column list),
			// while the projection work stays once per role.
			for _, sub := range rb.bucket.Snapshot() {
				deliver(sub, plan)
			}
			continue
		}

		// Row-level security applies. The column projection is claims-independent and
		// shared, but whether each subscriber may see THIS row depends on its claims, so
		// evaluate visibility per subscriber. Predicates read the full event row (a
		// filter may key on a column the role can't SELECT), not the projected columns.
		if !prepared {
			view, prepErr = h.rowEvaluator().Prepare(ev.evt.TableName, ev.evt.Columns, ev.evt.Row)
			prepared = true
		}
		for _, sub := range rb.bucket.Snapshot() {
			if h.rowAdmitted(p, rb.role, ev, sub.claims, view, prepErr) {
				deliver(sub, plan)
			}
		}
	}
}

// deliver queues one role's frames for a subscriber, sending the schema frame
// first whenever this connection has not been told this projected column list —
// on its first event, and again if the list drifts.
//
// A row is never queued without its announcement. If the queue is full when the
// announcement is offered, the row is dropped too and the signature is not
// recorded, so the next event announces again: a slow consumer loses a row
// (visible as a gap) rather than receiving one it would zip against a stale
// column list (silent mislabeling).
//
// Both drops are counted under their own frame kinds. Send does that for the
// frames it is offered, but the withheld row is never offered — so count it
// here, or a consumer losing every row would show zero event drops and only
// schema drops, and an operator alerting on the event kind would see nothing.
func deliver(sub *Subscriber, plan rolePlan) {
	if plan.announce && sub.needsSchema(plan.sig) {
		if !sub.Send(plan.schemaFrame()) {
			sub.metric.FrameDropped(plan.data.Kind)
			return
		}
		sub.recordSchema(plan.sig)
	}
	sub.Send(plan.data)
}

// rowAdmitted reports whether claims admit this event's row under the role's
// row-filter, counting a withheld row (with the reason) when they don't. It is
// the one admission step shared by the live fan-out (per subscriber) and replay
// (per connection), so the two delivery paths can't drift on how row-level
// security is evaluated.
//
// view is the event parsed once for the whole fan-out; prepErr is why there is
// none. An event that could not be prepared — no compiled schema for the table,
// a column list that is not the compiled generation's, an unparseable row —
// withholds from every row-filtered subscriber, never from the unfiltered roles
// that never asked the type layer anything.
func (h *Hub) rowAdmitted(p *policy.Policy, role string, ev *eventView, claims map[string]any, view RowView, prepErr error) bool {
	if prepErr != nil || view == nil {
		// view == nil with no error is a broken evaluator, not a verdict: withhold.
		h.metric.RowWithheld(ev.evt.TableName, role, WithheldReason(prepErr))
		return false
	}
	perms := policy.Evaluate(p, role, ev.evt.TableName, "select", claims)
	visible, reason := view.Visible(perms)
	if !visible {
		h.metric.RowWithheld(ev.evt.TableName, role, reason)
		return false
	}
	return true
}

// eventView is one published event decoded once per Broadcast: cells, the raw
// JSON value at each envelope column position, sliced positionally into the
// outgoing frame so a value's bytes are never re-encoded. The row-filter reads
// the ORIGINAL positional bytes (ev.evt.Row) through the type layer rather than
// any decoding done here — the parse that decides visibility is ClickHouse's own.
//
// raw and decoded carry the legacy no-policy passthrough: a payload that is not
// an EventMessage at all is forwarded verbatim when no policy store is wired,
// and refused when one is.
type eventView struct {
	raw     []byte
	evt     ingest.EventMessage
	decoded bool // raw parsed as an EventMessage
	usable  bool // ...and it declares a known format whose columns and row pair
	cells   []json.RawMessage
}

// newEventView decodes raw once for the whole fan-out. The envelope's own fields
// decode with UseNumber so nothing in it is rounded, and the row's cells are kept
// as raw bytes: the outgoing frame reuses them verbatim, so a 64-bit id past 2^53
// keeps every digit on the wire, and the visibility decision never sees a Go
// float at all.
func newEventView(raw []byte) *eventView {
	ev := &eventView{raw: raw}
	if !decodeEvent(raw, &ev.evt) {
		return ev
	}
	ev.decoded = true
	// The hub is a second consumer of the same subject as the ingest worker, and
	// acks independently of it, so a format only the worker refuses would stream
	// to clients while the worker parks it on the DLQ. Refusing it here keeps the
	// two readers agreeing on what the bytes mean. Today only a pre-v2 envelope
	// declares anything else, and it would fail pairing anyway on its empty
	// column list — this is what holds once a second format exists.
	if ev.evt.Format != ingest.FormatJSONCompactEachRow {
		return ev
	}
	ev.cells, ev.usable = pairRow(ev.evt.Columns, ev.evt.Row)
	return ev
}

// pairRow splits a compact row into its cells and checks that they pair with the
// column names. ok is false when the two cannot be paired — an undecodable row, a
// length that disagrees with the column list, a repeated column name, or an
// empty column list (which no length check catches, since a zero-length row
// agrees with it) — because there is then no way to say which value belongs to
// which column, and neither the announced schema frame nor a row-filter may
// guess.
//
// This is ALL that survives of the old name-keyed decode: the arity and drift
// check the schema frame needs. The values themselves are never decoded here —
// they go to the type layer as the bytes they arrived as, and out to the client
// as the same bytes.
func pairRow(cols []string, row json.RawMessage) (cells []json.RawMessage, ok bool) {
	if len(row) == 0 {
		return nil, false
	}
	if err := json.Unmarshal(row, &cells); err != nil {
		return nil, false
	}
	// A zero-column envelope pairs with anything of length zero — both `null`,
	// which unmarshals to a nil slice, and `[]` — and would then be announced as
	// a usable event carrying `row:[]`. There is no such thing as a positional
	// row over no columns, and the worker already refuses the same shape
	// (parseMsg's len(envelope.Columns) == 0), so refuse it here too rather than
	// let the two consumers disagree about an envelope neither can read.
	if len(cols) == 0 {
		return nil, false
	}
	if len(cells) != len(cols) {
		return nil, false
	}
	// A repeated name has no single meaning: the client zips the announced list
	// against the positional row, so a duplicate makes two positions
	// indistinguishable to it, and the type layer would read the row against a
	// column list the table cannot have. Unpairable, like a length mismatch.
	// Cannot arise from our own producer (the envelope's columns come from
	// system.columns, where ClickHouse forbids two columns of one name), so this
	// is defence for an envelope we did not write.
	seen := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		if _, dup := seen[c]; dup {
			return nil, false
		}
		seen[c] = struct{}{}
	}
	return cells, true
}

// decodeEvent parses raw as a published EventMessage, reporting whether it is one
// (a decode error, trailing garbage, or a missing table name all read as "not an
// EventMessage", which planForRole then fails closed under a policy).
func decodeEvent(raw []byte, evt *ingest.EventMessage) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(evt) != nil || evt.TableName == "" {
		return false
	}
	// Token, not More: More() is an in-array/object cursor, not an end-of-input
	// check — it reports false for a trailing "}" or "]" without consuming it. An
	// io.EOF from Token proves nothing follows the object, matching the strictness
	// of the json.Unmarshal this replaces.
	_, err := dec.Token()
	return err == io.EOF
}

// snapshotPolicy returns the current policy and whether filtering is configured.
// filter is false only when no store is wired (legacy passthrough); a wired store
// returning a nil policy is a deliberate lockout that Evaluate denies.
func (h *Hub) snapshotPolicy() (p *policy.Policy, filter bool) {
	if h.policy == nil {
		return nil, false
	}
	return h.policy(), true
}

// ReplayProjector returns the projection function for one connection's gap-fill:
// each call projects a single replayed event for the connection's role+claims into
// a ready-to-write replay frame, or ok=false to skip it (denied table, invalid
// payload, or a row the claims aren't entitled to see). It is a Hub method so
// replay shares the Hub's policy store and schema registry with the live fan-out —
// the handler can't accidentally project replay against a different (or nil)
// policy. Replay is already per-connection, so row-level security evaluates against
// this connection's claims directly; the returned closure holds one policy snapshot
// for the whole gap-fill (matching Broadcast's one-snapshot-per-event — a reload
// landing mid-replay applies from the first live event), so a large Last-Event-ID
// gap-fill doesn't pay a store read-lock per event.
// The closure is for a single goroutine — each connection makes its own. The live
// path uses Broadcast.
func (h *Hub) ReplayProjector(role string, sub *Subscriber) func(raw []byte) []Frame {
	p, filter := h.snapshotPolicy()
	// Schema-drift state is LOCAL to this gap-fill, not the connection's shared
	// lastSchema. Replay writes straight to the socket while live events queue
	// behind it, so sharing the state would let a live event's announcement
	// claim the slot and leave a replayed row ahead of it with nothing to zip
	// against.
	//
	// KNOWN LIMITATION, tracked in #543 and deferred to the schema-versioning
	// work: the two states
	// are not reconciled when they disagree. If the table's columns change while
	// a client is connected AND that client gap-fills across the change, replay
	// can leave the client's last-announced list older than the one the live
	// path already recorded at subscribe time — so live rows after the replay
	// arrive with no fresh announcement until the next drift or a reconnect.
	//
	// A client that checks arity catches most of it: a row whose LENGTH
	// disagrees with its list is dropped rather than guessed at, which covers
	// an added or removed column. It does not cover a same-length change — a
	// RENAME COLUMN, or a drop paired with an add — where the values zip under
	// the wrong names. So this costs correctness in that one case, not merely
	// availability; a reconnect resynchronizes.
	lastSig := ""
	return func(raw []byte) []Frame {
		ev := newEventView(raw)
		plan, ok := planForRole(p, filter, role, ev, KindReplay)
		if !ok {
			return nil
		}
		if plan.perms.HasRowFilter() {
			// Replay is per connection, so one prepared row serves exactly one
			// visibility question — but it is still closed immediately, because the
			// gap-fill loop can run for thousands of events.
			view, err := h.rowEvaluator().Prepare(ev.evt.TableName, ev.evt.Columns, ev.evt.Row)
			admitted := h.rowAdmitted(p, role, ev, sub.claims, view, err)
			if view != nil {
				view.Close()
			}
			if !admitted {
				return nil // this row is filtered out for these claims
			}
		}
		// Same schema-before-data contract as the live path: no replayed row is
		// ever the first thing this gap-fill writes.
		if plan.announce && lastSig != plan.sig {
			lastSig = plan.sig
			return []Frame{plan.schemaFrame(), plan.data}
		}
		return []Frame{plan.data}
	}
}

// SubscribeSchemaFrame builds the `event: schema` frame to send when a
// connection opens, before any data or replay frame — so a client knows the
// column list of the rows it is about to receive even on a table that is
// currently quiet. It reads the columns from the schema registry (there is no
// event to read them from yet) and projects them for the role exactly as the
// event path does, recording the signature on sub so the first data event does
// not repeat it.
//
// ok is false when there is nothing to announce: no registry, no schema for the
// table, or a role that cannot read it. That is not an error — the event path's
// drift check still sends a schema frame before the first data frame.
func (h *Hub) SubscribeSchemaFrame(table, role string, sub *Subscriber) (Frame, bool) {
	if h.registry == nil {
		return Frame{}, false
	}
	schema := h.registry.Get(table)
	if schema == nil {
		return Frame{}, false
	}
	p, filter := h.snapshotPolicy()
	var perms *policy.ResolvedPermissions
	if filter {
		// Column visibility is claims-independent, so nil claims, exactly as the
		// event path evaluates it.
		perms = policy.Evaluate(p, role, table, "select", nil)
		if !perms.Allowed {
			return Frame{}, false
		}
	}
	// The insertable subset, matching what an event's envelope carries — a
	// computed column never appears in a published row, so announcing it here
	// would guarantee a drift re-announcement on the very first event.
	_, projected := projectIndices(schema.InsertableColumnNames(), perms)
	sig := schemaSignature(table, projected)
	if !sub.needsSchema(sig) {
		return Frame{}, false // already announced (a live event beat us here)
	}
	// Recorded on build rather than on a successful write: the caller writes
	// this frame straight to the socket, and a failed write there ends the
	// connection outright, so there is no state left to be wrong about.
	sub.recordSchema(sig)
	return Frame{Kind: KindSchema, Data: schemaFrame(table, projected)}, true
}

// rolePlan is one role's finished view of one event: the ready-to-write data
// frame, the schema frame that must precede it on any connection that has not
// been told this column list, that list's signature, and the resolved
// permissions the caller needs for the per-subscriber row-filter decision.
//
// Everything in it derives only from the role+table policy entry
// (AllowColumns/DenyColumns), never from claims, so one plan serves a whole
// role bucket — the shared projection that lets one serialization serve every
// subscriber of a (role, table).
type rolePlan struct {
	perms *policy.ResolvedPermissions
	sig   string
	// table and projected are what an announcement would say. The frame itself
	// is built ON DEMAND in deliver, because in steady state every connection
	// already has the list and the bytes would be marshalled and thrown away
	// once per event per role — on the path that exists to serialize once.
	// announce is false for the legacy passthrough, which has no column list.
	announce  bool
	table     string
	projected []string
	data      Frame
}

// schemaFrame renders this plan's announcement. Only deliver calls it, and only
// when the connection is actually due one.
func (p rolePlan) schemaFrame() Frame {
	return Frame{Kind: KindSchema, Data: schemaFrame(p.table, p.projected)}
}

// planForRole applies role/table COLUMN policy to a decoded event (or passes a
// non-EventMessage JSON payload through untouched). Row-level security is NOT
// applied here; the caller checks it per subscriber when perms.HasRowFilter().
// ok=false means skip: the role can't read the table, or the payload is
// unusable. perms is nil for the legacy no-policy passthrough (which has no
// row-filter).
func planForRole(p *policy.Policy, filter bool, role string, ev *eventView, kind string) (rolePlan, bool) {
	if !ev.decoded {
		// Without a decoded EventMessage there's no table to evaluate policy against,
		// so fail closed whenever policy is configured: a malformed-but-valid-JSON
		// payload on ingest.<table> must not bypass column filtering. Pass through
		// only when no policy store is wired at all (the legacy/test passthrough),
		// where there is no column list to announce either.
		if filter || !json.Valid(ev.raw) {
			return rolePlan{}, false
		}
		return rolePlan{data: Frame{Kind: kind, Data: wireFrame("", ev.raw)}}, true
	}
	if !ev.usable {
		// The envelope decoded but declares a format we don't know, or its columns
		// and row don't pair. Either way nothing can be projected positionally and
		// no row-filter can be evaluated, so withhold from every role rather than
		// deliver values under guessed names.
		return rolePlan{}, false
	}

	var perms *policy.ResolvedPermissions
	if filter {
		// Column allow/deny is claims-independent, so evaluate it once with nil claims;
		// the caller re-evaluates the row-filter per subscriber against real claims.
		perms = policy.Evaluate(p, role, ev.evt.TableName, "select", nil)
		if !perms.Allowed {
			return rolePlan{}, false // role has no access to this table
		}
	}

	idx, projected := projectIndices(ev.evt.Columns, perms)
	return rolePlan{
		perms:     perms,
		sig:       schemaSignature(ev.evt.TableName, projected),
		announce:  true,
		table:     ev.evt.TableName,
		projected: projected,
		data:      Frame{Kind: kind, Data: wireFrame(ev.evt.ReceivedTimestamp, dataPayload(ev, idx))},
	}, true
}

// projectIndices selects the positions of cols the role may read, returning both
// the positions (to slice the compact row with) and the names at them (to
// announce). The single per-column decision is policy.IsColumnAllowed, shared
// with the query path so the two read surfaces can never drift. A nil perms (the
// no-policy passthrough) keeps every column.
func projectIndices(cols []string, perms *policy.ResolvedPermissions) (idx []int, projected []string) {
	idx = make([]int, 0, len(cols))
	projected = make([]string, 0, len(cols))
	for i, c := range cols {
		if perms.IsColumnAllowed(c, false) {
			idx = append(idx, i)
			projected = append(projected, c)
		}
	}
	return idx, projected
}

// schemaSignature identifies one announced column list for drift detection. The
// table is part of it so a stream that ever mixes tables re-announces rather
// than silently reusing another table's list; the separator cannot occur in a
// ClickHouse identifier's UTF-8 encoding, so distinct lists cannot collide.
func schemaSignature(table string, cols []string) string {
	return table + "\x00" + strings.Join(cols, "\x00")
}

// dataPayload renders the outgoing event body: the table, the timestamp, and the
// compact row reduced to the projected positions. The cells are copied as their
// original bytes rather than re-encoded, so a 64-bit integer keeps every digit.
func dataPayload(ev *eventView, idx []int) []byte {
	var b bytes.Buffer
	b.WriteString(`{"table_name":`)
	b.Write(mustJSONString(ev.evt.TableName))
	b.WriteString(`,"received_timestamp":`)
	b.Write(mustJSONString(ev.evt.ReceivedTimestamp))
	b.WriteString(`,"row":[`)
	for n, i := range idx {
		if n > 0 {
			b.WriteByte(',')
		}
		b.Write(ev.cells[i])
	}
	b.WriteString(`]}`)
	return b.Bytes()
}

// schemaEvent is the body of an `event: schema` frame: which table the rows that
// follow belong to, and what the positions in each row mean.
type schemaEvent struct {
	TableName string   `json:"table_name"`
	Columns   []string `json:"columns"`
}

// schemaFrame assembles one `event: schema` frame. It deliberately carries NO
// `id:` line: omitting the field leaves the client's Last-Event-ID untouched,
// while an empty one would CLEAR it and cost the connection its resumption
// point — and a schema frame has no event position of its own to offer.
func schemaFrame(table string, cols []string) []byte {
	payload, err := json.Marshal(schemaEvent{TableName: table, Columns: cols})
	if err != nil {
		// Unreachable: a string and a []string always marshal.
		payload = []byte(`{}`)
	}
	b := []byte("event: schema")
	for line := range bytes.SplitSeq(payload, []byte{'\n'}) {
		b = append(b, "\ndata: "...)
		b = append(b, line...)
	}
	return append(b, "\n\n"...)
}

// mustJSONString encodes s as a JSON string. json.Marshal of a string cannot
// fail, so the error is unreachable; the fallback keeps the frame valid JSON
// rather than emitting a truncated object if that ever changes.
func mustJSONString(s string) []byte {
	b, err := json.Marshal(s)
	if err != nil {
		return []byte(`""`)
	}
	return b
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
