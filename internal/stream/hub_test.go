package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// jwtClaims round-trips claims through a real signed token and the production
// auth middleware, returning them exactly as a live connection would carry them
// (numbers as json.Number, never float64 or a hand-typed string). Tests whose
// guarantee depends on the decoded TYPE of a claim — the numeric cases — must
// build claims this way: a hand-built map once used string tenants here and
// passed while the production decode path failed open (#381 review). String and
// nested-object claims decode unchanged, so literal maps stay faithful there.
func jwtClaims(t *testing.T, claims map[string]any) map[string]any {
	t.Helper()
	authn, err := auth.NewAuthenticator(auth.Config{JWTSecret: testutil.TestJWTSecret}, nil, nil)
	require.NoError(t, err)
	var got map[string]any
	h := authn.Middleware()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		c, ok := auth.ClaimsFromContext(r.Context())
		require.True(t, ok, "test token must authenticate")
		got = map[string]any(c)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.MakeJWT(t, claims))
	h.ServeHTTP(httptest.NewRecorder(), req)
	return got
}

// rawEvent marshals an EventMessage the way the ingest path publishes it: the
// row positionally, with its column names alongside. Keys are sorted so the
// order is deterministic (production uses the table's declaration order).
// testing.TB so the fan-out benchmark can build events too.
func rawEvent(tb testing.TB, table, ts string, data map[string]any) []byte {
	tb.Helper()
	return rawEventCols(tb, table, ts, slices.Sorted(maps.Keys(data)), data)
}

// rawEventCols is rawEvent with an explicit column order, for tests that pin a
// declaration order or publish a column the record omits.
func rawEventCols(tb testing.TB, table, ts string, cols []string, data map[string]any) []byte {
	tb.Helper()
	schema := make([]discovery.Column, len(cols))
	for i, c := range cols {
		schema[i] = discovery.Column{Name: c, Position: uint64(i + 1)}
	}
	row, err := ingest.EncodeCompactRow(schema, data)
	require.NoError(tb, err)
	raw, err := json.Marshal(ingest.EventMessage{
		TableName:         table,
		ReceivedTimestamp: ts,
		Format:            ingest.FormatJSONCompactEachRow,
		Columns:           cols,
		Row:               row,
	})
	require.NoError(tb, err)
	return raw
}

// recvFrame returns the next frame buffered for sub, failing if none is ready.
func recvFrame(t *testing.T, sub *Subscriber) Frame {
	t.Helper()
	select {
	case f := <-sub.Frames():
		return f
	case <-time.After(time.Second):
		t.Fatal("expected a frame, got none")
		return Frame{}
	}
}

// assertNoFrame fails if sub has any frame buffered — used to prove a row-filtered
// subscriber received nothing for a row it isn't entitled to see.
func assertNoFrame(t *testing.T, sub *Subscriber) {
	t.Helper()
	select {
	case f := <-sub.Frames():
		t.Fatalf("expected no frame, got %q", f.Data)
	default:
	}
}

// frameData parses the JSON object on the "data:" line of an SSE frame.
func frameData(t *testing.T, f Frame) map[string]any {
	t.Helper()
	var out map[string]any
	for line := range strings.SplitSeq(string(f.Data), "\n") {
		if rest, ok := strings.CutPrefix(line, "data: "); ok {
			require.NoError(t, json.Unmarshal([]byte(rest), &out))
			return out
		}
	}
	t.Fatalf("no data line in frame %q", f.Data)
	return nil
}

// frameColumns reads the announced column list out of an `event: schema` frame.
func frameColumns(t *testing.T, f Frame) []string {
	t.Helper()
	require.Equal(t, KindSchema, f.Kind)
	require.Contains(t, string(f.Data), "event: schema")
	require.NotContains(t, string(f.Data), "id:",
		"a schema frame must carry no id: line — an empty one would clear the client's resumption point")
	raw, ok := frameData(t, f)["columns"].([]any)
	require.True(t, ok, "schema frame must carry a columns array")
	cols := make([]string, len(raw))
	for i, c := range raw {
		cols[i] = c.(string)
	}
	return cols
}

// zipRowFrame turns a data frame's positional row back into a name→value map,
// the way a client does with the announced column list.
func zipRowFrame(t *testing.T, f Frame, cols []string) map[string]any {
	t.Helper()
	body := frameData(t, f)
	cells, ok := body["row"].([]any)
	require.True(t, ok, "data frame must carry a row array, got %v", body)
	require.Len(t, cells, len(cols), "row length must match the announced column list")
	out := make(map[string]any, len(cells))
	for i, c := range cols {
		out[c] = cells[i]
	}
	return out
}

// recvEvent drains sub's next event the way a client does: the `event: schema`
// frame announcing the column list, then the data frame with its positional row
// zipped back by name. Requiring the schema frame is the contract for the first
// event a connection sees — a row is unreadable without it.
func recvEvent(t *testing.T, sub *Subscriber) (Frame, []string, map[string]any) {
	t.Helper()
	cols := frameColumns(t, recvFrame(t, sub))
	data := recvFrame(t, sub)
	return data, cols, zipRowFrame(t, data, cols)
}

// recvEventCols is recvEvent for a LATER event on the same connection, where no
// schema frame is due because the column list has not changed.
func recvEventCols(t *testing.T, sub *Subscriber, cols []string) (Frame, map[string]any) {
	t.Helper()
	data := recvFrame(t, sub)
	require.NotEqual(t, KindSchema, data.Kind, "the column list is unchanged, so no re-announcement is due")
	return data, zipRowFrame(t, data, cols)
}

func TestHub_ProjectsOncePerRole_FanOutToAllSubscribers(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil) // nil store ⇒ passthrough, no filtering
	const topic = "ingest.clicks"

	a, b := NewSubscriber(nil, nil), NewSubscriber(nil, nil)
	hub.Add(topic, "public", a)
	hub.Add(topic, "public", b)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z", map[string]any{"page": "/home"}))

	fa, acols, _ := recvEvent(t, a)
	fb, bcols, _ := recvEvent(t, b)
	assert.Equal(t, KindEvent, fa.Kind)
	assert.Equal(t, []string{"page"}, acols)
	assert.Equal(t, acols, bcols, "both subscribers are told the same column list")
	require.Equal(t, fa.Data, fb.Data, "both subscribers receive identical frame bytes")
	// Same role ⇒ one serialization shared across the bucket: identical backing array.
	// require these before indexing &...Data[0] so an empty buffer can't panic.
	require.NotEmpty(t, fa.Data)
	require.NotEmpty(t, fb.Data)
	assert.Same(t, &fa.Data[0], &fb.Data[0], "the frame is serialized once and shared, not re-projected per subscriber")
	assert.True(t, strings.HasPrefix(string(fa.Data), "id: 2026-06-26T00:00:00Z\ndata: "), "id line carries received_timestamp")
}

func TestHub_ProjectsPerRole_ColumnFilterAndDenial(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				// "viewer" may read only "page"; "blocked" has no entry ⇒ denied.
				"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}},
			},
		},
	}
	hub := NewHub(policy.Static(p), nil, nil)
	const topic = "ingest.clicks"

	viewer := NewSubscriber(nil, nil)
	blocked := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", viewer)
	hub.Add(topic, "blocked", blocked)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"page": "/home", "secret": "hidden"}))

	_, cols, row := recvEvent(t, viewer)
	assert.Equal(t, []string{"page"}, cols, "the announced list is the role's projection")
	assert.Equal(t, "/home", row["page"])
	assert.NotContains(t, row, "secret", "denied column stripped before serialization")

	// The denied role gets nothing pushed.
	select {
	case f := <-blocked.Frames():
		t.Fatalf("denied role must receive no frame, got %q", f.Data)
	default:
	}
}

// TestPlanForRole_FailsClosedOnUnusablePayload is the #323 regression guard
// at the unit seam: with a policy configured (filter=true), a payload that did
// not decode to an EventMessage — so there is no table to evaluate policy
// against — must be dropped, never passed through unfiltered. Only the no-policy
// legacy passthrough (filter=false) may forward it, and invalid JSON is dropped
// either way. TestHub_PassthroughAndFailClosed drives the same rule end-to-end
// through Broadcast.
func TestPlanForRole_FailsClosedOnUnusablePayload(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		filter bool
		raw    []byte
		wantOK bool
	}{
		{"filtered valid JSON is dropped", true, []byte(`{"not":"an-event"}`), false},
		{"unfiltered valid JSON is forwarded (legacy passthrough)", false, []byte(`{"not":"an-event"}`), true},
		{"unfiltered invalid JSON is dropped", false, []byte("not json"), false},
		// The envelope decodes but its columns and row can't be paired: there is
		// no way to say which value belongs to which column, so no role gets it.
		{
			"row shorter than the column list is dropped", true,
			[]byte(`{"table_name":"clicks","format":"JSONCompactEachRow","columns":["a","b"],"row":["x"]}`), false,
		},
		{
			"row longer than the column list is dropped", true,
			[]byte(`{"table_name":"clicks","format":"JSONCompactEachRow","columns":["a"],"row":["x","y"]}`), false,
		},
		{
			"absent row is dropped", true,
			[]byte(`{"table_name":"clicks","format":"JSONCompactEachRow","columns":["a"]}`), false,
		},
		{
			"undecodable row is dropped", true,
			[]byte(`{"table_name":"clicks","format":"JSONCompactEachRow","columns":["a"],"row":"not-an-array"}`), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ok := planForRole(nil, tt.filter, "viewer", newEventView(tt.raw), KindEvent)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestHub_ProjectsPerRole_DistinctRolesGetDistinctFrames(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}},           // page only
				"editor": {Select: &policy.SelectPermissions{AllowColumns: []string{"page", "secret"}}}, // page + secret
			},
		},
	}
	hub := NewHub(policy.Static(p), nil, nil)
	const topic = "ingest.clicks"

	viewer, editor := NewSubscriber(nil, nil), NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", viewer)
	hub.Add(topic, "editor", editor)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"page": "/home", "secret": "hidden"}))

	fv, vcols, vdata := recvEvent(t, viewer)
	fe, ecols, edata := recvEvent(t, editor)

	assert.Equal(t, []string{"page"}, vcols)
	assert.Equal(t, []string{"page", "secret"}, ecols, "each role is told its own projection")
	assert.Equal(t, "/home", vdata["page"])
	assert.NotContains(t, vdata, "secret", "viewer's denied column is stripped")
	assert.Equal(t, "/home", edata["page"])
	assert.Equal(t, "hidden", edata["secret"], "editor sees a column the viewer can't")

	// The frame is built once PER ROLE, not once globally: the two roles' projections
	// differ, so their frames are serialized independently (distinct backing arrays).
	require.NotEmpty(t, fv.Data)
	require.NotEmpty(t, fe.Data)
	assert.NotSame(t, &fv.Data[0], &fe.Data[0], "distinct roles get independently serialized frames")
	assert.NotEqual(t, fv.Data, fe.Data, "distinct role projections produce distinct bytes")
}

// rowFilterPolicy scopes role "viewer" to column "page" only, and to rows whose
// tenant_id equals the caller's {{ jwt.tenant }} claim. The filter keys on tenant_id
// — a column viewer may NOT select — so it also exercises the rule that row
// visibility is evaluated against the FULL event, then columns are projected.
func rowFilterPolicy() *policy.Policy {
	return &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				"viewer": {Select: &policy.SelectPermissions{
					AllowColumns: []string{"page"},
					Filter:       map[string]policy.Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}},
				}},
			},
		},
	}
}

// TestHub_RowFilter_PerSubscriberIsolation is the #319 fix: two subscribers of the
// SAME role but different tenant claims each receive only their own tenant's rows
// over the live stream — the row-filter the query path applies is now applied here
// too, per subscriber. The third subscriber pins the #457 rule on this surface: a
// validly-signed token that doesn't carry the templated claim yields NO rows here,
// matching the constant-false predicate the query path binds for it.
func TestHub_RowFilter_PerSubscriberIsolation(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.Static(rowFilterPolicy()), nil, nil)
	const topic = "ingest.clicks"

	acme := NewSubscriber(jwtClaims(t, map[string]any{"tenant": "acme"}), nil)
	globex := NewSubscriber(jwtClaims(t, map[string]any{"tenant": "globex"}), nil)
	noTenant := NewSubscriber(jwtClaims(t, map[string]any{"role": "viewer"}), nil) // valid token, no tenant claim
	hub.Add(topic, "viewer", acme)
	hub.Add(topic, "viewer", globex)
	hub.Add(topic, "viewer", noTenant)

	// An acme row reaches only the acme subscriber, projected to the allowed column.
	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"}))
	_, cols, row := recvEvent(t, acme)
	assert.Equal(t, []string{"page"}, cols)
	assert.Equal(t, "/a", row["page"])
	assert.NotContains(t, row, "tenant_id", "the filtered column is not in viewer's projection")
	assert.NotContains(t, row, "secret", "denied column stripped")
	assertNoFrame(t, globex)
	// Unresolvable claim ⇒ no rows on the stream, matching the query path (#457).
	assertNoFrame(t, noTenant)

	// A globex row reaches only the globex subscriber.
	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"tenant_id": "globex", "page": "/g"}))
	_, _, grow := recvEvent(t, globex)
	assert.Equal(t, "/g", grow["page"])
	assertNoFrame(t, acme)
	assertNoFrame(t, noTenant)
}

// TestHub_RowFilter_ClaimsSnapshotImmuneToCallerMutation: NewSubscriber deep-copies
// the claims, so a caller that keeps the source map (the middleware-owned
// jwt.MapClaims outlives Hub.Add) can neither widen row visibility after
// registration nor race Broadcast's claims read. The mutations target a NESTED
// map value and an ARRAY element to prove the copy is deep on both structured
// arms (cloneClaimValue), not a top-level shallow copy.
func TestHub_RowFilter_ClaimsSnapshotImmuneToCallerMutation(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"tenant_id": {Eq: new("{{ jwt.org.tenant }}")}}}}},
		},
	}
	hub := NewHub(policy.Static(p), nil, nil)
	const topic = "ingest.clicks"

	org := map[string]any{"tenant": "globex"}
	claims := map[string]any{"org": org}
	sub := NewSubscriber(claims, nil)
	hub.Add(topic, "viewer", sub)

	org["tenant"] = "acme" // the caller mutates its retained map after registration

	hub.Broadcast(topic, rawEvent(t, "clicks", "t", map[string]any{"tenant_id": "acme", "page": "/a"}))
	assertNoFrame(t, sub) // visibility follows the snapshot ("globex"), not the mutation

	hub.Broadcast(topic, rawEvent(t, "clicks", "t", map[string]any{"tenant_id": "globex", "page": "/g"}))
	_, _, row := recvEvent(t, sub)
	assert.Equal(t, "/g", row["page"],
		"the snapshot keeps admitting the tenant the connection authenticated as")

	// The []any arm is just as authorization-relevant: an _in-shaped filter reads
	// array elements, so mutating a retained slice element must not move the
	// subscriber's row entitlement either.
	inPolicy := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"tenant_id": {In: new("{{ jwt.tenants }}")}}}}},
		},
	}
	inHub := NewHub(policy.Static(inPolicy), nil, nil)
	tenants := []any{"globex"}
	inSub := NewSubscriber(map[string]any{"tenants": tenants}, nil)
	inHub.Add(topic, "viewer", inSub)

	tenants[0] = "acme" // the caller mutates its retained slice after registration

	inHub.Broadcast(topic, rawEvent(t, "clicks", "t", map[string]any{"tenant_id": "acme", "page": "/a2"}))
	assertNoFrame(t, inSub) // membership follows the snapshot ("globex"), not the mutation

	inHub.Broadcast(topic, rawEvent(t, "clicks", "t", map[string]any{"tenant_id": "globex", "page": "/g2"}))
	_, _, inRow := recvEvent(t, inSub)
	assert.Equal(t, "/g2", inRow["page"],
		"the array snapshot keeps admitting the tenant list the connection authenticated with")
}

// TestHub_RowFilter_MissingColumn_FailsClosed: an event that lacks the filtered
// column can't be proven visible, so it is withheld rather than leaked.
func TestHub_RowFilter_MissingColumn_FailsClosed(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.Static(rowFilterPolicy()), nil, nil)
	const topic = "ingest.clicks"

	acme := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	hub.Add(topic, "viewer", acme)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"page": "/a"})) // no tenant_id
	assertNoFrame(t, acme)
}

// TestHub_RowFilter_SharedProjectionAcrossSameClaims: the column projection is still
// serialized once per role and shared — two subscribers with the same (matching)
// claims receive the identical frame bytes. Only the visibility decision is
// per-subscriber, not the serialization.
func TestHub_RowFilter_SharedProjectionAcrossSameClaims(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.Static(rowFilterPolicy()), nil, nil)
	const topic = "ingest.clicks"

	a := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	b := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	hub.Add(topic, "viewer", a)
	hub.Add(topic, "viewer", b)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a"}))

	fa, _, _ := recvEvent(t, a)
	fb, _, _ := recvEvent(t, b)
	require.NotEmpty(t, fa.Data)
	require.NotEmpty(t, fb.Data)
	assert.Same(t, &fa.Data[0], &fb.Data[0], "one serialization shared across same-role subscribers")
}

// TestHub_RowFilter_NumericOrdering_SchemaInformed drives the registry-backed path:
// with a numeric column type in the schema, an `amount > 100` filter compares
// numerically, so amount=9 is withheld (a lexicographic "9" > "100" comparison would
// have leaked it) and amount=250 is delivered. Without a registry the same ordering
// filter has no type to trust and withholds every row — fail closed, never the
// lexicographic leak (the schemaless window is real: boot-time discovery failure
// retries in the background while the server serves).
func TestHub_RowFilter_NumericOrdering_SchemaInformed(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{
			{Name: "amount", Type: "UInt64"},
			{Name: "page", Type: "String"},
		}},
	})
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"amount": {Gt: new("100")}}}}},
		},
	}
	hub := NewHub(policy.Static(p), reg, nil)
	const topic = "ingest.clicks"

	sub := NewSubscriber(nil, nil) // constant filter value ⇒ no claims needed
	hub.Add(topic, "viewer", sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"amount": float64(9), "page": "/a"}))
	assertNoFrame(t, sub) // 9 is not numerically > 100

	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"amount": float64(250), "page": "/b"}))
	_, _, row := recvEvent(t, sub)
	assert.Equal(t, float64(250), row["amount"])

	// Same policy, no schema registry: an ordering predicate can't be proven either
	// way, so both rows are withheld — including the one the schema-informed path
	// delivers above.
	noSchema := NewHub(policy.Static(p), nil, nil)
	blind := NewSubscriber(nil, nil)
	noSchema.Add(topic, "viewer", blind)
	noSchema.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"amount": float64(9), "page": "/a"}))
	noSchema.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"amount": float64(250), "page": "/b"}))
	assertNoFrame(t, blind)
}

// TestHub_RowFilter_FloatNarrowing_SchemaInformed drives storage-domain
// narrowing end-to-end through the registry: on a Float32 column, payload
// 16777217 stores as 16777216, so a `_gt: "16777216"` filter must withhold the
// event — the query path's WHERE over the stored row is false, and delivering
// the pre-narrowing payload was the ordering fail-open raised in review. A
// Float32-representable greater value still delivers.
func TestHub_RowFilter_FloatNarrowing_SchemaInformed(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{{Name: "score", Type: "Float32"}}},
	})
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"score": {Gt: new("16777216")}}}}},
		},
	}
	hub := NewHub(policy.Static(p), reg, nil)
	const topic = "ingest.clicks"
	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"score": json.Number("16777217")}))
	assertNoFrame(t, sub) // stores as 16777216: not greater once both operands narrow

	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"score": json.Number("16777218")}))
	_, _, row := recvEvent(t, sub)
	assert.Equal(t, float64(16777218), row["score"])
}

func TestHub_TopicIsolation(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	clicks, views := NewSubscriber(nil, nil), NewSubscriber(nil, nil)
	hub.Add("ingest.clicks", "public", clicks)
	hub.Add("ingest.views", "public", views)

	hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "t", map[string]any{"a": float64(1)}))

	f, _, _ := recvEvent(t, clicks)
	assert.NotEmpty(t, f.Data)
	select {
	case <-views.Frames():
		t.Fatal("a subscriber on another topic must not receive the event")
	default:
	}
}

// TestHub_PassthroughAndFailClosed drives the #323 fail-closed rule end-to-end
// through Broadcast: with a policy wired, a payload that did not decode to an
// EventMessage (no table to evaluate policy against) must be dropped, never
// passed through unfiltered; only the no-policy legacy passthrough may forward
// it. The unit seam is TestPlanForRole_FailsClosedOnUnusablePayload; the
// empty-table_name half is pinned in TestHub_ReplayProjector.
func TestHub_PassthroughAndFailClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		store    policy.Source
		payload  string
		wantData string // "" ⇒ expect no frame (event skipped)
	}{
		{
			name:     "non-EventMessage JSON passes through unchanged when no policy is wired",
			store:    nil,
			payload:  `{"custom":"data","value":42}`,
			wantData: "id: \ndata: {\"custom\":\"data\",\"value\":42}\n\n",
		},
		{
			name:    "invalid JSON is skipped",
			store:   nil,
			payload: "not json",
		},
		{
			name:    "non-EventMessage is dropped (fail closed) when a policy store is wired",
			store:   policy.Static(&policy.Policy{}),
			payload: `{"custom":"data","value":42}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hub := NewHub(tt.store, nil, nil)
			const topic = "ingest.custom"
			sub := NewSubscriber(nil, nil)
			hub.Add(topic, "public", sub)
			hub.Broadcast(topic, []byte(tt.payload))

			if tt.wantData == "" {
				select {
				case f := <-sub.Frames():
					t.Fatalf("expected no frame, got %q", f.Data)
				default:
				}
				return
			}
			assert.Equal(t, tt.wantData, string(recvFrame(t, sub).Data))
		})
	}
}

func TestHub_AddRemoveGCsBucketsAndTopics(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	const topic = "ingest.clicks"
	sub := NewSubscriber(nil, nil)

	hub.Add(topic, "public", sub)
	assert.Equal(t, 1, hub.Len(topic))

	hub.Remove(topic, "public", sub)
	assert.Equal(t, 0, hub.Len(topic))

	hub.mu.RLock()
	_, topicExists := hub.topics[topic]
	hub.mu.RUnlock()
	assert.False(t, topicExists, "an empty topic is garbage-collected")

	// Removing an already-gone registration is a no-op.
	assert.NotPanics(t, func() { hub.Remove(topic, "public", sub) })
}

func TestHub_BroadcastNoSubscribers_NoOp(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	assert.NotPanics(t, func() {
		hub.Broadcast("ingest.nobody", rawEvent(t, "clicks", "t", map[string]any{"a": float64(1)}))
	})
}

func TestHub_SlowConsumerDropIncrementsMetric(t *testing.T) {
	// No t.Parallel(): NewMetrics binds the global meter provider, swapped here.
	savedMP := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	m := NewMetrics()
	hub := NewHub(nil, nil, m)
	const topic = "ingest.clicks"
	// cap-2: the first broadcast fills it with the schema frame plus the row, so
	// the second undrained broadcast's row drops (its column list is unchanged, so
	// no second schema frame is due). The shared seam NewSubscriber wires metrics through.
	sub := newSubscriber(2, m)
	hub.Add(topic, "public", sub)

	raw := rawEvent(t, "clicks", "t", map[string]any{"a": float64(1)})
	hub.Broadcast(topic, raw) // fills the queue: schema + event
	hub.Broadcast(topic, raw) // dropped by Send, kind=event

	// The keepalive path drops through the same Send: a Push into the full
	// queue counts under the frame's own kind, with no counting at the call site.
	b := newSubscriberSet()
	b.Add(sub)
	b.Push(Frame{Kind: KindKeepalive, Data: []byte(": keepalive\n\n")})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	assert.Equal(t, int64(1), sumByNameKind(rm, "wavehouse_sse_dropped_frames_total", KindEvent))
	assert.Equal(t, int64(1), sumByNameKind(rm, "wavehouse_sse_dropped_frames_total", KindKeepalive))
}

func TestHub_ReplayProjector(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}}},
		},
	}
	hub := NewHub(policy.Static(p), nil, nil)
	raw := rawEvent(t, "clicks", "2026-06-26T00:00:00Z", map[string]any{"page": "/home", "secret": "x"})

	tests := []struct {
		name string
		role string
		raw  []byte
		want bool // whether a frame is produced (vs. skipped)
	}{
		{"allowed role projects with column filter", "viewer", raw, true},
		{"role without table access is skipped", "stranger", raw, false},
		// The empty-table_name half of #323, pinned on the fail-closed side:
		// {"table_name":"", …} decodes into an EventMessage but names no table to
		// evaluate policy against, so with a policy wired it must be dropped —
		// same conjunct as the non-EventMessage case in decodeEvent.
		{
			"empty table_name is dropped when policy is wired", "viewer",
			[]byte(`{"table_name":"","format":"JSONCompactEachRow","columns":["page"],"row":["/home"]}`), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			frames := hub.ReplayProjector(tt.role, NewSubscriber(nil, nil))(tt.raw)
			require.Equal(t, tt.want, len(frames) > 0)
			if !tt.want {
				return
			}
			// Replay carries the same schema-before-data contract as the live path.
			require.Len(t, frames, 2)
			cols := frameColumns(t, frames[0])
			assert.Equal(t, []string{"page"}, cols)
			assert.Equal(t, KindReplay, frames[1].Kind)
			row := zipRowFrame(t, frames[1], cols)
			assert.Equal(t, "/home", row["page"])
			assert.NotContains(t, row, "secret")
		})
	}
}

// TestHub_ReplayProjector_RowFilter exercises the row-filter branch of replay: the
// #319 fix applies row-level security on the per-connection replay path too, so a
// gap-fill event is projected only when the connection's claims satisfy the filter.
func TestHub_ReplayProjector_RowFilter(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.Static(rowFilterPolicy()), nil, nil)
	raw := rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"})

	t.Run("matching claims replay the row, projected to allowed columns", func(t *testing.T) {
		t.Parallel()
		project := hub.ReplayProjector("viewer", NewSubscriber(map[string]any{"tenant": "acme"}, nil))
		frames := project(raw)
		require.Len(t, frames, 2, "the first replayed row announces the column list")
		cols := frameColumns(t, frames[0])
		assert.Equal(t, KindReplay, frames[1].Kind)
		row := zipRowFrame(t, frames[1], cols)
		assert.Equal(t, "/a", row["page"])
		assert.NotContains(t, row, "secret", "denied column stripped on replay too")

		// The projector is reusable across a replay loop: a second event through the
		// same closure (cached column kinds) projects identically — and does NOT
		// re-announce a column list the connection already has.
		again := project(raw)
		require.Len(t, again, 1, "the column list is announced once per connection")
		assert.Equal(t, frames[1].Data, again[0].Data)
	})

	t.Run("non-matching claims withhold the row", func(t *testing.T) {
		t.Parallel()
		frames := hub.ReplayProjector("viewer", NewSubscriber(map[string]any{"tenant": "globex"}, nil))(raw)
		require.Empty(t, frames, "row must be withheld when claims don't satisfy the filter")
	})
}

func TestHub_ConcurrentAddRemoveBroadcast_Race(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	const topic = "ingest.clicks"
	raw := rawEvent(t, "clicks", "t", map[string]any{"a": float64(1)})

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(role string) {
			defer wg.Done()
			for range 50 {
				sub := NewSubscriber(nil, nil)
				hub.Add(topic, role, sub)
				hub.Broadcast(topic, raw)
				hub.Remove(topic, role, sub)
			}
		}([]string{"public", "viewer"}[i%2])
	}
	wg.Wait()
	assert.Equal(t, 0, hub.Len(topic), "every subscriber is removed")
}

// TestHub_ConcurrentRowFilteredBroadcast_Race is the row-filter twin of the
// passthrough race test above: with a filtered role, the fan-out goroutine reads
// each subscriber's claims (hub.Broadcast → sub.claims) while other goroutines
// construct, register and remove claims-bearing subscribers. Claims are immutable
// after construction, and publication happens-before the fan-out read via the
// bucket mutex in Add — this test makes the race detector watch exactly that edge,
// so a future claims setter (or any post-Add mutation) fails -race here instead of
// racing silently on a security decision.
func TestHub_ConcurrentRowFilteredBroadcast_Race(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.Static(rowFilterPolicy()), nil, nil)
	const topic = "ingest.clicks"
	raw := rawEvent(t, "clicks", "t", map[string]any{"tenant_id": "acme", "page": "/a"})

	var wg sync.WaitGroup
	for range 4 { // broadcasters: per-subscriber claims evaluation on every event
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				hub.Broadcast(topic, raw)
			}
		}()
	}
	for i := range 4 { // churners: subscribers with claims come and go concurrently
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			for range 50 {
				sub := NewSubscriber(map[string]any{"tenant": tenant}, nil)
				hub.Add(topic, "viewer", sub)
				hub.Remove(topic, "viewer", sub)
			}
		}([]string{"acme", "globex"}[i%2])
	}
	wg.Wait()
	assert.Equal(t, 0, hub.Len(topic), "every subscriber is removed")
}

// TestHub_RowFilter_BigIntegerExact: a bare JSON integer past 2^53 must keep its
// exact digits through the hub's decode (UseNumber), or the row filter compares a
// lossily-rounded value: tenant 10000000000000001's row would falsely equal a
// tenant claim of 10000000000000000 — float64 collapses the neighbors — and be
// delivered cross-tenant on the stream while the query path (ClickHouse stores the
// exact digits ingest forwarded) excludes it. The raw payload is hand-built —
// marshaling a Go float64 would already have destroyed the value this test is about.
func TestHub_RowFilter_BigIntegerExact(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{
			{Name: "tenant_id", Type: "UInt64"},
			{Name: "page", Type: "String"},
		}},
	})
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}}}}},
		},
	}
	hub := NewHub(policy.Static(p), reg, nil)
	const topic = "ingest.clicks"

	// Claims come from real signed tokens through the production middleware, so a
	// bare JSON-number tenant claim reaches the filter exactly as production
	// decodes it (json.Number since WithJSONNumber; before that fix, float64 —
	// which collapsed these neighbors and delivered the cross-tenant row).
	neighbor := NewSubscriber(jwtClaims(t, map[string]any{"tenant": json.Number("10000000000000000")}), nil)
	exact := NewSubscriber(jwtClaims(t, map[string]any{"tenant": json.Number("10000000000000001")}), nil)
	hub.Add(topic, "viewer", neighbor)
	hub.Add(topic, "viewer", exact)

	raw := []byte(`{"table_name":"clicks","received_timestamp":"t","format":"JSONCompactEachRow",` +
		`"columns":["page","tenant_id"],"row":["/a",10000000000000001]}`)
	hub.Broadcast(topic, raw)

	assertNoFrame(t, neighbor)
	frame, _, _ := recvEvent(t, exact)
	assert.Contains(t, string(frame.Data), "10000000000000001",
		"the wire frame carries the exact digits, not a float64 rounding")
}

// TestHub_RowFilter_TimestampInstantMatch: since #402, ingest canonicalizes
// DateTime/DateTime64 payload values to RFC 3339 UTC before publish, while policy
// authors write the ClickHouse-friendly zone-less spelling the query path wants.
// The row filter compares the two as instants through the discovery grammar (one
// parser shared with canonicalization), so the spellings agree; an operand the
// grammar can't read withholds the row.
func TestHub_RowFilter_TimestampInstantMatch(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{
			{Name: "created_at", Type: "DateTime"},
			{Name: "page", Type: "String"},
		}},
	})
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				// Zone-less constant, read in the column's zone (UTC here) on both
				// surfaces — the one spelling that works for the query path's SQL too.
				"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"created_at": {Eq: new("2026-06-21 04:00:00")}}}},
			},
		},
	}
	hub := NewHub(policy.Static(p), reg, nil)
	const topic = "ingest.clicks"

	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", sub)

	// The canonical wire spelling ingest publishes: same instant, different bytes.
	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"created_at": "2026-06-21T04:00:00Z", "page": "/a"}))
	f, _, _ := recvEvent(t, sub)
	assert.NotEmpty(t, f.Data, "canonical payload matches the zone-less constant as an instant")

	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"created_at": "2026-06-21T04:00:01Z", "page": "/a"}))
	assertNoFrame(t, sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t3", map[string]any{"created_at": "not a timestamp", "page": "/a"}))
	assertNoFrame(t, sub)
}

// TestHub_RowFilterWithheldIncrementsMetric: a row withheld by row-level security is
// otherwise invisible to operators (it is not a dropped frame — the queue was never
// tried). The wavehouse_sse_rows_withheld_total counter must tick for live fan-out
// and replay withholds alike, so "no matching rows" and "a filter is withholding
// everything" are distinguishable.
func TestHub_RowFilterWithheldIncrementsMetric(t *testing.T) {
	// No t.Parallel(): NewMetrics binds the global meter provider, swapped here.
	savedMP := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(savedMP)
	})

	hub := NewHub(policy.Static(rowFilterPolicy()), nil, NewMetrics())
	const topic = "ingest.clicks"
	acme := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	globex := NewSubscriber(map[string]any{"tenant": "globex"}, nil)
	hub.Add(topic, "viewer", acme)
	hub.Add(topic, "viewer", globex)

	raw := rawEvent(t, "clicks", "t", map[string]any{"tenant_id": "acme", "page": "/a"})
	hub.Broadcast(topic, raw) // delivered to acme, withheld from globex → 1

	frames := hub.ReplayProjector("viewer", NewSubscriber(map[string]any{"tenant": "globex"}, nil))(raw)
	require.Empty(t, frames) // replay withhold → 2

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	assert.Equal(t, int64(2), sumByName(rm, "wavehouse_sse_rows_withheld_total"))
	f, _, _ := recvEvent(t, acme)
	assert.NotEmpty(t, f.Data, "the entitled subscriber still gets the event")
	assertNoFrame(t, globex)
}

// BenchmarkBroadcast_RowFilteredFanout measures the per-subscriber cost a
// row-filtered role pays on the delivery hot path (#294/#353 vs #319): each
// subscriber's claims run through policy.Evaluate + RowVisible per event, where an
// unfiltered role shares one projection bucket-wide. Half the subscribers share the
// event's tenant (row visible), half don't (row withheld); either way each pays the
// per-subscriber evaluation, which is the cost under measurement. See #435 for the
// memoization follow-up this benchmark exists to arbitrate.
func BenchmarkBroadcast_RowFilteredFanout(b *testing.B) {
	const topic = "ingest.clicks"
	raw := rawEvent(b, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"})

	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			hub := NewHub(policy.Static(rowFilterPolicy()), nil, nil)
			subs := make([]*Subscriber, n)
			for i := range n {
				tenant := "acme"
				if i%2 == 1 {
					tenant = "globex"
				}
				subs[i] = NewSubscriber(map[string]any{"tenant": tenant}, nil)
				hub.Add(topic, "viewer", subs[i])
			}
			b.ReportAllocs()
			for b.Loop() {
				hub.Broadcast(topic, raw)
				// Drain the delivered frames so every iteration measures successful
				// row-filtered delivery: without this, queues fill after 64 events and
				// later iterations measure the dropped-send path instead. The drain is
				// one buffered-channel receive per visible subscriber — noise next to
				// the per-subscriber policy evaluation under measurement.
				for _, s := range subs {
					for len(s.out) > 0 {
						<-s.out
					}
				}
			}
		})
	}
}

// TestDecodeEvent_RequiresCleanEOF: Decoder.More is not an end-of-input check —
// it reports false for a trailing "}" or "]" without consuming it — so decodeEvent
// must read the decoder to io.EOF or a valid event followed by a stray delimiter
// would be accepted where json.Unmarshal (whose strictness this path preserves)
// rejects it.
func TestDecodeEvent_RequiresCleanEOF(t *testing.T) {
	t.Parallel()
	const valid = `{"table_name":"clicks","received_timestamp":"t","format":"JSONCompactEachRow","columns":["a"],"row":[1]}`
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{"clean event", valid, true},
		{"trailing whitespace", valid + " \n", true},
		{"trailing close-brace", valid + "}", false},
		{"trailing close-bracket", valid + "]", false},
		{"trailing second value", valid + " 42", false},
		{"trailing garbage", valid + "garbage", false},
		{"missing table name", `{"received_timestamp":"t","format":"JSONCompactEachRow","columns":["a"],"row":[1]}`, false},
		{"not json", "not json", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var evt ingest.EventMessage
			assert.Equal(t, tt.want, decodeEvent([]byte(tt.raw), &evt))
		})
	}
}

func TestWireFrame(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		id      string
		payload string
		want    string
	}{
		{"compact payload is one data line", "2026-06-26T00:00:00Z", `{"a":1}`, "id: 2026-06-26T00:00:00Z\ndata: {\"a\":1}\n\n"},
		{"blank id (passthrough)", "", `{"a":1}`, "id: \ndata: {\"a\":1}\n\n"},
		{"newline in payload starts a fresh data line", "", "line1\nline2", "id: \ndata: line1\ndata: line2\n\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, string(wireFrame(tt.id, []byte(tt.payload))))
		})
	}
}

// sumByNameKind totals the named Int64 sum instrument's datapoints carrying
// the given kind attribute — pins that a drop is labeled with the dropped
// frame's own kind, which sumByName's across-kinds total can't see.
func sumByNameKind(rm metricdata.ResourceMetrics, name, kind string) int64 {
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != name {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			if !ok {
				return 0
			}
			var total int64
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key("kind")); ok && v.AsString() == kind {
					total += dp.Value
				}
			}
			return total
		}
	}
	return 0
}

// sumByName totals all datapoints of an Int64 sum instrument across kinds.
func sumByName(rm metricdata.ResourceMetrics, name string) int64 {
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != name {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			if !ok {
				return 0
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

// TestHub_SchemaFrame_AnnouncedOncePerConnection: rows travel positionally, so a
// connection must be told the column list before its first row — and must NOT be
// told again while the list is unchanged, which would be pure noise on a busy
// stream.
func TestHub_SchemaFrame_AnnouncedOncePerConnection(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	const topic = "ingest.clicks"
	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "public", sub)

	hub.Broadcast(topic, rawEventCols(t, "clicks", "t1", []string{"page", "country"},
		map[string]any{"page": "/a", "country": "US"}))
	first, cols, row := recvEvent(t, sub)
	assert.Equal(t, []string{"page", "country"}, cols)
	assert.Equal(t, "/a", row["page"])
	assert.Equal(t, "US", row["country"])
	assert.True(t, strings.HasPrefix(string(first.Data), "id: t1\ndata: "),
		"the data frame keeps the id: framing that drives Last-Event-ID")

	hub.Broadcast(topic, rawEventCols(t, "clicks", "t2", []string{"page", "country"},
		map[string]any{"page": "/b", "country": "GB"}))
	_, row2 := recvEventCols(t, sub, cols)
	assert.Equal(t, "/b", row2["page"])
	assertNoFrame(t, sub)
}

// TestHub_SchemaFrame_ReannouncedOnDrift: a schema change mid-stream moves the
// positions, so the new list must be announced before the first row that uses
// it — otherwise a client zips values under the wrong names.
func TestHub_SchemaFrame_ReannouncedOnDrift(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	const topic = "ingest.clicks"
	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "public", sub)

	hub.Broadcast(topic, rawEventCols(t, "clicks", "t1", []string{"page"},
		map[string]any{"page": "/a"}))
	_, cols, _ := recvEvent(t, sub)
	assert.Equal(t, []string{"page"}, cols)

	// A column is added: same table, new positions.
	hub.Broadcast(topic, rawEventCols(t, "clicks", "t2", []string{"page", "country"},
		map[string]any{"page": "/b", "country": "US"}))
	_, cols2, row2 := recvEvent(t, sub)
	assert.Equal(t, []string{"page", "country"}, cols2)
	assert.Equal(t, "US", row2["country"])

	// Back to the original list: announced again, because the client's last
	// list is the wider one.
	hub.Broadcast(topic, rawEventCols(t, "clicks", "t3", []string{"page"},
		map[string]any{"page": "/c"}))
	_, cols3, row3 := recvEvent(t, sub)
	assert.Equal(t, []string{"page"}, cols3)
	assert.Equal(t, "/c", row3["page"])
}

// TestHub_SchemaFrame_PerConnectionNotPerRole: two subscribers of the same role
// each get their own announcement — a connection that joins mid-stream must be
// told the column list even though an earlier subscriber already was.
func TestHub_SchemaFrame_PerConnectionNotPerRole(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	const topic = "ingest.clicks"
	early := NewSubscriber(nil, nil)
	hub.Add(topic, "public", early)

	raw := rawEventCols(t, "clicks", "t1", []string{"page"}, map[string]any{"page": "/a"})
	hub.Broadcast(topic, raw)
	_, cols, _ := recvEvent(t, early)

	late := NewSubscriber(nil, nil)
	hub.Add(topic, "public", late)
	hub.Broadcast(topic, raw)

	_, lateCols, _ := recvEvent(t, late)
	assert.Equal(t, cols, lateCols, "the late joiner is told the same list")
	_, _ = recvEventCols(t, early, cols) // the early one is not told again
	assertNoFrame(t, early)
}

// TestHub_SubscribeSchemaFrame: a connection is told the column list at
// subscribe time, from the registry, so a client on a quiet table knows the
// shape before any row arrives — and the first row that does arrive does not
// repeat the announcement.
func TestHub_SubscribeSchemaFrame(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{
			{Name: "page", Type: "String"},
			{Name: "secret", Type: "String"},
		}},
	})
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}}},
		},
	}
	hub := NewHub(policy.Static(p), reg, nil)

	sub := NewSubscriber(nil, nil)
	f, ok := hub.SubscribeSchemaFrame("clicks", "viewer", sub)
	require.True(t, ok)
	assert.Equal(t, []string{"page"}, frameColumns(t, f), "announced list is the role's projection")

	// The first live event does not re-announce what the connection was told.
	hub.Add("ingest.clicks", "viewer", sub)
	hub.Broadcast("ingest.clicks", rawEventCols(t, "clicks", "t1", []string{"page", "secret"},
		map[string]any{"page": "/a", "secret": "x"}))
	data, row := recvEventCols(t, sub, []string{"page"})
	assert.Equal(t, "/a", row["page"])
	assert.NotContains(t, string(data.Data), "secret")
}

// TestHub_SubscribeSchemaFrame_NothingToAnnounce: with no registry, no schema
// for the table, or a role that can't read it, there is nothing to send at
// subscribe time — not an error, since the event path still announces before
// the first row.
func TestHub_SubscribeSchemaFrame_NothingToAnnounce(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{{Name: "page", Type: "String"}}},
	})
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{AllowColumns: []string{"page"}}}},
		},
	}
	tests := []struct {
		name  string
		hub   *Hub
		table string
		role  string
	}{
		{"no registry", NewHub(policy.Static(p), nil, nil), "clicks", "viewer"},
		{"unknown table", NewHub(policy.Static(p), reg, nil), "missing", "viewer"},
		{"role cannot read the table", NewHub(policy.Static(p), reg, nil), "clicks", "stranger"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ok := tt.hub.SubscribeSchemaFrame(tt.table, tt.role, NewSubscriber(nil, nil))
			assert.False(t, ok)
		})
	}
}

// TestHub_DataFrame_PreservesRawCellBytes: the outgoing row copies the published
// cells verbatim rather than re-encoding them, so a 64-bit id past 2^53 reaches
// the client with every digit — a float64 round trip would round it.
func TestHub_DataFrame_PreservesRawCellBytes(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	const topic = "ingest.clicks"
	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "public", sub)

	hub.Broadcast(topic, []byte(`{"table_name":"clicks","received_timestamp":"t",`+
		`"format":"JSONCompactEachRow","columns":["id","ratio"],"row":[9007199254740993,1.500]}`))

	f, cols, _ := recvEvent(t, sub)
	assert.Equal(t, []string{"id", "ratio"}, cols)
	assert.Contains(t, string(f.Data), "9007199254740993")
	assert.Contains(t, string(f.Data), "1.500", "trailing zeros survive because the bytes are copied")
}

// TestHub_ReplayProjector_SchemaTrackingIsIndependentOfLive: replay writes
// straight to the socket while live events queue behind it, so a live event
// claiming the connection's announcement slot must not leave a replayed row
// ahead of it with nothing to zip against. Replay announces on its own.
func TestHub_ReplayProjector_SchemaTrackingIsIndependentOfLive(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	sub := NewSubscriber(nil, nil)
	raw := rawEventCols(t, "clicks", "t1", []string{"page"}, map[string]any{"page": "/a"})

	// A live event records the connection's column list first...
	hub.Add("ingest.clicks", "public", sub)
	hub.Broadcast("ingest.clicks", raw)
	_, _, _ = recvEvent(t, sub)

	// ...and the gap-fill still announces before its first row.
	frames := hub.ReplayProjector("public", sub)(raw)
	require.Len(t, frames, 2)
	assert.Equal(t, []string{"page"}, frameColumns(t, frames[0]))
	assert.Equal(t, KindReplay, frames[1].Kind)
}

// TestHub_SchemaFrame_DroppedAnnouncementDropsItsRow: a full queue must not
// split an announcement from the row it describes. Dropping only the
// announcement would leave the client zipping that row against a stale column
// list — silent mislabeling, where a dropped row is a visible gap. The next
// event announces again, so a transient full queue doesn't break the connection
// for good.
func TestHub_SchemaFrame_DroppedAnnouncementDropsItsRow(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	const topic = "ingest.clicks"
	sub := newSubscriber(1, nil) // cap-1: room for the announcement, not the row
	hub.Add(topic, "public", sub)

	// Occupy the only slot, so this event's announcement is the frame that drops.
	require.True(t, sub.Send(Frame{Kind: KindKeepalive, Data: []byte(": x\n\n")}))
	hub.Broadcast(topic, rawEventCols(t, "clicks", "t1", []string{"page"}, map[string]any{"page": "/a"}))

	assert.Equal(t, ": x\n\n", string(recvFrame(t, sub).Data),
		"the row is withheld with its dropped announcement, not delivered alone")
	assertNoFrame(t, sub)

	// Nothing was recorded, so the drained connection is announced to again
	// rather than being sent rows it has no column list for.
	hub.Broadcast(topic, rawEventCols(t, "clicks", "t2", []string{"page"}, map[string]any{"page": "/b"}))
	assert.Equal(t, []string{"page"}, frameColumns(t, recvFrame(t, sub)),
		"the next event re-announces, so a transient full queue is recoverable")
}

// TestHub_SubscribeSchemaFrame_ExcludesComputedColumns: the connect-time
// announcement comes from the registry while every event's list comes from the
// envelope, which carries only insertable columns. If the two disagreed, the
// very first event would force a pointless drift re-announcement.
