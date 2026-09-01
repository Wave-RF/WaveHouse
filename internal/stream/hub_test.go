package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// rawEvent marshals an EventMessage the way the ingest path publishes it.
// testing.TB so the fan-out benchmark can build events too.
func rawEvent(tb testing.TB, table, ts string, data map[string]any) []byte {
	tb.Helper()
	raw, err := json.Marshal(ingest.EventMessage{TableName: table, ReceivedTimestamp: ts, Data: data})
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

func TestHub_ProjectsOncePerRole_FanOutToAllSubscribers(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil) // nil store ⇒ passthrough, no filtering
	const topic = "ingest.clicks"

	a, b := NewSubscriber(nil, nil), NewSubscriber(nil, nil)
	hub.Add(topic, "public", a)
	hub.Add(topic, "public", b)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z", map[string]any{"page": "/home"}))

	fa, fb := recvFrame(t, a), recvFrame(t, b)
	assert.Equal(t, KindEvent, fa.Kind)
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
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page"}}, // only "page"
					// "blocked" has no entry ⇒ denied.
				},
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

	data := frameData(t, recvFrame(t, viewer))
	inner := data["data"].(map[string]any)
	assert.Equal(t, "/home", inner["page"])
	assert.NotContains(t, inner, "secret", "denied column stripped before serialization")

	// The denied role gets nothing pushed.
	select {
	case f := <-blocked.Frames():
		t.Fatalf("denied role must receive no frame, got %q", f.Data)
	default:
	}
}

// TestProjectColumns_FailsClosedOnUndecodedPayload is the #323 regression guard
// at the unit seam: with a policy configured (filter=true), a payload that did
// not decode to an EventMessage — so there is no table to evaluate policy
// against — must be dropped, never passed through unfiltered. Only the no-policy
// legacy passthrough (filter=false) may forward it, and invalid JSON is dropped
// either way. TestHub_PassthroughAndFailClosed drives the same rule end-to-end
// through Broadcast.
func TestProjectColumns_FailsClosedOnUndecodedPayload(t *testing.T) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, ok := projectColumns(nil, tt.filter, "viewer", nil, tt.raw, false)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestHub_ProjectsPerRole_DistinctRolesGetDistinctFrames(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				Select: map[string]policy.RolePermissions{
					"viewer": {AllowColumns: []string{"page"}},           // page only
					"editor": {AllowColumns: []string{"page", "secret"}}, // page + secret
				},
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

	fv, fe := recvFrame(t, viewer), recvFrame(t, editor)
	vdata := frameData(t, fv)["data"].(map[string]any)
	edata := frameData(t, fe)["data"].(map[string]any)

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
				Select: map[string]policy.RolePermissions{
					"viewer": {
						AllowColumns: []string{"page"},
						Filter:       map[string]policy.Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}},
					},
				},
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
	inner := frameData(t, recvFrame(t, acme))["data"].(map[string]any)
	assert.Equal(t, "/a", inner["page"])
	assert.NotContains(t, inner, "tenant_id", "the filtered column is not in viewer's projection")
	assert.NotContains(t, inner, "secret", "denied column stripped")
	assertNoFrame(t, globex)
	// Unresolvable claim ⇒ no rows on the stream, matching the query path (#457).
	assertNoFrame(t, noTenant)

	// A globex row reaches only the globex subscriber.
	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"tenant_id": "globex", "page": "/g"}))
	assert.Equal(t, "/g", frameData(t, recvFrame(t, globex))["data"].(map[string]any)["page"])
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
			"clicks": {Select: map[string]policy.RolePermissions{
				"viewer": {Filter: map[string]policy.Filter{"tenant_id": {Eq: new("{{ jwt.org.tenant }}")}}},
			}},
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
	assert.Equal(t, "/g", frameData(t, recvFrame(t, sub))["data"].(map[string]any)["page"],
		"the snapshot keeps admitting the tenant the connection authenticated as")

	// The []any arm is just as authorization-relevant: an _in-shaped filter reads
	// array elements, so mutating a retained slice element must not move the
	// subscriber's row entitlement either.
	inPolicy := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {Select: map[string]policy.RolePermissions{
				"viewer": {Filter: map[string]policy.Filter{"tenant_id": {In: new("{{ jwt.tenants }}")}}},
			}},
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
	assert.Equal(t, "/g2", frameData(t, recvFrame(t, inSub))["data"].(map[string]any)["page"],
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

	fa, fb := recvFrame(t, a), recvFrame(t, b)
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
			"clicks": {Select: map[string]policy.RolePermissions{
				"viewer": {Filter: map[string]policy.Filter{"amount": {Gt: new("100")}}},
			}},
		},
	}
	hub := NewHub(policy.Static(p), reg, nil)
	const topic = "ingest.clicks"

	sub := NewSubscriber(nil, nil) // constant filter value ⇒ no claims needed
	hub.Add(topic, "viewer", sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"amount": float64(9), "page": "/a"}))
	assertNoFrame(t, sub) // 9 is not numerically > 100

	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"amount": float64(250), "page": "/b"}))
	assert.Equal(t, float64(250), frameData(t, recvFrame(t, sub))["data"].(map[string]any)["amount"])

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
			"clicks": {Select: map[string]policy.RolePermissions{
				"viewer": {Filter: map[string]policy.Filter{"score": {Gt: new("16777216")}}},
			}},
		},
	}
	hub := NewHub(policy.Static(p), reg, nil)
	const topic = "ingest.clicks"
	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"score": json.Number("16777217")}))
	assertNoFrame(t, sub) // stores as 16777216: not greater once both operands narrow

	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"score": json.Number("16777218")}))
	assert.Equal(t, float64(16777218), frameData(t, recvFrame(t, sub))["data"].(map[string]any)["score"])
}

func TestHub_TopicIsolation(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	clicks, views := NewSubscriber(nil, nil), NewSubscriber(nil, nil)
	hub.Add("ingest.clicks", "public", clicks)
	hub.Add("ingest.views", "public", views)

	hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "t", map[string]any{"a": float64(1)}))

	assert.NotEmpty(t, recvFrame(t, clicks).Data)
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
// it. The unit seam is TestProjectColumns_FailsClosedOnUndecodedPayload; the
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
	sub := newSubscriber(1, m) // cap-1: the second undrained broadcast drops; the shared seam NewSubscriber wires metrics through
	hub.Add(topic, "public", sub)

	raw := rawEvent(t, "clicks", "t", map[string]any{"a": float64(1)})
	hub.Broadcast(topic, raw) // fills the queue
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
			"clicks": {Select: map[string]policy.RolePermissions{"viewer": {AllowColumns: []string{"page"}}}},
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
			[]byte(`{"table_name":"","data":{"page":"/home"}}`), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, ok := hub.ReplayProjector(tt.role, nil)(tt.raw)
			require.Equal(t, tt.want, ok)
			if !tt.want {
				return
			}
			assert.Equal(t, KindReplay, f.Kind)
			inner := frameData(t, f)["data"].(map[string]any)
			assert.Equal(t, "/home", inner["page"])
			assert.NotContains(t, inner, "secret")
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
		project := hub.ReplayProjector("viewer", map[string]any{"tenant": "acme"})
		f, ok := project(raw)
		require.True(t, ok)
		assert.Equal(t, KindReplay, f.Kind)
		inner := frameData(t, f)["data"].(map[string]any)
		assert.Equal(t, "/a", inner["page"])
		assert.NotContains(t, inner, "secret", "denied column stripped on replay too")

		// The projector is reusable across a replay loop: a second event through the
		// same closure (cached column kinds) projects identically.
		f2, ok2 := project(raw)
		require.True(t, ok2)
		assert.Equal(t, f.Data, f2.Data)
	})

	t.Run("non-matching claims withhold the row", func(t *testing.T) {
		t.Parallel()
		_, ok := hub.ReplayProjector("viewer", map[string]any{"tenant": "globex"})(raw)
		require.False(t, ok, "row must be withheld when claims don't satisfy the filter")
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
			"clicks": {Select: map[string]policy.RolePermissions{
				"viewer": {Filter: map[string]policy.Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}}},
			}},
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

	raw := []byte(`{"table_name":"clicks","received_timestamp":"t","data":{"tenant_id":10000000000000001,"page":"/a"}}`)
	hub.Broadcast(topic, raw)

	assertNoFrame(t, neighbor)
	frame := recvFrame(t, exact)
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
			"clicks": {Select: map[string]policy.RolePermissions{
				// Zone-less constant, read in the column's zone (UTC here) on both
				// surfaces — the one spelling that works for the query path's SQL too.
				"viewer": {Filter: map[string]policy.Filter{"created_at": {Eq: new("2026-06-21 04:00:00")}}},
			}},
		},
	}
	hub := NewHub(policy.Static(p), reg, nil)
	const topic = "ingest.clicks"

	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", sub)

	// The canonical wire spelling ingest publishes: same instant, different bytes.
	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"created_at": "2026-06-21T04:00:00Z", "page": "/a"}))
	assert.NotEmpty(t, recvFrame(t, sub).Data, "canonical payload matches the zone-less constant as an instant")

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

	_, ok := hub.ReplayProjector("viewer", map[string]any{"tenant": "globex"})(raw)
	require.False(t, ok) // replay withhold → 2

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	assert.Equal(t, int64(2), sumByName(rm, "wavehouse_sse_rows_withheld_total"))
	assert.NotEmpty(t, recvFrame(t, acme).Data, "the entitled subscriber still gets the event")
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
	const valid = `{"table_name":"clicks","received_timestamp":"t","data":{"a":1}}`
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
		{"missing table name", `{"received_timestamp":"t","data":{"a":1}}`, false},
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
