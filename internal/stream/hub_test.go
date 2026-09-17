package stream

import (
	"bytes"
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
	"github.com/Wave-RF/WaveHouse/internal/typelayer"
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

// TestBroadcast_DuplicateColumnWithheld: an envelope whose columns name one
// column twice cannot be paired — the name-keyed map would keep the last value
// and silently drop the first, and that map is what a row-level filter is
// evaluated against, so a duplicate could decide visibility on a value the row
// never carried. Withheld from every role rather than delivered under a guessed
// reading, the same as a length mismatch.
//
// A nil policy store (passthrough, no filtering) and a positive control are
// both load-bearing: with a restrictive policy the row would be withheld for
// the wrong reason, and the control proves this hub delivers a well-formed
// envelope on the same connection.
//
// Unreachable from our own producer — the envelope's columns come from
// system.columns, where ClickHouse forbids two columns of one name — so this
// pins the defence for an envelope we did not write.
func TestBroadcast_DuplicateColumnWithheld(t *testing.T) {
	t.Parallel()
	const topic = "ingest.clicks"
	hub := NewHub(nil, nil, nil) // nil store ⇒ passthrough, so nothing else withholds
	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", sub)

	dup, err := json.Marshal(ingest.EventMessage{
		TableName:         "clicks",
		ReceivedTimestamp: "2026-06-26T00:00:00Z",
		Format:            ingest.FormatJSONCompactEachRow,
		Columns:           []string{"tenant", "tenant"},
		Row:               json.RawMessage(`["a","b"]`),
	})
	require.NoError(t, err)

	hub.Broadcast(topic, dup)
	assertNoFrame(t, sub)

	// Positive control: the same hub, same subscriber, a well-formed envelope.
	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"page": "/a"}))
	_, _, row := recvEvent(t, sub)
	assert.Equal(t, "/a", row["page"], "the hub delivers a pairable envelope on this connection")
}

// TestReplayProjector_UnpairableWithheld: the gap-fill path withholds an
// envelope that cannot be paired, the same as the live path above. Replay
// projects per connection rather than per role, so it is a separate seam with
// its own chance to deliver values under guessed column names — and the harder
// one to notice, since the client asked for a range and a short answer looks
// like an empty range.
//
// The pairable control is load-bearing: without it a projector that returned no
// frames for EVERY envelope would pass.
func TestReplayProjector_UnpairableWithheld(t *testing.T) {
	hub := NewHub(nil, nil, NewMetrics())
	project := hub.ReplayProjector("viewer", NewSubscriber(nil, nil))

	bad, err := json.Marshal(ingest.EventMessage{
		TableName:         "clicks",
		ReceivedTimestamp: "2026-06-26T00:00:00Z",
		Format:            ingest.FormatJSONCompactEachRow,
		Columns:           []string{"tenant", "tenant"},
		Row:               json.RawMessage(`["a","b"]`),
	})
	require.NoError(t, err)

	assert.Empty(t, project(bad), "a duplicate column name is unpairable: no frame, not a guessed reading")
	assert.NotEmpty(t, project(rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"page": "/a"})), "this projector does gap-fill a well-formed envelope")
}

// TestEventView_UnknownFormatWithheld: the worker refuses an envelope whose
// declared row format it does not know (ingest.parseMsg) and parks it on the
// DLQ. The hub consumes the same subject on its own consumer and acks
// independently, so without the matching refusal here those bytes would reach
// SSE clients as a normal row while never landing in ClickHouse — the two
// readers disagreeing about what the envelope means.
//
// Unreachable from our own producer today, which writes the constant: this
// pins the behaviour for the second format, which is what the field is for.
// The columns and row here pair perfectly, so ONLY the format can withhold it,
// and the positive control proves the withholding is the format's doing.
func TestEventView_UnknownFormatWithheld(t *testing.T) {
	const topic = "ingest.clicks"
	hub := NewHub(nil, nil, NewMetrics())
	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", sub)

	envelope := func(format string) []byte {
		raw, err := json.Marshal(ingest.EventMessage{
			TableName:         "clicks",
			ReceivedTimestamp: "2026-06-26T00:00:00Z",
			Format:            format,
			Columns:           []string{"page"},
			Row:               json.RawMessage(`["/a"]`),
		})
		require.NoError(t, err)
		return raw
	}

	hub.Broadcast(topic, envelope("JSONEachRow"))
	assertNoFrame(t, sub)
	assert.Empty(t, hub.ReplayProjector("viewer", sub)(envelope("JSONEachRow")),
		"the gap-fill path refuses it too, on the same grounds")

	hub.Broadcast(topic, envelope(ingest.FormatJSONCompactEachRow))
	_, _, row := recvEvent(t, sub)
	assert.Equal(t, "/a", row["page"], "the identical envelope is delivered under the format we do know")
}

// TestPairRow_Verdict enumerates the shapes that cannot be paired. Each one has
// to fail closed: the announced column list is what the client zips the
// positional row against, and it is also the list the type layer reads the row
// under, so a shape where the two cannot be lined up would decide visibility —
// and label values — on data the row never carried. The pairable case is the
// control that keeps the other seven honest: a pairRow that rejected everything
// would satisfy them alone.
func TestPairRow_Verdict(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		cols []string
		row  string
		ok   bool
	}{
		{"duplicate column", []string{"tenant", "tenant"}, `["a","b"]`, false},
		{"length mismatch", []string{"page", "button"}, `["/a"]`, false},
		{"undecodable row", []string{"page"}, `"not-an-array"`, false},
		{"empty row", []string{"page"}, ``, false},
		// A zero-column envelope pairs with anything of length zero, so BOTH
		// spellings have to be refused: `null` unmarshals to a nil slice and `[]`
		// to an empty one, and a length check alone accepts each. There is no
		// positional row over no columns, and the worker refuses the same shape.
		{"zero columns, null row", nil, `null`, false},
		{"zero columns, empty row", []string{}, `[]`, false},
		{"null row against real columns", []string{"page"}, `null`, false},
		{"pairable", []string{"page"}, `["/a"]`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cells, ok := pairRow(tt.cols, json.RawMessage(tt.row))
			assert.Equal(t, tt.ok, ok)
			if !tt.ok {
				assert.Nil(t, cells, "an unpairable envelope yields no cells to splice into a frame")
			}
		})
	}
}

// rawEventCols is rawEvent with an explicit column order, for tests that pin a
// declaration order or publish a column the record omits.
func rawEventCols(tb testing.TB, table, ts string, cols []string, data map[string]any) []byte {
	tb.Helper()
	// The positional line the ingest path publishes: one cell per column, in
	// order, a column the record omits as null. Built here rather than through a
	// production encoder because the wire shape is what these tests pin.
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, c := range cols {
		if i > 0 {
			buf.WriteByte(',')
		}
		v, ok := data[c]
		if !ok {
			buf.WriteString("null")
			continue
		}
		b, err := json.Marshal(v)
		require.NoError(tb, err)
		buf.Write(b)
	}
	buf.WriteByte(']')
	row := json.RawMessage(buf.Bytes())
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

// chtypesTable declares a table the way discovery would, numbering the columns
// in the order given. That order IS the wire order: an event's envelope carries
// the same list, and a row published under any other one is drift.
func chtypesTable(name string, cols ...discovery.Column) *discovery.TableSchema {
	for i := range cols {
		cols[i].Position = uint64(i + 1)
	}
	return &discovery.TableSchema{Name: name, Columns: cols}
}

// col is chtypesTable's shorthand; every column here is an ordinary stored one.
func col(name, chType string) discovery.Column {
	return discovery.Column{Name: name, Type: chType}
}

// chtypesHub is a Hub whose row filtering is decided the way production decides
// it: by the type layer, against the real ClickHouse 26.6 artifact. Every
// row-filter test goes through this rather than a stub, because the verdicts
// under test ARE ClickHouse's — storage-domain narrowing, instant equality
// across spellings, exactness past 2^53 — and a stub could only restate what
// the test already believes.
func chtypesHub(tb testing.TB, store policy.Source, metric *Metrics, tables ...*discovery.TableSchema) *Hub {
	tb.Helper()
	hub := NewHub(store, nil, metric)
	hub.RowEvaluator = NewRowEvaluator(newTestEngine(tb, tables...), nil)
	return hub
}

// engineBuild serializes Engine construction. typelayer.Engine.Bind sets
// chtypes' process-global Timezone under its OWN lock, so two Engines opened
// concurrently write it at once — harmless (both write "UTC") but a -race
// report, and these tests are parallel. Production has exactly one Engine and
// never hits it; the durable fix belongs in typelayer, not here.
var engineBuild sync.Mutex

func newTestEngine(tb testing.TB, tables ...*discovery.TableSchema) *typelayer.Engine {
	tb.Helper()
	engineBuild.Lock()
	defer engineBuild.Unlock()
	return typelayer.TestEngine(tb, tables...)
}

// clicksTable is the table the tenant-scoping tests publish into: the filtered
// column, the readable one, and one the role may not select. Declaration order
// matches the order rawEvent publishes (sorted by name).
func clicksTable() *discovery.TableSchema {
	return chtypesTable("clicks", col("page", "String"), col("secret", "String"), col("tenant_id", "String"))
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
	hub := chtypesHub(t, policy.Static(rowFilterPolicy()), nil, clicksTable())
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

	// A globex row reaches only the globex subscriber. Every event on a table
	// carries that table's full column list, so "secret" is present here too.
	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"tenant_id": "globex", "page": "/g", "secret": "y"}))
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
	tenantTable := chtypesTable("clicks", col("page", "String"), col("tenant_id", "String"))
	hub := chtypesHub(t, policy.Static(p), nil, tenantTable)
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
	inHub := chtypesHub(t, policy.Static(inPolicy), nil, tenantTable)
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

// TestHub_RowFilter_ColumnsDrift_FailsClosed: an event whose column list is not
// the one the compiled generation exports cannot be read positionally at all —
// the filtered column could be at any offset, or absent — so it is withheld
// rather than guessed at. With positional rows this subsumes the old "event
// lacks the filtered column" case: a row missing a column IS a different column
// list. The control proves the withholding is the drift's doing and not a
// permanently silent hub.
func TestHub_RowFilter_ColumnsDrift_FailsClosed(t *testing.T) {
	t.Parallel()
	hub := chtypesHub(t, policy.Static(rowFilterPolicy()), nil, clicksTable())
	const topic = "ingest.clicks"

	acme := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	hub.Add(topic, "viewer", acme)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"page": "/a"})) // a one-column envelope: not this generation's
	assertNoFrame(t, acme)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"page": "/a", "secret": "x", "tenant_id": "acme"}))
	_, _, row := recvEvent(t, acme)
	assert.Equal(t, "/a", row["page"], "the generation's own column list is delivered")
}

// TestHub_RowFilter_SharedProjectionAcrossSameClaims: the column projection is still
// serialized once per role and shared — two subscribers with the same (matching)
// claims receive the identical frame bytes. Only the visibility decision is
// per-subscriber, not the serialization.
func TestHub_RowFilter_SharedProjectionAcrossSameClaims(t *testing.T) {
	t.Parallel()
	hub := chtypesHub(t, policy.Static(rowFilterPolicy()), nil, clicksTable())
	const topic = "ingest.clicks"

	a := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	b := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	hub.Add(topic, "viewer", a)
	hub.Add(topic, "viewer", b)

	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"}))

	fa, _, _ := recvEvent(t, a)
	fb, _, _ := recvEvent(t, b)
	require.NotEmpty(t, fa.Data)
	require.NotEmpty(t, fb.Data)
	assert.Same(t, &fa.Data[0], &fb.Data[0], "one serialization shared across same-role subscribers")
}

// TestHub_RowFilter_NumericOrdering drives the type-layer path: on a UInt64
// column an `amount > 100` filter compares the way ClickHouse compares, so
// amount=9 is withheld (a lexicographic "9" > "100" would have leaked it) and
// amount=250 is delivered. With no type layer wired there is nothing that can
// answer the question at all, so every row is withheld — fail closed, never the
// lexicographic leak (the engineless window is real: a boot that cannot open an
// engine still serves).
func TestHub_RowFilter_NumericOrdering(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"amount": {Gt: new("100")}}}}},
		},
	}
	hub := chtypesHub(t, policy.Static(p), nil,
		chtypesTable("clicks", col("amount", "UInt64"), col("page", "String")))
	const topic = "ingest.clicks"

	sub := NewSubscriber(nil, nil) // constant filter value ⇒ no claims needed
	hub.Add(topic, "viewer", sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"amount": float64(9), "page": "/a"}))
	assertNoFrame(t, sub) // 9 is not numerically > 100

	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"amount": float64(250), "page": "/b"}))
	_, _, row := recvEvent(t, sub)
	assert.Equal(t, float64(250), row["amount"])

	// Same policy, no type layer: an ordering predicate can't be answered either
	// way, so both rows are withheld — including the one the wired path delivers.
	noEngine := NewHub(policy.Static(p), nil, nil)
	blind := NewSubscriber(nil, nil)
	noEngine.Add(topic, "viewer", blind)
	noEngine.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"amount": float64(9), "page": "/a"}))
	noEngine.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"amount": float64(250), "page": "/b"}))
	assertNoFrame(t, blind)
}

// TestHub_RowFilter_FloatNarrowing drives storage-domain narrowing end-to-end:
// on a Float32 column, payload 16777217 stores as 16777216, so a
// `_gt: "16777216"` filter must withhold the event — the query path's WHERE over
// the stored row is false, and delivering the pre-narrowing payload was the
// ordering fail-open raised in review. A Float32-representable greater value
// still delivers. The narrowing is no longer a Go re-derivation of ClickHouse's
// rule: the row is parsed into the column's real storage before anything is
// compared.
func TestHub_RowFilter_FloatNarrowing(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"score": {Gt: new("16777216")}}}}},
		},
	}
	hub := chtypesHub(t, policy.Static(p), nil, chtypesTable("clicks", col("score", "Float32")))
	const topic = "ingest.clicks"
	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"score": json.Number("16777217")}))
	assertNoFrame(t, sub) // stores as 16777216: not greater once both operands narrow

	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"score": json.Number("16777218")}))
	_, _, row := recvEvent(t, sub)
	assert.Equal(t, float64(16777218), row["score"])
}

// TestHub_RowFilter_Float32EqualityBindsInTheColumnsWidth: the constant has to
// be read in the COLUMN's float domain, not the widest one. A Float32 column
// stores 0.1 as 0.100000001490116…, which is not Float64's 0.1 — so binding the
// constant as Float64 made `= "0.1"` withhold the row and `!= "0.1"` ADMIT it,
// on a row /v1/query returns. Measured against a real 26.6 server in
// tests/integration/rowfilter_stream_test.go; pinned here so the regression
// costs a unit test rather than a Docker run.
func TestHub_RowFilter_Float32EqualityBindsInTheColumnsWidth(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		filter    policy.Filter
		delivered bool
	}{
		{"equality matches the stored Float32", policy.Filter{Eq: new("0.1")}, true},
		{"inequality does not", policy.Filter{Neq: new("0.1")}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &policy.Policy{Tables: map[string]policy.TablePolicy{
				"clicks": {"viewer": {Select: &policy.SelectPermissions{
					Filter: map[string]policy.Filter{"score": tt.filter},
				}}},
			}}
			hub := chtypesHub(t, policy.Static(p), nil, chtypesTable("clicks", col("score", "Float32")))
			sub := NewSubscriber(nil, nil)
			hub.Add("ingest.clicks", "viewer", sub)

			hub.Broadcast("ingest.clicks", rawEvent(t, "clicks", "t", map[string]any{"score": json.Number("0.1")}))
			if tt.delivered {
				f, _, _ := recvEvent(t, sub)
				assert.NotEmpty(t, f.Data)
			} else {
				assertNoFrame(t, sub)
			}
		})
	}
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
	hub := chtypesHub(t, policy.Static(rowFilterPolicy()), nil, clicksTable())
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
		// same closure projects identically — and does NOT re-announce a column
		// list the connection already has.
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
	hub := chtypesHub(t, policy.Static(rowFilterPolicy()), nil, clicksTable())
	const topic = "ingest.clicks"
	raw := rawEvent(t, "clicks", "t", map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"})

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
// exact digits all the way to the comparison, or the row filter compares a
// lossily-rounded value: tenant 10000000000000001's row would falsely equal a
// tenant claim of 10000000000000000 — float64 collapses the neighbors — and be
// delivered cross-tenant on the stream while the query path (ClickHouse stores the
// exact digits ingest forwarded) excludes it. The row bytes now go to ClickHouse's
// own parser untouched and the claim binds as a UInt64 parameter, so no Go float
// is on the path at all. The raw payload is hand-built — marshaling a Go float64
// would already have destroyed the value this test is about.
func TestHub_RowFilter_BigIntegerExact(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"tenant_id": {Eq: new("{{ jwt.tenant }}")}}}}},
		},
	}
	hub := chtypesHub(t, policy.Static(p), nil,
		chtypesTable("clicks", col("page", "String"), col("tenant_id", "UInt64")))
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

// TestHub_RowFilter_TimestampInstantMatch: the wire now carries ClickHouse's own
// rendering of a DateTime, and policy authors write the same zone-less spelling
// the query path wants. The filter compares them as instants because the row is
// parsed into the column's real storage before the predicate runs, so a
// different spelling of the same instant still matches; an operand the parser
// can't read withholds the row rather than guessing at it.
func TestHub_RowFilter_TimestampInstantMatch(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {
				// Zone-less constant, read in the column's zone (UTC here) on both
				// surfaces — the one spelling that works for the query path's SQL too.
				"viewer": {Select: &policy.SelectPermissions{Filter: map[string]policy.Filter{"created_at": {Eq: new("2026-06-21 04:00:00")}}}},
			},
		},
	}
	hub := chtypesHub(t, policy.Static(p), nil,
		chtypesTable("clicks", col("created_at", "DateTime"), col("page", "String")))
	const topic = "ingest.clicks"

	sub := NewSubscriber(nil, nil)
	hub.Add(topic, "viewer", sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"created_at": "2026-06-21 04:00:00", "page": "/a"}))
	f, cols, _ := recvEvent(t, sub)
	assert.NotEmpty(t, f.Data, "the wire rendering matches the zone-less constant")

	// A different spelling of the same instant matches too: the comparison is
	// between parsed instants, not between bytes.
	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"created_at": "2026-06-21T04:00:00Z", "page": "/a"}))
	f, _ = recvEventCols(t, sub, cols)
	assert.NotEmpty(t, f.Data)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t3", map[string]any{"created_at": "2026-06-21 04:00:01", "page": "/a"}))
	assertNoFrame(t, sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t4", map[string]any{"created_at": "not a timestamp", "page": "/a"}))
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

	hub := chtypesHub(t, policy.Static(rowFilterPolicy()), NewMetrics(), clicksTable())
	const topic = "ingest.clicks"
	acme := NewSubscriber(map[string]any{"tenant": "acme"}, nil)
	globex := NewSubscriber(map[string]any{"tenant": "globex"}, nil)
	hub.Add(topic, "viewer", acme)
	hub.Add(topic, "viewer", globex)

	raw := rawEvent(t, "clicks", "t", map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"})
	hub.Broadcast(topic, raw) // delivered to acme, withheld from globex → 1

	frames := hub.ReplayProjector("viewer", NewSubscriber(map[string]any{"tenant": "globex"}, nil))(raw)
	require.Empty(t, frames) // replay withhold → 2

	// A column list the compiled generation does not export: a FAULT, not a
	// filter verdict, and the label is the only thing that says so. It withholds
	// from BOTH subscribers — nothing about this row is readable — → 4.
	hub.Broadcast(topic, rawEvent(t, "clicks", "t", map[string]any{"page": "/a"}))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	assert.Equal(t, int64(4), sumByName(rm, "wavehouse_sse_rows_withheld_total"))
	assert.Equal(t, int64(2), sumByNameAttr(rm, "wavehouse_sse_rows_withheld_total", "reason", ReasonFilter),
		"the two claim mismatches are ordinary filtering")
	assert.Equal(t, int64(2), sumByNameAttr(rm, "wavehouse_sse_rows_withheld_total", "reason", ReasonDrift),
		"drift must be distinguishable from a policy decision")
	f, _, _ := recvEvent(t, acme)
	assert.NotEmpty(t, f.Data, "the entitled subscriber still gets the event")
	assertNoFrame(t, globex)
}

// BenchmarkBroadcast_RowFilteredFanout measures the per-subscriber cost a
// row-filtered role pays on the delivery hot path (#294/#353 vs #319): the row is
// parsed once per event, then each subscriber's claims run through
// policy.Evaluate and one compiled-predicate evaluation, where an unfiltered role
// shares one projection bucket-wide. Half the subscribers share the event's
// tenant (row visible), half don't (row withheld); either way each pays the
// per-subscriber evaluation, which is the cost under measurement. See #435 for
// the memoization follow-up this benchmark exists to arbitrate.
func BenchmarkBroadcast_RowFilteredFanout(b *testing.B) {
	const topic = "ingest.clicks"
	raw := rawEvent(b, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"})

	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			hub := chtypesHub(b, policy.Static(rowFilterPolicy()), nil, clicksTable())
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
// sumByNameAttr sums one counter's data points restricted to a single attribute
// value — the withheld counter's "reason" is a closed set, and the point of the
// label is that a fault does not read as a filter verdict.
func sumByNameAttr(rm metricdata.ResourceMetrics, name, key, want string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if v, found := dp.Attributes.Value(attribute.Key(key)); found && v.AsString() == want {
					total += dp.Value
				}
			}
		}
	}
	return total
}

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
func TestHub_SubscribeSchemaFrame_ExcludesComputedColumns(t *testing.T) {
	t.Parallel()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{
			{Name: "page", Type: "String"},
			{Name: "digest", Type: "String", DefaultKind: "MATERIALIZED", HasDefault: true},
			{Name: "country", Type: "String"},
		}},
	})
	hub := NewHub(nil, reg, nil)

	sub := NewSubscriber(nil, nil)
	f, ok := hub.SubscribeSchemaFrame("clicks", "public", sub)
	require.True(t, ok)
	cols := frameColumns(t, f)
	assert.Equal(t, []string{"page", "country"}, cols)

	// The first event announces nothing new, because the lists agree.
	hub.Add("ingest.clicks", "public", sub)
	hub.Broadcast("ingest.clicks", rawEventCols(t, "clicks", "t1", cols,
		map[string]any{"page": "/a", "country": "US"}))
	_, row := recvEventCols(t, sub, cols)
	assert.Equal(t, "/a", row["page"])
}
