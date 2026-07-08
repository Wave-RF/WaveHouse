package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// rawEvent marshals an EventMessage the way the ingest path publishes it. It takes
// testing.TB so both tests (*testing.T) and benchmarks (*testing.B) can build events.
func rawEvent(t testing.TB, table, ts string, data map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(ingest.EventMessage{TableName: table, ReceivedTimestamp: ts, Data: data})
	require.NoError(t, err)
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

	a, b := NewSubscriber(), NewSubscriber()
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
	hub := NewHub(policy.NewMemoryStore(p), nil, nil)
	const topic = "ingest.clicks"

	viewer := NewSubscriber()
	blocked := NewSubscriber()
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
	hub := NewHub(policy.NewMemoryStore(p), nil, nil)
	const topic = "ingest.clicks"

	viewer, editor := NewSubscriber(), NewSubscriber()
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
// too, per subscriber.
func TestHub_RowFilter_PerSubscriberIsolation(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.NewMemoryStore(rowFilterPolicy()), nil, nil)
	const topic = "ingest.clicks"

	acme := NewSubscriber()
	acme.SetClaims(map[string]any{"tenant": "acme"})
	globex := NewSubscriber()
	globex.SetClaims(map[string]any{"tenant": "globex"})
	hub.Add(topic, "viewer", acme)
	hub.Add(topic, "viewer", globex)

	// An acme row reaches only the acme subscriber, projected to the allowed column.
	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"}))
	inner := frameData(t, recvFrame(t, acme))["data"].(map[string]any)
	assert.Equal(t, "/a", inner["page"])
	assert.NotContains(t, inner, "tenant_id", "the filtered column is not in viewer's projection")
	assert.NotContains(t, inner, "secret", "denied column stripped")
	assertNoFrame(t, globex)

	// A globex row reaches only the globex subscriber.
	hub.Broadcast(topic, rawEvent(t, "clicks", "2026-06-26T00:00:01Z",
		map[string]any{"tenant_id": "globex", "page": "/g"}))
	assert.Equal(t, "/g", frameData(t, recvFrame(t, globex))["data"].(map[string]any)["page"])
	assertNoFrame(t, acme)
}

// TestHub_RowFilter_MissingColumn_FailsClosed: an event that lacks the filtered
// column can't be proven visible, so it is withheld rather than leaked.
func TestHub_RowFilter_MissingColumn_FailsClosed(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.NewMemoryStore(rowFilterPolicy()), nil, nil)
	const topic = "ingest.clicks"

	acme := NewSubscriber()
	acme.SetClaims(map[string]any{"tenant": "acme"})
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
	hub := NewHub(policy.NewMemoryStore(rowFilterPolicy()), nil, nil)
	const topic = "ingest.clicks"

	a, b := NewSubscriber(), NewSubscriber()
	a.SetClaims(map[string]any{"tenant": "acme"})
	b.SetClaims(map[string]any{"tenant": "acme"})
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
// have leaked it) and amount=250 is delivered.
func TestHub_RowFilter_NumericOrdering_SchemaInformed(t *testing.T) {
	t.Parallel()
	reg := discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{
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
	hub := NewHub(policy.NewMemoryStore(p), reg, nil)
	const topic = "ingest.clicks"

	sub := NewSubscriber() // constant filter value ⇒ no claims needed
	hub.Add(topic, "viewer", sub)

	hub.Broadcast(topic, rawEvent(t, "clicks", "t1", map[string]any{"amount": float64(9), "page": "/a"}))
	assertNoFrame(t, sub) // 9 is not numerically > 100

	hub.Broadcast(topic, rawEvent(t, "clicks", "t2", map[string]any{"amount": float64(250), "page": "/b"}))
	assert.Equal(t, float64(250), frameData(t, recvFrame(t, sub))["data"].(map[string]any)["amount"])
}

func TestHub_TopicIsolation(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil, nil)
	clicks, views := NewSubscriber(), NewSubscriber()
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

func TestHub_PassthroughAndFailClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		store    *policy.Store
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
			store:   policy.NewMemoryStore(&policy.Policy{}),
			payload: `{"custom":"data","value":42}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hub := NewHub(tt.store, nil, nil)
			const topic = "ingest.custom"
			sub := NewSubscriber()
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
	sub := NewSubscriber()

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

	hub := NewHub(nil, nil, NewMetrics())
	const topic = "ingest.clicks"
	sub := newSubscriber(1) // cap-1: the second undrained broadcast drops
	hub.Add(topic, "public", sub)

	raw := rawEvent(t, "clicks", "t", map[string]any{"a": float64(1)})
	hub.Broadcast(topic, raw) // fills the queue
	hub.Broadcast(topic, raw) // dropped

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	assert.Equal(t, int64(1), sumByName(rm, "wavehouse_sse_dropped_frames_total"))
}

func TestHub_ReplayFrame(t *testing.T) {
	t.Parallel()
	p := &policy.Policy{
		Tables: map[string]policy.TablePolicy{
			"clicks": {Select: map[string]policy.RolePermissions{"viewer": {AllowColumns: []string{"page"}}}},
		},
	}
	hub := NewHub(policy.NewMemoryStore(p), nil, nil)
	raw := rawEvent(t, "clicks", "2026-06-26T00:00:00Z", map[string]any{"page": "/home", "secret": "x"})

	tests := []struct {
		name string
		role string
		want bool // whether a frame is produced (vs. skipped)
	}{
		{"allowed role projects with column filter", "viewer", true},
		{"role without table access is skipped", "stranger", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, ok := hub.ReplayFrame(tt.role, nil, raw)
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

// TestHub_ReplayFrame_RowFilter exercises the row-filter branch of ReplayFrame: the
// #319 fix applies row-level security on the per-connection replay path too, so a
// gap-fill event is projected only when the connection's claims satisfy the filter.
func TestHub_ReplayFrame_RowFilter(t *testing.T) {
	t.Parallel()
	hub := NewHub(policy.NewMemoryStore(rowFilterPolicy()), nil, nil)
	raw := rawEvent(t, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"})

	t.Run("matching claims replay the row, projected to allowed columns", func(t *testing.T) {
		t.Parallel()
		f, ok := hub.ReplayFrame("viewer", map[string]any{"tenant": "acme"}, raw)
		require.True(t, ok)
		assert.Equal(t, KindReplay, f.Kind)
		inner := frameData(t, f)["data"].(map[string]any)
		assert.Equal(t, "/a", inner["page"])
		assert.NotContains(t, inner, "secret", "denied column stripped on replay too")
	})

	t.Run("non-matching claims withhold the row", func(t *testing.T) {
		t.Parallel()
		_, ok := hub.ReplayFrame("viewer", map[string]any{"tenant": "globex"}, raw)
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
				sub := NewSubscriber()
				hub.Add(topic, role, sub)
				hub.Broadcast(topic, raw)
				hub.Remove(topic, role, sub)
			}
		}([]string{"public", "viewer"}[i%2])
	}
	wg.Wait()
	assert.Equal(t, 0, hub.Len(topic), "every subscriber is removed")
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

// BenchmarkBroadcast_RowFilteredFanout measures one Broadcast on a row-filtered
// topic as the subscriber count grows. Every subscriber of a filtered role triggers
// a per-subscriber policy.Evaluate + RowVisible on each event (hub.go Broadcast), so
// this exercises the O(subscribers) allocation path CodeRabbit flagged on PR #381.
// It isolates that fan-out cost: the shared column projection is built once per
// event, and full outbound queues merely drop (a nil-metric no-op), so the
// allocs/op reported here are dominated by the per-subscriber evaluation.
//
//	go test ./internal/stream/ -run '^$' -bench BenchmarkBroadcast_RowFilteredFanout -benchmem
func BenchmarkBroadcast_RowFilteredFanout(b *testing.B) {
	const topic = "ingest.clicks"
	raw := rawEvent(b, "clicks", "2026-06-26T00:00:00Z",
		map[string]any{"tenant_id": "acme", "page": "/a", "secret": "x"})

	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			hub := NewHub(policy.NewMemoryStore(rowFilterPolicy()), nil, nil)
			// Half the subscribers share the event's tenant (row visible), half don't
			// (row withheld); either way each still pays the per-subscriber Evaluate,
			// which is the cost under measurement.
			for i := range n {
				sub := NewSubscriber()
				tenant := "acme"
				if i%2 == 1 {
					tenant = "globex"
				}
				sub.SetClaims(map[string]any{"tenant": tenant})
				hub.Add(topic, "viewer", sub)
			}
			b.ReportAllocs()
			for b.Loop() {
				hub.Broadcast(topic, raw)
			}
		})
	}
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
