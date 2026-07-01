package stream

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// rawEvent marshals an EventMessage the way the ingest path publishes it.
func rawEvent(t *testing.T, table, ts string, data map[string]any) []byte {
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
	hub := NewHub(nil, nil) // nil store ⇒ passthrough, no filtering
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
	hub := NewHub(policy.NewMemoryStore(p), nil)
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
	hub := NewHub(policy.NewMemoryStore(p), nil)
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

func TestHub_TopicIsolation(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil)
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
			hub := NewHub(tt.store, nil)
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
	hub := NewHub(nil, nil)
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
	hub := NewHub(nil, nil)
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

	hub := NewHub(nil, NewMetrics())
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
	hub := NewHub(policy.NewMemoryStore(p), nil)
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
			f, ok := hub.ReplayFrame(tt.role, raw)
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

func TestHub_ConcurrentAddRemoveBroadcast_Race(t *testing.T) {
	t.Parallel()
	hub := NewHub(nil, nil)
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
