package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// sseMetrics builds the stream metrics on a manual reader, the global meter
// provider swapped until the test ends: its callers must not call t.Parallel.
func sseMetrics(t *testing.T) (*stream.Metrics, func() map[string]int64) {
	t.Helper()
	saved := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(saved)
	})
	// dropped totals wavehouse_sse_dropped_frames_total by table and reason,
	// keyed "table/reason".
	dropped := func() map[string]int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		out := map[string]int64{}
		for _, sm := range rm.ScopeMetrics {
			for _, md := range sm.Metrics {
				if md.Name != "wavehouse_sse_dropped_frames_total" {
					continue
				}
				for _, dp := range md.Data.(metricdata.Sum[int64]).DataPoints {
					table, _ := dp.Attributes.Value(attribute.Key("table"))
					reason, _ := dp.Attributes.Value(attribute.Key("reason"))
					out[table.AsString()+"/"+reason.AsString()] += dp.Value
				}
			}
		}
		return out
	}
	return stream.NewMetrics(), dropped
}

// logLines decodes the captured JSON records whose msg is msg.
func logLines(t *testing.T, buf *logtest.Buffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	for sc.Scan() {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &rec))
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// slowClient lets the handshake and the schema frame through, then holds
// every write until release is closed: a client reading slower than events
// arrive.
type slowClient struct {
	*httptest.ResponseRecorder
	mu      sync.Mutex
	writes  int
	release chan struct{}
}

func (c *slowClient) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	opening := c.writes <= 2
	c.mu.Unlock()
	if !opening {
		<-c.release
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ResponseRecorder.Write(p)
}

// goneClient takes the handshake and the schema frame, then fails every
// write: a client that has disconnected.
type goneClient struct {
	*httptest.ResponseRecorder
	writes int
}

func (c *goneClient) Write(p []byte) (int, error) {
	c.writes++
	if c.writes > 2 {
		return 0, errors.New("broken pipe")
	}
	return c.ResponseRecorder.Write(p)
}

// Each stream's outbound queue is its tenant's stream.subscriber_queue, read
// as the stream opens: a smaller one drops sooner, and the queue state the
// slow-consumer line reports is that size.
func TestSSE_SubscriberQueueIsTheTenants(t *testing.T) {
	m, dropped := sseMetrics(t)
	logs := logtest.Capture(t, slog.LevelInfo)
	hub := stream.NewHub(nil, clicksRegistry(t), m)
	h := &StreamHandler{Hub: hub, Metrics: m, SubscriberQueue: func(s *settings.Store) int {
		require.Same(t, testStore, s, "the request tenant's store")
		return 8
	}}
	topic := mq.Topic{Tenant: tenant.Default, Table: "clicks"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &slowClient{ResponseRecorder: httptest.NewRecorder(), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(w, withTenant(httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)))
	}()
	require.Eventually(t, func() bool { return hub.Len(topic) == 1 }, 5*time.Second, 5*time.Millisecond)

	const events = 20
	for range events {
		hub.Broadcast(topic, []byte(`{"n":1}`))
	}
	// The queue holds 8; the handler may hold one more, blocked writing it.
	lost := dropped()["clicks/"+stream.DropBufferFull]
	assert.GreaterOrEqual(t, lost, int64(events-9))
	assert.LessOrEqual(t, lost, int64(events-8))

	close(w.release)
	require.Eventually(t, func() bool {
		return len(logLines(t, logs, "SSE stream dropping frames")) == 1
	}, 5*time.Second, 5*time.Millisecond)
	warn := logLines(t, logs, "SSE stream dropping frames")[0]
	assert.InDelta(t, 8, warn["queue_capacity"], 0)
	cancel()
	<-done
}

// A slow consumer's drops are counted as buffer_full under the streamed
// table, logged at WARN with its queue state, and its close line says how it
// ended and how much it lost.
func TestSSE_SlowConsumerDropsAreAttributed(t *testing.T) {
	m, dropped := sseMetrics(t)
	logs := logtest.Capture(t, slog.LevelInfo)
	hub := stream.NewHub(nil, clicksRegistry(t), m)
	h := &StreamHandler{Hub: hub, Metrics: m}
	topic := mq.Topic{Tenant: tenant.Default, Table: "clicks"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &slowClient{ResponseRecorder: httptest.NewRecorder(), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(w, withTenant(httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)))
	}()
	require.Eventually(t, func() bool { return hub.Len(topic) == 1 }, 5*time.Second, 5*time.Millisecond)

	const events = 100
	for range events {
		hub.Broadcast(topic, []byte(`{"n":1}`))
	}
	// The queue holds 64; the handler may hold one more, blocked writing it.
	lost := dropped()["clicks/"+stream.DropBufferFull]
	assert.GreaterOrEqual(t, lost, int64(events-65))
	assert.LessOrEqual(t, lost, int64(events-64))

	close(w.release)
	require.Eventually(t, func() bool {
		return len(logLines(t, logs, "SSE stream dropping frames")) == 1
	}, 5*time.Second, 5*time.Millisecond)
	warn := logLines(t, logs, "SSE stream dropping frames")[0]
	assert.Equal(t, "WARN", warn["level"])
	assert.Equal(t, "clicks", warn["table"])
	assert.InDelta(t, float64(lost), warn["frames_dropped"], 0)
	assert.InDelta(t, 64, warn["queue_peak"], 0)
	assert.InDelta(t, 64, warn["queue_capacity"], 0)

	cancel()
	<-done
	require.Len(t, logLines(t, logs, "SSE stream opened"), 1)
	closed := logLines(t, logs, "SSE stream closed")
	require.Len(t, closed, 1)
	assert.Equal(t, "WARN", closed[0]["level"], "a stream that dropped frames closes at WARN")
	assert.Equal(t, endClientGone, closed[0]["end"])
	assert.InDelta(t, float64(lost), closed[0]["frames_dropped_total"], 0)
}

// A frame the handler fails to write is counted as client_gone, not as a
// slow consumer's drop, and labeled with the table only when the schema has
// it: the name comes from the client.
func TestSSE_WriteFailureCountsClientGone(t *testing.T) {
	tests := []struct {
		table string
		label string
	}{
		{"clicks", "clicks"},
		{"no_such_table", ""},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			m, dropped := sseMetrics(t)
			logs := logtest.Capture(t, slog.LevelInfo)
			hub := stream.NewHub(nil, clicksRegistry(t), m)
			h := &StreamHandler{Hub: hub, Metrics: m}
			topic := mq.Topic{Tenant: tenant.Default, Table: tt.table}

			w := &goneClient{ResponseRecorder: httptest.NewRecorder()}
			if tt.label == "" {
				w.writes = 1 // no schema frame for a table the schema lacks
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				h.Handle(w, withTenant(httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/stream?table="+tt.table, nil)))
			}()
			require.Eventually(t, func() bool { return hub.Len(topic) == 1 }, 5*time.Second, 5*time.Millisecond)
			hub.Broadcast(topic, []byte(`{"n":1}`))
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("a failed write ends the stream")
			}

			assert.Equal(t, map[string]int64{tt.label + "/" + stream.DropClientGone: 1}, dropped())
			closed := logLines(t, logs, "SSE stream closed")
			require.Len(t, closed, 1)
			assert.Equal(t, "INFO", closed[0]["level"])
			assert.Equal(t, endClientGone, closed[0]["end"])
			assert.Equal(t, tt.table, closed[0]["table"], "the log names what the client asked for")
		})
	}
}

// clicksRegistry is a schema registry source whose every tenant has one
// table, clicks.
func clicksRegistry(t *testing.T) stream.RegistrySource {
	t.Helper()
	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{{Name: "n", Type: "Int64"}}},
	})
	return func(tenant.ID) *discovery.SchemaRegistry { return reg }
}

// What a stream the server ends still has queued is counted as
// stream_closed; what a departed client's has, as client_gone.
func TestSSE_CloseStreamCountsQueuedFramesByEnd(t *testing.T) {
	tests := []struct {
		end    string
		reason string
	}{
		{endShutdown, stream.DropStreamClosed},
		{endEvicted, stream.DropStreamClosed},
		{endClientGone, stream.DropClientGone},
	}
	for _, tt := range tests {
		t.Run(tt.end, func(t *testing.T) {
			m, dropped := sseMetrics(t)
			logs := logtest.Capture(t, slog.LevelInfo)
			sub := stream.NewTableSubscriber("clicks", nil, m, 0)
			sub.Send(stream.Frame{Kind: stream.KindEvent})
			sub.Send(stream.Frame{Kind: stream.KindKeepalive})

			(&StreamHandler{Metrics: m}).closeStream(context.Background(), sub, "clicks", "viewer", tt.end, time.Second)

			assert.Equal(t, map[string]int64{"clicks/" + tt.reason: 2}, dropped())
			closed := logLines(t, logs, "SSE stream closed")
			require.Len(t, closed, 1)
			assert.Equal(t, "INFO", closed[0]["level"], "no buffer_full drop, so no WARN")
			assert.Equal(t, tt.end, closed[0]["end"])
			assert.Equal(t, "viewer", closed[0]["role"])
			assert.InDelta(t, 2, closed[0]["frames_discarded"], 0)
		})
	}
}
