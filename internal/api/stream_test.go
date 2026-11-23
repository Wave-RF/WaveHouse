package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Per-role projection (column filtering, table denial, passthrough, the id: line)
// is exercised in internal/stream (hub_test.go, filter_test.go) now that the
// delivery hot path lives there. These tests cover the handler: request
// validation, the keepalive byte-pump, and wheel teardown.

func TestSSE_RejectsMissingOrInvalidTable(t *testing.T) {
	t.Parallel()
	h := &StreamHandler{Hub: stream.NewHub(nil, nil, nil)}

	cases := []struct {
		name    string
		table   string
		errBody string
	}{
		{"missing", "", "missing required query parameter: table"},
		{"empty after url decode", "", "missing required query parameter: table"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := "/v1/stream"
			if tc.table != "" {
				target += "?table=" + url.QueryEscape(tc.table)
			}
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
			w := httptest.NewRecorder()
			h.Handle(w, req)
			testutil.AssertJSONContains(t, w, http.StatusBadRequest, map[string]any{"error": tc.errBody})
			testutil.AssertJSONErrorResponse(t, w)
		})
	}
}

func TestSSE_AcceptsSafeTableName(t *testing.T) {
	t.Parallel()
	h := &StreamHandler{Hub: stream.NewHub(nil, nil, nil)}

	// Use a request context that's already cancelled so the handler exits
	// the live-stream select loop immediately instead of blocking the test.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(req))
	// Past the validation gate — header set to text/event-stream, not the
	// 400-path application/json.
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	// X-Accel-Buffering: no disables nginx-class proxy buffering so SSE events
	// flush in real time (the client never sees it — nginx strips it).
	assert.Equal(t, "no", w.Header().Get("X-Accel-Buffering"))
}

func TestSSE_EmitsHeartbeatsWhenIdle(t *testing.T) {
	t.Parallel()
	// A short period keeps the test fast; production tuning lives in config.
	// One bucket so the (sole) idle connection is pushed on every tick.
	hb := stream.NewHeartbeater(20*time.Millisecond, 1)
	go hb.Run(t.Context())

	h := &StreamHandler{Hub: stream.NewHub(nil, nil, nil), Heartbeater: fixedWheel(hb)}

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)
	w := httptest.NewRecorder()

	// Handle blocks in its select loop until the context is cancelled. Run it in
	// a goroutine, let a few heartbeat intervals elapse on an otherwise-idle
	// stream, then cancel and join before reading the buffer — so the test never
	// touches w concurrently with the handler.
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(w, withTenant(req))
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	body := w.Body.String()
	assert.Contains(t, body, ": connected\n\n", "handshake comment should be sent on open")
	// The keepalive is the minimal SSE comment ":\n\n"; the ": connected\n\n"
	// handshake has a space after the colon, so it isn't counted here.
	assert.GreaterOrEqual(t, strings.Count(body, ":\n\n"), 1,
		"an idle stream should emit at least one keepalive comment; body=%q", body)
}

// TestSSE_WheelTickRacesHandlerTeardown drives a fast keepalive wheel while many
// handlers connect and abruptly disconnect mid-stream. Under `go test -race` this
// exercises the wheel writing keepalives to a connection whose handler is tearing
// down — the "push to a dying connection" path — and asserts every connection
// deregisters from the wheel afterward (no leaked bucket membership).
func TestSSE_WheelTickRacesHandlerTeardown(t *testing.T) {
	t.Parallel()
	hb := stream.NewHeartbeater(6*time.Millisecond, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hb.Run(ctx)

	h := &StreamHandler{Hub: stream.NewHub(nil, nil, nil), Heartbeater: fixedWheel(hb)}

	const conns = 40
	var wg sync.WaitGroup
	for range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, rcancel := context.WithCancel(context.Background())
			req := httptest.NewRequestWithContext(rctx, http.MethodGet, "/v1/stream?table=clicks", nil)
			w := httptest.NewRecorder()
			hdone := make(chan struct{})
			go func() {
				defer close(hdone)
				h.Handle(w, withTenant(req))
			}()
			time.Sleep(8 * time.Millisecond) // let the wheel push at least once
			rcancel()                        // client "disconnects" mid-stream
			<-hdone
		}()
	}
	wg.Wait()

	assert.Equal(t, 0, hb.Len(),
		"every handler runs its deferred Remove on disconnect; the wheel ring drains")
}

// Two tenants stream the same table name on two topics (#583): a connection
// registers under its own tenant's, where only that tenant's events are
// broadcast.
func TestSSE_SubscribesUnderTheRequestTenant(t *testing.T) {
	t.Parallel()
	hub := stream.NewHub(nil, nil, nil)
	h := &StreamHandler{Hub: hub}
	tenants := nestedTenants(t, map[string]string{"acme": fullConfig(100), "globex": fullConfig(200)})

	// One context for both connections: cancelling it is the clients going away.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for _, id := range []tenant.ID{"acme", "globex"} {
		store, ok := tenants.For(id)
		require.True(t, ok)
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)
		req = req.WithContext(WithStore(req.Context(), store))
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Handle(httptest.NewRecorder(), req)
		}()
	}
	require.Eventually(t, func() bool {
		return hub.Len(mq.Topic{Tenant: "acme", Table: "clicks"}) == 1 && hub.Len(mq.Topic{Tenant: "globex", Table: "clicks"}) == 1
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, 0, hub.Len(mq.Topic{Tenant: tenant.Default, Table: "clicks"}), "neither connection is the default tenant's")

	cancel()
	wg.Wait()
	assert.Equal(t, 0, hub.Len(mq.Topic{Tenant: "acme", Table: "clicks"}), "every subscriber is removed")
}

// A stream is kept alive by its own tenant's wheel (#597): the handler asks
// for the wheel of the request's tenant, so one tenant's interval is never
// another's cadence. A tenant with no wheel is one no longer served, whose
// stream is ending anyway: it opens, and goes without.
func TestSSE_KeptAliveByItsTenantsWheel(t *testing.T) {
	t.Parallel()
	tenants := nestedTenants(t, map[string]string{"acme": fullConfig(100), "globex": fullConfig(200), "initech": fullConfig(300)})
	// acme's wheel nudges many times within the test and globex's never does.
	wheels := map[tenant.ID]*stream.Heartbeater{
		"acme":   stream.NewHeartbeater(10*time.Millisecond, 1),
		"globex": stream.NewHeartbeater(time.Hour, 1),
	}
	for _, hb := range wheels {
		go hb.Run(t.Context())
	}
	hub := stream.NewHub(nil, nil, nil)
	h := &StreamHandler{Hub: hub, Heartbeater: func(s *settings.Store) *stream.Heartbeater { return wheels[s.Tenant()] }}

	// One context for every connection: cancelling it is the clients going away.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	bodies := map[tenant.ID]*streamRecorder{}
	for _, id := range []tenant.ID{"acme", "globex", "initech"} {
		store, ok := tenants.For(id)
		require.True(t, ok)
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)
		req = req.WithContext(WithStore(req.Context(), store))
		w := &streamRecorder{ResponseRecorder: httptest.NewRecorder()}
		bodies[id] = w
		wg.Go(func() { h.Handle(w, req) })
	}
	keepalives := func(id tenant.ID) int { return strings.Count(bodies[id].written(), ":\n\n") }
	require.Eventually(t, func() bool {
		return wheels["acme"].Len() == 1 && wheels["globex"].Len() == 1 && hub.Len(mq.Topic{Tenant: "initech", Table: "clicks"}) == 1
	}, 5*time.Second, 5*time.Millisecond, "each stream joins its own tenant's wheel")
	require.Eventually(t, func() bool { return keepalives("acme") >= 3 }, 5*time.Second, 5*time.Millisecond,
		"acme's stream is nudged by acme's wheel")
	cancel()
	wg.Wait()

	assert.Zero(t, keepalives("globex"), "globex's stream is not nudged at acme's interval")
	assert.Zero(t, keepalives("initech"), "a stream with no wheel is not kept alive")
	assert.Contains(t, bodies["initech"].written(), ": connected", "but it is open")
	assert.Zero(t, wheels["acme"].Len()+wheels["globex"].Len(), "each stream leaves the wheel it joined")
}

// streamRecorder is a ResponseRecorder whose body a test may read while the
// handler is still writing the stream.
type streamRecorder struct {
	*httptest.ResponseRecorder
	mu sync.Mutex
}

func (r *streamRecorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ResponseRecorder.Write(p)
}

func (r *streamRecorder) written() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Body.String()
}

// blockingReplayer is a gap-fill that never catches up: it signals once it
// has started, then holds until its context ends.
type blockingReplayer struct{ started chan struct{} }

func (b blockingReplayer) ReplaySince(ctx context.Context, _ mq.Topic, _ time.Time, _ func([]byte) bool) error {
	close(b.started)
	<-ctx.Done()
	return ctx.Err()
}

// scriptedReplayer answers every gap-fill with its events, in order, and
// reports the time each one was asked to replay from.
type scriptedReplayer struct {
	events []string
	since  chan time.Time
}

func (r scriptedReplayer) ReplaySince(_ context.Context, _ mq.Topic, since time.Time, send func([]byte) bool) error {
	r.since <- since
	for _, e := range r.events {
		if !send([]byte(e)) {
			return nil
		}
	}
	return nil
}

// clickEvent is a published event of the clicks table carrying n, received at
// receivedAt.
func clickEvent(receivedAt string, n int) string {
	return fmt.Sprintf(`{"table_name":"clicks","scope":"","received_timestamp":%q,"format":"JSONCompactEachRow","columns":["n"],"row":[%d]}`, receivedAt, n)
}

// rowFrames is the data frames of an SSE body, each as written, keyed by the
// row it carries.
func rowFrames(body string) map[string]string {
	frames := map[string]string{}
	for frame := range strings.SplitSeq(body, "\n\n") {
		if _, row, ok := strings.Cut(frame, `"row":`); ok && !strings.HasPrefix(frame, "event: schema") {
			frames[strings.TrimSuffix(row, "}")] = frame
		}
	}
	return frames
}

// Only a live row moves the client's resume point. A replay sends rows in the
// order they were written, then the ones still waiting, so a replayed row's
// timestamp says nothing about which others the client has: it is written
// with no id: line, and a client that drops mid-replay resumes from where the
// replay began. A live row keeps its received_timestamp as its id.
func TestSSE_OnlyLiveRowsMoveTheResumePoint(t *testing.T) {
	t.Parallel()
	hub := stream.NewHub(nil, nil, nil)
	replayer := scriptedReplayer{
		// Written late: the newer row was inserted first.
		events: []string{clickEvent("2026-11-23T10:00:05Z", 2), clickEvent("2026-11-23T10:00:01Z", 1)},
		since:  make(chan time.Time, 1),
	}
	h := &StreamHandler{Hub: hub, Replayer: replayer}
	topic := mq.Topic{Tenant: tenant.Default, Table: "clicks"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)
	req.Header.Set("Last-Event-ID", "2026-11-23T10:00:00Z")
	w := &streamRecorder{ResponseRecorder: httptest.NewRecorder()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(w, withTenant(req))
	}()
	require.Eventually(t, func() bool { return len(rowFrames(w.written())) == 2 }, 5*time.Second, 5*time.Millisecond, "the replay was not written")
	assert.Equal(t, time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC), <-replayer.since)

	hub.Broadcast(topic, []byte(clickEvent("2026-11-23T10:00:09Z", 3)))
	require.Eventually(t, func() bool { return len(rowFrames(w.written())) == 3 }, 5*time.Second, 5*time.Millisecond, "the live row was not written")
	cancel()
	<-done

	body := w.written()
	frames := rowFrames(body)
	for _, row := range []string{"[2]", "[1]"} {
		assert.True(t, strings.HasPrefix(frames[row], "data: "), "a replayed row is written from its data: %q", frames[row])
	}
	assert.True(t, strings.HasPrefix(frames["[3]"], "id: 2026-11-23T10:00:09Z\ndata: "), "a live row leads with its id: %q", frames["[3]"])
	assert.Equal(t, 1, strings.Count(body, "\nid:"), "one id: line in the stream, the live row's: %q", body)
	assert.Less(t, strings.Index(body, `"row":[1]`), strings.Index(body, `"row":[3]`), "the replay is written ahead of what arrived live meanwhile")
}

// When the MQ's live feed may have skipped rows the hub ends every stream
// (stream.Hub.EvictAll), and a row broadcast after that is not written to a
// stream on its way out: its id would move the client past the rows it was
// never sent, where the last one it holds is the point to replay from.
func TestSSE_EndsWhenTheLiveFeedIsInterrupted(t *testing.T) {
	t.Parallel()
	hub := stream.NewHub(nil, nil, nil)
	h := &StreamHandler{Hub: hub}
	topic := mq.Topic{Tenant: tenant.Default, Table: "clicks"}

	// The request is never cancelled: the handler returns only by ending the
	// stream itself.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &streamRecorder{ResponseRecorder: httptest.NewRecorder()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(w, withTenant(httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)))
	}()
	require.Eventually(t, func() bool { return hub.Len(topic) == 1 }, 5*time.Second, 5*time.Millisecond)
	hub.Broadcast(topic, []byte(clickEvent("2026-11-23T10:00:01Z", 1)))
	require.Eventually(t, func() bool { return len(rowFrames(w.written())) == 1 }, 5*time.Second, 5*time.Millisecond)

	hub.EvictAll()
	hub.Broadcast(topic, []byte(clickEvent("2026-11-23T10:00:03Z", 3))) // row 2 was skipped
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream outlived the feed's interruption")
	}
	assert.Zero(t, hub.Len(topic))
	frames := rowFrames(w.written())
	assert.Contains(t, frames, "[1]")
	assert.NotContains(t, frames, "[3]", "a row past the gap reached a stream that was ending")
}

// A stream ends on its own once its tenant stops being served — removed or
// rejected by a reload — whether it is idle, mid-gap-fill, or was admitted by
// TenantMW before the reload and registered with the hub after it pruned.
func TestSSE_EndsWhenItsTenantIsNoLongerServed(t *testing.T) {
	t.Parallel()
	topic := mq.Topic{Tenant: tenant.Default, Table: "clicks"}
	served := func(tenant.ID) bool { return true }
	unserved := func(tenant.ID) bool { return false }
	// handle runs the stream in the background; the request is never
	// cancelled, so the handler returns only by ending the stream itself.
	handle := func(t *testing.T, h *StreamHandler, lastEventID string) (<-chan struct{}, *httptest.ResponseRecorder) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel) // only a stream that failed to end is still open here
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?table=clicks", nil)
		if lastEventID != "" {
			req.Header.Set("Last-Event-ID", lastEventID)
		}
		w := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.Handle(w, withTenant(req))
		}()
		return done, w
	}
	ended := func(t *testing.T, done <-chan struct{}) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the stream outlived its tenant")
		}
	}

	t.Run("idle", func(t *testing.T) {
		t.Parallel()
		hub := stream.NewHub(nil, nil, nil)
		done, _ := handle(t, &StreamHandler{Hub: hub, Served: served}, "")
		require.Eventually(t, func() bool { return hub.Len(topic) == 1 }, 5*time.Second, 5*time.Millisecond)
		hub.Prune(unserved)
		ended(t, done)
		assert.Zero(t, hub.Len(topic))
	})
	t.Run("mid gap-fill", func(t *testing.T) {
		t.Parallel()
		hub := stream.NewHub(nil, nil, nil)
		replayer := blockingReplayer{started: make(chan struct{})}
		done, _ := handle(t, &StreamHandler{Hub: hub, Replayer: replayer, Served: served}, "2026-09-24T00:00:00Z")
		select {
		case <-replayer.started:
		case <-time.After(5 * time.Second):
			t.Fatal("the gap-fill never started")
		}
		hub.Prune(unserved)
		ended(t, done)
	})
	t.Run("no longer served when it registers", func(t *testing.T) {
		t.Parallel()
		hub := stream.NewHub(nil, nil, nil)
		hb := stream.NewHeartbeater(time.Hour, 1)
		done, w := handle(t, &StreamHandler{Hub: hub, Heartbeater: fixedWheel(hb), Served: unserved}, "")
		ended(t, done)
		assert.Zero(t, hub.Len(topic), "the deferred Remove ran")
		assert.Zero(t, hb.Len(), "it never reached the keepalive wheel")
		assert.Contains(t, w.Body.String(), ": connected", "admitted, then ended")
	})
}
