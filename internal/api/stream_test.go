package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/stream"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
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

	h := &StreamHandler{Hub: stream.NewHub(nil, nil, nil), Heartbeater: hb}

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

	h := &StreamHandler{Hub: stream.NewHub(nil, nil, nil), Heartbeater: hb}

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

// blockingReplayer is a gap-fill that never catches up: it signals once it
// has started, then holds until its context ends.
type blockingReplayer struct{ started chan struct{} }

func (b blockingReplayer) ReplaySince(ctx context.Context, _ mq.Topic, _ time.Time, _ func([]byte) bool) error {
	close(b.started)
	<-ctx.Done()
	return ctx.Err()
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
		done, w := handle(t, &StreamHandler{Hub: hub, Heartbeater: hb, Served: unserved}, "")
		ended(t, done)
		assert.Zero(t, hub.Len(topic), "the deferred Remove ran")
		assert.Zero(t, hb.Len(), "it never reached the keepalive wheel")
		assert.Contains(t, w.Body.String(), ": connected", "admitted, then ended")
	})
}
