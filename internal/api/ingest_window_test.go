package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/storedir"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eventLines is n NDJSON clicks records with ids e1..en.
func eventLines(t *testing.T, n int) []string {
	t.Helper()
	lines := make([]string, n)
	for i := range n {
		lines[i] = jsonLine(t, map[string]any{"page": "/p", "event_id": fmt.Sprintf("e%d", i+1)})
	}
	return lines
}

func clickKey(i int) dedupe.Key { return dedupe.Key{Table: "clicks", ID: fmt.Sprintf("e%d", i)} }

// A batch is reserved, published and committed a window at a time: one
// Reserve and one Commit per window, whatever the batch size.
func TestIngest_Windows_OneReserveAndCommitPerWindow(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 255, 256, 257, 600} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			dedup := testutil.NewMockDeduplicator()
			h := dedupHandler(t, pub, dedup, false)

			w := httptest.NewRecorder()
			h.Handle(w, withTenant(ndjsonRequest(t, "clicks", eventLines(t, n)...)))
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, n, decodeBatchResult(t, w).Succeeded)
			windows := (n + ingestWindow - 1) / ingestWindow
			assert.Equal(t, windows, dedup.Reserves)
			assert.Equal(t, windows, dedup.Commits)
			assert.Len(t, pub.Published(), n)
			assert.True(t, dedup.Committed(clickKey(n)))
		})
	}
}

// A publish failing at record k settles its window: the records before k are
// committed, k is released when the queue refused it and left to lapse when
// the outcome is unknown, the rest of the window is released, and later
// windows are never reserved. A whole-batch retry after a refusal publishes
// every record exactly once.
func TestIngest_Windows_PublishFailureAtK(t *testing.T) {
	t.Parallel()
	const n = 600
	refused := fmt.Errorf("%w: maximum bytes exceeded", mq.ErrQueueFull)
	tests := []struct {
		name   string
		k      int
		err    error
		status int
	}{
		{"refused first record", 1, refused, http.StatusServiceUnavailable},
		{"refused mid first window", 100, refused, http.StatusServiceUnavailable},
		{"refused last of first window", 256, refused, http.StatusServiceUnavailable},
		{"refused first of second window", 257, refused, http.StatusServiceUnavailable},
		{"refused mid last window", 590, refused, http.StatusServiceUnavailable},
		{"uncertain mid first window", 100, context.DeadlineExceeded, http.StatusInternalServerError},
		{"uncertain mid second window", 400, context.DeadlineExceeded, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{Err: tt.err, ErrAfter: tt.k - 1}
			dedup := testutil.NewMockDeduplicator()
			h := dedupHandler(t, pub, dedup, false)
			lines := eventLines(t, n)

			w := httptest.NewRecorder()
			h.Handle(w, withTenant(ndjsonRequest(t, "clicks", lines...)))
			require.Equal(t, tt.status, w.Code)
			assert.Len(t, pub.Published(), tt.k-1)

			windowEnd := min((tt.k-1)/ingestWindow*ingestWindow+ingestWindow, n)
			definite := errors.Is(tt.err, mq.ErrQueueFull)
			var released []dedupe.Key
			for _, c := range dedup.Released {
				released = append(released, c.Key)
			}
			var wantReleased []dedupe.Key
			for i := tt.k; i <= windowEnd; i++ {
				if i > tt.k || definite {
					wantReleased = append(wantReleased, clickKey(i))
				}
			}
			assert.Equal(t, wantReleased, released)
			if tt.k > 1 {
				assert.True(t, dedup.Committed(clickKey(1)))
				assert.True(t, dedup.Committed(clickKey(tt.k-1)), "published before the failure")
			}
			assert.False(t, dedup.Committed(clickKey(tt.k)))
			assert.Equal(t, !definite, dedup.Pending(clickKey(tt.k)), "an uncertain publish leaves its claim to lapse")
			if windowEnd < n {
				assert.False(t, dedup.Pending(clickKey(windowEnd+1)), "a later window is never reserved")
			}
			if !definite {
				return
			}

			pub.Err = nil
			w = httptest.NewRecorder()
			h.Handle(w, withTenant(ndjsonRequest(t, "clicks", lines...)))
			require.Equal(t, http.StatusOK, w.Code)
			resp := decodeBatchResult(t, w)
			assert.Equal(t, tt.k-1, resp.Duplicates)
			assert.Equal(t, n-(tt.k-1), resp.Succeeded)
			assert.Len(t, pub.Published(), n, "every record exactly once")
		})
	}
}

// A dedupe store that cannot answer now fails the request with 503 and a
// short Retry-After, which the SDK retries — not the 500 of a broken store.
// Earlier windows stay published and committed.
func TestIngest_Dedup_UnavailableIs503(t *testing.T) {
	t.Parallel()
	notOpen := dedupe.NewManaged(func() (dedupe.Deduplicator, error) { return nil, errors.New("disk gone") })
	require.Error(t, notOpen.Apply(true))
	throttled := testutil.NewMockDeduplicator()
	throttled.Err = fmt.Errorf("%w: throttled", dedupe.ErrUnavailable)
	secondWindow := testutil.NewMockDeduplicator()
	secondWindow.Err, secondWindow.ErrAfter = throttled.Err, 1

	tests := []struct {
		name      string
		dedup     dedupe.Deduplicator
		n         int
		published int
	}{
		{"store not open", notOpen, 1, 0},
		{"backend throttled", throttled, 3, 0},
		{"second window throttled", secondWindow, ingestWindow + 1, ingestWindow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			h := dedupHandler(t, pub, tt.dedup, false)
			w := httptest.NewRecorder()
			h.Handle(w, withTenant(ndjsonRequest(t, "clicks", eventLines(t, tt.n)...)))
			assert.Equal(t, http.StatusServiceUnavailable, w.Code)
			assert.Equal(t, "5", w.Header().Get("Retry-After"))
			assert.Contains(t, w.Body.String(), "dedupe store unavailable")
			assert.Len(t, pub.Published(), tt.published)
		})
	}
	t.Run("single object", func(t *testing.T) {
		t.Parallel()
		pub := &testutil.MockPublisher{}
		h := dedupHandler(t, pub, throttled, false)
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(ingestRequest(t, "clicks", map[string]any{"page": "/", "event_id": "e1"})))
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Equal(t, "5", w.Header().Get("Retry-After"))
		assert.Empty(t, pub.Published())
	})
}

// One id held by another request stops its window before anything in it is
// published and gives back the window's other claims; windows before it stay
// committed.
func TestIngest_Windows_InFlightReleasesTheWindow(t *testing.T) {
	t.Parallel()
	pub := &testutil.MockPublisher{}
	dedup := testutil.NewMockDeduplicator()
	held := ingestWindow + 2
	dedup.Hold(clickKey(held))
	h := dedupHandler(t, pub, dedup, false)

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ndjsonRequest(t, "clicks", eventLines(t, ingestWindow+3)...)))
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "30", w.Header().Get("Retry-After"))
	assert.Len(t, pub.Published(), ingestWindow)
	assert.True(t, dedup.Committed(clickKey(ingestWindow)))
	for _, i := range []int{ingestWindow + 1, ingestWindow + 3} {
		assert.False(t, dedup.Pending(clickKey(i)), "e%d released", i)
	}
	assert.True(t, dedup.Pending(clickKey(held)), "the other request's claim is untouched")
}

// Rejects, duplicates and repeats keep their places in the results across
// windows, over both batch formats.
func TestIngest_Windows_OutcomesStayInOrder(t *testing.T) {
	t.Parallel()
	records := []map[string]any{
		{"page": "/a", "event_id": "e1"},
		{"page": "/b", "event_id": "e1"}, // repeat inside one window
		{"page": "/c", "nope": 1},        // reject
		{"page": "/d", "event_id": "e2"},
		{"page": "/e", "event_id": "e1"}, // repeat across windows
		{"page": "/f"},                   // no id: published un-deduped
	}
	requests := map[string]func() *http.Request{
		"ndjson": func() *http.Request {
			lines := make([]string, len(records))
			for i, r := range records {
				lines[i] = jsonLine(t, r)
			}
			return ndjsonRequest(t, "clicks", lines...)
		},
		"json array": func() *http.Request { return ingestRequest(t, "clicks", records) },
	}
	for name, req := range requests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pub := &testutil.MockPublisher{}
			dedup := testutil.NewMockDeduplicator()
			h := dedupHandler(t, pub, dedup, false)
			h.window = 3

			w := httptest.NewRecorder()
			h.Handle(w, withTenant(req()))
			require.Equal(t, http.StatusOK, w.Code)
			resp := decodeBatchResult(t, w)
			assert.Equal(t, []recordResult{
				{Index: 1, Ok: true},
				{Index: 2, Duplicate: true},
				{Index: 3, Error: resp.Results[2].Error},
				{Index: 4, Ok: true},
				{Index: 5, Duplicate: true},
				{Index: 6, Ok: true},
			}, resp.Results)
			assert.NotEmpty(t, resp.Results[2].Error)
			assert.Equal(t, 6, resp.Total)
			assert.Len(t, pub.Published(), 3)
			assert.Equal(t, 2, dedup.Reserves)
		})
	}
}

// The embedded queue must remember an idempotency key for at least two
// leases plus a second: the in-flight 503 of an uncertain publish sends the
// full lease as Retry-After, so a client that obeys it can republish up to
// ~2*lease after the original Reserve, and a claim's expiry can itself round
// up by up to a second (a DynamoDB backend, for one). Only the queue's
// duplicate window running at least that long guarantees it still drops the
// retry's second copy.
func TestIngest_DedupeLeaseFitsTheDuplicateWindow(t *testing.T) {
	t.Parallel()
	assert.LessOrEqual(t, 2*dedupe.DefaultLease+time.Second, mq.EmbeddedDuplicateWindow)
}

// faultyPublisher publishes through a real broker and fails the calls fail
// picks: before sending (the queue refused it) or after (the outcome unknown
// to the caller, though the event is stored).
type faultyPublisher struct {
	mq.Publisher
	mu    sync.Mutex
	calls int
	fail  func(call int) (sendFirst bool, err error)
}

func (p *faultyPublisher) Publish(ctx context.Context, topic mq.Topic, data []byte, opts ...mq.PublishOpt) error {
	p.mu.Lock()
	p.calls++
	sendFirst, err := p.fail(p.calls)
	p.mu.Unlock()
	if err == nil || sendFirst {
		if pubErr := p.Publisher.Publish(ctx, topic, data, opts...); pubErr != nil {
			return pubErr
		}
	}
	return err
}

// realPipeline is an ingest handler over the embedded broker and Pebble
// store, with pub's faults in front of the broker, and a count of the events
// in the tenant's queue.
func realPipeline(t *testing.T, fail func(call int) (bool, error)) (*IngestHandler, func() int) {
	t.Helper()
	broker, err := mq.NewEmbedded(storedir.New(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = broker.Close() })
	require.NoError(t, broker.SetMaxBytes(t.Context(), testStore.Tenant(), 64<<20))
	store := dedupe.NewEmbedded(t.TempDir()).Tenant(testStore.Tenant())
	require.NoError(t, store.Apply(true))
	t.Cleanup(func() { _ = store.Close() })

	h := dedupHandler(t, nil, store, false)
	h.Publisher = &faultyPublisher{Publisher: broker, fail: fail}
	count := func() int {
		n := 0
		require.NoError(t, broker.ReplaySince(t.Context(), mq.Topic{Tenant: testStore.Tenant(), Table: "clicks"}, time.Time{},
			func([]byte) bool { n++; return true }))
		return n
	}
	return h, count
}

// #384 end to end: a publish the queue refused, then the client's retry, ends
// in exactly one event in the queue — and a later retry is a duplicate.
func TestIngest_Dedup_FailedPublishThenRetryIsOneEvent(t *testing.T) {
	t.Parallel()
	h, count := realPipeline(t, func(call int) (bool, error) {
		if call == 1 {
			return false, fmt.Errorf("%w: maximum bytes exceeded", mq.ErrQueueFull)
		}
		return false, nil
	})
	body := map[string]any{"page": "/home", "event_id": "e1"}
	codes := make([]int, 3)
	for i := range codes {
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(ingestRequest(t, "clicks", body)))
		codes[i] = w.Code
		if i == 2 {
			assert.Contains(t, w.Body.String(), `"duplicate":true`)
		}
	}
	assert.Equal(t, []int{http.StatusServiceUnavailable, http.StatusOK, http.StatusOK}, codes)
	assert.Equal(t, 1, count())
}

// A publish that stored the event but reported a failure, then the client's
// retry: in-flight until the lease lapses, then republished under the same
// idempotency key, which the queue drops — one event, and the id committed.
func TestIngest_Dedup_UncertainPublishThenRetryIsOneEvent(t *testing.T) {
	t.Parallel()
	h, count := realPipeline(t, func(call int) (bool, error) {
		if call == 1 {
			return true, context.DeadlineExceeded
		}
		return false, nil
	})
	h.DedupeLease = 2 * time.Second
	lines := eventLines(t, 3)
	send := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.Handle(w, withTenant(ndjsonRequest(t, "clicks", lines...)))
		return w
	}

	w := send()
	require.Equal(t, http.StatusInternalServerError, w.Code)
	w = send()
	require.Equal(t, http.StatusServiceUnavailable, w.Code, "the uncertain claim is still held")
	assert.Equal(t, "2", w.Header().Get("Retry-After"))

	var last *httptest.ResponseRecorder
	require.Eventually(t, func() bool {
		last = send()
		return last.Code == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond)
	assert.Equal(t, 3, decodeBatchResult(t, last).Succeeded, "the lapsed claim is claimed again and republished")
	assert.Equal(t, 3, count(), "the republished e1 was dropped by the queue")

	w = send()
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 3, decodeBatchResult(t, w).Duplicates)
}

// countingDedup counts the Commits that reach a store: on Pebble each is one
// fsync.
type countingDedup struct {
	dedupe.Deduplicator
	mu      sync.Mutex
	commits int
}

func (c *countingDedup) Commit(ctx context.Context, claims []dedupe.Claim, retention time.Duration) error {
	c.mu.Lock()
	c.commits++
	c.mu.Unlock()
	return c.Deduplicator.Commit(ctx, claims, retention)
}

// pebbleBatchHandler is a handler over a real Pebble store behind a Commit
// counter, publishing to a mock queue.
func pebbleBatchHandler(tb testing.TB, window int) (*IngestHandler, *countingDedup) {
	tb.Helper()
	store := dedupe.NewEmbedded(tb.TempDir()).Tenant(testStore.Tenant())
	require.NoError(tb, store.Apply(true))
	tb.Cleanup(func() { _ = store.Close() })
	counted := &countingDedup{Deduplicator: store}
	h := NewIngestHandler(fixedRegistry(testRegistry(tb)), &testutil.MockPublisher{})
	h.Dedup = staticDedup(counted)
	h.DedupeSettings = func(*settings.Store, string) (bool, string, bool) { return true, "event_id", false }
	h.window = window
	return h, counted
}

// Windows cut the per-record fsyncs on Pebble: a 1,000-record batch commits in
// four syncs rather than a thousand.
func TestIngest_Windows_OneSyncPerWindowOnPebble(t *testing.T) {
	t.Parallel()
	h, counted := pebbleBatchHandler(t, 0)
	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ndjsonRequest(t, "clicks", eventLines(t, 1000)...)))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 4, counted.commits)
}

// BenchmarkIngest_DedupBatchOnPebble compares a 1,000-record batch committed
// per record (window 1, the pre-window behavior) with the default window.
func BenchmarkIngest_DedupBatchOnPebble(b *testing.B) {
	for _, window := range []int{1, ingestWindow} {
		b.Run(fmt.Sprintf("window=%d", window), func(b *testing.B) {
			h, counted := pebbleBatchHandler(b, window)
			var body strings.Builder
			iter := 0
			for b.Loop() {
				iter++
				body.Reset()
				for i := range 1000 {
					fmt.Fprintf(&body, `{"page":"/p","event_id":"%d-%d"}`+"\n", iter, i)
				}
				req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/ingest?table=clicks", strings.NewReader(body.String()))
				req.Header.Set("Content-Type", "application/x-ndjson")
				w := httptest.NewRecorder()
				h.Handle(w, withTenant(req))
				if w.Code != http.StatusOK {
					b.Fatalf("status %d", w.Code)
				}
			}
			b.ReportMetric(float64(counted.commits)/float64(iter), "syncs/op")
		})
	}
}
