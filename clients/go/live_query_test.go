package wavehouse

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// bareStream builds a StreamController that never dials anything — events are
// injected with emitEvent, exactly how the run loop feeds real ones.
func bareStream() *StreamController {
	return &StreamController{
		status:  StatusLive,
		eventCh: make(chan StreamEvent, 16),
		done:    make(chan struct{}),
		cancel:  func() {},
	}
}

func liveEvent(ts string) StreamEvent {
	return StreamEvent{Table: "clicks", Timestamp: ts, Data: map[string]any{"page": "/home"}}
}

func awaitInitial(t *testing.T, ch <-chan []map[string]any) []map[string]any {
	t.Helper()
	select {
	case rows := <-ch:
		return rows
	case <-time.After(5 * time.Second):
		t.Fatal("Initial never fired")
		return nil
	}
}

func TestLiveQuery_InitialThenLiveWithDedup(t *testing.T) {
	sc := bareStream()
	fetched := []map[string]any{
		{"page": "/a", "received_timestamp": "2026-01-01T00:00:05Z"},
		// Descending order: the max timestamp is NOT the last row.
		{"page": "/b", "received_timestamp": "2026-01-01T00:00:03Z"},
	}
	gate := make(chan struct{})
	initialCh := make(chan []map[string]any, 1)
	nextCh := make(chan StreamEvent, 8)

	lq := newLiveQuery(sc,
		func(context.Context) ([]map[string]any, error) {
			<-gate
			return fetched, nil
		},
		&StreamSubscriber{
			Initial: func(rows []map[string]any, err error) {
				if err != nil {
					t.Errorf("Initial err: %v", err)
				}
				initialCh <- rows
			},
			Next: func(e StreamEvent) { nextCh <- e },
		})
	defer lq.Close()

	// Buffered while the backfill is in flight; deduped against the *max*
	// backfilled timestamp (5Z despite descending order) on flush.
	sc.emitEvent(liveEvent("2026-01-01T00:00:04Z")) // ≤ max backfill → skipped
	sc.emitEvent(liveEvent("2026-01-01T00:00:06Z")) // newer → delivered
	close(gate)

	rows := awaitInitial(t, initialCh)
	if len(rows) != 2 {
		t.Fatalf("want 2 backfill rows, got %d", len(rows))
	}

	select {
	case e := <-nextCh:
		if e.Timestamp != "2026-01-01T00:00:06Z" {
			t.Fatalf("want the newer event only, got %s", e.Timestamp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live event never delivered")
	}
	select {
	case e := <-nextCh:
		t.Fatalf("stale event delivered despite dedup: %s", e.Timestamp)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestLiveQuery_BuffersDuringBackfill(t *testing.T) {
	sc := bareStream()
	gate := make(chan struct{})
	initialCh := make(chan []map[string]any, 1)
	nextCh := make(chan StreamEvent, 8)

	lq := newLiveQuery(sc,
		func(context.Context) ([]map[string]any, error) {
			<-gate
			return []map[string]any{{"received_timestamp": "2026-01-01T00:00:01Z"}}, nil
		},
		&StreamSubscriber{
			Initial: func(rows []map[string]any, _ error) { initialCh <- rows },
			Next:    func(e StreamEvent) { nextCh <- e },
		})
	defer lq.Close()

	// Events arriving mid-backfill are buffered, then flushed post-Initial.
	sc.emitEvent(liveEvent("2026-01-01T00:00:02Z"))
	sc.emitEvent(liveEvent("2026-01-01T00:00:00Z")) // older than backfill → dropped in flush
	close(gate)

	awaitInitial(t, initialCh)
	select {
	case e := <-nextCh:
		if e.Timestamp != "2026-01-01T00:00:02Z" {
			t.Fatalf("want buffered 02Z event, got %s", e.Timestamp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("buffered event never flushed")
	}
	select {
	case e := <-nextCh:
		t.Fatalf("pre-backfill event should have been deduped: %s", e.Timestamp)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestLiveQuery_FetchErrorReportedOnce(t *testing.T) {
	sc := bareStream()
	errCh := make(chan error, 1)

	lq := newLiveQuery(sc,
		func(context.Context) ([]map[string]any, error) { return nil, errors.New("boom") },
		&StreamSubscriber{
			Initial: func(_ []map[string]any, err error) { errCh <- err },
		})
	defer lq.Close()

	select {
	case err := <-errCh:
		if err == nil || err.Error() != "boom" {
			t.Fatalf("want boom, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Initial never fired on fetch error")
	}
}

func TestLiveQuery_NoCallbacksAfterClose(t *testing.T) {
	sc := bareStream()
	initialCh := make(chan []map[string]any, 1)
	var delivered atomic.Int64

	lq := newLiveQuery(sc,
		func(context.Context) ([]map[string]any, error) { return nil, nil },
		&StreamSubscriber{
			Initial: func(rows []map[string]any, _ error) { initialCh <- rows },
			Next:    func(StreamEvent) { delivered.Add(1) },
			Status:  func(StreamStatus) { delivered.Add(1) },
			Error:   func(error) { delivered.Add(1) },
		})
	awaitInitial(t, initialCh)

	lq.Close()
	before := delivered.Load()
	sc.emitEvent(liveEvent("2026-01-01T00:00:09Z"))
	sc.emitError(errors.New("late"))
	sc.setStatus(StatusReconnecting)
	time.Sleep(50 * time.Millisecond)
	if got := delivered.Load(); got != before {
		t.Fatalf("callbacks fired after Close: before=%d after=%d", before, got)
	}
}
