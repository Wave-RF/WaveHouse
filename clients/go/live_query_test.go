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

// TestLiveQuery_BackfillThenLive: events arriving while the backfill is in
// flight are buffered, flushed once Initial has fired, and deduplicated
// against the *maximum* backfilled timestamp — not the last row, which a
// descending sort makes the oldest.
func TestLiveQuery_BackfillThenLive(t *testing.T) {
	tests := []struct {
		name     string
		backfill []string // received_timestamp per backfilled row, in server order
		emit     []string // events emitted while the backfill is gated
		wantLive string   // the only emitted event that survives dedup
	}{
		{
			name:     "the dedup bound is the max backfill timestamp, not the last row",
			backfill: []string{"2026-01-01T00:00:05Z", "2026-01-01T00:00:03Z"}, // descending
			emit:     []string{"2026-01-01T00:00:04Z", "2026-01-01T00:00:06Z"},
			wantLive: "2026-01-01T00:00:06Z",
		},
		{
			name:     "an event older than the backfill is dropped on flush",
			backfill: []string{"2026-01-01T00:00:01Z"},
			emit:     []string{"2026-01-01T00:00:02Z", "2026-01-01T00:00:00Z"},
			wantLive: "2026-01-01T00:00:02Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := make([]map[string]any, len(tt.backfill))
			for i, ts := range tt.backfill {
				rows[i] = map[string]any{"page": "/a", "received_timestamp": ts}
			}
			gate := make(chan struct{})
			initialCh := make(chan []map[string]any, 1)
			nextCh := make(chan StreamEvent, 8)

			sc := bareStream()
			lq := newLiveQuery(sc,
				func(context.Context) ([]map[string]any, error) {
					<-gate
					return rows, nil
				},
				&StreamSubscriber{
					Initial: func(got []map[string]any, err error) {
						if err != nil {
							t.Errorf("Initial err: %v", err)
						}
						initialCh <- got
					},
					Next: func(e StreamEvent) { nextCh <- e },
				})
			defer lq.Close()

			for _, ts := range tt.emit {
				sc.emitEvent(liveEvent(ts))
			}
			close(gate)

			if got := awaitInitial(t, initialCh); len(got) != len(rows) {
				t.Fatalf("want %d backfill rows, got %d", len(rows), len(got))
			}
			select {
			case e := <-nextCh:
				if e.Timestamp != tt.wantLive {
					t.Fatalf("want %s delivered, got %s", tt.wantLive, e.Timestamp)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("buffered live event never flushed")
			}
			select {
			case e := <-nextCh:
				t.Fatalf("stale event delivered despite dedup: %s", e.Timestamp)
			case <-time.After(100 * time.Millisecond):
			}
		})
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
