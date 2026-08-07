package wavehouse

import (
	"context"
	"sync"
	"time"
)

// LiveQueryHandle controls a live query that combines historical backfill
// with a real-time stream.
type LiveQueryHandle struct {
	stream    *StreamController
	cancel    context.CancelFunc
	unsub     func()
	closeOnce sync.Once

	mu        sync.Mutex
	buffer    []StreamEvent
	buffering bool
	closed    bool
}

// newLiveQuery starts a live query: opens the stream immediately, fetches
// historical data, deduplicates buffered events, then goes live.
func newLiveQuery(
	stream *StreamController,
	fetchFn func(ctx context.Context) ([]map[string]any, error),
	sub *StreamSubscriber,
) *LiveQueryHandle {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel is called in Close()
	lq := &LiveQueryHandle{
		stream:    stream,
		cancel:    cancel,
		buffering: true,
	}

	// Step 1: Subscribe to live events and buffer them. User callbacks are
	// invoked outside lq.mu so a subscriber may call Close() without
	// deadlocking.
	lq.unsub = stream.Subscribe(&StreamSubscriber{
		Next: func(event StreamEvent) {
			lq.mu.Lock()
			if lq.closed {
				lq.mu.Unlock()
				return
			}
			if lq.buffering {
				lq.buffer = append(lq.buffer, event)
				lq.mu.Unlock()
				return
			}
			lq.mu.Unlock()
			if sub.Next != nil {
				sub.Next(event)
			}
		},
		Status: func(s StreamStatus) {
			if !lq.isClosed() && sub.Status != nil {
				sub.Status(s)
			}
		},
		Error: func(err error) {
			if !lq.isClosed() && sub.Error != nil {
				sub.Error(err)
			}
		},
	})

	// Step 2–5: Fetch historical and flush.
	go func() {
		rows, err := fetchFn(ctx)
		if ctx.Err() != nil || lq.isClosed() {
			return
		}

		// Step 3: Deliver initial snapshot.
		if sub.Initial != nil {
			sub.Initial(rows, err)
		}

		if err != nil {
			lq.mu.Lock()
			lq.buffering = false
			lq.buffer = nil
			lq.mu.Unlock()
			return
		}

		// Step 4: Dedup bound — the maximum backfilled timestamp, compared as
		// parsed times. OrderBy(..., "desc") makes the *last* row the oldest,
		// and RFC3339 strings with varying fractional digits don't sort
		// lexically, so neither "last row" nor raw string compare is safe.
		var lastTS time.Time
		for _, row := range rows {
			if s, ok := row["received_timestamp"].(string); ok {
				if ts, perr := time.Parse(time.RFC3339Nano, s); perr == nil && ts.After(lastTS) {
					lastTS = ts
				}
			}
		}

		// Step 5: Flush buffered events. buffering stays true until the
		// buffer is provably empty under the lock — prevents concurrent
		// sub.Next calls and preserves delivery order.
		for {
			lq.mu.Lock()
			if lq.closed {
				lq.mu.Unlock()
				return
			}
			pending := lq.buffer
			lq.buffer = nil
			if len(pending) == 0 {
				lq.buffering = false
				lq.mu.Unlock()
				break
			}
			lq.mu.Unlock()

			for _, event := range pending {
				if lq.isClosed() {
					return
				}
				// Skip events already delivered in the backfill.
				// Sub-millisecond received_timestamp precision makes
				// boundary collisions rare.
				if !lastTS.IsZero() {
					if ts, perr := time.Parse(time.RFC3339Nano, event.Timestamp); perr == nil && !ts.After(lastTS) {
						continue
					}
				}
				if sub.Next != nil {
					sub.Next(event)
				}
			}
		}
	}()

	return lq
}

func (lq *LiveQueryHandle) isClosed() bool {
	lq.mu.Lock()
	defer lq.mu.Unlock()
	return lq.closed
}

// Close shuts down the live query and the underlying stream. The close state
// is applied synchronously: no new subscriber callbacks start after Close
// returns (a callback already in flight may still complete).
func (lq *LiveQueryHandle) Close() {
	lq.closeOnce.Do(func() {
		lq.mu.Lock()
		lq.closed = true
		lq.buffer = nil
		lq.mu.Unlock()
		lq.unsub()
		lq.cancel()
		lq.stream.Close()
	})
}
