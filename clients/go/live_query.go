package wavehouse

import (
	"context"
	"sync"
)

// LiveQueryHandle controls a live query that combines historical backfill
// with a real-time stream.
type LiveQueryHandle struct {
	stream    *StreamController
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// newLiveQuery starts a live query: opens the stream immediately, fetches
// historical data, deduplicates buffered events, then goes live.
func newLiveQuery(
	stream *StreamController,
	fetchFn func(ctx context.Context) ([]map[string]any, error),
	sub *StreamSubscriber,
	filters []QueryFilter,
) *LiveQueryHandle {
	ctx, cancel := context.WithCancel(context.Background())
	lq := &LiveQueryHandle{
		stream: stream,
		cancel: cancel,
	}

	var (
		mu        sync.Mutex
		buffer    []StreamEvent
		buffering = true
		closed    = false
	)

	// Step 1: Subscribe to live events and buffer them.
	stream.Subscribe(&StreamSubscriber{
		Next: func(event StreamEvent) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			if buffering {
				buffer = append(buffer, event)
			} else if sub.Next != nil {
				sub.Next(event)
			}
		},
		Status: func(s StreamStatus) {
			if sub.Status != nil {
				sub.Status(s)
			}
		},
		Error: func(err error) {
			if sub.Error != nil {
				sub.Error(err)
			}
		},
	})

	// Step 2–5: Fetch historical and flush.
	go func() {
		rows, err := fetchFn(ctx)
		if ctx.Err() != nil {
			return
		}

		// Step 3: Deliver initial snapshot.
		if sub.Initial != nil {
			sub.Initial(rows, err)
		}

		if err != nil {
			mu.Lock()
			buffering = false
			buffer = nil
			mu.Unlock()
			return
		}

		// Step 4: Deduplicate buffered events.
		var lastTimestamp string
		if len(rows) > 0 {
			lastRow := rows[len(rows)-1]
			if ts, ok := lastRow["received_timestamp"].(string); ok {
				lastTimestamp = ts
			}
		}

		// Step 5: Flush buffered events newer than the fetch.
		//
		// buffering stays true for the whole flush: events that arrive
		// concurrently (after Subscribe's Next handler releases mu but
		// before we're done here) must keep landing in buffer rather than
		// being dispatched directly by the live path, or two goroutines
		// could call sub.Next at once. We only flip buffering to false
		// once a lock-protected check finds the buffer empty, which
		// guarantees no event is ever handed to sub.Next by both paths
		// and that delivery stays in arrival order.
		for {
			mu.Lock()
			if closed {
				mu.Unlock()
				return
			}
			pending := buffer
			buffer = nil
			if len(pending) == 0 {
				buffering = false
				mu.Unlock()
				break
			}
			mu.Unlock()

			for _, event := range pending {
				mu.Lock()
				c := closed
				mu.Unlock()
				if c {
					return
				}
				// Use <= (not <) to filter events whose timestamp matches the last
				// historical row — those rows were already delivered in the backfill
				// response. If two distinct events share a timestamp and only one
				// appeared in the backfill, the duplicate is lost; this matches the
				// TS SDK's dedup behavior and is acceptable because received_timestamp
				// has sub-millisecond precision in practice.
				if lastTimestamp != "" && event.Timestamp <= lastTimestamp {
					continue
				}
				if sub.Next != nil {
					sub.Next(event)
				}
			}
		}
	}()

	// Cleanup on context cancel.
	go func() {
		<-ctx.Done()
		mu.Lock()
		closed = true
		buffer = nil
		mu.Unlock()
	}()

	return lq
}

// Close shuts down the live query and the underlying stream.
func (lq *LiveQueryHandle) Close() {
	lq.closeOnce.Do(func() {
		lq.cancel()
		lq.stream.Close()
	})
}
