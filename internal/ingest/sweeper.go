package ingest

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
)

// Sweeper implements the Active Sweeper pattern. It runs every minute and
// asks the MQ to purge the ingest events that satisfy BOTH conditions:
//   - ACKed by the buffer consumer (written to ClickHouse)
//   - Older than the gap window (no longer needed for SSE replay)
//
// This guarantees: healthy state keeps exactly gap_window of rolling data;
// ClickHouse down freezes purging; a catastrophic outage fills the queue to
// its byte budget and triggers backpressure (mq.ErrQueueFull). How the MQ
// finds the purge point is its own business (see mq.Purger).
type Sweeper struct {
	purger mq.Purger
	// gapWindow is the history to keep, read on every sweep so a reload of
	// stream.gap_window_minutes applies from the next sweep without a
	// restart. The ingest queue is one stream for every tenant and a purge
	// is one bound over it, so in production this is the longest window
	// among the tenants being served (internal/app's longestGapWindow); a
	// tenant's own window follows once the streams are per tenant (#583
	// story 5b).
	gapWindow func() time.Duration
}

// NewSweeper creates the Active Sweeper. gapWindow is resolved per sweep.
// TODO: (future) need leader election or shared lock to only run one instance of the sweeper in clustered mode
func NewSweeper(purger mq.Purger, gapWindow func() time.Duration) *Sweeper {
	return &Sweeper{
		purger:    purger,
		gapWindow: gapWindow,
	}
}

// Start runs the sweep loop every minute. Blocks until ctx is cancelled.
func (s *Sweeper) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Sweeper) sweep(ctx context.Context) {
	_, err := s.purger.PurgeAcked(ctx, BufferConsumerName, time.Now().Add(-s.gapWindow()))
	if err != nil {
		if errors.Is(err, mq.ErrConsumerNotFound) {
			// Consumer may not exist yet if no messages have been ingested.
			slog.WarnContext(ctx, "sweeper: buffer consumer not found (may not exist yet)", "error", err)
			return
		}
		slog.ErrorContext(ctx, "sweeper: purge", "error", err)
	}
}
