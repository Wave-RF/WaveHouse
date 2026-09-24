package ingest

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Sweeper implements the Active Sweeper pattern. It runs every minute and
// asks the MQ to purge the ingest events that satisfy BOTH conditions:
//   - ACKed by the buffer consumer (written to ClickHouse)
//   - Older than their tenant's gap window (no longer needed for SSE replay)
//
// This guarantees: healthy state keeps exactly each tenant's gap_window of
// rolling data; ClickHouse down freezes purging; a catastrophic outage fills
// a tenant's queue to its byte budget and triggers backpressure
// (mq.ErrQueueFull). How the MQ finds the purge point is its own business
// (see mq.Purger).
type Sweeper struct {
	purger mq.Purger
	// gapWindows is the history to keep for each tenant being served, read on
	// every sweep so a reload of stream.gap_window_minutes applies from the
	// next sweep without a restart. A tenant it does not name — one removed
	// or rejected — keeps no history (mq.Purger.PurgeAcked).
	gapWindows func() map[tenant.ID]time.Duration
}

// NewSweeper creates the Active Sweeper. gapWindows is resolved per sweep.
// TODO: (future) need leader election or shared lock to only run one instance of the sweeper in clustered mode
func NewSweeper(purger mq.Purger, gapWindows func() map[tenant.ID]time.Duration) *Sweeper {
	return &Sweeper{
		purger:     purger,
		gapWindows: gapWindows,
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
	now := time.Now()
	windows := s.gapWindows()
	cutoffs := make(map[tenant.ID]time.Time, len(windows))
	for id, window := range windows {
		cutoffs[id] = now.Add(-window)
	}
	_, err := s.purger.PurgeAcked(ctx, BufferConsumerName, cutoffs)
	if err != nil {
		if errors.Is(err, mq.ErrConsumerNotFound) {
			// Consumer may not exist yet if no messages have been ingested.
			slog.WarnContext(ctx, "sweeper: buffer consumer not found (may not exist yet)", "error", err)
			return
		}
		slog.ErrorContext(ctx, "sweeper: purge", "error", err)
	}
}
