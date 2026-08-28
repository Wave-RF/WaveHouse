package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/nats-io/nats.go/jetstream"
)

// Sweeper implements the Active Sweeper pattern. It runs every minute and
// purges messages from the JetStream stream that satisfy BOTH conditions:
//   - ACKed by the buffer consumer (written to ClickHouse)
//   - Older than the gap window (no longer needed for SSE replay)
//
// Purge target = MIN(buffer_ack_floor + 1, gap_window_seq).
// This guarantees: healthy state keeps exactly gap_window of rolling data;
// ClickHouse down freezes purging; catastrophic outage fills to MaxBytes
// and triggers backpressure via DiscardNew.
type Sweeper struct {
	js jetstream.JetStream
	// gapWindow is read on every sweep (settings.Store.GapWindow in
	// production) so a reload of stream.gap_window_minutes applies from the
	// next sweep without a restart.
	gapWindow func() time.Duration
	logger    *slog.Logger
}

// NewSweeper creates an Active Sweeper. gapWindow is resolved per sweep.
// TODO: (future) need leader election or shared lock to only run one instance of the sweeper in clustered mode
func NewSweeper(js jetstream.JetStream, gapWindow func() time.Duration, logger *slog.Logger) *Sweeper {
	return &Sweeper{
		js:        js,
		gapWindow: gapWindow,
		logger:    logger,
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
	stream, err := s.js.Stream(ctx, mq.StreamName())
	if err != nil {
		s.logger.Error("sweeper: get stream", "error", err)
		return
	}

	// 1. Get buffer consumer's AckFloor — highest contiguous ACKed sequence.
	cons, err := s.js.Consumer(ctx, mq.StreamName(), BufferConsumerName)
	if err != nil {
		// Consumer may not exist yet if no messages have been ingested.
		s.logger.Warn("sweeper: buffer consumer not found (may not exist yet)", "error", err)
		return
	}
	consInfo, err := cons.Info(ctx)
	if err != nil {
		s.logger.Error("sweeper: consumer info", "error", err)
		return
	}
	ackFloor := consInfo.AckFloor.Stream

	// 2. Binary search for the first sequence within the gap window.
	gapSeq, err := s.findGapSequence(ctx, stream)
	if err != nil {
		s.logger.Error("sweeper: find gap sequence", "error", err)
		return
	}

	if gapSeq == 0 {
		s.logger.Debug("sweeper: all messages within gap window, skipping purge")
		return
	}

	// 3. Purge target = MIN(ackFloor + 1, gapSeq).
	//    - ackFloor + 1: first un-ACKed seq (everything before is in CH).
	//    - gapSeq: first seq within gap window (everything before is expired).
	//    PurgeSequence(target) removes all messages with seq < target.
	target := min(ackFloor+1, gapSeq)

	if target <= 1 {
		return
	}

	// 4. Purge all messages older than the target.
	if err := stream.Purge(ctx, jetstream.WithPurgeSequence(target)); err != nil {
		s.logger.Error("sweeper: purge", "error", err, "target_seq", target)
		return
	}

	s.logger.Info("sweeper: purged",
		"purged_below_seq", target,
		"ack_floor", ackFloor,
		"gap_seq", gapSeq,
	)
}

// findGapSequence uses binary search over the stream to find the first sequence
// whose timestamp is >= (now - gapWindow). Returns 0 if the stream is empty.
// This takes ~15 lightweight GetMsg calls instead of creating an ephemeral consumer.
func (s *Sweeper) findGapSequence(ctx context.Context, stream jetstream.Stream) (uint64, error) {
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("stream info: %w", err)
	}

	first := info.State.FirstSeq
	last := info.State.LastSeq
	if first == 0 || last == 0 || first > last {
		return 0, nil
	}

	cutoff := time.Now().Add(-s.gapWindow())

	// If the oldest message is already within the window, keep everything.
	oldest, err := stream.GetMsg(ctx, first)
	if err == nil && !oldest.Time.Before(cutoff) {
		return 0, nil
	}

	lo, hi := first, last
	result := last + 1 // Default: keep everything if all within window.

	for lo <= hi {
		mid := lo + (hi-lo)/2
		msg, err := stream.GetMsg(ctx, mid)
		if err != nil {
			// Sequence may have been purged; scan forward.
			lo = mid + 1
			continue
		}
		if msg.Time.Before(cutoff) {
			lo = mid + 1
		} else {
			result = mid
			hi = mid - 1
		}
	}

	return result, nil
}
