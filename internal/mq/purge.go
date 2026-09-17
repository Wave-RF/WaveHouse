package mq

import (
	"context"
	"fmt"
	"time"
)

// streamState is the slice of a JetStream stream's state this package reads.
type streamState struct {
	FirstSeq uint64 // oldest stored sequence, 0 when empty
	LastSeq  uint64 // newest stored sequence, 0 when empty
	Msgs     uint64 // messages stored, across every subject
	// Subjects maps subject → message count for the subjects matching the
	// filter passed to state; nil when no filter was given.
	Subjects map[string]uint64
}

// sequencedStream is what the purge needs from a stream: its sequence bounds,
// per-sequence timestamps, a consumer's ack floor, and a purge-below. An
// interface so the sequence arithmetic is testable without a server.
type sequencedStream interface {
	// state reports the stream's sequence bounds and message count. A
	// non-empty subjectFilter also fills streamState.Subjects.
	state(ctx context.Context, subjectFilter string) (streamState, error)
	// messageTime returns the stored timestamp of the message at seq. A purged
	// or never-stored sequence is an error.
	messageTime(ctx context.Context, seq uint64) (time.Time, error)
	// purgeBelow removes every message with a sequence lower than seq.
	purgeBelow(ctx context.Context, seq uint64) error
	// consumerAckFloor returns the named consumer's ack floor: the highest
	// stream sequence below which every message has been acked.
	// ErrConsumerNotFound when the consumer has not been created.
	consumerAckFloor(ctx context.Context, consumer string) (uint64, error)
}

// purgeReport is what one purge did, for the log line.
type purgeReport struct {
	purged   bool
	target   uint64 // everything below this sequence was removed
	ackFloor uint64
	gapSeq   uint64
}

// purgeAcked removes the messages that are both acked by consumer and stored
// before cutoff:
//
//	purge target = MIN(ack_floor + 1, first sequence at or after cutoff)
//
// This guarantees: healthy state keeps exactly the replay window of rolling
// data; ClickHouse down freezes purging (the ack floor stops moving); a
// catastrophic outage fills the stream to MaxBytes and triggers backpressure
// via DiscardNew.
func purgeAcked(ctx context.Context, s sequencedStream, consumer string, cutoff time.Time) (purgeReport, error) {
	// 1. The consumer's ack floor — highest contiguous acked sequence.
	ackFloor, err := s.consumerAckFloor(ctx, consumer)
	if err != nil {
		return purgeReport{}, err
	}

	// 2. Binary search for the first sequence at or after the cutoff.
	gapSeq, err := findGapSequence(ctx, s, cutoff)
	if err != nil {
		return purgeReport{}, fmt.Errorf("find gap sequence: %w", err)
	}
	report := purgeReport{ackFloor: ackFloor, gapSeq: gapSeq}
	if gapSeq == 0 {
		return report, nil // everything stored is still inside the window
	}

	// 3. Purge target = MIN(ackFloor + 1, gapSeq).
	//    - ackFloor + 1: first unacked seq (everything before is written).
	//    - gapSeq: first seq inside the window (everything before is expired).
	//    purgeBelow(target) removes all messages with seq < target.
	report.target = min(ackFloor+1, gapSeq)
	if report.target <= 1 {
		return report, nil
	}

	if err := s.purgeBelow(ctx, report.target); err != nil {
		return report, fmt.Errorf("purge below %d: %w", report.target, err)
	}
	report.purged = true
	return report, nil
}

// findGapSequence uses binary search over the stream to find the first sequence
// whose timestamp is >= cutoff. Returns 0 if the stream is empty or nothing is
// older than cutoff. This takes ~15 lightweight messageTime lookups instead of
// creating an ephemeral consumer.
func findGapSequence(ctx context.Context, s sequencedStream, cutoff time.Time) (uint64, error) {
	state, err := s.state(ctx, "")
	if err != nil {
		return 0, fmt.Errorf("stream info: %w", err)
	}

	first := state.FirstSeq
	last := state.LastSeq
	if first == 0 || last == 0 || first > last {
		return 0, nil
	}

	// If the oldest message is already within the window, keep everything.
	oldest, err := s.messageTime(ctx, first)
	if err == nil && !oldest.Before(cutoff) {
		return 0, nil
	}

	lo, hi := first, last
	result := last + 1 // Default: everything is older than the cutoff.

	for lo <= hi {
		mid := lo + (hi-lo)/2
		ts, err := s.messageTime(ctx, mid)
		if err != nil {
			// Sequence may have been purged; scan forward.
			lo = mid + 1
			continue
		}
		if ts.Before(cutoff) {
			lo = mid + 1
		} else {
			result = mid
			hi = mid - 1
		}
	}

	return result, nil
}
