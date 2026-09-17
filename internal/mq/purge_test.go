package mq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStream is a sequencedStream with canned answers. purgeBelow calls are
// recorded in purged.
type fakeStream struct {
	stateVal streamState
	stateErr error

	messageTimeFn func(ctx context.Context, seq uint64) (time.Time, error)

	ackFloorVal uint64
	ackFloorErr error

	mu       sync.Mutex
	purged   []uint64
	purgeErr error
}

func (f *fakeStream) state(context.Context, string) (streamState, error) {
	return f.stateVal, f.stateErr
}

func (f *fakeStream) messageTime(ctx context.Context, seq uint64) (time.Time, error) {
	if f.messageTimeFn == nil {
		return time.Time{}, errSequenceNotFound
	}
	return f.messageTimeFn(ctx, seq)
}

func (f *fakeStream) purgeBelow(_ context.Context, seq uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purged = append(f.purged, seq)
	return f.purgeErr
}

func (f *fakeStream) consumerAckFloor(context.Context, string) (uint64, error) {
	return f.ackFloorVal, f.ackFloorErr
}

// ---------------------------------------------------------------------------
// purgeAcked() tests
// ---------------------------------------------------------------------------

func TestPurgeAcked(t *testing.T) {
	t.Parallel()
	now := time.Now()
	defaultGapWindow := 5 * time.Minute
	cutoff := now.Add(-defaultGapWindow)

	// expiredBelow returns a MessageTimeFn where seqs <= threshold are before the
	// cutoff and seqs above are within the gap window.
	expiredBelow := func(threshold uint64) func(context.Context, uint64) (time.Time, error) {
		return func(_ context.Context, seq uint64) (time.Time, error) {
			if seq <= threshold {
				return cutoff.Add(-time.Second), nil
			}
			return cutoff.Add(time.Second), nil
		}
	}
	allWithinWindow := func(context.Context, uint64) (time.Time, error) {
		return now.Add(-1 * time.Minute), nil
	}
	allExpired := func(context.Context, uint64) (time.Time, error) {
		return now.Add(-10 * time.Minute), nil
	}

	tests := []struct {
		name           string
		gapWindow      time.Duration // 0 → defaultGapWindow
		state          streamState
		messageTimeFn  func(ctx context.Context, seq uint64) (time.Time, error)
		purgeErr       error
		ackFloor       uint64
		ackFloorErr    error
		wantPurgeCount int
		wantPurgeSeq   uint64 // the purgeBelow target when wantPurgeCount == 1
		wantErr        error  // matched with ErrorIs; wantAnyErr for an error with no sentinel
		wantAnyErr     bool
	}{
		{
			// gapSeq = 101, ackFloor+1 = 151 → target = min(151, 101) = 101
			name:           "gap sequence is the bottleneck",
			state:          streamState{FirstSeq: 1, LastSeq: 200},
			messageTimeFn:  expiredBelow(100),
			ackFloor:       150,
			wantPurgeCount: 1,
			wantPurgeSeq:   101,
		},
		{
			// gapSeq = 51, ackFloor+1 = 31 → target = min(31, 51) = 31
			name:           "ack floor is the bottleneck",
			state:          streamState{FirstSeq: 1, LastSeq: 200},
			messageTimeFn:  expiredBelow(50),
			ackFloor:       30,
			wantPurgeCount: 1,
			wantPurgeSeq:   31,
		},
		{
			name:           "no purge when all messages within window",
			gapWindow:      10 * time.Minute,
			state:          streamState{FirstSeq: 1, LastSeq: 50},
			messageTimeFn:  allWithinWindow,
			ackFloor:       50,
			wantPurgeCount: 0,
		},
		{
			// The consumer is created on the first ingested message; before that
			// the sweeper has nothing to reconcile against and must not purge.
			name:           "no purge when consumer not found",
			state:          streamState{FirstSeq: 1, LastSeq: 50},
			ackFloorErr:    ErrConsumerNotFound,
			wantPurgeCount: 0,
			wantErr:        ErrConsumerNotFound,
		},
		{
			// ackFloor+1 = 1, gapSeq = large → target = min(1, X) = 1 → skip
			name:           "no purge when target <= 1",
			state:          streamState{FirstSeq: 1, LastSeq: 10},
			messageTimeFn:  allExpired,
			ackFloor:       0,
			wantPurgeCount: 0,
		},
		{
			name:           "no purge when consumer info fails",
			state:          streamState{FirstSeq: 1, LastSeq: 50},
			ackFloorErr:    errors.New("info unavailable"),
			wantPurgeCount: 0,
			wantAnyErr:     true,
		},
		{
			// The purge call is still recorded by the fake; its error is returned.
			// gapSeq = 51, ackFloor+1 = 101 → target = 51
			name:           "purge error is returned",
			state:          streamState{FirstSeq: 1, LastSeq: 200},
			messageTimeFn:  expiredBelow(50),
			purgeErr:       errors.New("purge failed"),
			ackFloor:       100,
			wantPurgeCount: 1,
			wantPurgeSeq:   51,
			wantAnyErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := tt.gapWindow
			if gw == 0 {
				gw = defaultGapWindow
			}

			fs := &fakeStream{
				stateVal:      tt.state,
				messageTimeFn: tt.messageTimeFn,
				purgeErr:      tt.purgeErr,
				ackFloorVal:   tt.ackFloor,
				ackFloorErr:   tt.ackFloorErr,
			}

			report, err := purgeAcked(context.Background(), fs, "buffer", now.Add(-gw))

			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.wantAnyErr:
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
			assert.Equal(t, err == nil && tt.wantPurgeCount == 1, report.purged)
			assert.Len(t, fs.purged, tt.wantPurgeCount)
			if tt.wantPurgeCount == 1 && len(fs.purged) == 1 {
				assert.Equal(t, tt.wantPurgeSeq, fs.purged[0], "purge target = min(ackFloor+1, gapSeq)")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// findGapSequence() tests
// ---------------------------------------------------------------------------

func TestFindGapSequence(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute
	cutoff := now.Add(-gapWindow)

	allWithinWindow := func(context.Context, uint64) (time.Time, error) {
		return now.Add(-1 * time.Minute), nil
	}
	allExpired := func(context.Context, uint64) (time.Time, error) {
		return now.Add(-20 * time.Minute), nil
	}
	// boundary at seq 60: seqs 1..60 are before cutoff, 61..100 are within window.
	// Minute (not second) margins: the impl recomputes cutoff at call time, so
	// parallel-subtest scheduling delay eats into the margin — 1s margins flaked
	// on loaded 2-core CI runners (#283).
	boundaryAt60 := func(_ context.Context, seq uint64) (time.Time, error) {
		if seq <= 60 {
			return cutoff.Add(-time.Duration(61-seq) * time.Minute), nil
		}
		return cutoff.Add(time.Duration(seq-60) * time.Minute), nil //nolint:gosec // test-only, values are small
	}
	// missingBetween returns a lookup where seqs lo..hi hold no message, seqs
	// below boundary are old, and seqs from boundary on are within the window.
	missingBetween := func(lo, hi, boundary uint64) func(context.Context, uint64) (time.Time, error) {
		return func(_ context.Context, seq uint64) (time.Time, error) {
			if seq >= lo && seq <= hi {
				return time.Time{}, errSequenceNotFound
			}
			if seq < boundary {
				return cutoff.Add(-time.Minute), nil
			}
			return cutoff.Add(time.Minute), nil
		}
	}
	lookupFailsAt := func(bad uint64) func(context.Context, uint64) (time.Time, error) {
		return func(_ context.Context, seq uint64) (time.Time, error) {
			if seq == bad {
				return time.Time{}, errors.New("request timeout")
			}
			return cutoff.Add(-time.Minute), nil
		}
	}

	tests := []struct {
		name          string
		gapWindow     time.Duration
		state         streamState
		stateErr      error
		messageTimeFn func(ctx context.Context, seq uint64) (time.Time, error)
		wantSeq       uint64
		wantErrSub    string // substring of expected error; "" means no error
	}{
		{
			name:      "empty stream",
			gapWindow: 5 * time.Minute,
			state:     streamState{FirstSeq: 0, LastSeq: 0},
			wantSeq:   0,
		},
		{
			name:      "first seq greater than last seq",
			gapWindow: 5 * time.Minute,
			state:     streamState{FirstSeq: 10, LastSeq: 5},
			wantSeq:   0,
		},
		{
			name:          "all messages within window → 0",
			gapWindow:     10 * time.Minute,
			state:         streamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: allWithinWindow,
			wantSeq:       0,
		},
		{
			// All expired → result should be last+1 = 101 (binary search default).
			name:          "all expired → returns last+1",
			gapWindow:     5 * time.Minute,
			state:         streamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: allExpired,
			wantSeq:       101,
		},
		{
			name:          "boundary detection finds first seq within window",
			gapWindow:     gapWindow,
			state:         streamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: boundaryAt60,
			wantSeq:       61,
		},
		{
			// seqs 40..60 hold nothing, <40 are old, >60 are within window. The
			// bound stops at the hole: purging below 40 removes every old
			// message, the same ones a bound of 61 would.
			name:          "missing range straddles the boundary",
			gapWindow:     gapWindow,
			state:         streamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: missingBetween(40, 60, 61),
			wantSeq:       40,
		},
		{
			// The boundary is 40 and the hole (45..55) is inside the window. A
			// missing midpoint must not discard the lower half: answering 56
			// here would purge 40..44, which are inside the replay window.
			name:          "missing range after the boundary keeps the window intact",
			gapWindow:     gapWindow,
			state:         streamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: missingBetween(45, 55, 40),
			wantSeq:       40,
		},
		{
			// The boundary is 70 and the hole (45..55) is below it. The bound
			// stops where the hole starts — purging less than it could, never
			// more; the next sweep starts past the hole and finds 70.
			name:          "missing range before the boundary under-purges, never over",
			gapWindow:     gapWindow,
			state:         streamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: missingBetween(45, 55, 70),
			wantSeq:       45,
		},
		{
			// A lookup that fails says nothing about the sequence, so there is
			// no safe bound to return.
			name:          "failed lookup aborts the search",
			gapWindow:     gapWindow,
			state:         streamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: lookupFailsAt(50),
			wantErrSub:    "message time at 50",
		},
		{
			name:          "failed lookup of the oldest message aborts the search",
			gapWindow:     gapWindow,
			state:         streamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: lookupFailsAt(1),
			wantErrSub:    "message time at 1",
		},
		{
			name:       "stream info error",
			gapWindow:  5 * time.Minute,
			stateErr:   errors.New("stream info unavailable"),
			wantErrSub: "stream info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs := &fakeStream{
				stateVal:      tt.state,
				stateErr:      tt.stateErr,
				messageTimeFn: tt.messageTimeFn,
			}

			seq, err := findGapSequence(context.Background(), fs, now.Add(-tt.gapWindow))

			if tt.wantErrSub != "" {
				assert.ErrorContains(t, err, tt.wantErrSub)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantSeq, seq)
		})
	}
}
