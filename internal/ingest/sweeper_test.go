package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
)

// Shared MQ mocks live in internal/testutil/mocks.go — see
// testutil.MockStreamManager / MockStream.

// streamManagerFor resolves every stream lookup to ms, or fails with err.
func streamManagerFor(ms *testutil.MockStream, err error) *testutil.MockStreamManager {
	return &testutil.MockStreamManager{
		StreamFn: func(context.Context, string) (mq.Stream, error) {
			if err != nil {
				return nil, err
			}
			return ms, nil
		},
	}
}

// ---------------------------------------------------------------------------
// sweep() tests
// ---------------------------------------------------------------------------

func TestSweep(t *testing.T) {
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
		streamErr      error         // if non-nil, the stream lookup returns this (no MockStream is constructed)
		state          mq.StreamState
		messageTimeFn  func(ctx context.Context, seq uint64) (time.Time, error)
		purgeErr       error
		ackFloor       uint64
		ackFloorErr    error
		wantPurgeCount int
		wantPurgeSeq   uint64 // the PurgeBelow target when wantPurgeCount == 1
	}{
		{
			// gapSeq = 101, ackFloor+1 = 151 → target = min(151, 101) = 101
			name:           "gap sequence is the bottleneck",
			state:          mq.StreamState{FirstSeq: 1, LastSeq: 200},
			messageTimeFn:  expiredBelow(100),
			ackFloor:       150,
			wantPurgeCount: 1,
			wantPurgeSeq:   101,
		},
		{
			// gapSeq = 51, ackFloor+1 = 31 → target = min(31, 51) = 31
			name:           "ack floor is the bottleneck",
			state:          mq.StreamState{FirstSeq: 1, LastSeq: 200},
			messageTimeFn:  expiredBelow(50),
			ackFloor:       30,
			wantPurgeCount: 1,
			wantPurgeSeq:   31,
		},
		{
			name:           "no purge when all messages within window",
			gapWindow:      10 * time.Minute,
			state:          mq.StreamState{FirstSeq: 1, LastSeq: 50},
			messageTimeFn:  allWithinWindow,
			ackFloor:       50,
			wantPurgeCount: 0,
		},
		{
			// The consumer is created on the first ingested message; before that
			// the sweeper has nothing to reconcile against and must not purge.
			name:           "no purge when consumer not found",
			state:          mq.StreamState{FirstSeq: 1, LastSeq: 50},
			ackFloorErr:    mq.ErrConsumerNotFound,
			wantPurgeCount: 0,
		},
		{
			name:      "no purge when stream lookup fails (must not panic)",
			streamErr: errors.New("stream unavailable"),
		},
		{
			// ackFloor+1 = 1, gapSeq = large → target = min(1, X) = 1 → skip
			name:           "no purge when target <= 1",
			state:          mq.StreamState{FirstSeq: 1, LastSeq: 10},
			messageTimeFn:  allExpired,
			ackFloor:       0,
			wantPurgeCount: 0,
		},
		{
			name:           "no purge when consumer info fails",
			state:          mq.StreamState{FirstSeq: 1, LastSeq: 50},
			ackFloorErr:    errors.New("info unavailable"),
			wantPurgeCount: 0,
		},
		{
			// Purge call is still recorded by the mock; sweep must not panic on the error.
			// gapSeq = 51, ackFloor+1 = 101 → target = 51
			name:           "purge error is logged",
			state:          mq.StreamState{FirstSeq: 1, LastSeq: 200},
			messageTimeFn:  expiredBelow(50),
			purgeErr:       errors.New("purge failed"),
			ackFloor:       100,
			wantPurgeCount: 1,
			wantPurgeSeq:   51,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := tt.gapWindow
			if gw == 0 {
				gw = defaultGapWindow
			}

			var ms *testutil.MockStream
			if tt.streamErr == nil {
				ms = &testutil.MockStream{
					StateVal:      tt.state,
					MessageTimeFn: tt.messageTimeFn,
					PurgeErr:      tt.purgeErr,
					AckFloorVal:   tt.ackFloor,
					AckFloorErr:   tt.ackFloorErr,
				}
			}

			s := NewSweeper(streamManagerFor(ms, tt.streamErr), func() time.Duration { return gw }, testutil.NopLogger())
			s.sweep(context.Background())

			if ms != nil {
				assert.Len(t, ms.Purged, tt.wantPurgeCount)
				if tt.wantPurgeCount == 1 && len(ms.Purged) == 1 {
					assert.Equal(t, tt.wantPurgeSeq, ms.Purged[0], "purge target = min(ackFloor+1, gapSeq)")
				}
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
	// seqs 40..60 missing (already purged), <40 are old, >60 are within window.
	sparseSequences := func(_ context.Context, seq uint64) (time.Time, error) {
		if seq >= 40 && seq <= 60 {
			return time.Time{}, errors.New("not found")
		}
		if seq < 40 {
			return cutoff.Add(-time.Minute), nil
		}
		return cutoff.Add(time.Minute), nil
	}

	tests := []struct {
		name          string
		gapWindow     time.Duration
		state         mq.StreamState
		stateErr      error
		messageTimeFn func(ctx context.Context, seq uint64) (time.Time, error)
		wantSeq       uint64
		wantErrSub    string // substring of expected error; "" means no error
	}{
		{
			name:      "empty stream",
			gapWindow: 5 * time.Minute,
			state:     mq.StreamState{FirstSeq: 0, LastSeq: 0},
			wantSeq:   0,
		},
		{
			name:      "first seq greater than last seq",
			gapWindow: 5 * time.Minute,
			state:     mq.StreamState{FirstSeq: 10, LastSeq: 5},
			wantSeq:   0,
		},
		{
			name:          "all messages within window → 0",
			gapWindow:     10 * time.Minute,
			state:         mq.StreamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: allWithinWindow,
			wantSeq:       0,
		},
		{
			// All expired → result should be last+1 = 101 (binary search default).
			name:          "all expired → returns last+1",
			gapWindow:     5 * time.Minute,
			state:         mq.StreamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: allExpired,
			wantSeq:       101,
		},
		{
			name:          "boundary detection finds first seq within window",
			gapWindow:     gapWindow,
			state:         mq.StreamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: boundaryAt60,
			wantSeq:       61,
		},
		{
			// Binary search skips missing seqs; first available within window is 61.
			name:          "sparse sequences",
			gapWindow:     gapWindow,
			state:         mq.StreamState{FirstSeq: 1, LastSeq: 100},
			messageTimeFn: sparseSequences,
			wantSeq:       61,
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
			ms := &testutil.MockStream{
				StateVal:      tt.state,
				StateErr:      tt.stateErr,
				MessageTimeFn: tt.messageTimeFn,
			}

			s := NewSweeper(nil, func() time.Duration { return tt.gapWindow }, testutil.NopLogger())
			seq, err := s.findGapSequence(context.Background(), ms)

			if tt.wantErrSub != "" {
				assert.ErrorContains(t, err, tt.wantErrSub)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantSeq, seq)
		})
	}
}

// ---------------------------------------------------------------------------
// Start() context cancellation test
// ---------------------------------------------------------------------------

func TestStart_ContextCancellation(t *testing.T) {
	t.Parallel()
	streams := streamManagerFor(nil, errors.New("not used"))

	s := NewSweeper(streams, func() time.Duration { return 5 * time.Minute }, testutil.NopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Success — Start returned.
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}
