package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

// Shared JetStream mocks live in internal/testutil/mocks.go — see
// testutil.MockJetStream / MockStream / MockConsumer.

// ---------------------------------------------------------------------------
// sweep() tests
// ---------------------------------------------------------------------------

func TestSweep(t *testing.T) {
	t.Parallel()
	now := time.Now()
	defaultGapWindow := 5 * time.Minute
	cutoff := now.Add(-defaultGapWindow)

	// expiredBelow returns a GetMsgFn where seqs <= threshold are before the
	// cutoff and seqs above are within the gap window.
	expiredBelow := func(threshold uint64) func(context.Context, uint64, ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
		return func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			if seq <= threshold {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Second)}, nil
		}
	}
	allWithinWindow := func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
		return &jetstream.RawStreamMsg{Time: now.Add(-1 * time.Minute)}, nil
	}
	allExpired := func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
		return &jetstream.RawStreamMsg{Time: now.Add(-10 * time.Minute)}, nil
	}

	tests := []struct {
		name            string
		gapWindow       time.Duration // 0 → defaultGapWindow
		streamErr       error         // if non-nil, StreamFn returns this (no MockStream is constructed)
		streamInfo      *jetstream.StreamInfo
		getMsgFn        func(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error)
		purgeErr        error
		consumerErr     error // if non-nil, ConsumerFn returns this (no MockConsumer is constructed)
		consumerInfo    *jetstream.ConsumerInfo
		consumerInfoErr error
		wantPurgeCount  int
	}{
		{
			// gapSeq ~101, ackFloor+1 = 151 → target = min(151, 101) = 101
			name:           "gap sequence is the bottleneck",
			streamInfo:     &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200}},
			getMsgFn:       expiredBelow(100),
			consumerInfo:   &jetstream.ConsumerInfo{AckFloor: jetstream.SequenceInfo{Stream: 150}},
			wantPurgeCount: 1,
		},
		{
			// gapSeq ~51, ackFloor+1 = 31 → target = min(31, 51) = 31
			name:           "ack floor is the bottleneck",
			streamInfo:     &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200}},
			getMsgFn:       expiredBelow(50),
			consumerInfo:   &jetstream.ConsumerInfo{AckFloor: jetstream.SequenceInfo{Stream: 30}},
			wantPurgeCount: 1,
		},
		{
			name:           "no purge when all messages within window",
			gapWindow:      10 * time.Minute,
			streamInfo:     &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50}},
			getMsgFn:       allWithinWindow,
			consumerInfo:   &jetstream.ConsumerInfo{AckFloor: jetstream.SequenceInfo{Stream: 50}},
			wantPurgeCount: 0,
		},
		{
			name:           "no purge when consumer not found",
			streamInfo:     &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50}},
			consumerErr:    errors.New("consumer not found"),
			wantPurgeCount: 0,
		},
		{
			name:      "no purge when stream lookup fails (must not panic)",
			streamErr: errors.New("stream unavailable"),
		},
		{
			// ackFloor+1 = 1, gapSeq = large → target = min(1, X) = 1 → skip
			name:           "no purge when target <= 1",
			streamInfo:     &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 10}},
			getMsgFn:       allExpired,
			consumerInfo:   &jetstream.ConsumerInfo{AckFloor: jetstream.SequenceInfo{Stream: 0}},
			wantPurgeCount: 0,
		},
		{
			name:            "no purge when consumer info fails",
			streamInfo:      &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50}},
			consumerInfoErr: errors.New("info unavailable"),
			wantPurgeCount:  0,
		},
		{
			// Purge call is still recorded by the mock; sweep must not panic on the error.
			name:           "purge error is logged",
			streamInfo:     &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200}},
			getMsgFn:       expiredBelow(50),
			purgeErr:       errors.New("purge failed"),
			consumerInfo:   &jetstream.ConsumerInfo{AckFloor: jetstream.SequenceInfo{Stream: 100}},
			wantPurgeCount: 1,
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
					InfoVal:  tt.streamInfo,
					GetMsgFn: tt.getMsgFn,
					PurgeErr: tt.purgeErr,
				}
			}
			var mc *testutil.MockConsumer
			if tt.consumerErr == nil {
				mc = &testutil.MockConsumer{
					InfoVal: tt.consumerInfo,
					InfoErr: tt.consumerInfoErr,
				}
			}
			js := &testutil.MockJetStream{
				StreamFn: func(context.Context, string) (jetstream.Stream, error) {
					if tt.streamErr != nil {
						return nil, tt.streamErr
					}
					return ms, nil
				},
				ConsumerFn: func(context.Context, string, string) (jetstream.Consumer, error) {
					if tt.consumerErr != nil {
						return nil, tt.consumerErr
					}
					return mc, nil
				},
			}

			s := NewSweeper(js, gw, testutil.NopLogger())
			s.sweep(context.Background())

			if ms != nil {
				assert.Len(t, ms.Purged, tt.wantPurgeCount)
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

	allWithinWindow := func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
		return &jetstream.RawStreamMsg{Time: now.Add(-1 * time.Minute)}, nil
	}
	allExpired := func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
		return &jetstream.RawStreamMsg{Time: now.Add(-20 * time.Minute)}, nil
	}
	// boundary at seq 60: seqs 1..60 are before cutoff, 61..100 are within window.
	// Minute (not second) margins: the impl recomputes cutoff at call time, so
	// parallel-subtest scheduling delay eats into the margin — 1s margins flaked
	// on loaded 2-core CI runners (#283).
	boundaryAt60 := func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
		if seq <= 60 {
			return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Duration(61-seq) * time.Minute)}, nil
		}
		return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Duration(int64(seq-60)) * time.Minute)}, nil //nolint:gosec // test-only, values are small
	}
	// seqs 40..60 missing (already purged), <40 are old, >60 are within window.
	sparseSequences := func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
		if seq >= 40 && seq <= 60 {
			return nil, errors.New("not found")
		}
		if seq < 40 {
			return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Minute)}, nil
		}
		return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Minute)}, nil
	}

	tests := []struct {
		name          string
		gapWindow     time.Duration
		streamInfo    *jetstream.StreamInfo
		streamInfoErr error
		getMsgFn      func(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error)
		wantSeq       uint64
		wantErrSub    string // substring of expected error; "" means no error
	}{
		{
			name:       "empty stream",
			gapWindow:  5 * time.Minute,
			streamInfo: &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 0, LastSeq: 0}},
			wantSeq:    0,
		},
		{
			name:       "first seq greater than last seq",
			gapWindow:  5 * time.Minute,
			streamInfo: &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 10, LastSeq: 5}},
			wantSeq:    0,
		},
		{
			name:       "all messages within window → 0",
			gapWindow:  10 * time.Minute,
			streamInfo: &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100}},
			getMsgFn:   allWithinWindow,
			wantSeq:    0,
		},
		{
			// All expired → result should be last+1 = 101 (binary search default).
			name:       "all expired → returns last+1",
			gapWindow:  5 * time.Minute,
			streamInfo: &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100}},
			getMsgFn:   allExpired,
			wantSeq:    101,
		},
		{
			name:       "boundary detection finds first seq within window",
			gapWindow:  gapWindow,
			streamInfo: &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100}},
			getMsgFn:   boundaryAt60,
			wantSeq:    61,
		},
		{
			// Binary search skips missing seqs; first available within window is 61.
			name:       "sparse sequences",
			gapWindow:  gapWindow,
			streamInfo: &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100}},
			getMsgFn:   sparseSequences,
			wantSeq:    61,
		},
		{
			name:          "stream info error",
			gapWindow:     5 * time.Minute,
			streamInfoErr: errors.New("stream info unavailable"),
			wantErrSub:    "stream info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ms := &testutil.MockStream{
				InfoVal:  tt.streamInfo,
				InfoErr:  tt.streamInfoErr,
				GetMsgFn: tt.getMsgFn,
			}

			s := NewSweeper(nil, tt.gapWindow, testutil.NopLogger())
			seq, err := s.findGapSequence(context.Background(), ms)

			if tt.wantErrSub != "" {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tt.wantErrSub)
				}
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
	js := &testutil.MockJetStream{
		StreamFn: func(context.Context, string) (jetstream.Stream, error) {
			return nil, errors.New("not used")
		},
	}

	s := NewSweeper(js, 5*time.Minute, testutil.NopLogger())
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
