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

func TestSweep_GapSeqIsBottleneck(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute

	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200},
		},
		Msgs: map[uint64]*jetstream.RawStreamMsg{
			1:   {Time: now.Add(-10 * time.Minute)},
			100: {Time: now.Add(-6 * time.Minute)},
			101: {Time: now.Add(-4 * time.Minute)},
			200: {Time: now},
		},
		GetMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			cutoff := now.Add(-gapWindow)
			// Simulate: seqs 1-100 are before cutoff, 101+ are within window.
			if seq <= 100 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Second)}, nil
		},
	}

	mc := &testutil.MockConsumer{
		InfoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 150},
		},
	}

	js := &testutil.MockJetStream{
		StreamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		ConsumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, gapWindow, testutil.NopLogger())
	s.sweep(context.Background())

	// gapSeq ~101, ackFloor+1 = 151 → target = min(151, 101) = 101
	assert.Len(t, ms.Purged, 1, "should have called Purge once")
}

func TestSweep_AckFloorIsBottleneck(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute

	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200},
		},
		GetMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			cutoff := now.Add(-gapWindow)
			if seq <= 50 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Second)}, nil
		},
	}

	mc := &testutil.MockConsumer{
		InfoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 30},
		},
	}

	js := &testutil.MockJetStream{
		StreamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		ConsumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, gapWindow, testutil.NopLogger())
	s.sweep(context.Background())

	// gapSeq ~51, ackFloor+1 = 31 → target = min(31, 51) = 31
	assert.Len(t, ms.Purged, 1, "should have called Purge once")
}

func TestSweep_AllWithinWindow_NoPurge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 10 * time.Minute

	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50},
		},
		GetMsgFn: func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{Time: now.Add(-1 * time.Minute)}, nil
		},
	}

	mc := &testutil.MockConsumer{
		InfoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 50},
		},
	}

	js := &testutil.MockJetStream{
		StreamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		ConsumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, gapWindow, testutil.NopLogger())
	s.sweep(context.Background())

	assert.Empty(t, ms.Purged, "no purge when all messages within gap window")
}

func TestSweep_ConsumerNotFound_NoPurge(t *testing.T) {
	t.Parallel()
	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50},
		},
	}

	js := &testutil.MockJetStream{
		StreamFn: func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		ConsumerFn: func(context.Context, string, string) (jetstream.Consumer, error) {
			return nil, errors.New("consumer not found")
		},
	}

	s := NewSweeper(js, 5*time.Minute, testutil.NopLogger())
	s.sweep(context.Background())

	assert.Empty(t, ms.Purged, "no purge when consumer not found")
}

func TestSweep_StreamError_NoPurge(t *testing.T) {
	t.Parallel()
	js := &testutil.MockJetStream{
		StreamFn: func(context.Context, string) (jetstream.Stream, error) {
			return nil, errors.New("stream unavailable")
		},
	}

	s := NewSweeper(js, 5*time.Minute, testutil.NopLogger())
	// Should not panic.
	s.sweep(context.Background())
}

func TestSweep_TargetLessOrEqualOne_NoPurge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute

	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 10},
		},
		GetMsgFn: func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{Time: now.Add(-10 * time.Minute)}, nil
		},
	}

	mc := &testutil.MockConsumer{
		InfoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 0},
		},
	}

	js := &testutil.MockJetStream{
		StreamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		ConsumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, gapWindow, testutil.NopLogger())
	s.sweep(context.Background())

	// ackFloor+1 = 1, gapSeq = some large number → target = min(1, X) = 1 → skip
	assert.Empty(t, ms.Purged, "no purge when target <= 1")
}

func TestSweep_ConsumerInfoError_NoPurge(t *testing.T) {
	t.Parallel()
	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50},
		},
	}

	mc := &testutil.MockConsumer{InfoErr: errors.New("info unavailable")}

	js := &testutil.MockJetStream{
		StreamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		ConsumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, 5*time.Minute, testutil.NopLogger())
	s.sweep(context.Background())

	assert.Empty(t, ms.Purged, "no purge when consumer info fails")
}

func TestSweep_PurgeError_Logged(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute

	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200},
		},
		GetMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			cutoff := now.Add(-gapWindow)
			if seq <= 50 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Second)}, nil
		},
		PurgeErr: errors.New("purge failed"),
	}

	mc := &testutil.MockConsumer{
		InfoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 100},
		},
	}

	js := &testutil.MockJetStream{
		StreamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		ConsumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, gapWindow, testutil.NopLogger())
	// Should not panic even when purge fails.
	s.sweep(context.Background())
}

// ---------------------------------------------------------------------------
// findGapSequence() tests
// ---------------------------------------------------------------------------

func TestFindGapSequence_EmptyStream(t *testing.T) {
	t.Parallel()
	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 0, LastSeq: 0},
		},
	}

	s := NewSweeper(nil, 5*time.Minute, testutil.NopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), seq)
}

func TestFindGapSequence_FirstSeqGTLastSeq(t *testing.T) {
	t.Parallel()
	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 10, LastSeq: 5},
		},
	}

	s := NewSweeper(nil, 5*time.Minute, testutil.NopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), seq)
}

func TestFindGapSequence_AllWithinWindow(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100},
		},
		GetMsgFn: func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{Time: now.Add(-1 * time.Minute)}, nil
		},
	}

	s := NewSweeper(nil, 10*time.Minute, testutil.NopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), seq, "all messages within window → 0")
}

func TestFindGapSequence_AllExpired(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100},
		},
		GetMsgFn: func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{Time: now.Add(-20 * time.Minute)}, nil
		},
	}

	s := NewSweeper(nil, 5*time.Minute, testutil.NopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	// All expired → result should be last+1 = 101 (binary search default)
	assert.Equal(t, uint64(101), seq)
}

func TestFindGapSequence_BoundaryDetection(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute
	cutoff := now.Add(-gapWindow)

	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100},
		},
		GetMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			// seqs 1-60 are before cutoff, 61-100 are within window
			if seq <= 60 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Duration(61-seq) * time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Duration(int64(seq-60)) * time.Second)}, nil //nolint:gosec // test-only, values are small
		},
	}

	s := NewSweeper(nil, gapWindow, testutil.NopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	assert.Equal(t, uint64(61), seq, "first seq within gap window should be 61")
}

func TestFindGapSequence_SparseSequences(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute
	cutoff := now.Add(-gapWindow)

	ms := &testutil.MockStream{
		InfoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100},
		},
		GetMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			// Seqs 40-60 are "purged" (missing). Seqs before 40 are old, after 60 are within window.
			if seq >= 40 && seq <= 60 {
				return nil, errors.New("not found")
			}
			if seq < 40 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Minute)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Minute)}, nil
		},
	}

	s := NewSweeper(nil, gapWindow, testutil.NopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	// With sparse seqs, binary search skips missing seqs.
	// The first available seq within window should be 61.
	assert.Equal(t, uint64(61), seq)
}

func TestFindGapSequence_StreamInfoError(t *testing.T) {
	t.Parallel()
	ms := &testutil.MockStream{InfoErr: errors.New("stream info unavailable")}

	s := NewSweeper(nil, 5*time.Minute, testutil.NopLogger())
	_, err := s.findGapSequence(context.Background(), ms)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stream info")
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
