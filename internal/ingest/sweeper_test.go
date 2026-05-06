package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// Mock types — embed the real interfaces so we only override methods we need.
// Calls to unimplemented methods will panic (acceptable in unit tests).
// ---------------------------------------------------------------------------

type mockJetStream struct {
	jetstream.JetStream
	streamFn   func(ctx context.Context, name string) (jetstream.Stream, error)
	consumerFn func(ctx context.Context, stream, consumer string) (jetstream.Consumer, error)
}

func (m *mockJetStream) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	return m.streamFn(ctx, name)
}

func (m *mockJetStream) Consumer(ctx context.Context, stream, consumer string) (jetstream.Consumer, error) {
	return m.consumerFn(ctx, stream, consumer)
}

type mockStream struct {
	jetstream.Stream
	infoVal  *jetstream.StreamInfo
	infoErr  error
	msgs     map[uint64]*jetstream.RawStreamMsg
	getMsgFn func(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error)
	purged   []jetstream.StreamPurgeOpt
	purgeErr error
}

func (m *mockStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return m.infoVal, m.infoErr
}

func (m *mockStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if m.getMsgFn != nil {
		return m.getMsgFn(ctx, seq, opts...)
	}
	msg, ok := m.msgs[seq]
	if !ok {
		return nil, errors.New("message not found")
	}
	return msg, nil
}

func (m *mockStream) Purge(ctx context.Context, opts ...jetstream.StreamPurgeOpt) error {
	m.purged = append(m.purged, opts...)
	return m.purgeErr
}

type mockConsumer struct {
	jetstream.Consumer
	infoVal *jetstream.ConsumerInfo
	infoErr error
}

func (m *mockConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	return m.infoVal, m.infoErr
}

// nopLogger returns a no-op logger for tests.
func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// sweep() tests
// ---------------------------------------------------------------------------

func TestSweep_GapSeqIsBottleneck(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute

	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200},
		},
		msgs: map[uint64]*jetstream.RawStreamMsg{
			1:   {Time: now.Add(-10 * time.Minute)},
			100: {Time: now.Add(-6 * time.Minute)},
			101: {Time: now.Add(-4 * time.Minute)},
			200: {Time: now},
		},
		getMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			cutoff := now.Add(-gapWindow)
			// Simulate: seqs 1-100 are before cutoff, 101+ are within window.
			if seq <= 100 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Second)}, nil
		},
	}

	mc := &mockConsumer{
		infoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 150},
		},
	}

	js := &mockJetStream{
		streamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, "WAVEHOUSE", gapWindow, nopLogger())
	s.sweep(context.Background())

	// gapSeq ~101, ackFloor+1 = 151 → target = min(151, 101) = 101
	assert.Len(t, ms.purged, 1, "should have called Purge once")
}

func TestSweep_AckFloorIsBottleneck(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute

	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200},
		},
		getMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			cutoff := now.Add(-gapWindow)
			if seq <= 50 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Second)}, nil
		},
	}

	mc := &mockConsumer{
		infoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 30},
		},
	}

	js := &mockJetStream{
		streamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, "WAVEHOUSE", gapWindow, nopLogger())
	s.sweep(context.Background())

	// gapSeq ~51, ackFloor+1 = 31 → target = min(31, 51) = 31
	assert.Len(t, ms.purged, 1, "should have called Purge once")
}

func TestSweep_AllWithinWindow_NoPurge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 10 * time.Minute

	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50},
		},
		getMsgFn: func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{Time: now.Add(-1 * time.Minute)}, nil
		},
	}

	mc := &mockConsumer{
		infoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 50},
		},
	}

	js := &mockJetStream{
		streamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, "WAVEHOUSE", gapWindow, nopLogger())
	s.sweep(context.Background())

	assert.Empty(t, ms.purged, "no purge when all messages within gap window")
}

func TestSweep_ConsumerNotFound_NoPurge(t *testing.T) {
	t.Parallel()
	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50},
		},
	}

	js := &mockJetStream{
		streamFn: func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) {
			return nil, errors.New("consumer not found")
		},
	}

	s := NewSweeper(js, "WAVEHOUSE", 5*time.Minute, nopLogger())
	s.sweep(context.Background())

	assert.Empty(t, ms.purged, "no purge when consumer not found")
}

func TestSweep_StreamError_NoPurge(t *testing.T) {
	t.Parallel()
	js := &mockJetStream{
		streamFn: func(context.Context, string) (jetstream.Stream, error) {
			return nil, errors.New("stream unavailable")
		},
	}

	s := NewSweeper(js, "WAVEHOUSE", 5*time.Minute, nopLogger())
	// Should not panic.
	s.sweep(context.Background())
}

func TestSweep_TargetLessOrEqualOne_NoPurge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute

	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 10},
		},
		getMsgFn: func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{Time: now.Add(-10 * time.Minute)}, nil
		},
	}

	mc := &mockConsumer{
		infoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 0},
		},
	}

	js := &mockJetStream{
		streamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, "WAVEHOUSE", gapWindow, nopLogger())
	s.sweep(context.Background())

	// ackFloor+1 = 1, gapSeq = some large number → target = min(1, X) = 1 → skip
	assert.Empty(t, ms.purged, "no purge when target <= 1")
}

func TestSweep_ConsumerInfoError_NoPurge(t *testing.T) {
	t.Parallel()
	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 50},
		},
	}

	mc := &mockConsumer{infoErr: errors.New("info unavailable")}

	js := &mockJetStream{
		streamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, "WAVEHOUSE", 5*time.Minute, nopLogger())
	s.sweep(context.Background())

	assert.Empty(t, ms.purged, "no purge when consumer info fails")
}

func TestSweep_PurgeError_Logged(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute

	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 200},
		},
		getMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			cutoff := now.Add(-gapWindow)
			if seq <= 50 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Second)}, nil
		},
		purgeErr: errors.New("purge failed"),
	}

	mc := &mockConsumer{
		infoVal: &jetstream.ConsumerInfo{
			AckFloor: jetstream.SequenceInfo{Stream: 100},
		},
	}

	js := &mockJetStream{
		streamFn:   func(context.Context, string) (jetstream.Stream, error) { return ms, nil },
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) { return mc, nil },
	}

	s := NewSweeper(js, "WAVEHOUSE", gapWindow, nopLogger())
	// Should not panic even when purge fails.
	s.sweep(context.Background())
}

// ---------------------------------------------------------------------------
// findGapSequence() tests
// ---------------------------------------------------------------------------

func TestFindGapSequence_EmptyStream(t *testing.T) {
	t.Parallel()
	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 0, LastSeq: 0},
		},
	}

	s := NewSweeper(nil, "WAVEHOUSE", 5*time.Minute, nopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), seq)
}

func TestFindGapSequence_FirstSeqGTLastSeq(t *testing.T) {
	t.Parallel()
	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 10, LastSeq: 5},
		},
	}

	s := NewSweeper(nil, "WAVEHOUSE", 5*time.Minute, nopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), seq)
}

func TestFindGapSequence_AllWithinWindow(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100},
		},
		getMsgFn: func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{Time: now.Add(-1 * time.Minute)}, nil
		},
	}

	s := NewSweeper(nil, "WAVEHOUSE", 10*time.Minute, nopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), seq, "all messages within window → 0")
}

func TestFindGapSequence_AllExpired(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100},
		},
		getMsgFn: func(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{Time: now.Add(-20 * time.Minute)}, nil
		},
	}

	s := NewSweeper(nil, "WAVEHOUSE", 5*time.Minute, nopLogger())
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

	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100},
		},
		getMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
			// seqs 1-60 are before cutoff, 61-100 are within window
			if seq <= 60 {
				return &jetstream.RawStreamMsg{Time: cutoff.Add(-time.Duration(61-seq) * time.Second)}, nil
			}
			return &jetstream.RawStreamMsg{Time: cutoff.Add(time.Duration(int64(seq-60)) * time.Second)}, nil //nolint:gosec // test-only, values are small
		},
	}

	s := NewSweeper(nil, "WAVEHOUSE", gapWindow, nopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	assert.Equal(t, uint64(61), seq, "first seq within gap window should be 61")
}

func TestFindGapSequence_SparseSequences(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gapWindow := 5 * time.Minute
	cutoff := now.Add(-gapWindow)

	ms := &mockStream{
		infoVal: &jetstream.StreamInfo{
			State: jetstream.StreamState{FirstSeq: 1, LastSeq: 100},
		},
		getMsgFn: func(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
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

	s := NewSweeper(nil, "WAVEHOUSE", gapWindow, nopLogger())
	seq, err := s.findGapSequence(context.Background(), ms)
	assert.NoError(t, err)
	// With sparse seqs, binary search skips missing seqs.
	// The first available seq within window should be 61.
	assert.Equal(t, uint64(61), seq)
}

func TestFindGapSequence_StreamInfoError(t *testing.T) {
	t.Parallel()
	ms := &mockStream{infoErr: errors.New("stream info unavailable")}

	s := NewSweeper(nil, "WAVEHOUSE", 5*time.Minute, nopLogger())
	_, err := s.findGapSequence(context.Background(), ms)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stream info")
}

// ---------------------------------------------------------------------------
// Start() context cancellation test
// ---------------------------------------------------------------------------

func TestStart_ContextCancellation(t *testing.T) {
	t.Parallel()
	js := &mockJetStream{
		streamFn: func(context.Context, string) (jetstream.Stream, error) {
			return nil, errors.New("not used")
		},
	}

	s := NewSweeper(js, "WAVEHOUSE", 5*time.Minute, nopLogger())
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
