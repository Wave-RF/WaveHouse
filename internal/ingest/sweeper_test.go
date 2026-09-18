package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The purge-point arithmetic is the MQ's (internal/mq/purge_test.go); the
// sweeper owns only when to ask and what window to ask for.

func TestSweep_AsksForTheBufferConsumerAndTheGapWindow(t *testing.T) {
	t.Parallel()
	gapWindow := 5 * time.Minute
	purger := &testutil.MockPurger{Purged: true}
	s := NewSweeper(purger, tenant.Default, func(tenant.ID) time.Duration { return gapWindow })

	before := time.Now()
	s.sweep(context.Background())
	after := time.Now()

	require.Len(t, purger.Calls, 1)
	call := purger.Calls[0]
	assert.Equal(t, BufferConsumerName, call.Consumer)
	assert.False(t, call.OlderThan.Before(before.Add(-gapWindow)), "cutoff is now - gap window")
	assert.False(t, call.OlderThan.After(after.Add(-gapWindow)), "cutoff is now - gap window")
}

func TestSweep_RereadsTheGapWindowEverySweep(t *testing.T) {
	t.Parallel()
	gapWindow := time.Minute
	purger := &testutil.MockPurger{}
	s := NewSweeper(purger, tenant.Default, func(tenant.ID) time.Duration { return gapWindow })

	s.sweep(context.Background())
	gapWindow = time.Hour // a settings reload
	s.sweep(context.Background())

	require.Len(t, purger.Calls, 2)
	assert.Greater(t, purger.Calls[0].OlderThan.Sub(purger.Calls[1].OlderThan), 50*time.Minute)
}

func TestSweep_ErrorsDoNotPanic(t *testing.T) {
	t.Parallel()
	for _, err := range []error{mq.ErrConsumerNotFound, errors.New("broker unavailable")} {
		purger := &testutil.MockPurger{Err: err}
		s := NewSweeper(purger, tenant.Default, func(tenant.ID) time.Duration { return time.Minute })
		s.sweep(context.Background())
		assert.Len(t, purger.Calls, 1)
	}
}

// ---------------------------------------------------------------------------
// Start() context cancellation test
// ---------------------------------------------------------------------------

func TestStart_ContextCancellation(t *testing.T) {
	t.Parallel()
	s := NewSweeper(&testutil.MockPurger{}, tenant.Default, func(tenant.ID) time.Duration { return 5 * time.Minute })

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
