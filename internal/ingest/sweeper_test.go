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

func TestSweep_AsksForTheBufferConsumerAndEachTenantsGapWindow(t *testing.T) {
	t.Parallel()
	windows := map[tenant.ID]time.Duration{"acme": 5 * time.Minute, "globex": time.Hour}
	purger := &testutil.MockPurger{Purged: true}
	s := NewSweeper(purger, func() map[tenant.ID]time.Duration { return windows })

	before := time.Now()
	s.sweep(context.Background())
	after := time.Now()

	require.Len(t, purger.Calls, 1)
	call := purger.Calls[0]
	assert.Equal(t, BufferConsumerName, call.Consumer)
	require.Len(t, call.OlderThan, 2, "one cutoff per tenant served")
	for id, window := range windows {
		cutoff := call.OlderThan[id]
		assert.False(t, cutoff.Before(before.Add(-window)), "%s: cutoff is now - its own gap window", id)
		assert.False(t, cutoff.After(after.Add(-window)), "%s: cutoff is now - its own gap window", id)
	}
}

func TestSweep_RereadsTheGapWindowsEverySweep(t *testing.T) {
	t.Parallel()
	windows := map[tenant.ID]time.Duration{"acme": time.Minute}
	purger := &testutil.MockPurger{}
	s := NewSweeper(purger, func() map[tenant.ID]time.Duration { return windows })

	s.sweep(context.Background())
	windows = map[tenant.ID]time.Duration{"acme": time.Hour, "globex": time.Minute} // a settings reload
	s.sweep(context.Background())

	require.Len(t, purger.Calls, 2)
	assert.Greater(t, purger.Calls[0].OlderThan["acme"].Sub(purger.Calls[1].OlderThan["acme"]), 50*time.Minute)
	assert.NotContains(t, purger.Calls[0].OlderThan, tenant.ID("globex"))
	assert.Contains(t, purger.Calls[1].OlderThan, tenant.ID("globex"), "a tenant adopted since is named from the next sweep")
}

func TestSweep_ErrorsDoNotPanic(t *testing.T) {
	t.Parallel()
	for _, err := range []error{mq.ErrConsumerNotFound, errors.New("broker unavailable")} {
		purger := &testutil.MockPurger{Err: err}
		s := NewSweeper(purger, func() map[tenant.ID]time.Duration { return map[tenant.ID]time.Duration{"acme": time.Minute} })
		s.sweep(context.Background())
		assert.Len(t, purger.Calls, 1)
	}
}

// ---------------------------------------------------------------------------
// Start() context cancellation test
// ---------------------------------------------------------------------------

func TestStart_ContextCancellation(t *testing.T) {
	t.Parallel()
	s := NewSweeper(&testutil.MockPurger{}, func() map[tenant.ID]time.Duration { return nil })

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
