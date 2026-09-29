package coord_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

const retry = 5 * time.Millisecond

func TestMain(m *testing.M) {
	logtest.Silence()
	os.Exit(m.Run())
}

// runElected starts RunElected in the background and returns its result.
func runElected(ctx context.Context, c coord.Coordinator, fn func(context.Context, coord.Term) error) <-chan error {
	res := make(chan error, 1)
	go func() { res <- coord.RunElected(ctx, c, "sweeper", retry, fn) }()
	return res
}

func wait(t *testing.T, res <-chan error) error {
	t.Helper()
	select {
	case err := <-res:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("RunElected did not return")
		return nil
	}
}

func TestRunElected_WaitsForTheLeaseThenRuns(t *testing.T) {
	l := coord.NewLocal()
	rival, err := l.Peer().TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err)

	running := make(chan coord.Term, 1)
	ctx, cancel := context.WithCancel(t.Context())
	res := runElected(ctx, l, func(ctx context.Context, term coord.Term) error {
		running <- term
		<-ctx.Done()
		return ctx.Err()
	})

	select {
	case <-running:
		t.Fatal("ran while another holder had the lease")
	case <-time.After(10 * retry):
	}
	require.NoError(t, rival.Resign(t.Context()))
	term := <-running
	assert.Greater(t, term.Token(), rival.Token())

	cancel()
	require.NoError(t, wait(t, res), "a stop is clean whatever fn reports on its way out")
	_, err = l.Peer().TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err, "the lease is handed on when the loop stops")
}

func TestRunElected_CampaignsAgainAfterLoss(t *testing.T) {
	l := coord.NewLocal()
	terms := make(chan coord.Term, 2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	res := runElected(ctx, l, func(ctx context.Context, term coord.Term) error {
		terms <- term
		<-ctx.Done()
		return errors.New("stopping") // after the term ended: its stop, not a failure
	})

	first := <-terms
	l.Revoke("sweeper")
	second := <-terms
	assert.Greater(t, second.Token(), first.Token())
	require.ErrorIs(t, first.Err(), coord.ErrLost)

	cancel()
	require.NoError(t, wait(t, res))
}

func TestRunElected_ReturnsFnsError(t *testing.T) {
	l := coord.NewLocal()
	boom := errors.New("boom")
	err := wait(t, runElected(t.Context(), l, func(context.Context, coord.Term) error { return boom }))
	require.ErrorIs(t, err, boom)
	_, err = l.Peer().TryAcquire(t.Context(), "sweeper")
	require.NoError(t, err, "a failed term is resigned")
}

func TestRunElected_CampaignsAgainAfterFnReturns(t *testing.T) {
	var runs atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	res := runElected(ctx, coord.NewLocal(), func(context.Context, coord.Term) error {
		if runs.Add(1) == 3 {
			cancel()
		}
		return nil
	})
	require.NoError(t, wait(t, res))
	assert.Equal(t, int32(3), runs.Load())
}

// failing is a coordinator whose campaigns fail with err.
type failing struct {
	err   error
	calls atomic.Int32
}

func (f *failing) TryAcquire(context.Context, string) (coord.Term, error) {
	f.calls.Add(1)
	return nil, f.err
}
func (f *failing) Close(context.Context) error { return nil }

func TestRunElected_CampaignErrors(t *testing.T) {
	t.Run("closed ends the loop", func(t *testing.T) {
		l := coord.NewLocal()
		require.NoError(t, l.Close(t.Context()))
		err := wait(t, runElected(t.Context(), l, func(context.Context, coord.Term) error { return nil }))
		require.ErrorIs(t, err, coord.ErrClosed)
	})
	t.Run("an unreachable backend is retried", func(t *testing.T) {
		f := &failing{err: errors.New("connection refused")}
		ctx, cancel := context.WithCancel(t.Context())
		res := runElected(ctx, f, func(context.Context, coord.Term) error { return nil })
		require.Eventually(t, func() bool { return f.calls.Load() >= 3 }, 5*time.Second, retry)
		cancel()
		require.NoError(t, wait(t, res))
	})
	t.Run("a canceled campaign is a stop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.NoError(t, wait(t, runElected(ctx, coord.NewLocal(), func(context.Context, coord.Term) error {
			t.Error("ran under a canceled context")
			return nil
		})))
	})
}
