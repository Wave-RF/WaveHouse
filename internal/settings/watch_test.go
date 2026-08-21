package settings

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStore_Watch_ReloadsOnChange pins the watcher end to end: an edit to a
// settings file lands in the snapshot without any explicit reload call. The
// debounce makes exact timing untestable, so the assertion polls.
func TestStore_Watch_ReloadsOnChange(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 100}}`),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Watch(ctx) }()

	// Give the watcher a beat to register before writing, so the event isn't
	// emitted before the watch exists.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": 900}}`)), 0o600))

	assert.Eventually(t, func() bool { return s.DefaultMaxRows() == 900 },
		5*time.Second, 50*time.Millisecond, "watcher should adopt the edited file")

	// An invalid edit is debounced, rejected, and the snapshot survives.
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), FileConfig), []byte(`not json`), 0o600))
	time.Sleep(4 * watchDebounce)
	assert.Equal(t, 900, s.DefaultMaxRows(), "invalid edit must keep the previous snapshot")

	cancel()
	assert.NoError(t, <-done)
}

// TestStore_Watch_MissingDir pins the setup contract: a nonexistent directory
// is a returned error (main.go logs it and degrades to SIGHUP + the ops
// endpoint), not a silent no-op loop.
func TestStore_Watch_MissingDir(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, nil)
	require.NoError(t, os.RemoveAll(s.Dir()))
	err := s.Watch(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "settings watcher")
}
