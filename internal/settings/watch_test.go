package settings

import (
	"context"
	"fmt"
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
// startWatch runs s.Watch in the background and proves the watch is live
// before returning: it writes a valid config.json with maxRows and waits for
// the watcher to adopt it. A fixed sleep could not distinguish "watch
// registered" from "write raced ahead of w.Add", and with the watch proven
// live, the caller's later mutations can't be mistaken for setup races.
func startWatch(ctx context.Context, t *testing.T, s *Store, maxRows int) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.Watch(ctx) }()
	// Keep writing until the watcher picks one up: the write and w.Add race,
	// and a write that lands before the watch exists emits no event.
	// EventuallyWithT rather than Eventually: the condition runs off the test
	// goroutine, where a bare require.NoError would FailNow the wrong
	// goroutine and stall the poll until timeout instead of failing loudly.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.NoError(c, os.WriteFile(filepath.Join(s.Dir(), FileConfig), []byte(configJSON(fmt.Sprintf(`{"query": {"default_max_rows": %d}}`, maxRows))), 0o600))
		assert.Equal(c, maxRows, s.DefaultMaxRows())
	}, 5*time.Second, 2*watchDebounce, "watcher should adopt the readiness write")
	return done
}

func TestStore_Watch_ReloadsOnChange(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 100}}`),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startWatch(ctx, t, s, 900)

	// An invalid edit is debounced, rejected, and the snapshot survives.
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), FileConfig), []byte(`not json`), 0o600))
	time.Sleep(4 * watchDebounce)
	assert.Equal(t, 900, s.DefaultMaxRows(), "invalid edit must keep the previous snapshot")

	cancel()
	assert.NoError(t, <-done)
}

// TestStore_Watch_CatchesUpOnStart pins the boot gap: an edit that lands after
// Open's read but before the watch exists fires no event, so Watch reloads
// once as soon as the watch is registered instead of waiting for the next one.
func TestStore_Watch_CatchesUpOnStart(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 100}}`),
	})
	// The "in-between" edit: after Open, before Watch.
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": 700}}`)), 0o600))
	assert.Equal(t, 100, s.DefaultMaxRows(), "no watch yet, so nothing has adopted the edit")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Watch(ctx) }()
	assert.Eventually(t, func() bool { return s.DefaultMaxRows() == 700 },
		5*time.Second, 50*time.Millisecond, "Watch should reload once on start without any further edit")

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

// TestStore_Watch_SurvivesDirectoryRecreate pins the recreate contract:
// fsnotify drops a watch whose directory is removed, so without the parent
// watch + re-add a delete-and-recreate would leave later edits unwatched.
func TestStore_Watch_SurvivesDirectoryRecreate(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 100}}`),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startWatch(ctx, t, s, 200)

	// Remove the whole directory: a rejected reload, snapshot survives.
	require.NoError(t, os.RemoveAll(s.Dir()))
	time.Sleep(4 * watchDebounce)
	assert.Equal(t, 200, s.DefaultMaxRows(), "vanished directory must keep the previous snapshot")

	// Recreate it with new content: the watcher must pick it up again.
	files := validFiles()
	files[FileConfig] = configJSON(`{"query": {"default_max_rows": 300}}`)
	require.NoError(t, os.Mkdir(s.Dir(), 0o750))
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), name), []byte(content), 0o600))
	}
	assert.Eventually(t, func() bool { return s.DefaultMaxRows() == 300 },
		5*time.Second, 50*time.Millisecond, "recreated directory should be adopted")

	// And edits inside the recreated directory are watched again.
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": 400}}`)), 0o600))
	assert.Eventually(t, func() bool { return s.DefaultMaxRows() == 400 },
		5*time.Second, 50*time.Millisecond, "edits after recreate should be watched")

	cancel()
	assert.NoError(t, <-done)
}
