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
	done := make(chan error, 1)
	go func() { done <- s.Watch(ctx) }()
	time.Sleep(100 * time.Millisecond)

	// Remove the whole directory: a rejected reload, snapshot survives.
	require.NoError(t, os.RemoveAll(s.Dir()))
	time.Sleep(4 * watchDebounce)
	assert.Equal(t, 100, s.DefaultMaxRows(), "vanished directory must keep the previous snapshot")

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
