package settings

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// watchDebounce coalesces filesystem event bursts into one reload. Editors
// write-then-rename, Kubernetes ConfigMap updates republish the whole `..data`
// tree, and a human saving four files produces four events — reloading on
// each would validate half-written directories and spam the log. A trailing
// debounce reloads once, after the burst goes quiet.
const watchDebounce = 250 * time.Millisecond

// Watch blocks until ctx is done, watching the settings directory and running
// the shared reload path when its contents change. A change that fails
// validation is logged and skipped — the previous good snapshot stays — so an
// operator mid-edit degrades to a log line, never to a broken server.
//
// The directory itself is watched (not the four files): atomic-writer flows
// replace files wholesale, and Kubernetes ConfigMap mounts swap a symlink,
// both of which per-file watches lose track of. The parent directory is
// watched too, because fsnotify silently drops a watch whose directory is
// removed or renamed: with only the directory watch, a delete-and-recreate
// (or a rename-over, the atomic way to replace a whole directory) would leave
// every later edit unwatched. Parent events are filtered to the settings
// directory's own name, and the directory watch is re-added on every reload
// so it is restored once the directory exists again.
//
// The setup error is returned (directory missing, fd limits); runtime watcher
// errors are logged and the loop continues — SIGHUP and the ops reload
// endpoint remain as triggers even if the watcher degrades.
func (s *Store) Watch(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("settings watcher: %w", err)
	}
	defer func() { _ = w.Close() }()
	dir := filepath.Clean(s.dir)
	if err := w.Add(dir); err != nil {
		return fmt.Errorf("settings watcher: watch %s: %w", s.dir, err)
	}
	// Best effort: a parent that can't be watched (e.g. "/" permissions)
	// costs only the recreate case, not the watcher.
	if parent := filepath.Dir(dir); parent != dir {
		if err := w.Add(parent); err != nil && s.logger != nil {
			s.logger.Warn("settings watcher: parent directory not watched; a deleted-and-recreated settings directory won't reload until SIGHUP or POST /v1/ops/settings/reload", "parent", parent, "error", err)
		}
	}

	// The timer starts disarmed; each relevant event re-arms it, so the
	// reload fires watchDebounce after the *last* event of a burst.
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.Events:
			if !ok {
				return nil
			}
			// Chmod-only events can't change content and are noisy on some
			// platforms; everything else (create/write/remove/rename) can.
			if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			// Parent-directory events matter only when they are about the
			// settings directory itself (a sibling changing is noise).
			if name := filepath.Clean(ev.Name); filepath.Dir(name) != dir && name != dir {
				continue
			}
			timer.Reset(watchDebounce)
		case werr, ok := <-w.Errors:
			if !ok {
				return nil
			}
			if s.logger != nil {
				s.logger.Error("settings watcher error", "dir", s.dir, "error", werr)
			}
		case <-timer.C:
			// Re-arm the directory watch before reloading: after a remove or
			// rename fsnotify has dropped it, and Add is a no-op while it
			// still exists. Failure (directory currently absent) is expected
			// mid-replace; the next parent event retries.
			_ = w.Add(dir)
			s.TriggerReload("watch")
		}
	}
}
