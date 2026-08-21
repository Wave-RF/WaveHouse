package settings

import (
	"context"
	"fmt"
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
// both of which per-file watches lose track of. The setup error is returned
// (directory missing, fd limits); runtime watcher errors are logged and the
// loop continues — SIGHUP and the ops reload endpoint remain as triggers even
// if the watcher degrades.
func (s *Store) Watch(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("settings watcher: %w", err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(s.dir); err != nil {
		return fmt.Errorf("settings watcher: watch %s: %w", s.dir, err)
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
			timer.Reset(watchDebounce)
		case werr, ok := <-w.Errors:
			if !ok {
				return nil
			}
			if s.logger != nil {
				s.logger.Error("settings watcher error", "dir", s.dir, "error", werr)
			}
		case <-timer.C:
			s.TriggerReload("watch")
		}
	}
}
