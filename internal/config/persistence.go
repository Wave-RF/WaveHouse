package config

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
)

// WarnIfFreshDataDir logs a startup `WARN` if dir doesn't already exist or is
// empty, signalling that we're about to start with no prior state.
//
// On a first-ever run this is expected; on every subsequent run it should be
// silent. So when the warning *does* fire on a redeploy, it's the most direct
// possible signal that the persistent volume isn't actually persisting:
// either it wasn't mounted, the wrong path was mounted, or the volume was
// recreated.
//
// `kind` is a short label for the log message (e.g. "nats", "pebble").
func WarnIfFreshDataDir(logger *slog.Logger, kind, dir string) {
	if dir == "" {
		return
	}

	entries, err := os.ReadDir(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		logger.Warn(
			"data directory does not exist — starting with no prior state. If this is a redeploy, your persistent volume is not actually persisting; verify your mount.",
			"kind", kind,
			"path", dir,
		)
	case err != nil:
		logger.Warn("could not read data directory", "kind", kind, "path", dir, "error", err)
	case len(entries) == 0:
		logger.Warn(
			"data directory is empty — starting with no prior state. If this is a redeploy, your persistent volume is not actually persisting; verify your mount.",
			"kind", kind,
			"path", dir,
		)
	default:
		logger.Info("data directory found with prior state", "kind", kind, "path", dir)
	}
}
