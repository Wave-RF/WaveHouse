package config

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
)

// LogStorageInitError emits an error log for a storage-init failure, with a
// UID-65532 hint when the failure looks like a permission denial — the
// dominant signature of a Docker bind mount whose host directory is owned
// by root rather than the distroless `nonroot` user. Keeps the actionable
// remediation right next to the error line so operators don't have to dig.
//
// `kind` is a short label (e.g. "mq", "dedupe"). The caller still decides
// whether to exit; this only logs.
func LogStorageInitError(logger *slog.Logger, kind, path string, err error) {
	fields := []any{"error", err, "path", path}
	if errors.Is(err, os.ErrPermission) {
		fields = append(fields,
			"hint",
			"if running in a container with a host bind mount, the host directory must be owned by UID 65532 (the `nonroot` user in the distroless image). Try `sudo chown -R 65532:65532 /your/host/path`. Named volumes inherit ownership automatically and don't need this.",
		)
	}
	logger.Error(kind+" init failed", fields...)
}

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
