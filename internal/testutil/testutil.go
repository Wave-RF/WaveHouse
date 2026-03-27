package testutil

import (
	"io"
	"log/slog"
)

// NopLogger returns a *slog.Logger that discards all output.
// Use in tests to suppress noisy log output from embedded NATS, etc.
func NopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
