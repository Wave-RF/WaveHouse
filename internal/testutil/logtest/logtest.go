// Package logtest points slog's default logger somewhere a test can use. The
// packages log through slog.Default() rather than an injected logger, so a
// test reaches their output by swapping the default. It imports nothing from
// the repository, so every package's tests can use it.
package logtest

import (
	"bytes"
	"io"
	"log/slog"
	"sync"
	"testing"
)

// Silence discards everything logged through the default logger. Call it from
// a package's TestMain so a failing test's output is not buried in log lines.
func Silence() {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// Buffer is a concurrency-safe log sink: the code under test may log from
// its own goroutines while the test reads.
type Buffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *Buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// Bytes returns a copy of what has been logged so far.
func (b *Buffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.b.Bytes())
}

// Capture routes the default logger to the returned Buffer as JSON records at
// level and above, until the test ends. The default logger is process-wide,
// so a test that captures must not call t.Parallel: serial tests never
// overlap the package's parallel ones, which is what keeps another test's
// lines out of the buffer.
func Capture(t testing.TB, level slog.Level) *Buffer {
	t.Helper()
	buf := &Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}
