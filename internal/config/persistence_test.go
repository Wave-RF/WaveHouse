package config

import (
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLog routes the default logger to the returned buffer. The default
// logger is process-wide, so the tests that call it run serially (see
// logtest.Capture).
func captureLog(t *testing.T) *logtest.Buffer {
	t.Helper()
	return logtest.Capture(t, slog.LevelDebug)
}

func records(t *testing.T, buf *logtest.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimRight(buf.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		out = append(out, rec)
	}
	return out
}

func TestWarnIfFreshDataDir_Missing(t *testing.T) {
	buf := captureLog(t)

	missing := filepath.Join(t.TempDir(), "does-not-exist")
	WarnIfFreshDataDir("nats", missing)

	recs := records(t, buf)
	require.Len(t, recs, 1)
	assert.Equal(t, "WARN", recs[0]["level"])
	assert.Contains(t, recs[0]["msg"], "does not exist")
	assert.Equal(t, "nats", recs[0]["kind"])
	assert.Equal(t, missing, recs[0]["path"])
}

func TestWarnIfFreshDataDir_Empty(t *testing.T) {
	buf := captureLog(t)

	// t.TempDir() returns a fresh empty dir.
	WarnIfFreshDataDir("pebble", t.TempDir())

	recs := records(t, buf)
	require.Len(t, recs, 1)
	assert.Equal(t, "WARN", recs[0]["level"])
	assert.Contains(t, recs[0]["msg"], "empty")
}

func TestWarnIfFreshDataDir_Populated(t *testing.T) {
	buf := captureLog(t)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "marker"), []byte("x"), 0o600))

	WarnIfFreshDataDir("nats", dir)

	recs := records(t, buf)
	require.Len(t, recs, 1)
	assert.Equal(t, "INFO", recs[0]["level"])
	assert.Contains(t, recs[0]["msg"], "prior state")
}

func TestWarnIfFreshDataDir_EmptyDirArgIsNoop(t *testing.T) {
	buf := captureLog(t)

	WarnIfFreshDataDir("nats", "")

	assert.Empty(t, buf.String())
}

func TestLogStorageInitError_AddsHintOnPermissionDenied(t *testing.T) {
	buf := captureLog(t)

	// fs.ErrPermission wraps to os.ErrPermission via errors.Is — this is
	// the canonical "permission denied" signal across the stdlib filesystem
	// surface, so any wrapped EACCES/EPERM bubbling up from NATS or Pebble
	// will satisfy the same check.
	LogStorageInitError("mq", "/app/data/nats", fs.ErrPermission)

	recs := records(t, buf)
	require.Len(t, recs, 1)
	assert.Equal(t, "ERROR", recs[0]["level"])
	assert.Contains(t, recs[0]["msg"], "mq init failed")
	assert.Contains(t, recs[0]["hint"], "UID 65532")
	assert.Contains(t, recs[0]["hint"], "chown")
}

func TestLogStorageInitError_NoHintOnGenericError(t *testing.T) {
	buf := captureLog(t)

	LogStorageInitError("mq", "/app/data/nats", errors.New("disk full"))

	recs := records(t, buf)
	require.Len(t, recs, 1)
	assert.Equal(t, "ERROR", recs[0]["level"])
	_, hasHint := recs[0]["hint"]
	assert.False(t, hasHint, "non-permission errors should not get the UID hint")
}
