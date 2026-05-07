package config

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func captureLog(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), &buf
}

func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
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
	t.Parallel()
	logger, buf := captureLog(t)

	missing := filepath.Join(t.TempDir(), "does-not-exist")
	WarnIfFreshDataDir(logger, "nats", missing)

	recs := records(t, buf)
	require.Len(t, recs, 1)
	assert.Equal(t, "WARN", recs[0]["level"])
	assert.Contains(t, recs[0]["msg"], "does not exist")
	assert.Equal(t, "nats", recs[0]["kind"])
	assert.Equal(t, missing, recs[0]["path"])
}

func TestWarnIfFreshDataDir_Empty(t *testing.T) {
	t.Parallel()
	logger, buf := captureLog(t)

	// t.TempDir() returns a fresh empty dir.
	WarnIfFreshDataDir(logger, "pebble", t.TempDir())

	recs := records(t, buf)
	require.Len(t, recs, 1)
	assert.Equal(t, "WARN", recs[0]["level"])
	assert.Contains(t, recs[0]["msg"], "empty")
}

func TestWarnIfFreshDataDir_Populated(t *testing.T) {
	t.Parallel()
	logger, buf := captureLog(t)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "marker"), []byte("x"), 0o600))

	WarnIfFreshDataDir(logger, "nats", dir)

	recs := records(t, buf)
	require.Len(t, recs, 1)
	assert.Equal(t, "INFO", recs[0]["level"])
	assert.Contains(t, recs[0]["msg"], "prior state")
}

func TestWarnIfFreshDataDir_EmptyDirArgIsNoop(t *testing.T) {
	t.Parallel()
	logger, buf := captureLog(t)

	WarnIfFreshDataDir(logger, "nats", "")

	assert.Empty(t, buf.String())
}
