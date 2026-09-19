package logtest

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCapture_RoutesAndRestoresTheDefaultLogger(t *testing.T) {
	prev := slog.Default()
	t.Run("captured", func(t *testing.T) {
		buf := Capture(t, slog.LevelWarn)
		slog.Info("below the level")
		slog.Warn("kept", "k", "v")
		assert.NotContains(t, buf.String(), "below the level")
		assert.Contains(t, buf.String(), `"msg":"kept"`)
		assert.Contains(t, string(buf.Bytes()), `"k":"v"`)
	})
	assert.Same(t, prev, slog.Default(), "the previous default is restored when the capturing test ends")
}

func TestSilence(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	Silence()
	assert.NotSame(t, prev, slog.Default())
	slog.Error("discarded")
}
