package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunInitSettings(t *testing.T) {
	t.Run("writes a directory validate accepts", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "settings")
		assert.Equal(t, 0, runInitSettings([]string{dir}))
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Len(t, entries, 4)
		assert.Equal(t, 0, runValidate([]string{dir}), "the seed must pass its own gate")
	})

	t.Run("refuses a non-empty directory", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("mine"), 0o600))
		assert.Equal(t, 1, runInitSettings([]string{dir}))
		data, err := os.ReadFile(filepath.Join(dir, "keep.txt")) //nolint:gosec // G304: path is rooted in t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "mine", string(data))
	})

	t.Run("missing argument is a usage error", func(t *testing.T) {
		assert.Equal(t, 2, runInitSettings(nil))
	})

	t.Run("too many arguments is a usage error", func(t *testing.T) {
		assert.Equal(t, 2, runInitSettings([]string{"a", "b"}))
	})

	t.Run("-h prints help and exits 0", func(t *testing.T) {
		assert.Equal(t, 0, runInitSettings([]string{"-h"}))
	})
}
