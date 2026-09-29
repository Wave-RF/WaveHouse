package storedir

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_RemovedAfterTheTestsOwnCleanups(t *testing.T) {
	var dir string
	t.Run("store", func(t *testing.T) {
		dir = New(t)
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Empty(t, entries, "the store starts empty")
		// Registered after New, as a broker's Close is, so it runs first and
		// what it writes is removed with the rest.
		t.Cleanup(func() {
			obs := filepath.Join(dir, "jetstream", "obs")
			require.NoError(t, os.MkdirAll(obs, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(obs, "o.dat"), nil, 0o600))
		})
	})
	assert.NoDirExists(t, dir)
}
