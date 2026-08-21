//go:build unix

package settings

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidate_NonRegularFile pins the stat gate: a FIFO squatting on a
// settings filename must be rejected as a finding, not block the read forever
// waiting for a writer (os.ReadFile on a FIFO hangs). The test completing at
// all is the non-blocking assertion.
func TestValidate_NonRegularFile(t *testing.T) {
	t.Parallel()
	files := validFiles()
	delete(files, FilePipes)
	dir := writeDir(t, files)
	require.NoError(t, syscall.Mkfifo(filepath.Join(dir, FilePipes), 0o600))

	doc, findings := parse(dir)
	assert.Nil(t, doc)
	out := findingStrings(findings)
	assert.Contains(t, out, "pipes.json: not a regular file")
	assert.NotContains(t, out, "missing", "one problem, one finding")
}

// TestValidate_SymlinkedFiles pins the layout Kubernetes ConfigMap mounts
// publish: each settings file is a symlink into a dot-prefixed data directory.
// The stat gate follows symlinks, so this must validate clean — a regression
// to Lstat here would break every ConfigMap-mounted deployment.
func TestValidate_SymlinkedFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "..data")
	require.NoError(t, os.Mkdir(dataDir, 0o700))
	for name, content := range validFiles() {
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, name), []byte(content), 0o600))
		require.NoError(t, os.Symlink(filepath.Join(dataDir, name), filepath.Join(dir, name)))
	}

	doc, findings := parse(dir)
	require.NotNil(t, doc, "findings: %s", findingStrings(findings))
	assert.Empty(t, findings)
}
