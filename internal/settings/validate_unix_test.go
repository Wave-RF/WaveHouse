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

	doc, findings := ValidateDir(dir)
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

	doc, findings := ValidateDir(dir)
	require.NotNil(t, doc, "findings: %s", findingStrings(findings))
	assert.Empty(t, findings)
}

// TestValidate_SymlinkedTenantFolders is the nested form of the same mount:
// each tenant folder at the root is a symlink into the dot-prefixed data
// directory. A directory entry's own type says "symlink", so reading the
// shape off it would call every tenant a loose file.
func TestValidate_SymlinkedTenantFolders(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	data := writeTree(t, map[string]map[string]string{"acme": validFiles(), "globex": validFiles()})
	dataDir := filepath.Join(root, "..data")
	require.NoError(t, os.Rename(data, dataDir))
	for _, folder := range []string{"acme", "globex"} {
		require.NoError(t, os.Symlink(filepath.Join(dataDir, folder), filepath.Join(root, folder)))
	}

	tree, findings := Validate(root)
	require.Empty(t, findings)
	require.NotNil(t, tree)
	assert.True(t, tree.Nested)
	assert.Len(t, tree.Tenants, 2)
}

// A finding about a tenant's folder itself, which ValidateDir reports with no
// file, names the folder.
func TestValidate_UnreadableTenantFolder(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permission bits")
	}
	root := writeTree(t, map[string]map[string]string{"acme": validFiles(), "globex": validFiles()})
	locked := filepath.Join(root, "globex")
	require.NoError(t, os.Chmod(locked, 0o000))
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) }) //nolint:gosec // G302: restores a test directory so TempDir cleanup can remove it

	tree, findings := Validate(root)
	require.NotNil(t, tree)
	require.Len(t, findings, 1, "findings: %s", findingStrings(findings))
	assert.Equal(t, "globex", findings[0].File)
	assert.Contains(t, findings[0].Message, "read settings directory")
	assert.Nil(t, tree.Tenants["globex"].Doc)
	assert.NotNil(t, tree.Tenants["acme"].Doc)
}

// A tenant folder's symlink whose target is gone cannot be stat'ed, so it is
// not a folder — and the finding says that, rather than sending its reader to
// look for a stray file. It is a finding about the root: no Tree.
func TestValidate_DanglingTenantSymlink(t *testing.T) {
	t.Parallel()
	root := writeTree(t, map[string]map[string]string{"acme": validFiles()})
	require.NoError(t, os.Symlink(filepath.Join(root, "..data", "globex"), filepath.Join(root, "globex")))

	tree, findings := Validate(root)
	assert.Nil(t, tree)
	require.Len(t, findings, 1, "findings: %s", findingStrings(findings))
	assert.Equal(t, "globex", findings[0].File)
	assert.Contains(t, findings[0].Message, "stat: ")
	assert.Contains(t, findings[0].Message, "no such file or directory")
	assert.NotContains(t, findings[0].Message, "unexpected file")
}
