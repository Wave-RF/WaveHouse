package settings

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// writeTree materializes a nested root: tenant folder → file name → content.
func writeTree(t *testing.T, tenants map[string]map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for folder, files := range tenants {
		dir := filepath.Join(root, folder)
		require.NoError(t, os.Mkdir(dir, 0o750))
		for name, content := range files {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
		}
	}
	return root
}

// A flat root goes through ValidateDir untouched: same findings, same
// document, whatever is wrong with it. This is what keeps a single-tenant
// directory byte-identical across the move to a tree-shaped Validate.
func TestValidate_FlatRootIsValidateDir(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		root    func(t *testing.T) string
		wantDoc bool
	}{
		{name: "valid", root: func(t *testing.T) string { return writeDir(t, validFiles()) }, wantDoc: true},
		{name: "empty root", root: func(t *testing.T) string { return t.TempDir() }},
		{name: "only a stray file", root: func(t *testing.T) string { return writeDir(t, map[string]string{"notes.txt": "x"}) }},
		{name: "missing file", root: func(t *testing.T) string {
			files := validFiles()
			delete(files, FileConfig)
			return writeDir(t, files)
		}},
		{name: "a folder beside the files", root: func(t *testing.T) string {
			dir := writeDir(t, validFiles())
			require.NoError(t, os.Mkdir(filepath.Join(dir, "acme"), 0o700))
			return dir
		}},
		{name: "a folder squatting on a settings file name", root: func(t *testing.T) string {
			files := validFiles()
			delete(files, FileRoles)
			dir := writeDir(t, files)
			require.NoError(t, os.Mkdir(filepath.Join(dir, FileRoles), 0o700))
			return dir
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := tt.root(t)
			wantDoc, wantFindings := ValidateDir(root)
			require.Equal(t, tt.wantDoc, wantDoc != nil, "findings: %s", findingStrings(wantFindings))

			tree, findings := Validate(root)
			assert.Equal(t, wantFindings, findings)
			require.NotNil(t, tree)
			assert.False(t, tree.Nested)
			require.Len(t, tree.Tenants, 1)
			got := tree.Tenants[tenant.Default]
			assert.Equal(t, wantDoc, got.Doc)
			assert.Equal(t, wantFindings, got.Findings)
		})
	}
}

// A root that cannot be listed has no shape to report: no Tree, and the
// finding ValidateDir has always given.
func TestValidate_UnusableRoot(t *testing.T) {
	t.Parallel()
	missing := filepath.Join(t.TempDir(), "nope")
	tree, findings := Validate(missing)
	assert.Nil(t, tree)
	assert.Contains(t, findingStrings(findings), "does not exist")

	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	tree, findings = Validate(file)
	assert.Nil(t, tree)
	assert.Contains(t, findingStrings(findings), "not a directory")
}

func TestValidate_NestedRoot(t *testing.T) {
	t.Parallel()
	acme := validFiles()
	acme[FileConfig] = configJSON(`{"query": {"default_max_rows": 111}}`)
	root := writeTree(t, map[string]map[string]string{"acme": acme, "0": validFiles()})
	// Dot-prefixed entries stay invisible at the root too: the machinery of a
	// ConfigMap mount sits beside the tenant folders.
	require.NoError(t, os.Mkdir(filepath.Join(root, "..data"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".root.swp"), []byte("vim"), 0o600))

	tree, findings := Validate(root)
	require.Empty(t, findings)
	require.NotNil(t, tree)
	assert.True(t, tree.Nested)
	require.Len(t, tree.Tenants, 2)
	require.NotNil(t, tree.Tenants["acme"].Doc)
	assert.Equal(t, 111, *tree.Tenants["acme"].Doc.Config.Query.DefaultMaxRows)
	require.NotNil(t, tree.Tenants[tenant.Default].Doc, "a 0 folder is an ordinary tenant of a nested root")
}

// One bad folder is that tenant's problem alone: its findings name the
// folder, its document is withheld, and the tenant beside it validates.
func TestValidate_NestedRejectedFolder(t *testing.T) {
	t.Parallel()
	globex := validFiles()
	globex[FileConfig] = configJSON(`{"query": {"default_max_rows": -1}}`)
	delete(globex, FilePipes)
	root := writeTree(t, map[string]map[string]string{"acme": validFiles(), "globex": globex})

	tree, findings := Validate(root)
	require.NotNil(t, tree)
	require.True(t, HasErrors(findings))
	out := findingStrings(findings)
	assert.Contains(t, out, "error: globex/pipes.json: missing")
	assert.Contains(t, out, "error: globex/config.json: query.default_max_rows: must be >= 1")
	assert.NotContains(t, out, "acme")

	assert.NotNil(t, tree.Tenants["acme"].Doc)
	assert.Empty(t, tree.Tenants["acme"].Findings)
	assert.Nil(t, tree.Tenants["globex"].Doc)
	assert.Equal(t, findings, tree.Tenants["globex"].Findings, "the flat list is the tenants' findings in folder order")
}

// A folder whose name is not a tenant id can never be served, so it is a
// finding against that folder and defines no tenant; the rest of the root
// stands.
func TestValidate_NestedFolderNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, folder, want string
	}{
		{name: "dot", folder: "acme.bak", want: `error: acme.bak: folder name is not a tenant id: tenant id has '.' at byte 4`},
		{name: "space", folder: "acme corp", want: `error: acme corp: folder name is not a tenant id: tenant id has ' ' at byte 4`},
		{name: "over the length cap", folder: strings.Repeat("a", tenant.MaxLen+1), want: "folder name is not a tenant id: tenant id is 65 bytes, the limit is 64"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := writeTree(t, map[string]map[string]string{"acme": validFiles(), tt.folder: validFiles()})
			tree, findings := Validate(root)
			require.Len(t, findings, 1, "findings: %s", findingStrings(findings))
			assert.Contains(t, findings[0].String(), tt.want)
			require.NotNil(t, tree)
			require.Len(t, tree.Tenants, 1)
			assert.NotNil(t, tree.Tenants["acme"].Doc)
		})
	}
}

// A loose file beside the tenant folders is a finding about the root itself:
// no Tree, though every folder is still checked so one pass reports it all.
func TestValidate_NestedLooseFile(t *testing.T) {
	t.Parallel()
	globex := validFiles()
	delete(globex, FileRoles)
	root := writeTree(t, map[string]map[string]string{"acme": validFiles(), "globex": globex})
	require.NoError(t, os.WriteFile(filepath.Join(root, "notes.txt"), []byte("scratch"), 0o600))

	tree, findings := Validate(root)
	assert.Nil(t, tree)
	out := findingStrings(findings)
	assert.Contains(t, out, "error: notes.txt: unexpected file — a nested settings directory holds only tenant folders")
	assert.Contains(t, out, "error: globex/roles.json: missing")
}
