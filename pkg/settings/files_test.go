package settings

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/pkg/policy"
)

// bytesOf is a fixture as a writer holds it: the files by name, as bytes.
func bytesOf(files map[string]string) map[string][]byte {
	out := make(map[string][]byte, len(files))
	for name, content := range files {
		out[name] = []byte(content)
	}
	return out
}

// Names are read as directory entries are: a dot-prefixed entry is skipped,
// any other stray name is unexpected, and a nil or absent file is missing.
func TestValidateFiles_NamesReadAsDirectoryEntries(t *testing.T) {
	t.Parallel()
	files := bytesOf(validFiles())
	files["polices.json"] = []byte(`{}`) // the canonical typo
	files[".config.json.swp"] = []byte("vim")
	files[FilePipes] = nil
	delete(files, FileRoles)

	doc, findings := ValidateFiles(files)
	assert.Nil(t, doc)
	out := findingStrings(findings)
	assert.Contains(t, out, "polices.json: unexpected file")
	assert.NotContains(t, out, ".config.json.swp")
	assert.Contains(t, out, "pipes.json: missing")
	assert.Contains(t, out, "roles.json: missing")
}

// The permission-bits warning needs a file: in memory a secret is just a
// value, and the writer that sets it is the one that will choose the mode.
func TestValidateFiles_NoPermissionBits(t *testing.T) {
	t.Parallel()
	files := validFiles()
	files[FileConfig] = configJSON(`{"clickhouse": {"password": "s3cret"}, "auth": {"jwt_secret": "hmac"}}`)
	doc, findings := ValidateFiles(bytesOf(files))
	require.NotNil(t, doc, "findings: %s", findingStrings(findings))
	assert.Empty(t, findings)
}

// TestSeed_RoundTripsThroughTheTypes pins what a writer relies on: the seed
// decodes into the public types, and what those types marshal back passes
// ValidateFiles with the findings the seed itself gets — so a program that
// starts from Seed, edits the structs and marshals them cannot emit a shape
// the server refuses.
func TestSeed_RoundTripsThroughTheTypes(t *testing.T) {
	t.Parallel()
	seed, err := Seed()
	require.NoError(t, err)

	var roles RolesFile
	require.NoError(t, json.Unmarshal(seed[FileRoles], &roles))
	var pol policy.Policy
	require.NoError(t, json.Unmarshal(seed[FilePolicies], &pol))
	var pipesFile PipesFile
	require.NoError(t, json.Unmarshal(seed[FilePipes], &pipesFile))
	var config TenantConfig
	require.NoError(t, json.Unmarshal(seed[FileConfig], &config))

	files := map[string][]byte{}
	for name, v := range map[string]any{FileRoles: roles, FilePolicies: pol, FilePipes: pipesFile, FileConfig: config} {
		files[name], err = json.Marshal(v)
		require.NoError(t, err)
	}

	doc, findings := ValidateFiles(files)
	require.NotNil(t, doc, "findings: %s", findingStrings(findings))
	assert.False(t, HasErrors(findings))
	_, want := ValidateFiles(seed)
	assert.Equal(t, want, findings, "the marshaled seed reports what the seed does")
	assert.Contains(t, findingStrings(findings), "no policy", "the seed's one finding: it ships no policy on purpose")
}
