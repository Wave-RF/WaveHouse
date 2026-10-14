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

// roundTrip decodes files into the public types and marshals them back: what
// a writer that edits the structs sends.
func roundTrip(t *testing.T, files map[string][]byte) map[string][]byte {
	t.Helper()
	var roles RolesFile
	require.NoError(t, json.Unmarshal(files[FileRoles], &roles))
	var pol policy.Policy
	require.NoError(t, json.Unmarshal(files[FilePolicies], &pol))
	var pipesFile PipesFile
	require.NoError(t, json.Unmarshal(files[FilePipes], &pipesFile))
	var config TenantConfig
	require.NoError(t, json.Unmarshal(files[FileConfig], &config))

	out := map[string][]byte{}
	for name, v := range map[string]any{FileRoles: roles, FilePolicies: pol, FilePipes: pipesFile, FileConfig: config} {
		data, err := json.Marshal(v)
		require.NoError(t, err)
		out[name] = data
	}
	return out
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

	doc, findings := ValidateFiles(roundTrip(t, seed))
	require.NotNil(t, doc, "findings: %s", findingStrings(findings))
	assert.False(t, HasErrors(findings))
	_, want := ValidateFiles(seed)
	assert.Equal(t, want, findings, "the marshaled seed reports what the seed does")
	assert.Contains(t, findingStrings(findings), "no policy", "the seed's one finding: it ships no policy on purpose")
}

// TestDocuments_RoundTripThroughTheTypes is the same contract for what a
// writer actually sends, which the seed's empty policy and pipes never
// exercise: a policy with grants on both operations, a claim-template
// filter and check, aggregation rules and the typed limits (Millis and
// ByteSize decode a string and encode a number); a pipe with parameters,
// a default and an allowlist; a config with per-table overrides, headers and
// secrets. The marshaled form must validate to the same findings and the
// same Document as the hand-written one, so a field added later with a
// one-way encoding, or a MarshalJSON that drops something, fails here.
func TestDocuments_RoundTripThroughTheTypes(t *testing.T) {
	t.Parallel()
	files := bytesOf(map[string]string{
		FileRoles: `{"roles": ["public", "analyst", "admin"]}`,
		FilePolicies: `{"default_role": "public", "tables": {"clicks": {
			"analyst": {
				"select": {"allow_columns": ["page", "tenant", "ts"], "filter": {"tenant": {"_eq": "{{ jwt.claim.tenant }}"}}, "allowed_aggregations": ["count", "sum"], "max_rows": 100, "max_execution_time": "5s", "max_rows_to_read": 1000000, "max_memory_usage": "1GiB"},
				"insert": {"allow_columns": ["page", "tenant"], "check": {"tenant": {"_eq": "{{ jwt.claim.tenant }}"}}}
			},
			"public": {"select": {"deny_columns": ["tenant"], "denied_aggregations": ["uniq"]}}
		}}}`,
		FilePipes:  `{"pipes": [{"name": "top_pages", "sql": "SELECT page FROM clicks LIMIT {{limit}}", "description": "Top pages", "allowed_roles": ["analyst"], "parameters": [{"name": "limit", "type": "number", "required": true}, {"name": "region", "type": "string", "default": "eu"}]}]}`,
		FileConfig: configJSON(`{"clickhouse": {"password": "s3cret", "headers": {"X-Trace": "1"}}, "auth": {"jwt_secret": "hmac"}, "dedupe": {"retention": "720h", "tables": {"clicks": {"id_field": "click_id", "retention": "24h"}}}, "dlq": {"tables": {"clicks": {"enabled": false}}}}`),
	})
	wantDoc, wantFindings := ValidateFiles(files)
	require.NotNil(t, wantDoc, "the fixture must be valid as written: %s", findingStrings(wantFindings))
	require.Empty(t, wantFindings)

	doc, findings := ValidateFiles(roundTrip(t, files))
	assert.Equal(t, wantFindings, findings)
	assert.Equal(t, wantDoc, doc, "the marshaled documents must decode to what the hand-written ones do")
}
