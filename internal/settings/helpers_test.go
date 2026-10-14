package settings

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/pkg/settings"
)

// The fixtures below mirror pkg/settings's test helpers: the two packages'
// tests build the same directories, and a test in one package cannot reach
// the other's unexported helpers.

// writeDir materializes a settings directory from name → content.
func writeDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	return dir
}

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

// validFiles is a complete, correct directory; tests override single entries.
func validFiles() map[string]string {
	return map[string]string{
		settings.FileRoles:    `{"roles": ["public", "analyst", "admin"]}`,
		settings.FilePolicies: `{"default_role": "public", "tables": {"clicks": {"analyst": {"select": {"max_rows": 100}}}}}`,
		settings.FilePipes:    `{"pipes": [{"name": "top_clicks", "sql": "SELECT 1", "allowed_roles": ["analyst"], "parameters": [{"name": "limit", "type": "number"}]}]}`,
		settings.FileConfig:   configJSON(`{"dedupe": {"tables": {"clicks": {"id_field": "click_id"}}}}`),
	}
}

// configJSON returns the seed config.json with patch merged over it, one
// level deep (a patched block's keys replace the seed's, the rest of the
// block is kept). Every key but dedupe.retention is required, so tests that
// care about one key build a complete document from the seed rather than
// repeating all of them.
func configJSON(patch string) string {
	seed, err := settings.Seed()
	if err != nil {
		panic(err)
	}
	var base, over map[string]map[string]json.RawMessage
	if err := json.Unmarshal(seed[settings.FileConfig], &base); err != nil {
		panic(err)
	}
	if err := json.Unmarshal([]byte(patch), &over); err != nil {
		panic(err)
	}
	for block, keys := range over {
		if base[block] == nil {
			base[block] = map[string]json.RawMessage{}
		}
		for k, v := range keys {
			base[block][k] = v
		}
	}
	out, err := json.Marshal(base)
	if err != nil {
		panic(err)
	}
	return string(out)
}

func findingStrings(findings []settings.Finding) string {
	parts := make([]string, len(findings))
	for i, f := range findings {
		parts[i] = f.String()
	}
	return strings.Join(parts, "\n")
}
