package settings

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLoadedStore materializes a valid directory (with overrides applied) and
// returns a store that has adopted it.
func newLoadedStore(t *testing.T, overrides map[string]string) *Store {
	t.Helper()
	files := validFiles()
	for name, content := range overrides {
		files[name] = content
	}
	s, findings := Open(writeDir(t, files), nil)
	require.NotNil(t, s, "findings: %s", findingStrings(findings))
	return s
}

func TestStore_ReloadAdoptsAndRejects(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 500}}`),
	})
	assert.Equal(t, 500, s.DefaultMaxRows())

	// Break the directory: the reload must report the error and keep the
	// previous snapshot — a bad edit can never evict the last good document.
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": -1}}`)), 0o600))
	findings, adopted := s.Reload()
	assert.False(t, adopted)
	assert.True(t, HasErrors(findings))
	assert.Equal(t, 500, s.DefaultMaxRows(), "rejected reload must keep the previous snapshot")

	// Fix it: the next reload adopts again.
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": 700}}`)), 0o600))
	findings, adopted = s.Reload()
	require.True(t, adopted, "findings: %s", findingStrings(findings))
	assert.Equal(t, 700, s.DefaultMaxRows())
}

func TestStore_ReloadWithWarningsAdopts(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FilePolicies: `{}`, // empty policy: legal, warned (total lockout)
		FilePipes:    `{}`, // drop the pipe so its analyst role reference doesn't dangle
	})
	findings, adopted := s.Reload()
	assert.True(t, adopted, "warnings alone must not block adoption")
	assert.NotEmpty(t, findings)
	assert.False(t, HasErrors(findings))
}

func TestStore_DedupeFor_Cascade(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"dedupe": {"require_id": true, "tables": {"clicks": {"id_field": "click_id"}, "views": {"require_id": false}}}}`),
	})

	tests := []struct {
		name, table, wantID string
		wantRequire         bool
	}{
		{name: "table overrides id_field, inherits require_id", table: "clicks", wantID: "click_id", wantRequire: true},
		{name: "table overrides require_id, inherits id_field", table: "views", wantID: "event_id", wantRequire: false},
		{name: "unlisted table gets globals", table: "other", wantID: "event_id", wantRequire: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id, req := s.DedupeFor(tt.table)
			assert.Equal(t, tt.wantID, id)
			assert.Equal(t, tt.wantRequire, req)
		})
	}
}

// TestStore_OpenRejectsInvalid pins the boot contract: an invalid directory
// yields no Store at all — there is no "store without a document" state and
// no compiled defaults to fall back on.
func TestStore_OpenRejectsInvalid(t *testing.T) {
	t.Parallel()
	files := validFiles()
	files[FileConfig] = `{}` // every key missing
	s, findings := Open(writeDir(t, files), nil)
	assert.Nil(t, s)
	assert.True(t, HasErrors(findings))

	s, findings = Open(filepath.Join(t.TempDir(), "nope"), nil)
	assert.Nil(t, s)
	assert.True(t, HasErrors(findings))
}

// TestStore_SurvivesVanishedDirectory pins the runtime half of the same
// contract: once adopted, the snapshot outlives its files — deleting the
// directory is just a rejected reload.
func TestStore_SurvivesVanishedDirectory(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 42}}`),
	})
	require.NoError(t, os.RemoveAll(s.Dir()))
	findings, adopted := s.Reload()
	assert.False(t, adopted)
	assert.True(t, HasErrors(findings))
	assert.Equal(t, 42, s.DefaultMaxRows())
	id, req := s.DedupeFor("clicks")
	assert.Equal(t, "event_id", id)
	assert.False(t, req)
}

// TestStore_SeedIsValid pins that the shipped starter directory passes its
// own gate: `wavehouse init-settings` must never write something
// `wavehouse validate` rejects, and the defaults are readable back.
func TestStore_SeedIsValid(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "settings")
	require.NoError(t, WriteSeed(dir))
	s, findings := Open(dir, nil)
	require.NotNil(t, s, "findings: %s", findingStrings(findings))
	assert.False(t, HasErrors(findings))
	// The one expected finding: an empty policies.json is fail-closed and
	// says so. The seed ships no policy on purpose — a policy is a tenant's
	// decision (dev-policy.yaml is the opt-in trial one).
	assert.Len(t, findings, 1, "findings: %s", findingStrings(findings))
	assert.Contains(t, findingStrings(findings), "no policy")
	id, req := s.DedupeFor("anything")
	assert.Equal(t, "event_id", id)
	assert.False(t, req)
	assert.Equal(t, 10000, s.DefaultMaxRows())
	assert.Equal(t, 60*time.Second, s.SchemaRefreshInterval())
	assert.Equal(t, []string{"*"}, s.CORSOrigins())

	// Non-empty directory: refused, contents untouched.
	require.NoError(t, os.WriteFile(filepath.Join(dir, FileConfig), []byte(`{}`), 0o600))
	err := WriteSeed(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not empty")
	data, _ := os.ReadFile(filepath.Join(dir, FileConfig)) //nolint:gosec // G304: path is rooted in t.TempDir()
	assert.Equal(t, `{}`, string(data))
}

func TestStore_TypedAccessors(t *testing.T) {
	t.Parallel()
	s := newLoadedStore(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 250}, "schema": {"refresh_interval": 5}, "cors": {"allowed_origins": ["https://app.example.com"]}}`),
	})
	assert.Equal(t, 250, s.DefaultMaxRows())
	assert.Equal(t, 5*time.Second, s.SchemaRefreshInterval())
	assert.Equal(t, []string{"https://app.example.com"}, s.CORSOrigins())
}
