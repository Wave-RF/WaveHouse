package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeConfigFile (over)writes path with contents, so a test can simulate the
// operator editing config.yaml between boots and reloads.
func writeConfigFile(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
}

func newTestReloader(t *testing.T, boot string, apply func(*Config)) (*Reloader, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfigFile(t, path, boot)
	cfg, err := Load(path)
	require.NoError(t, err)
	return NewReloader(path, cfg, testutil.NopLogger(), apply), path
}

func TestReloader_AppliesHotFieldsAndFlagsRestartOnly(t *testing.T) {
	t.Parallel()
	var got *Config
	r, path := newTestReloader(t, "dedupe:\n  id_field: event_id\n  require_id: false\nserver:\n  port: 8080\n",
		func(next *Config) { got = next })

	// Hot fields and a restart-only section change together: the hot ones
	// apply, the port is only reported.
	writeConfigFile(t, path, "dedupe:\n  id_field: view_id\n  require_id: true\nserver:\n  port: 9090\n")
	res, err := r.Reload()
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"dedupe.id_field", "dedupe.require_id"}, res.Applied)
	assert.Equal(t, []string{"server"}, res.RestartRequired)
	require.NotNil(t, got, "apply must run on a successful reload")
	assert.Equal(t, "view_id", got.Dedupe.IDField)
	assert.True(t, got.Dedupe.RequireID)

	// A second reload with an unchanged file: no new hot changes, but the
	// pending port edit keeps being reported — restart_required diffs the file
	// against the booted config, not the previous reload, so the report can't
	// be missed by whoever reloads next.
	res, err = r.Reload()
	require.NoError(t, err)
	assert.Empty(t, res.Applied)
	assert.Equal(t, []string{"server"}, res.RestartRequired)

	// Reverting the port clears the report: the file again matches what the
	// process is running, so nothing is pending — empty (not nil) slices, so
	// the endpoint's JSON renders [] rather than null.
	writeConfigFile(t, path, "dedupe:\n  id_field: view_id\n  require_id: true\nserver:\n  port: 8080\n")
	res, err = r.Reload()
	require.NoError(t, err)
	assert.Empty(t, res.Applied)
	assert.Empty(t, res.RestartRequired)
	assert.NotNil(t, res.Applied)
	assert.NotNil(t, res.RestartRequired)
}

func TestReloader_InvalidConfigKeepsRunningConfig(t *testing.T) {
	t.Parallel()
	applyRuns := 0
	r, path := newTestReloader(t, "dedupe:\n  id_field: event_id\n", func(*Config) { applyRuns++ })

	// A file that fails validation must change nothing: error out, no apply.
	writeConfigFile(t, path, "clickhouse:\n  http_scheme: gopher\n")
	_, err := r.Reload()
	require.Error(t, err)
	assert.Zero(t, applyRuns, "a failed reload must not apply anything")

	// Fixing the file works on the next attempt, and the diff runs against
	// the still-current boot config — proving the bad load didn't clobber it.
	writeConfigFile(t, path, "dedupe:\n  id_field: view_id\n")
	res, err := r.Reload()
	require.NoError(t, err)
	assert.Equal(t, []string{"dedupe.id_field"}, res.Applied)
	assert.Equal(t, 1, applyRuns)
}

func TestReloader_MissingFileKeepsRunningConfig(t *testing.T) {
	t.Parallel()
	applyRuns := 0
	r, path := newTestReloader(t, "dedupe:\n  id_field: view_id\n", func(*Config) { applyRuns++ })

	// The file the process booted from disappears (renamed, deleted, mount
	// lost). Load alone would fall back to env + defaults and revert
	// dedupe.id_field to event_id — the reload must refuse instead.
	require.NoError(t, os.Remove(path))
	_, err := r.Reload()
	require.Error(t, err)
	assert.Zero(t, applyRuns, "a reload without the boot config file must not apply anything")

	// Restoring the file recovers on the next attempt: the guard rejects the
	// vanished file per reload, it doesn't latch a failed state.
	writeConfigFile(t, path, "dedupe:\n  id_field: event_id\n")
	res, err := r.Reload()
	require.NoError(t, err)
	assert.Equal(t, []string{"dedupe.id_field"}, res.Applied)
	assert.Equal(t, 1, applyRuns)
}

// TestReloader_EnvOnlyBootReloadsWithoutFile pins the guard's boundary: a
// process that booted with no file at all (the container/env-only mode) keeps
// reloading without error — honestly reporting nothing to apply.
func TestReloader_EnvOnlyBootReloadsWithoutFile(t *testing.T) {
	t.Setenv("WH_DEDUPE_ID_FIELD", "env_id") // no t.Parallel with Setenv

	path := filepath.Join(t.TempDir(), "config.yaml") // never created
	cfg, err := Load(path)
	require.NoError(t, err)
	r := NewReloader(path, cfg, testutil.NopLogger(), nil)

	res, err := r.Reload()
	require.NoError(t, err)
	assert.Empty(t, res.Applied)
	assert.Empty(t, res.RestartRequired)
}

// TestReloader_EnvPinnedKeyShadowsFileEdit documents the caveat: an env-pinned
// key shadows file edits, so the reload honestly reports no change.
func TestReloader_EnvPinnedKeyShadowsFileEdit(t *testing.T) {
	t.Setenv("WH_DEDUPE_ID_FIELD", "pinned_id") // no t.Parallel with Setenv

	r, path := newTestReloader(t, "dedupe:\n  id_field: event_id\n", nil)

	writeConfigFile(t, path, "dedupe:\n  id_field: view_id\n")
	res, err := r.Reload()
	require.NoError(t, err)
	assert.Empty(t, res.Applied, "an env-pinned key must shadow the file edit")
}

// TestReloadDiff_CoversEveryConfigField is the drift guard: it fails when a
// Config field is in neither hotFields nor restartSections.
func TestReloadDiff_CoversEveryConfigField(t *testing.T) {
	t.Parallel()
	covered := map[string]bool{}
	for _, f := range hotFields {
		covered[f.name] = true
	}
	for _, s := range restartSections {
		covered[s.name] = true
	}

	yamlName := func(f reflect.StructField) string {
		return strings.Split(f.Tag.Get("yaml"), ",")[0]
	}

	ct := reflect.TypeOf(Config{})
	for i := range ct.NumField() {
		name := yamlName(ct.Field(i))
		// Dedupe is classified per-field (id_field/require_id are hot,
		// enabled is restart-only), so recurse instead of expecting "dedupe".
		if name == "dedupe" {
			dt := reflect.TypeOf(Dedupe{})
			for j := range dt.NumField() {
				sub := "dedupe." + yamlName(dt.Field(j))
				assert.True(t, covered[sub],
					"config field %q is unclassified — add it to hotFields or restartSections in reload.go", sub)
			}
			continue
		}
		assert.True(t, covered[name],
			"config field %q is unclassified — add it to hotFields or restartSections in reload.go", name)
	}
}
