package settings

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// newLoadedRegistry materializes a valid directory (with overrides applied)
// and returns a registry that has adopted it.
func newLoadedRegistry(t *testing.T, overrides map[string]string) *Registry {
	t.Helper()
	files := validFiles()
	for name, content := range overrides {
		files[name] = content
	}
	reg, findings := Open(writeDir(t, files))
	require.NotNil(t, reg, "findings: %s", findingStrings(findings))
	return reg
}

func TestRegistry_For(t *testing.T) {
	t.Parallel()
	store := newLoadedStore(t, nil)
	reg := NewRegistry(store)

	got, ok := reg.For(tenant.Default)
	require.True(t, ok)
	assert.Same(t, store, got)

	got, ok = reg.For(tenant.ID("acme"))
	assert.False(t, ok, "only the default tenant exists")
	assert.Nil(t, got)
}

func TestRegistry_ReloadAdoptsAndRejects(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 500}}`),
	})
	s, _ := reg.For(tenant.Default)
	assert.Equal(t, 500, s.DefaultMaxRows())

	// Break the directory: the reload must report the error and keep the
	// previous snapshot — a bad edit can never evict the last good document.
	require.NoError(t, os.WriteFile(filepath.Join(reg.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": -1}}`)), 0o600))
	findings, adopted := reg.Reload("test")
	assert.False(t, adopted)
	assert.True(t, HasErrors(findings))
	assert.Equal(t, 500, s.DefaultMaxRows(), "rejected reload must keep the previous snapshot")

	// Fix it: the next reload adopts again, into the same store — the handle a
	// long-lived consumer holds keeps reading the latest document.
	require.NoError(t, os.WriteFile(filepath.Join(reg.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": 700}}`)), 0o600))
	findings, adopted = reg.Reload("test")
	require.True(t, adopted, "findings: %s", findingStrings(findings))
	assert.Equal(t, 700, s.DefaultMaxRows())
}

func TestRegistry_ReloadWithWarningsAdopts(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, map[string]string{
		FilePolicies: `{}`, // empty policy: legal, warned (total lockout)
		FilePipes:    `{}`, // drop the pipe so its analyst role reference doesn't dangle
	})
	findings, adopted := reg.Reload("test")
	assert.True(t, adopted, "warnings alone must not block adoption")
	assert.NotEmpty(t, findings)
	assert.False(t, HasErrors(findings))
}

// TestOpen_RejectsInvalid pins the boot contract: an invalid directory yields
// no Registry at all — there is no "store without a document" state and no
// compiled defaults to fall back on.
func TestOpen_RejectsInvalid(t *testing.T) {
	t.Parallel()
	files := validFiles()
	files[FileConfig] = `{}` // every key missing
	reg, findings := Open(writeDir(t, files))
	assert.Nil(t, reg)
	assert.True(t, HasErrors(findings))

	reg, findings = Open(filepath.Join(t.TempDir(), "nope"))
	assert.Nil(t, reg)
	assert.True(t, HasErrors(findings))
}

// TestRegistry_SurvivesVanishedDirectory pins the runtime half of the same
// contract: once adopted, the snapshot outlives its files — deleting the
// directory is just a rejected reload.
func TestRegistry_SurvivesVanishedDirectory(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, map[string]string{
		FileConfig: configJSON(`{"query": {"default_max_rows": 42}}`),
	})
	s, _ := reg.For(tenant.Default)
	require.NoError(t, os.RemoveAll(reg.Dir()))
	findings, adopted := reg.Reload("test")
	assert.False(t, adopted)
	assert.True(t, HasErrors(findings))
	assert.Equal(t, 42, s.DefaultMaxRows())
	_, id, req := s.DedupeFor("clicks")
	assert.Equal(t, "event_id", id)
	assert.False(t, req)
}

// TestRegistry_AfterAdoptRunsOnlyOnAdoption pins the lifecycle hook contract:
// it fires after every successful reload (with the new snapshot already
// visible), names the tenant that reload adopted, and never fires on a
// rejected one.
func TestRegistry_AfterAdoptRunsOnlyOnAdoption(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, nil)
	s, _ := reg.For(tenant.Default)
	var seen []bool
	reg.AfterAdopt(func(adopted []tenant.ID) {
		assert.Equal(t, []tenant.ID{tenant.Default}, adopted)
		seen = append(seen, s.DedupeEnabled())
	})

	require.NoError(t, os.WriteFile(filepath.Join(reg.Dir(), FileConfig), []byte(configJSON(`{"dedupe": {"enabled": true}}`)), 0o600))
	_, adopted := reg.Reload("test")
	require.True(t, adopted)
	assert.Equal(t, []bool{true}, seen, "hook sees the newly adopted snapshot")

	require.NoError(t, os.WriteFile(filepath.Join(reg.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": 0}}`)), 0o600))
	_, adopted = reg.Reload("test")
	require.False(t, adopted)
	assert.Equal(t, []bool{true}, seen, "rejected reload must not fire the hook")
}

// Until the registry holds one store per tenant folder, a nested root is
// refused rather than read as something it is not.
func TestOpen_RefusesNestedRoot(t *testing.T) {
	t.Parallel()
	reg, findings := Open(writeTree(t, map[string]map[string]string{"acme": validFiles()}))
	assert.Nil(t, reg)
	assert.Contains(t, findingStrings(findings), "one folder per tenant is not served yet")
}
