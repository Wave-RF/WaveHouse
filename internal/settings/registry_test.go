package settings

import (
	"fmt"
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

// writeTenant (re)writes one tenant folder of a nested root.
func writeTenant(t *testing.T, root, folder string, files map[string]string) {
	t.Helper()
	dir := filepath.Join(root, folder)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
}

// maxRowsFiles is a valid tenant folder whose query.default_max_rows tells
// the tenants of a test apart.
func maxRowsFiles(maxRows int) map[string]string {
	files := validFiles()
	files[FileConfig] = configJSON(fmt.Sprintf(`{"query": {"default_max_rows": %d}}`, maxRows))
	return files
}

// brokenFiles is a tenant folder Validate rejects.
func brokenFiles() map[string]string {
	files := validFiles()
	files[FileConfig] = configJSON(`{"query": {"default_max_rows": -1}}`)
	return files
}

func TestOpen_NestedRoot(t *testing.T) {
	t.Parallel()
	reg, findings := Open(writeTree(t, map[string]map[string]string{"acme": maxRowsFiles(111), "globex": maxRowsFiles(222)}))
	require.NotNil(t, reg, "findings: %s", findingStrings(findings))
	assert.Empty(t, findings)
	assert.True(t, reg.Nested())

	acme, ok := reg.For("acme")
	require.True(t, ok)
	globex, ok := reg.For("globex")
	require.True(t, ok)
	assert.NotSame(t, acme, globex)
	assert.Equal(t, 111, acme.DefaultMaxRows())
	assert.Equal(t, 222, globex.DefaultMaxRows())

	// A nested root defines the tenants it holds folders for and no other:
	// the default tenant exists only as a 0 folder.
	_, ok = reg.For(tenant.Default)
	assert.False(t, ok)
	store, known := reg.Resolve(tenant.Default)
	assert.Nil(t, store)
	assert.False(t, known)

	assert.False(t, newLoadedRegistry(t, nil).Nested())
}

// Fail closed per tenant, at boot: the registry opens, the rejected tenant is
// known but not served, and the tenant beside it is.
func TestOpen_NestedRejectedFolder(t *testing.T) {
	t.Parallel()
	root := writeTree(t, map[string]map[string]string{"acme": maxRowsFiles(111), "globex": brokenFiles()})
	reg, findings := Open(root)
	require.NotNil(t, reg, "one bad folder must not cost the pod its other tenants")
	assert.Contains(t, findingStrings(findings), "error: globex/config.json: query.default_max_rows")

	_, ok := reg.For("acme")
	assert.True(t, ok)
	_, ok = reg.For("globex")
	assert.False(t, ok, "a rejected tenant is not served")
	store, known := reg.Resolve("globex")
	assert.Nil(t, store)
	assert.True(t, known, "rejected is not unknown: its requests are refused, not 404ed")

	// Fixing the folder and reloading is the whole recovery.
	writeTenant(t, root, "globex", maxRowsFiles(222))
	findings, adopted := reg.Reload("test")
	require.True(t, adopted, "findings: %s", findingStrings(findings))
	globex, ok := reg.For("globex")
	require.True(t, ok)
	assert.Equal(t, 222, globex.DefaultMaxRows())
}

// A nested root whose every folder is rejected still opens: nothing is
// served, and a reload of the fixed folders brings the tenants up.
func TestOpen_NestedEveryFolderRejected(t *testing.T) {
	t.Parallel()
	reg, findings := Open(writeTree(t, map[string]map[string]string{"acme": brokenFiles()}))
	require.NotNil(t, reg)
	assert.True(t, HasErrors(findings))
	_, ok := reg.For("acme")
	assert.False(t, ok)
}

// A finding about the root itself refuses boot in either shape.
func TestOpen_NestedLooseFileRefusesBoot(t *testing.T) {
	t.Parallel()
	root := writeTree(t, map[string]map[string]string{"acme": validFiles()})
	require.NoError(t, os.WriteFile(filepath.Join(root, "notes.txt"), []byte("scratch"), 0o600))
	reg, findings := Open(root)
	assert.Nil(t, reg)
	assert.Contains(t, findingStrings(findings), "notes.txt: unexpected file")
}

// Fail closed per tenant, on reload: no previous-snapshot fallback. The
// rejected tenant stops being served while the reload still adopts the tenant
// beside it; a request already admitted keeps reading the document it was
// admitted under; and the recovery lands in the same store.
func TestRegistry_NestedReloadRejectsOneTenant(t *testing.T) {
	t.Parallel()
	root := writeTree(t, map[string]map[string]string{"acme": maxRowsFiles(111), "globex": maxRowsFiles(222)})
	reg, _ := Open(root)
	require.NotNil(t, reg)
	var hooks [][]tenant.ID
	reg.AfterAdopt(func(adopted []tenant.ID) { hooks = append(hooks, adopted) })
	admitted, _ := reg.For("globex")

	writeTenant(t, root, "acme", maxRowsFiles(333))
	writeTenant(t, root, "globex", brokenFiles())
	findings, adopted := reg.Reload("test")
	assert.False(t, adopted, "not everything was adopted")
	assert.Contains(t, findingStrings(findings), "error: globex/config.json")
	assert.Equal(t, [][]tenant.ID{{"acme"}}, hooks, "the hooks hear of the tenants that were adopted, and only those")

	acme, _ := reg.For("acme")
	assert.Equal(t, 333, acme.DefaultMaxRows(), "the tenant beside the rejected one is adopted")
	_, ok := reg.For("globex")
	assert.False(t, ok)
	_, known := reg.Resolve("globex")
	assert.True(t, known)
	assert.Equal(t, 222, admitted.DefaultMaxRows(), "a request already holding the store finishes on the document it started with")

	writeTenant(t, root, "globex", maxRowsFiles(444))
	_, adopted = reg.Reload("test")
	require.True(t, adopted)
	recovered, ok := reg.For("globex")
	require.True(t, ok)
	assert.Same(t, admitted, recovered, "a tenant keeps one store for life")
	assert.Equal(t, 444, recovered.DefaultMaxRows())
	assert.Equal(t, [][]tenant.ID{{"acme"}, {"acme", "globex"}}, hooks)
}

// All is the served tenants in id order: a rejected tenant is left out, like
// everywhere else, and comes back with its folder.
func TestRegistry_All(t *testing.T) {
	t.Parallel()
	root := writeTree(t, map[string]map[string]string{"globex": maxRowsFiles(222), "acme": maxRowsFiles(111), "broken": brokenFiles()})
	reg, _ := Open(root)
	require.NotNil(t, reg)

	served := func() (ids []tenant.ID, maxRows []int) {
		for id, store := range reg.All() {
			ids = append(ids, id)
			maxRows = append(maxRows, store.DefaultMaxRows())
		}
		return ids, maxRows
	}
	ids, maxRows := served()
	assert.Equal(t, []tenant.ID{"acme", "globex"}, ids)
	assert.Equal(t, []int{111, 222}, maxRows)

	writeTenant(t, root, "broken", maxRowsFiles(333))
	_, adopted := reg.Reload("test")
	require.True(t, adopted)
	ids, _ = served()
	assert.Equal(t, []tenant.ID{"acme", "broken", "globex"}, ids)

	// Stopping early is the iterator's contract, not the caller's problem.
	for id := range reg.All() {
		assert.Equal(t, tenant.ID("acme"), id)
		break
	}
}

// A reload mirrors the folders: a new one is served, a removed one is
// forgotten. A folder whose name is not a tenant id is reported and skipped.
func TestRegistry_NestedReloadMirrorsTheFolders(t *testing.T) {
	t.Parallel()
	root := writeTree(t, map[string]map[string]string{"acme": maxRowsFiles(111), "globex": maxRowsFiles(222)})
	reg, _ := Open(root)
	require.NotNil(t, reg)

	writeTenant(t, root, "initech", maxRowsFiles(333))
	require.NoError(t, os.RemoveAll(filepath.Join(root, "acme")))
	findings, adopted := reg.Reload("test")
	require.True(t, adopted, "findings: %s", findingStrings(findings))

	initech, ok := reg.For("initech")
	require.True(t, ok, "a new folder is a new tenant")
	assert.Equal(t, 333, initech.DefaultMaxRows())
	store, known := reg.Resolve("acme")
	assert.Nil(t, store)
	assert.False(t, known, "a removed folder is an unknown tenant, not a rejected one")

	writeTenant(t, root, "acme.bak", validFiles())
	findings, adopted = reg.Reload("test")
	assert.False(t, adopted)
	assert.Contains(t, findingStrings(findings), "acme.bak: folder name is not a tenant id")
	_, ok = reg.For("globex")
	assert.True(t, ok, "a badly named folder costs no tenant its settings")
}

// ReloadTenant reads one tenant's folder and nothing else: the tenant beside
// it is neither adopted nor dropped, whatever state its folder is in.
func TestRegistry_ReloadTenant(t *testing.T) {
	t.Parallel()
	root := writeTree(t, map[string]map[string]string{"acme": maxRowsFiles(111), "globex": maxRowsFiles(222)})
	reg, _ := Open(root)
	require.NotNil(t, reg)
	var hooks [][]tenant.ID
	reg.AfterAdopt(func(adopted []tenant.ID) { hooks = append(hooks, adopted) })
	acme, _ := reg.For("acme")
	globex, _ := reg.For("globex")

	// Both folders change on disk; only the named one is read.
	writeTenant(t, root, "acme", maxRowsFiles(333))
	writeTenant(t, root, "globex", brokenFiles())
	findings, adopted, known := reg.ReloadTenant("acme", "test")
	require.True(t, known)
	require.True(t, adopted, "findings: %s", findingStrings(findings))
	assert.Equal(t, 333, acme.DefaultMaxRows())
	assert.Equal(t, 222, globex.DefaultMaxRows())
	_, ok := reg.For("globex")
	assert.True(t, ok, "a broken folder nobody asked to reload costs its tenant nothing")
	assert.Equal(t, [][]tenant.ID{{"acme"}}, hooks)

	// Reloading the broken one is what drops it: no previous-snapshot fallback.
	findings, adopted, known = reg.ReloadTenant("globex", "test")
	require.True(t, known)
	assert.False(t, adopted)
	assert.Contains(t, findingStrings(findings), "error: globex/config.json: query.default_max_rows")
	_, ok = reg.For("globex")
	assert.False(t, ok)
	_, known = reg.Resolve("globex")
	assert.True(t, known)
	_, ok = reg.For("acme")
	assert.True(t, ok)
	assert.Equal(t, [][]tenant.ID{{"acme"}}, hooks, "a rejected folder runs no hook")

	// And reloading the fixed one brings it back, in the store it always had.
	writeTenant(t, root, "globex", maxRowsFiles(444))
	_, adopted, _ = reg.ReloadTenant("globex", "test")
	require.True(t, adopted)
	recovered, ok := reg.For("globex")
	require.True(t, ok)
	assert.Same(t, globex, recovered)
	assert.Equal(t, 444, recovered.DefaultMaxRows())

	// A tenant the registry does not hold is not looked for on disk: picking
	// up a new folder is a whole-tree reload's job.
	writeTenant(t, root, "initech", maxRowsFiles(555))
	findings, adopted, known = reg.ReloadTenant("initech", "test")
	assert.False(t, known)
	assert.False(t, adopted)
	assert.Empty(t, findings)
	_, ok = reg.For("initech")
	assert.False(t, ok)

	// A folder that is gone is a rejected tenant here — the registry still
	// holds it — and a forgotten one after a whole-tree reload.
	require.NoError(t, os.RemoveAll(filepath.Join(root, "acme")))
	findings, adopted, known = reg.ReloadTenant("acme", "test")
	assert.True(t, known)
	assert.False(t, adopted)
	require.Len(t, findings, 1)
	assert.Equal(t, "acme", findings[0].File)
	assert.Contains(t, findings[0].Message, "does not exist")
	_, known = reg.Resolve("acme")
	assert.True(t, known)
	reg.Reload("test")
	_, known = reg.Resolve("acme")
	assert.False(t, known)
}

// A flat directory is its default tenant's folder, so reloading that tenant
// is Reload — keep-previous on a rejection included — and it has no other.
func TestRegistry_ReloadTenant_FlatDirectory(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, map[string]string{FileConfig: configJSON(`{"query": {"default_max_rows": 500}}`)})
	s, _ := reg.For(tenant.Default)

	require.NoError(t, os.WriteFile(filepath.Join(reg.Dir(), FileConfig), []byte(configJSON(`{"query": {"default_max_rows": 700}}`)), 0o600))
	_, adopted, known := reg.ReloadTenant(tenant.Default, "test")
	assert.True(t, known)
	assert.True(t, adopted)
	assert.Equal(t, 700, s.DefaultMaxRows())

	require.NoError(t, os.WriteFile(filepath.Join(reg.Dir(), FileConfig), []byte(`not json`), 0o600))
	findings, adopted, known := reg.ReloadTenant(tenant.Default, "test")
	assert.True(t, known)
	assert.False(t, adopted)
	assert.Contains(t, findingStrings(findings), "error: config.json:", "a flat directory's findings carry no folder")
	got, ok := reg.For(tenant.Default)
	require.True(t, ok, "a flat directory keeps its previous document")
	assert.Equal(t, 700, got.DefaultMaxRows())

	_, _, known = reg.ReloadTenant("acme", "test")
	assert.False(t, known)
}

// A finding about the root itself rejects the reload whole: nothing on disk
// is adopted, no tenant is dropped, no hook runs.
func TestRegistry_RootLevelFailureChangesNothing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		damage func(t *testing.T, root string)
		want   string
	}{
		{name: "a loose file beside the folders", want: "notes.txt: unexpected file", damage: func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "notes.txt"), []byte("scratch"), 0o600))
		}},
		{name: "the root is gone", want: "does not exist", damage: func(t *testing.T, root string) {
			require.NoError(t, os.RemoveAll(root))
		}},
		{name: "the root turned flat", want: "changed shape", damage: func(t *testing.T, root string) {
			require.NoError(t, os.RemoveAll(filepath.Join(root, "acme")))
			require.NoError(t, os.RemoveAll(filepath.Join(root, "globex")))
			writeTenant(t, root, ".", validFiles())
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := writeTree(t, map[string]map[string]string{"acme": maxRowsFiles(111), "globex": maxRowsFiles(222)})
			reg, _ := Open(root)
			require.NotNil(t, reg)
			reg.AfterAdopt(func([]tenant.ID) { t.Error("a reload rejected whole must not run the hooks") })

			// A change that would be adopted, were the reload not rejected whole.
			writeTenant(t, root, "globex", maxRowsFiles(999))
			tt.damage(t, root)
			findings, adopted := reg.Reload("test")
			assert.False(t, adopted)
			assert.Contains(t, findingStrings(findings), tt.want)

			for id, want := range map[tenant.ID]int{"acme": 111, "globex": 222} {
				store, ok := reg.For(id)
				require.True(t, ok, "%s must still be served", id)
				assert.Equal(t, want, store.DefaultMaxRows())
			}
		})
	}
}

// The shape is fixed at Open in the other direction too: a flat directory
// that turns into tenant folders is a rejected reload, and the document it
// was serving stays.
func TestRegistry_FlatRootTurnedNestedIsRejected(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, map[string]string{FileConfig: configJSON(`{"query": {"default_max_rows": 42}}`)})
	for _, name := range Files() {
		require.NoError(t, os.Remove(filepath.Join(reg.Dir(), name)))
	}
	writeTenant(t, reg.Dir(), "acme", validFiles())

	findings, adopted := reg.Reload("test")
	assert.False(t, adopted)
	assert.Contains(t, findingStrings(findings), "changed shape")
	s, ok := reg.For(tenant.Default)
	require.True(t, ok)
	assert.Equal(t, 42, s.DefaultMaxRows())
	_, ok = reg.For("acme")
	assert.False(t, ok)
}
