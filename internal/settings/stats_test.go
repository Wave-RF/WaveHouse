package settings

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/pkg/settings"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

var errReconcile = errors.New("reconcile failed")

func TestStats_FlatReloads(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, nil)
	st := reg.Stats()
	assert.Equal(t, 1, st.Served)
	assert.Zero(t, st.Rejected)
	assert.Equal(t, map[ReloadKey]int64{{"boot", ResultAdopted}: 1}, st.Reloads)
	assert.True(t, st.LastSuccessful)
	boot := st.LastSuccess
	require.False(t, boot.IsZero())

	require.NoError(t, os.WriteFile(filepath.Join(reg.Dir(), settings.FileConfig), []byte(configJSON(`{"query": {"default_max_rows": -1}}`)), 0o600))
	reg.Reload("sighup")
	st = reg.Stats()
	assert.Equal(t, int64(1), st.Reloads[ReloadKey{"sighup", ResultRejected}])
	assert.False(t, st.LastSuccessful)
	assert.Equal(t, boot, st.LastSuccess, "a rejected reload is not a success")
	assert.Equal(t, 1, st.Served, "a flat directory keeps serving its previous settings")

	require.NoError(t, os.WriteFile(filepath.Join(reg.Dir(), settings.FileConfig), []byte(configJSON(`{}`)), 0o600))
	reg.Reload("api")
	st = reg.Stats()
	assert.Equal(t, int64(1), st.Reloads[ReloadKey{"api", ResultAdopted}])
	assert.True(t, st.LastSuccessful)
	assert.False(t, st.LastSuccess.Before(boot))
}

// A hook that fails to reconcile makes the reload partial, whatever the
// folders said: the settings were adopted, but a component is not following
// them.
func TestStats_HookFailureMakesTheReloadPartial(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, nil)
	boot := reg.Stats().LastSuccess
	fail := true
	reg.AfterAdopt(func([]tenant.ID) {
		var err error
		if fail {
			err = errReconcile
		}
		reg.Reconciled("clickhouse", err)
	})
	reg.AfterAdopt(func([]tenant.ID) { reg.Reconciled("mq", nil) })

	_, adopted := reg.Reload("api")
	assert.True(t, adopted, "the reload's own answer is about the folders")
	st := reg.Stats()
	assert.Equal(t, int64(1), st.Reloads[ReloadKey{"api", ResultPartial}])
	assert.Equal(t, map[string]int64{"clickhouse": 1}, st.ReconcileFailures, "a nil error records nothing")
	assert.False(t, st.LastSuccessful)
	assert.Equal(t, boot, st.LastSuccess)

	fail = false
	reg.Reload("api")
	st = reg.Stats()
	assert.Equal(t, int64(1), st.Reloads[ReloadKey{"api", ResultAdopted}], "the next reload starts clean")
	assert.True(t, st.LastSuccessful)
	assert.Equal(t, map[string]int64{"clickhouse": 1}, st.ReconcileFailures)
}

// The boot apply runs after Open has recorded the boot reload: its failure
// takes the boot's success back.
func TestStats_BootApplyFailure(t *testing.T) {
	t.Parallel()
	reg := newLoadedRegistry(t, nil)
	reg.Reconciled("dedupe", errReconcile)
	reg.Reconciled("mq", errReconcile)
	st := reg.Stats()
	assert.Equal(t, map[ReloadKey]int64{{"boot", ResultPartial}: 1}, st.Reloads)
	assert.Equal(t, map[string]int64{"dedupe": 1, "mq": 1}, st.ReconcileFailures)
	assert.False(t, st.LastSuccessful)
	assert.True(t, st.LastSuccess.IsZero(), "no reload has been a success")

	reg.Reload("watch")
	st = reg.Stats()
	assert.True(t, st.LastSuccessful)
	assert.False(t, st.LastSuccess.IsZero())
}

func TestStats_NestedTenants(t *testing.T) {
	t.Parallel()
	root := writeTree(t, map[string]map[string]string{"acme": maxRowsFiles(111), "globex": maxRowsFiles(222), "initech": maxRowsFiles(333)})
	reg, _ := Open(root)
	require.NotNil(t, reg)

	writeTenant(t, root, "globex", brokenFiles())
	require.NoError(t, os.RemoveAll(filepath.Join(root, "initech")))
	reg.Reload("api")
	st := reg.Stats()
	assert.Equal(t, 1, st.Served)
	assert.Equal(t, 1, st.Rejected)
	assert.Equal(t, int64(1), st.Removed)
	assert.Equal(t, int64(1), st.Reloads[ReloadKey{"api", ResultPartial}])
	assert.False(t, st.LastSuccessful)

	_, _, known := reg.ReloadTenant("globex", "api")
	require.True(t, known)
	st = reg.Stats()
	assert.Equal(t, int64(1), st.Reloads[ReloadKey{"api", ResultRejected}], "one folder read and rejected adopted nothing")

	writeTenant(t, root, "globex", maxRowsFiles(444))
	reg.ReloadTenant("globex", "api")
	st = reg.Stats()
	assert.Equal(t, int64(1), st.Reloads[ReloadKey{"api", ResultAdopted}])
	assert.Equal(t, 2, st.Served)
	assert.Zero(t, st.Rejected)
	assert.True(t, st.LastSuccessful)

	_, _, known = reg.ReloadTenant("nobody", "api")
	require.False(t, known)
	assert.Equal(t, st.Reloads, reg.Stats().Reloads, "an unknown tenant reads nothing and records nothing")
}
