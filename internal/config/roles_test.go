package config

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_RolesDefaultToEveryRole(t *testing.T) {
	t.Parallel()
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, []Role{RoleAPI, RoleIngest, RoleSweeper}, cfg.Roles)
	for _, r := range AllRoles() {
		assert.True(t, cfg.Has(r), r)
	}
	host, err := os.Hostname()
	require.NoError(t, err)
	assert.Regexp(t, "^"+regexp.QuoteMeta(host)+"-[0-9a-f]{8}$", cfg.InstanceID)

	again, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.NotEqual(t, cfg.InstanceID, again.InstanceID, "a restarted process is a new instance")
}

// cleanenv splits a slice variable on commas; the entries are trimmed, and
// their order is not significant.
func TestLoad_RolesFromEnv(t *testing.T) {
	t.Setenv("WH_ROLES", "sweeper, api ,ingest")
	t.Setenv("WH_INSTANCE_ID", " pod-a ")
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, []Role{RoleSweeper, RoleAPI, RoleIngest}, cfg.Roles)
	assert.Equal(t, "pod-a", cfg.InstanceID)
}

// One role parses to one entry — refused here only because the embedded MQ
// cannot be split, which is the message a split gets until a shared MQ lands.
func TestLoad_OneRoleFromEnvIsRefusedOnTheEmbeddedMQ(t *testing.T) {
	t.Setenv("WH_ROLES", "ingest")
	_, err := Load("nonexistent.yaml")
	require.ErrorContains(t, err, "roles ingest with mq.backend=embedded")
}

func TestLoad_EmptyRolesFromEnv(t *testing.T) {
	t.Setenv("WH_ROLES", "")
	_, err := Load("nonexistent.yaml")
	require.ErrorContains(t, err, "roles (WH_ROLES)")
}

func TestLoad_RolesFromYAML(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
roles: [sweeper, api, ingest]
instance_id: pod-b
`), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, []Role{RoleSweeper, RoleAPI, RoleIngest}, cfg.Roles, "the file's list, not the default")
	assert.Equal(t, "pod-b", cfg.InstanceID)
}

func TestUnboundEnv_KnowsTheProcessVariables(t *testing.T) {
	t.Parallel()
	assert.Empty(t, unboundEnv([]string{"WH_ROLES=api", "WH_INSTANCE_ID=pod-a"}))
}

func TestValidate_Roles(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		roles []Role
		want  string
	}{
		{"empty", nil, "roles (WH_ROLES) is empty"},
		{"empty entry", []Role{RoleAPI, "", RoleIngest}, "roles (WH_ROLES) api,,ingest has an empty entry"},
		{"unknown", []Role{RoleAPI, "worker"}, `roles (WH_ROLES) "worker" is not a role; valid: api,ingest,sweeper`},
		{"duplicate", []Role{RoleAPI, RoleIngest, RoleAPI}, `roles (WH_ROLES) names "api" twice`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultBackends()
			cfg.Roles = tc.roles
			require.ErrorContains(t, cfg.Validate(), tc.want)
		})
	}
}

// Rules 2 and 5 of the #613 design. The embedded MQ refuses every split. A
// shared queue, which no backend offers yet and so is set directly, lets a
// process run any subset — except api without ingest or ingest without api
// over a local cache: the worker's invalidation would miss the API's cache. A
// sweeper-only process holds no cache, so it passes.
func TestValidate_RoleSplits(t *testing.T) {
	t.Parallel()
	all := AllRoles()
	for _, tc := range []struct {
		name  string
		roles []Role
		mq    MQBackend
		cache CacheBackend
		want  string // "" is valid
	}{
		{"every role, embedded", all, MQEmbedded, CacheLocal, ""},
		{"api, embedded", []Role{RoleAPI}, MQEmbedded, CacheLocal, "roles api with mq.backend=embedded: the embedded MQ lives inside this process"},
		{"api+ingest, embedded", []Role{RoleAPI, RoleIngest}, MQEmbedded, CacheLocal, "roles api,ingest with mq.backend=embedded"},
		{"sweeper, embedded, shared cache", []Role{RoleSweeper}, MQEmbedded, "shared", "roles sweeper with mq.backend=embedded"},

		{"every role, shared queue", all, "shared", CacheLocal, ""},
		{"api+ingest, shared queue", []Role{RoleAPI, RoleIngest}, "shared", CacheLocal, ""},
		{"sweeper, shared queue", []Role{RoleSweeper}, "shared", CacheLocal, ""},
		{"api, local cache", []Role{RoleAPI}, "shared", CacheLocal, "roles api with cache.backend=local: api and ingest run in different processes"},
		{"ingest, local cache", []Role{RoleIngest}, "shared", CacheLocal, "roles ingest with cache.backend=local"},
		{"api+sweeper, local cache", []Role{RoleAPI, RoleSweeper}, "shared", CacheLocal, "roles api,sweeper with cache.backend=local"},
		{"ingest+sweeper, local cache", []Role{RoleIngest, RoleSweeper}, "shared", CacheLocal, "roles ingest,sweeper with cache.backend=local"},
		{"api, shared cache", []Role{RoleAPI}, "shared", "shared", ""},
		{"ingest, shared cache", []Role{RoleIngest}, "shared", "shared", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultBackends()
			cfg.Roles, cfg.MQ.Backend, cfg.Cache.Backend = tc.roles, tc.mq, tc.cache
			// validateTopology directly: a literal backend this build lacks
			// is refused by validateBackends first.
			err := cfg.validateTopology()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// A process without the api role opens no cache and no dedupe store, so
// neither shared-queue warning is its, and Pebble never needs its data_dir.
func TestRoles_WithoutTheAPIRole(t *testing.T) {
	t.Parallel()
	cfg := defaultBackends()
	cfg.MQ.Backend = "shared"
	cfg.Roles = []Role{RoleSweeper}
	assert.Empty(t, cfg.Warnings())
	assert.False(t, cfg.NeedsDataDir(), "only the api role opens Pebble")
	cfg.Roles = []Role{RoleAPI, RoleIngest}
	assert.Len(t, cfg.Warnings(), 2)
	assert.True(t, cfg.NeedsDataDir())
}
