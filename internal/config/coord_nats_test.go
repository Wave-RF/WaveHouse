package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_CoordNATS(t *testing.T) {
	t.Setenv("WH_MQ_BACKEND", "nats")
	t.Setenv("WH_MQ_NATS_URLS", "nats://nats:4222")
	t.Setenv("WH_COORD_BACKEND", "nats")
	t.Setenv("WH_COORD_NATS_BUCKET", "leases")
	cfg, err := Load("nonexistent.yaml")
	require.NoError(t, err)
	assert.Equal(t, Coord{Backend: CoordNATS, NATS: CoordNATSConfig{Bucket: "leases"}}, cfg.Coord)
}

func TestLoad_CoordNATSFromYAML(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mq:
  backend: nats
  nats:
    urls: ["nats://a:4222"]
coord:
  backend: nats
  nats:
    bucket: prod_leases
`), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, CoordNATSConfig{Bucket: "prod_leases"}, cfg.Coord.NATS)
	assert.Empty(t, CoordNATSConfig{}.Bucket, "empty is the prefix's bucket, named by internal/mq")
}

func TestUnboundEnv_KnowsTheCoordNATSVariables(t *testing.T) {
	t.Parallel()
	assert.Empty(t, unboundEnv([]string{"WH_COORD_NATS_BUCKET=x"}))
}

// Rules 3 and 4 (#613 core G.3): NATS leases need the NATS connection, and a
// process sweeping a shared queue needs a shared lease. Only the sweeper runs
// under a lease, so a process without it may keep coord.backend=local.
func TestValidate_CoordAgainstMQ(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		mq    MQBackend
		coord CoordBackend
		roles []Role
		want  string
	}{
		{"embedded, local", MQEmbedded, CoordLocal, AllRoles(), ""},
		{"nats, nats", MQNATS, CoordNATS, AllRoles(), ""},
		{"rule 3: nats leases on embedded", MQEmbedded, CoordNATS, AllRoles(), "coord.backend=nats with mq.backend=embedded"},
		{"rule 4: sweeping a shared queue on a local lease", MQNATS, CoordLocal, AllRoles(), "coord.backend=local with mq.backend=nats in a process running the sweeper"},
		{"rule 4: a sweeper-only process", MQNATS, CoordLocal, []Role{RoleSweeper}, "set coord.backend=nats"},
		{"rule 4 spares a process without the sweeper", MQNATS, CoordLocal, []Role{RoleAPI, RoleIngest}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := natsBackends()
			cfg.MQ.Backend, cfg.Coord.Backend, cfg.Roles = tc.mq, tc.coord, tc.roles
			err := cfg.Validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidate_CoordNATSBucket(t *testing.T) {
	t.Parallel()
	for bucket, ok := range map[string]bool{"": true, "wh_coord": true, "Prod-Leases_2": true, "wh.coord": false, "wh coord": false, "a>": false} {
		cfg := natsBackends()
		cfg.Coord.NATS.Bucket = bucket
		err := cfg.Validate()
		if ok {
			assert.NoError(t, err, "bucket %q", bucket)
			continue
		}
		require.Error(t, err, "bucket %q", bucket)
		assert.Contains(t, err.Error(), "coord.nats.bucket (WH_COORD_NATS_BUCKET)")
	}
}

// The block is read only under coord.backend=nats; otherwise boot says so.
func TestWarnings_CoordNATSIgnored(t *testing.T) {
	t.Parallel()
	cfg := defaultBackends()
	cfg.Coord.NATS.Bucket = "wh.bad"
	require.NoError(t, cfg.Validate())
	assert.Equal(t, []string{"coord.nats is set but coord.backend=local: the block is ignored"}, cfg.Warnings())
}
