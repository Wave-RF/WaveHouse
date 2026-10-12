package app

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/observability"
)

func TestIdentity_NamesTheBuildInstanceAndRoles(t *testing.T) {
	t.Parallel()
	a := &App{
		cfg:   &config.Config{InstanceID: "pod-a-1234abcd", Roles: []config.Role{config.RoleAPI, config.RoleIngest}},
		build: BuildInfo{Version: "v1.2.3", GitCommit: "abc123", BuildTime: "2026-10-12T00:00:00Z"},
	}
	assert.Equal(t, observability.Identity{
		ServiceName: "wavehouse",
		Version:     "v1.2.3",
		GitCommit:   "abc123",
		BuildTime:   "2026-10-12T00:00:00Z",
		InstanceID:  "pod-a-1234abcd",
		Roles:       []string{"api", "ingest"},
	}, a.identity())
}
