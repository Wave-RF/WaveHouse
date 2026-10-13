package app

import (
	"slices"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// identity is how this process names itself in its telemetry: the build it
// runs, its instance_id and its roles (#772). A file of its own so the
// observability wiring in wire.go only calls it.
func (a *App) identity() observability.Identity {
	// Sorted: the order roles is written in means nothing, so it must not
	// split a dashboard's series.
	roles := make([]string, len(a.cfg.Roles))
	for i, r := range a.cfg.Roles {
		roles[i] = string(r)
	}
	slices.Sort(roles)
	return observability.Identity{
		ServiceName: serviceName,
		Version:     a.build.Version,
		GitCommit:   a.build.GitCommit,
		BuildTime:   a.build.BuildTime,
		InstanceID:  a.cfg.InstanceID,
		Roles:       roles,
	}
}
