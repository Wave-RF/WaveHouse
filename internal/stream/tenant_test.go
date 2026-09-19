package stream

import (
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// staticPolicy is a PolicySource fixed to p, whatever the tenant.
func staticPolicy(p *policy.Policy) PolicySource {
	return func(tenant.ID) *policy.Policy { return p }
}
