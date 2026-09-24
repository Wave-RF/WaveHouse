package stream

import (
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// topicOf is the default tenant's topic for table: what a flat directory's
// one tenant subscribes to and publishes on.
func topicOf(table string) mq.Topic {
	return mq.Topic{Tenant: tenant.Default, Table: table}
}

// staticPolicy is a PolicySource fixed to p, whatever the tenant.
func staticPolicy(p *policy.Policy) PolicySource {
	return func(tenant.ID) *policy.Policy { return p }
}
