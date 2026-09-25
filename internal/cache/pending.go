package cache

import (
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// pendingBumps holds the token bumps an invalidation could not deliver, until
// a retry lands them. Repeats of one key coalesce; past max keys the set
// collapses to one tenant bump per affected tenant — coarser, never less.
// Landing a bump late is still correct: a fresh token orphans the pre-write
// entries and any fill in between.
type pendingBumps struct {
	prefix string
	max    int

	mu   sync.Mutex
	keys map[string]pendingKey
	gen  uint64
}

type pendingKey struct {
	tenant tenant.ID
	gen    uint64 // when last added; take drops a key only if it is unchanged
}

func newPendingBumps(prefix string, maxKeys int) *pendingBumps {
	return &pendingBumps{prefix: prefix, max: maxKeys, keys: map[string]pendingKey{}}
}

// add records keys of tenant id as owed a bump.
func (p *pendingBumps) add(id tenant.ID, keys ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gen++
	for _, k := range keys {
		p.keys[k] = pendingKey{tenant: id, gen: p.gen}
	}
	if len(p.keys) <= p.max {
		return
	}
	collapsed := make(map[string]pendingKey, len(p.keys))
	for _, pk := range p.keys {
		collapsed[tenantTokenKey(p.prefix, pk.tenant)] = pendingKey{tenant: pk.tenant, gen: p.gen}
	}
	p.keys = collapsed
}

// snapshot returns the keys owed a bump with the generation each was added
// at, for a later done.
func (p *pendingBumps) snapshot() map[string]uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]uint64, len(p.keys))
	for k, pk := range p.keys {
		out[k] = pk.gen
	}
	return out
}

// done drops keys whose bump landed — unless one was added again since the
// snapshot, whose bump the landed one may have preceded.
func (p *pendingBumps) done(landed map[string]uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, gen := range landed {
		if pk, ok := p.keys[k]; ok && pk.gen == gen {
			delete(p.keys, k)
		}
	}
}

func (p *pendingBumps) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.keys)
}
