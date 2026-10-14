package settings

import (
	"maps"
	"sync"
	"time"
)

// Reload outcomes, the Result of a ReloadKey.
const (
	// ResultAdopted is a reload that adopted every tenant's folder, and that
	// every consumer reporting through Reconciled reconciled with.
	ResultAdopted = "adopted"
	// ResultPartial is a reload that applied, but with a tenant's folder
	// rejected or a consumer that failed to reconcile with it.
	ResultPartial = "partial"
	// ResultRejected is a reload refused whole: a flat directory with an
	// error, a finding about the directory itself, or the one tenant
	// ReloadTenant read rejected. A nested reload rejecting every folder
	// is partial.
	ResultRejected = "rejected"
)

// ReloadKey counts reloads by the trigger that fired them and their result.
type ReloadKey struct{ Trigger, Result string }

// ReloadStats is how this process's settings reloads have gone (#776), for
// the metrics internal/app exports: reloads are per process, so a tenant can
// be rejected or half-reconciled on one process and fine on the next.
type ReloadStats struct {
	// Served and Rejected count the tenants the registry holds now.
	Served, Rejected int
	// Removed counts the tenants reloads have dropped since boot.
	Removed int64
	// Reloads counts every reload since boot, Open's included.
	Reloads map[ReloadKey]int64
	// ReconcileFailures counts the failures reported through Reconciled,
	// by component.
	ReconcileFailures map[string]int64
	// LastSuccessful is whether the latest reload's result was
	// ResultAdopted.
	LastSuccessful bool
	// LastSuccess is when a reload's result last was ResultAdopted; zero
	// if none has been.
	LastSuccess time.Time
}

// stats is the registry's record behind ReloadStats.
type stats struct {
	mu       sync.Mutex
	reloads  map[ReloadKey]int64
	failures map[string]int64
	removed  int64
	// last is the latest reload's key, and lastSuccess and prevSuccess the
	// success time it left and the one before it, for a boot failure to
	// take back (Reconciled).
	last                     ReloadKey
	lastSuccess, prevSuccess time.Time
	// hooking is set while a reload runs its hooks, and hookFailed by a
	// failure reported meanwhile.
	hooking, hookFailed bool
}

// beginHooks marks the hooks of a reload as running.
func (s *stats) beginHooks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooking, s.hookFailed = true, false
}

// endHooks marks them done, reporting whether one failed.
func (s *stats) endHooks() (failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooking = false
	return s.hookFailed
}

// record counts one reload.
func (s *stats) record(trigger, result string, removed int, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reloads == nil {
		s.reloads = map[ReloadKey]int64{}
	}
	s.last = ReloadKey{Trigger: trigger, Result: result}
	s.reloads[s.last]++
	s.removed += int64(removed)
	if result == ResultAdopted {
		s.prevSuccess, s.lastSuccess = s.lastSuccess, now
	}
}

// Reconciled reports how a consumer that follows the adopted settings
// (component names it: "clickhouse", "dedupe", "mq") fared at reconciling
// with them, from its AfterAdopt hook or from the boot apply that follows
// Open. A nil err records nothing. A failure is counted, and makes the
// reload it follows partial: the one whose hooks are running, or, from the
// boot apply, Open's own, which then no longer counts as adopted — so read
// Stats for a counter only once boot is done, or it would go down.
func (r *Registry) Reconciled(component string, err error) {
	if err == nil {
		return
	}
	s := &r.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures == nil {
		s.failures = map[string]int64{}
	}
	s.failures[component]++
	if s.hooking {
		s.hookFailed = true
		return
	}
	if s.last.Result == ResultAdopted {
		if s.reloads[s.last]--; s.reloads[s.last] == 0 {
			delete(s.reloads, s.last)
		}
		s.last.Result = ResultPartial
		s.reloads[s.last]++
		s.lastSuccess = s.prevSuccess
	}
}

// Stats returns how this process's reloads have gone so far.
func (r *Registry) Stats() ReloadStats {
	var st ReloadStats
	for _, e := range *r.tenants.Load() {
		if e.rejected {
			st.Rejected++
		} else {
			st.Served++
		}
	}
	s := &r.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	st.Removed = s.removed
	st.Reloads = maps.Clone(s.reloads)
	st.ReconcileFailures = maps.Clone(s.failures)
	st.LastSuccessful = s.last.Result == ResultAdopted
	st.LastSuccess = s.lastSuccess
	return st
}
