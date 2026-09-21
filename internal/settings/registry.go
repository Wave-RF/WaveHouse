package settings

import (
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Registry maps a tenant id to the Store holding that tenant's adopted
// settings, and owns everything that changes one. Open validates the
// directory and adopts it up front. Reload is the single code path every
// later trigger — SIGHUP, the directory watcher, and POST
// /v1/ops/settings/reload — funnels through. The stores are passive: a
// document pointer their getters read.
//
// What a rejected directory costs depends on its shape (#583), which is fixed
// at Open — switching shapes is stop, restructure, start:
//
//   - Flat, the four files in the directory itself: tenant.Default alone. Open
//     refuses an invalid directory, so a flat *Registry never exists without a
//     good document behind it, and a reload swaps the snapshot only when no
//     finding is an error, so a bad edit (or a deleted file, or a vanished
//     directory) can never evict the last good document.
//   - Nested, one folder per tenant: fail closed per tenant, at Open and on
//     reload alike. A folder with an error finding stops being served — the
//     registry still knows the tenant, so its requests are refused rather
//     than unknown — and every other tenant carries on; there is no
//     previous-snapshot fallback. A reload mirrors the folders: a new one is
//     served and a removed one is forgotten. A finding about the directory
//     itself (a loose file, an unreadable directory, a changed shape) rejects
//     the reload whole and leaves every tenant as it was; at Open it refuses
//     boot.
type Registry struct {
	dir    string
	nested bool

	// mu serializes Reload: concurrent triggers queue rather than racing
	// validate-then-swap sequences (a stale document must not overwrite a newer one).
	mu sync.Mutex
	// afterAdopt runs under mu after every reload that adopted a tenant, in
	// registration order — for consumers that own a resource whose lifecycle
	// follows a setting (the Pebble store behind dedupe.enabled) rather than
	// reading the snapshot per call.
	afterAdopt []func(adopted []tenant.ID)

	// tenants is replaced whole by a reload, never edited, so a lookup is one
	// lock-free load.
	tenants atomic.Pointer[map[tenant.ID]entry]
}

// entry is one tenant the registry knows.
type entry struct {
	// store is created when the tenant's folder first validates and never
	// replaced, so the handle For returns keeps reading the latest adopted
	// document.
	store *Store
	// rejected marks a nested tenant whose folder failed its last
	// validation. The store keeps its document, for the requests already
	// admitted under it; the registry just stops handing it out.
	rejected bool
}

// Open validates dir and returns a Registry serving it. A rejected directory
// — flat and invalid, or of either shape with a finding about the directory
// itself — returns a nil Registry with the findings: the caller (boot)
// refuses to start. A nested directory with a rejected tenant folder still
// opens; that tenant alone is not served.
func Open(dir string) (*Registry, []Finding) {
	r := newRegistry(map[tenant.ID]entry{})
	r.dir = dir
	r.mu.Lock()
	defer r.mu.Unlock()
	findings, _, applied := r.reload("boot", true)
	if !applied {
		return nil, findings
	}
	return r, findings
}

// NewRegistry returns a flat Registry serving store as tenant.Default, with
// no directory behind it: what a test that fixes its settings holds.
func NewRegistry(store *Store) *Registry {
	return newRegistry(map[tenant.ID]entry{tenant.Default: {store: store}})
}

func newRegistry(tenants map[tenant.ID]entry) *Registry {
	r := &Registry{}
	r.tenants.Store(&tenants)
	return r
}

// Dir returns the directory this registry reads.
func (r *Registry) Dir() string { return r.dir }

// Nested reports the directory's shape: one folder per tenant (true) or the
// four files of tenant.Default (false).
func (r *Registry) Nested() bool { return r.nested }

// For returns the store of tenant id, or false when the registry is not
// serving that tenant — it has no such tenant, or the tenant's folder was
// rejected.
func (r *Registry) For(id tenant.ID) (*Store, bool) {
	store, _ := r.Resolve(id)
	return store, store != nil
}

// Resolve is For for a caller that answers the two misses differently: a nil
// store with known=true is a tenant whose folder was rejected, and with
// known=false a tenant the registry has never heard of.
func (r *Registry) Resolve(id tenant.ID) (store *Store, known bool) {
	e, known := (*r.tenants.Load())[id]
	if !known || e.rejected {
		return nil, known
	}
	return e.store, true
}

// Reload re-validates the directory and adopts what it finds (see Registry
// for what a rejection costs in each shape). Warnings don't block adoption,
// matching `wavehouse validate`. The returned bool reports whether everything
// was adopted: no finding is an error. In a nested directory false can
// therefore mean adopted in part — the tenants whose folders validated were
// adopted and the ones in the findings were not. trigger names the path that
// fired ("boot", "sighup", "watch", "api") and tags every log line so
// operators can tell them apart.
func (r *Registry) Reload(trigger string) ([]Finding, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	findings, adopted, _ := r.reload(trigger, false)
	return findings, adopted
}

// reload is Reload under mu. boot marks Open's first pass, which takes the
// directory's shape rather than holding it to one. applied reports whether
// the registry took the tree at all.
func (r *Registry) reload(trigger string, boot bool) (findings []Finding, adopted, applied bool) {
	tree, findings := Validate(r.dir)
	switch {
	case tree == nil:
	case boot:
		r.nested = tree.Nested
	case tree.Nested != r.nested:
		findings = append(findings, Finding{Severity: SeverityError, Message: "the settings directory changed shape between the four files and one folder per tenant — switching is stop, restructure, start"})
		tree = nil
	}
	// A flat directory with an error finding changes nothing, like a finding
	// about the directory itself: the previous document stays.
	if tree != nil && !tree.Nested && tree.Tenants[tenant.Default].Doc == nil {
		tree = nil
	}

	var adoptedIDs, rejectedIDs []tenant.ID
	if tree != nil {
		prev := *r.tenants.Load()
		next := make(map[tenant.ID]entry, len(tree.Tenants))
		// Sorted so the hooks and the log name the tenants in one order every run.
		for _, id := range slices.Sorted(maps.Keys(tree.Tenants)) {
			e := prev[id]
			if doc := tree.Tenants[id].Doc; doc != nil {
				if e.store == nil {
					e.store = &Store{}
				}
				e.store.adopt(doc)
				e.rejected = false
				adoptedIDs = append(adoptedIDs, id)
			} else {
				e.rejected = true
				rejectedIDs = append(rejectedIDs, id)
			}
			next[id] = e
		}
		r.tenants.Store(&next)
		if len(adoptedIDs) > 0 {
			for _, fn := range r.afterAdopt {
				fn(adoptedIDs)
			}
		}
	}

	var errs, warns int
	for _, f := range findings {
		if f.Severity == SeverityError {
			errs++
			slog.Error("settings finding", "trigger", trigger, "finding", f.String())
		} else {
			warns++
			slog.Warn("settings finding", "trigger", trigger, "finding", f.String())
		}
	}
	switch {
	case tree == nil && boot:
		slog.Error("settings rejected", "trigger", trigger, "dir", r.dir, "errors", errs, "warnings", warns)
	case tree == nil:
		slog.Error("settings rejected — keeping previous settings", "trigger", trigger, "dir", r.dir, "errors", errs, "warnings", warns)
	case errs > 0:
		slog.Error("settings adopted in part — a tenant whose folder was rejected answers 503 until a reload adopts it", "trigger", trigger, "dir", r.dir, "adopted", len(adoptedIDs), "rejected", rejectedIDs, "errors", errs, "warnings", warns)
	case r.nested:
		slog.Info("settings adopted", "trigger", trigger, "dir", r.dir, "tenants", len(adoptedIDs), "warnings", warns)
	default:
		slog.Info("settings adopted", "trigger", trigger, "dir", r.dir, "warnings", warns)
	}
	return findings, errs == 0, tree != nil
}

// AfterAdopt registers fn to run after each subsequent reload that adopted a
// tenant, serialized with the reload itself, with the tenants it adopted.
// Open's boot adoption has already happened by the time a caller can
// register, so the caller applies the boot state itself.
func (r *Registry) AfterAdopt(fn func(adopted []tenant.ID)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.afterAdopt = append(r.afterAdopt, fn)
}
