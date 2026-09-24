package settings

import (
	"iter"
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
//     served and a removed one is forgotten, the last one too. A finding about
//     the directory itself (a loose file, an unreadable directory, a changed
//     shape) rejects the reload whole and leaves every tenant as it was; at
//     Open it refuses boot.
type Registry struct {
	dir    string
	nested bool

	// mu serializes Reload: concurrent triggers queue rather than racing
	// validate-then-swap sequences (a stale document must not overwrite a newer one).
	mu sync.Mutex
	// afterAdopt runs under mu after every reload the registry applied, in
	// registration order, with the tenants that reload adopted — none when it
	// only rejected or removed — for consumers that own a resource whose
	// lifecycle follows the tenants served and their settings (the dedupe
	// store behind each tenant's dedupe.enabled) rather than reading the
	// snapshot per call.
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

// adopt returns e after a validation pass over id's folder: holding doc, or
// rejected when the folder yielded none.
func (e entry) adopt(id tenant.ID, doc *Document) entry {
	if doc == nil {
		e.rejected = true
		return e
	}
	if e.store == nil {
		e.store = &Store{tenant: id}
	}
	e.store.adopt(doc)
	e.rejected = false
	return e
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
// no directory behind it: what a test that fixes its settings holds. The
// store is stamped with that id, as one a Registry creates is.
func NewRegistry(store *Store) *Registry {
	store.tenant = tenant.Default
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

// All iterates over the tenants being served, in id order — for a consumer
// that owns one resource every tenant shares and has to weigh their settings
// against each other.
func (r *Registry) All() iter.Seq2[tenant.ID, *Store] {
	return func(yield func(tenant.ID, *Store) bool) {
		tenants := *r.tenants.Load()
		for _, id := range slices.Sorted(maps.Keys(tenants)) {
			if e := tenants[id]; !e.rejected && !yield(id, e.store) {
				return
			}
		}
	}
}

// Known iterates over every tenant the registry holds, served or rejected,
// in id order — for a consumer that must keep a rejected tenant's resources
// current too: the tenant comes back into service with them, and a rejection
// is the common reload failure (a typo, fixed and reloaded minutes later).
func (r *Registry) Known() iter.Seq[tenant.ID] {
	return func(yield func(tenant.ID) bool) {
		for _, id := range slices.Sorted(maps.Keys(*r.tenants.Load())) {
			if !yield(id) {
				return
			}
		}
	}
}

// Reload re-validates the directory and adopts what it finds (see Registry
// for what a rejection costs in each shape). Warnings don't block adoption,
// matching `wavehouse validate`. The returned bool reports whether everything
// was adopted: no finding is an error. In a nested directory false can
// therefore mean adopted in part — the tenants whose folders validated were
// adopted, warnings and all, and the ones with an error finding were not.
// trigger names the path that fired ("boot", "sighup", "watch", "api") and
// tags every log line so operators can tell them apart.
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
	tree, findings := r.validate(boot)
	switch {
	case tree == nil:
	case boot:
		r.nested = tree.Nested
	case tree.Nested != r.nested:
		booted := "the four files"
		if r.nested {
			booted = "one folder per tenant"
		}
		findings = append(findings, Finding{Severity: SeverityError, Message: "the settings directory no longer has the shape this server booted with (" + booted + ") — switching shapes is stop, restructure, start"})
		tree = nil
	}
	// A flat directory with an error finding changes nothing, like a finding
	// about the directory itself: the previous document stays.
	if tree != nil && !tree.Nested && tree.Tenants[tenant.Default].Doc == nil {
		tree = nil
	}

	var adoptedIDs, rejectedIDs, removedIDs []tenant.ID
	if tree != nil {
		prev := *r.tenants.Load()
		next := make(map[tenant.ID]entry, len(tree.Tenants))
		// Sorted so the hooks and the log name the tenants in one order every run.
		for _, id := range slices.Sorted(maps.Keys(tree.Tenants)) {
			next[id] = prev[id].adopt(id, tree.Tenants[id].Doc)
			if next[id].rejected {
				rejectedIDs = append(rejectedIDs, id)
			} else {
				adoptedIDs = append(adoptedIDs, id)
			}
		}
		// A tenant whose folder is gone leaves the map with no finding to show
		// for it, and every request of its turns into a 404: name it in the log.
		for _, id := range slices.Sorted(maps.Keys(prev)) {
			if _, kept := next[id]; !kept {
				removedIDs = append(removedIDs, id)
			}
		}
		r.tenants.Store(&next)
		r.adopted(adoptedIDs)
	}

	errs, warns := logFindings(trigger, findings)
	switch {
	case tree == nil && boot:
		slog.Error("settings rejected", "trigger", trigger, "dir", r.dir, "errors", errs, "warnings", warns)
	case tree == nil:
		slog.Error("settings rejected — keeping previous settings", "trigger", trigger, "dir", r.dir, "errors", errs, "warnings", warns)
	case errs > 0:
		slog.Error("settings adopted in part — a tenant whose folder was rejected answers 503 until a reload adopts it", "trigger", trigger, "dir", r.dir, "adopted", len(adoptedIDs), "rejected", rejectedIDs, "removed", removedIDs, "errors", errs, "warnings", warns)
	case len(removedIDs) > 0:
		slog.Warn("settings adopted — a tenant whose folder is gone is no longer served", "trigger", trigger, "dir", r.dir, "tenants", len(adoptedIDs), "removed", removedIDs, "warnings", warns)
	case r.nested:
		slog.Info("settings adopted", "trigger", trigger, "dir", r.dir, "tenants", len(adoptedIDs), "warnings", warns)
	default:
		slog.Info("settings adopted", "trigger", trigger, "dir", r.dir, "warnings", warns)
	}
	return findings, errs == 0, tree != nil
}

// validate is Validate, but for an empty root under a registry serving
// folders, which it reads as the nested directory it is with no folder left
// rather than as Validate's flat directory missing its files — so removing
// the last tenant's folder removes the tenant, like any other. At Open an
// empty root still reads as flat: nothing says it was meant to hold folders.
func (r *Registry) validate(boot bool) (*Tree, []Finding) {
	if !boot && r.nested && emptyRoot(r.dir) {
		return &Tree{Nested: true, Tenants: map[tenant.ID]TenantResult{}}, nil
	}
	return Validate(r.dir)
}

// ReloadTenant is Reload for one tenant's folder of a nested directory: the
// rest of the directory is not read, so it can neither adopt nor drop another
// tenant — what the writer of one folder calls when that folder is complete.
// The folder is adopted, or the tenant stops being served. known is false for
// a tenant the registry does not hold, and nothing is read: a whole-tree
// Reload is what picks up a new folder. A flat directory is its default
// tenant's folder, so there this is Reload.
func (r *Registry) ReloadTenant(id tenant.ID, trigger string) (findings []Finding, adopted, known bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev := *r.tenants.Load()
	e, known := prev[id]
	if !known {
		return nil, false, false
	}
	if !r.nested {
		findings, adopted, _ = r.reload(trigger, false)
		return findings, adopted, true
	}

	doc, findings := validateFolder(r.dir, id.String())
	next := maps.Clone(prev)
	next[id] = e.adopt(id, doc)
	r.tenants.Store(&next)
	var adoptedIDs []tenant.ID
	if doc != nil {
		adoptedIDs = []tenant.ID{id}
	}
	r.adopted(adoptedIDs)
	errs, warns := logFindings(trigger, findings)
	if doc == nil {
		slog.Error("settings rejected — the tenant answers 503 until a reload adopts its folder", "trigger", trigger, "dir", r.dir, "tenant", id, "errors", errs, "warnings", warns)
	} else {
		slog.Info("settings adopted", "trigger", trigger, "dir", r.dir, "tenant", id, "warnings", warns)
	}
	return findings, doc != nil, true
}

// adopted runs the AfterAdopt hooks for an applied reload that adopted ids —
// none, when it only rejected or removed. Under mu.
func (r *Registry) adopted(ids []tenant.ID) {
	for _, fn := range r.afterAdopt {
		fn(ids)
	}
}

// logFindings logs each finding under its trigger and counts them by severity.
func logFindings(trigger string, findings []Finding) (errs, warns int) {
	for _, f := range findings {
		if f.Severity == SeverityError {
			errs++
			slog.Error("settings finding", "trigger", trigger, "finding", f.String())
		} else {
			warns++
			slog.Warn("settings finding", "trigger", trigger, "finding", f.String())
		}
	}
	return errs, warns
}

// AfterAdopt registers fn to run after each subsequent reload the registry
// applied, serialized with the reload itself, with the tenants it adopted:
// none for a reload that only rejected or removed a tenant, which a consumer
// holding a resource per tenant needs to hear of as much as an adoption. A
// reload rejected whole runs nothing, since nothing changed. Open's boot
// adoption has already happened by the time a caller can register, so the
// caller applies the boot state itself.
func (r *Registry) AfterAdopt(fn func(adopted []tenant.ID)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.afterAdopt = append(r.afterAdopt, fn)
}
