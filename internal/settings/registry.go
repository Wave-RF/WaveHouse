package settings

import (
	"log/slog"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Registry maps a tenant id to the Store holding that tenant's adopted
// settings, and owns everything that changes one. Open validates the
// directory and adopts it up front, so a *Registry never exists without a
// good document behind it. Reload is the single code path every later
// trigger — SIGHUP, the directory watcher, and POST /v1/ops/settings/reload —
// funnels through: re-validate the directory, and swap the snapshot only when
// no finding is an error, so a bad edit (or a deleted file, or a vanished
// directory) can never evict the last good document. The stores are passive:
// a document pointer their getters read.
//
// It holds exactly one store, under tenant.Default: the settings directory is
// one tenant's four files.
type Registry struct {
	dir string

	// mu serializes Reload: concurrent triggers queue rather than racing
	// validate-then-swap sequences (a stale document must not overwrite a newer one).
	mu sync.Mutex
	// afterAdopt runs under mu after every successful swap, in registration
	// order — for consumers that own a resource whose lifecycle follows a
	// setting (the Pebble store behind dedupe.enabled) rather than reading
	// the snapshot per call.
	afterAdopt []func(adopted []tenant.ID)

	stores map[tenant.ID]*Store
}

// Open validates dir and returns a Registry holding its document. A rejected
// directory returns a nil Registry with the findings — the caller (boot)
// refuses to start; it must never run without adopted settings.
func Open(dir string) (*Registry, []Finding) {
	r := NewRegistry(&Store{})
	r.dir = dir
	findings, adopted := r.Reload("boot")
	if !adopted {
		return nil, findings
	}
	return r, findings
}

// NewRegistry returns a Registry serving store as tenant.Default, with no
// directory behind it: what a test that fixes its settings holds.
func NewRegistry(store *Store) *Registry {
	return &Registry{stores: map[tenant.ID]*Store{tenant.Default: store}}
}

// Dir returns the directory this registry reads.
func (r *Registry) Dir() string { return r.dir }

// For returns the store of tenant id, or false when no such tenant exists.
func (r *Registry) For(id tenant.ID) (*Store, bool) {
	s, ok := r.stores[id]
	return s, ok
}

// Reload re-validates the directory and adopts the parsed document when no
// finding is an error (warnings don't block adoption, matching `wavehouse
// validate`). On a rejected reload the previous snapshot stays in place.
// The returned bool reports whether the document was adopted. trigger names
// the path that fired ("boot", "sighup", "watch", "api") and tags every log
// line so operators can tell them apart.
func (r *Registry) Reload(trigger string) ([]Finding, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tree, findings := Validate(r.dir)
	if tree != nil && tree.Nested {
		findings = append(findings, Finding{Severity: SeverityError, Message: "one folder per tenant is not served yet — the settings directory must hold the four files itself"})
		tree = nil
	}
	store := r.stores[tenant.Default]
	var doc *Document
	if tree != nil {
		doc = tree.Tenants[tenant.Default].Doc
	}
	adopted := doc != nil
	if adopted {
		store.adopt(doc)
		for _, fn := range r.afterAdopt {
			fn([]tenant.ID{tenant.Default})
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
	case adopted:
		slog.Info("settings adopted", "trigger", trigger, "dir", r.dir, "warnings", warns)
	case store.doc() == nil:
		slog.Error("settings rejected", "trigger", trigger, "dir", r.dir, "errors", errs, "warnings", warns)
	default:
		slog.Error("settings rejected — keeping previous settings", "trigger", trigger, "dir", r.dir, "errors", errs, "warnings", warns)
	}
	return findings, adopted
}

// AfterAdopt registers fn to run after each subsequent successful reload,
// serialized with the reload itself, with the tenants that reload adopted.
// Open's boot adoption has already happened by the time a caller can
// register, so the caller applies the boot state itself.
func (r *Registry) AfterAdopt(fn func(adopted []tenant.ID)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.afterAdopt = append(r.afterAdopt, fn)
}
