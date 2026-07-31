package settings

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/config"
)

// The two configuration tiers are disjoint: a knob is declared either in the
// cold boot config (internal/config — yaml + WH_* env, restart to apply) or
// here (the control db via the admin API, applied live), never both. That makes
// update semantics structural — where a knob lives is its contract — and
// makes shadowing (an env var pinning a live-tunable value) impossible.
//
// Enforced at init so a binary violating it refuses to start, not just fail a
// CI test: any import of this package (the server, the API layer, any test)
// trips it. disjoint_test.go re-runs the same walk as an executable spec. The
// operator-facing half of the invariant lives in config.Load, which refuses
// retired keys still present in the file or environment.
func init() { mustBeDisjointFromColdConfig() }

func mustBeDisjointFromColdConfig() {
	hot := leafPaths(reflect.TypeFor[Values](), "json")
	cold := leafPaths(reflect.TypeFor[config.Config](), "yaml")
	for path := range hot {
		if _, clash := cold[path]; clash {
			panic(fmt.Sprintf("settings: %q is declared in both boot config (internal/config) and runtime settings (internal/settings); a knob lives in exactly one tier", path))
		}
	}

	// Runtime settings must be invisible to cleanenv: no yaml/env tags, ever.
	// Without this, re-adding an env tag would quietly resurrect the
	// env-shadows-a-live-value failure mode the tier split exists to kill.
	walkFields(reflect.TypeFor[Values](), func(f reflect.StructField) {
		for _, tag := range []string{"yaml", "env", "env-default"} {
			if f.Tag.Get(tag) != "" {
				panic(fmt.Sprintf("settings: field %s carries a %q struct tag; runtime settings are never read from the config file or environment", f.Name, tag))
			}
		}
	})
}

// leafPaths flattens a config struct into dotted key paths ("dedupe.id_field"),
// naming each level by the given struct tag and falling back to the lowercased
// field name (cleanenv's own fallback) when untagged.
func leafPaths(t reflect.Type, tagKey string) map[string]struct{} {
	out := map[string]struct{}{}
	var walk func(t reflect.Type, prefix string)
	walk = func(t reflect.Type, prefix string) {
		for f := range t.Fields() {
			if f.PkgPath != "" {
				continue // unexported
			}
			name, _, _ := strings.Cut(f.Tag.Get(tagKey), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = strings.ToLower(f.Name)
			}
			path := name
			if prefix != "" {
				path = prefix + "." + name
			}
			if f.Type.Kind() == reflect.Struct {
				walk(f.Type, path)
				continue
			}
			out[path] = struct{}{}
		}
	}
	walk(t, "")
	return out
}

// walkFields visits every exported field reachable from t, depth-first.
func walkFields(t reflect.Type, fn func(reflect.StructField)) {
	for f := range t.Fields() {
		if f.PkgPath != "" {
			continue
		}
		fn(f)
		if f.Type.Kind() == reflect.Struct {
			walkFields(f.Type, fn)
		}
	}
}
