package settings

import (
	"reflect"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/stretchr/testify/assert"
)

// The executable spec of the two-tier invariant: every runtime-settings leaf
// is absent from the cold config, and no runtime-settings field is visible to
// cleanenv. init() already panics on violation — refusing to even start a
// violating binary — this test states the same rule where reviewers look for
// it.
func TestColdAndHotConfigAreDisjoint(t *testing.T) {
	t.Parallel()

	hot := leafPaths(reflect.TypeFor[Values](), "json")
	cold := leafPaths(reflect.TypeFor[config.Config](), "yaml")
	assert.NotEmpty(t, hot)
	assert.NotEmpty(t, cold)
	for path := range hot {
		_, clash := cold[path]
		assert.False(t, clash,
			"%s is declared in both boot config and runtime settings — a knob lives in exactly one tier", path)
	}

	walkFields(reflect.TypeFor[Values](), func(f reflect.StructField) {
		for _, tag := range []string{"yaml", "env", "env-default"} {
			assert.Empty(t, f.Tag.Get(tag),
				"settings field %s carries a %q tag; runtime settings must stay invisible to cleanenv", f.Name, tag)
		}
	})
}

func TestMustBeDisjoint_CurrentTypesPass(t *testing.T) {
	t.Parallel()
	assert.NotPanics(t, mustBeDisjointFromColdConfig)
}

func TestLeafPaths(t *testing.T) {
	t.Parallel()

	type inner struct {
		A string `json:"a"`
		B int    `json:"b,omitempty"` // tag options are stripped
	}
	type fixture struct {
		Section inner  `json:"section"`
		Top     string `json:"top"`
		Skipped string `json:"-"`
		NoTag   string // falls back to the lowercased field name
	}

	got := leafPaths(reflect.TypeFor[fixture](), "json")
	assert.Equal(t, map[string]struct{}{
		"section.a": {},
		"section.b": {},
		"top":       {},
		"notag":     {},
	}, got)
}
