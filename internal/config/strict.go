package config

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// rejectUnknownKeys re-reads the YAML as a generic tree and walks it against
// the Config struct's yaml tags, returning an error that lists every key the
// struct doesn't declare. cleanenv itself is lenient by design, which is
// exactly wrong for boot config once keys have moved to the settings
// directory: `dlq: enabled: false` left behind in a config.yaml would
// otherwise be read, ignored, and believed.
func rejectUnknownKeys(path string) error {
	data, err := os.ReadFile(path) //nolint:gosec // G304: the config path is operator-controlled by design
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var tree map[string]any
	if err := yaml.Unmarshal(data, &tree); err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	unknown := unknownKeys("", tree, reflect.TypeFor[Config]())
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("unknown key(s) in %s: %s — boot config only holds what the Config struct declares; behavioral tunables live in the settings directory (%s)", path, strings.Join(unknown, ", "), EnvSettingsDir)
}

// unknownKeys returns the dotted paths in tree that have no matching yaml tag
// in t, recursing into nested structs.
func unknownKeys(prefix string, tree map[string]any, t reflect.Type) []string {
	fields := map[string]reflect.Type{}
	for i := range t.NumField() {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		fields[tag] = f.Type
	}
	var out []string
	for key, val := range tree {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		ft, ok := fields[key]
		if !ok {
			out = append(out, path)
			continue
		}
		sub, isMap := val.(map[string]any)
		if isMap && ft.Kind() == reflect.Struct {
			out = append(out, unknownKeys(path, sub, ft)...)
		}
	}
	return out
}
