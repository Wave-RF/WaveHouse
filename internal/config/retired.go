package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// retiredKeys are former config keys that moved to the runtime-settings store
// (internal/settings; changed via the admin API). cleanenv ignores unknown
// yaml keys and stale env vars silently — so without this check, an operator
// upgrading with `dedupe.require_id: true` still set would have their
// tripwire silently revert to the default. Loading refuses to start instead,
// with a pointer to the new home (same fail-loud posture as a bad
// policy.file_path).
var retiredKeys = []struct{ yamlPath, envVar, movedTo string }{
	{"dedupe.id_field", "WH_DEDUPE_ID_FIELD", "PUT /v1/admin/settings/dedupe"},
	{"dedupe.require_id", "WH_DEDUPE_REQUIRE_ID", "PUT /v1/admin/settings/dedupe"},
}

// checkRetiredKeys errors if any retired key is still present in the
// environment or, when a config file was read, in the file. rawFile is the
// file's bytes (nil/empty when no file was loaded); it is re-parsed leniently
// — this check never introduces new parse failures, cleanenv already
// surfaced those.
func checkRetiredKeys(rawFile []byte) error {
	for _, k := range retiredKeys {
		if _, set := os.LookupEnv(k.envVar); set {
			return fmt.Errorf("%s has moved to the runtime-settings store: unset it and use %s instead (docs: configuration → Runtime settings)", k.envVar, k.movedTo)
		}
	}
	if len(rawFile) == 0 {
		return nil
	}
	var doc map[string]any
	if yaml.Unmarshal(rawFile, &doc) != nil {
		return nil // parse errors are cleanenv's to report, not this check's
	}
	for _, k := range retiredKeys {
		if yamlHas(doc, k.yamlPath) {
			return fmt.Errorf("config key %q has moved to the runtime-settings store: remove it from the file and use %s instead (docs: configuration → Runtime settings)", k.yamlPath, k.movedTo)
		}
	}
	return nil
}

// yamlHas reports whether the dotted path exists in the parsed document.
func yamlHas(doc map[string]any, dotted string) bool {
	cur := any(doc)
	for part := range strings.SplitSeq(dotted, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		if cur, ok = m[part]; !ok {
			return false
		}
	}
	return true
}
