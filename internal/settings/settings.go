package settings

import "fmt"

// Values is the full runtime-settings document: one struct per section, one
// control-db table per section (so a section's fields update atomically).
// JSON tags only — these fields must stay invisible to cleanenv (enforced in
// disjoint.go).
type Values struct {
	Dedupe Dedupe `json:"dedupe"`
}

// Dedupe holds the live-tunable dedupe knobs. The dedupe.enabled switch is
// deliberately NOT here: it owns the Pebble store lifecycle, which makes it
// boot config (see internal/config).
type Dedupe struct {
	IDField   string `json:"id_field"`
	RequireID bool   `json:"require_id"`
}

// Defaults returns the compiled defaults — the effective values whenever no
// override row is stored in the control db. Keep every default a safe, conservative choice:
// a deployment that never touches the settings API must behave correctly.
func Defaults() Values {
	return Values{Dedupe: Dedupe{IDField: "event_id", RequireID: false}}
}

// Validate checks a candidate Values document. It runs on every path that
// takes values into the cache — admin PUT and boot load — so an invalid
// document can never become the live snapshot (and the dedupe_settings CHECK
// constraints repeat the same rules in storage).
func (v Values) Validate() error {
	if v.Dedupe.RequireID && v.Dedupe.IDField == "" {
		return fmt.Errorf("dedupe.require_id requires a non-empty dedupe.id_field")
	}
	return nil
}
