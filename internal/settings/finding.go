package settings

import "strings"

// Severity classifies a finding. Errors make the directory invalid; warnings
// flag config that is legal but almost certainly not what the author meant
// (e.g. a grant scoping the admin role, which is an unconditional bypass).
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// Finding is one validation result, located as precisely as the failure
// allows: File is empty for directory-level findings, Path is a dotted JSON
// path ("tables.clicks.analyst") and empty for whole-file findings.
// The JSON shape is part of the ops API: POST /v1/ops/settings/reload returns
// findings verbatim.
type Finding struct {
	Severity Severity `json:"severity"`
	File     string   `json:"file,omitempty"`
	Path     string   `json:"path,omitempty"`
	Message  string   `json:"message"`
}

// String renders "error: policies.json: tables.clicks.admin: <message>",
// dropping the locators a finding doesn't have.
func (f Finding) String() string {
	parts := make([]string, 0, 3)
	if f.File != "" {
		parts = append(parts, f.File)
	}
	if f.Path != "" {
		parts = append(parts, f.Path)
	}
	parts = append(parts, f.Message)
	return string(f.Severity) + ": " + strings.Join(parts, ": ")
}

// HasErrors reports whether any finding is an error (warnings alone leave the
// directory valid).
func HasErrors(findings []Finding) bool {
	for _, f := range findings {
		if f.Severity == SeverityError {
			return true
		}
	}
	return false
}
