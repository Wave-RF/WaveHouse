package settings

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Tree is a validated settings root (#583). A flat root — the four files in
// the root itself — is tenant.Default alone. A nested root holds one folder
// per tenant and nothing else, the folder name being the tenant id.
type Tree struct {
	Nested bool
	// Tenants holds every tenant the root defines. Doc is nil for a tenant
	// whose folder has an error finding.
	Tenants map[tenant.ID]TenantResult
}

// TenantResult is one tenant's share of a validation pass.
type TenantResult struct {
	Doc      *Document
	Findings []Finding
}

// Validate checks a settings root of either shape in one pass and returns
// every finding, the way ValidateDir does for one directory. The shape is
// read off the root: holding any of the four settings file names makes it
// flat, whatever else is there; otherwise holding a folder makes it nested;
// otherwise — empty, missing, unreadable — it is flat again, so ValidateDir
// names the problem exactly as it always has. Mixing the shapes needs no rule
// of its own: a flat root rejects a folder and a nested root rejects a loose
// file.
//
// A flat root's findings are ValidateDir's, untouched. A nested root's carry
// the tenant folder in File ("acme/policies.json"), and a folder whose name
// is not a tenant id is a finding against that folder alone. The Tree is nil
// when the finding is about the root itself — it cannot be listed, or a
// nested root holds a loose file — since no tenant can then be trusted to be
// what the root's author meant.
func Validate(root string) (*Tree, []Finding) {
	folders, loose, listed := listRoot(root)
	if len(folders) == 0 {
		doc, findings := ValidateDir(root)
		if !listed && doc == nil {
			return nil, findings
		}
		return &Tree{Tenants: map[tenant.ID]TenantResult{tenant.Default: {Doc: doc, Findings: findings}}}, findings
	}

	var findings []Finding
	for _, name := range loose {
		findings = append(findings, Finding{Severity: SeverityError, File: name, Message: "unexpected file — a nested settings directory holds only tenant folders, each named by its tenant id"})
	}
	tree := &Tree{Nested: true, Tenants: make(map[tenant.ID]TenantResult, len(folders))}
	for _, name := range folders {
		id, err := tenant.Parse(name)
		if err != nil {
			findings = append(findings, Finding{Severity: SeverityError, File: name, Message: fmt.Sprintf("folder name is not a tenant id: %v", err)})
			continue
		}
		doc, fs := ValidateDir(filepath.Join(root, name))
		for i := range fs {
			// path, not filepath: File is part of the ops API, one spelling on every OS.
			fs[i].File = path.Join(name, fs[i].File)
		}
		tree.Tenants[id] = TenantResult{Doc: doc, Findings: fs}
		findings = append(findings, fs...)
	}
	if len(loose) > 0 {
		return nil, findings
	}
	return tree, findings
}

// listRoot sorts the root's entries into tenant folders and loose files, in
// name order. It returns no folders for a flat root — one holding any of the
// four settings file names — and listed=false when the root cannot be read.
// Dot-prefixed entries are skipped, as ValidateDir skips them.
func listRoot(root string) (folders, loose []string, listed bool) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, false
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if slices.Contains(Files(), name) {
			return nil, nil, true
		}
		// Stat, not the entry's own type: a Kubernetes ConfigMap mount
		// publishes each folder as a symlink into its `..data` directory.
		if info, err := os.Stat(filepath.Join(root, name)); err == nil && info.IsDir() {
			folders = append(folders, name)
		} else {
			loose = append(loose, name)
		}
	}
	return folders, loose, true
}
