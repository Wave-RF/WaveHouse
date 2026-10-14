package settings

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Wave-RF/WaveHouse/pkg/settings"
)

// WriteSeed materializes the starter directory at dir, creating it if
// needed. It refuses a non-empty directory (the `initdb` contract): an
// existing settings directory is someone's config, never something to
// overwrite.
func WriteSeed(dir string) error {
	// World-readable on purpose: the operator who runs bootstrap is
	// routinely not the user the server runs as (root on the host, UID 65532
	// in the container), and the server only needs to read it.
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // G301: config directory the server reads as another user
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("%s is not empty — refusing to overwrite an existing settings directory", dir)
	}
	files, err := settings.Seed()
	if err != nil {
		return err
	}
	for _, name := range settings.Files() {
		if err := os.WriteFile(filepath.Join(dir, name), files[name], 0o644); err != nil { //nolint:gosec // G306: the seed's secret keys are empty, and the server runs as another user in the container; the operator tightens the mode when filling them in (deployment docs)
			return err
		}
	}
	return nil
}
