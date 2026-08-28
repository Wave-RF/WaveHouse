package settings

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// seedFS holds the starter settings directory: every file present, every
// key set to its default. The checked-in seed/ directory is the ONE place
// defaults live — the binary has no compiled fallbacks. Its one consumer is
// this embed, so `wavehouse bootstrap` can write the directory anywhere
// without a source tree; the container images ship no settings (the operator
// mounts or seeds /app/settings), same as they ship no policy file.
//
//go:embed seed/*.json
var seedFS embed.FS

// Seed returns the starter settings directory's files by name.
func Seed() (map[string][]byte, error) {
	out := make(map[string][]byte, len(Files()))
	for _, name := range Files() {
		data, err := fs.ReadFile(seedFS, "seed/"+name)
		if err != nil {
			return nil, fmt.Errorf("embedded seed %s: %w", name, err)
		}
		out[name] = data
	}
	return out, nil
}

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
	files, err := Seed()
	if err != nil {
		return err
	}
	for _, name := range Files() {
		if err := os.WriteFile(filepath.Join(dir, name), files[name], 0o644); err != nil { //nolint:gosec // G306: settings files hold no secrets and are read by the server user
			return err
		}
	}
	return nil
}
