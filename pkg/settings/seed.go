package settings

import (
	"embed"
	"fmt"
	"io/fs"
)

// seedFS holds the starter settings directory: every file present, every
// key set to its default. The checked-in seed/ directory is the ONE place
// defaults live — the binary's one compiled fallback is dedupe.retention
// (missing means "0", forever). Its one consumer is this embed, so
// `wavehouse bootstrap` (WriteSeed, in internal/settings) can write the
// directory anywhere without a source tree; the container images ship no
// settings (the operator mounts or seeds /app/settings), same as they ship
// no policy file.
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
