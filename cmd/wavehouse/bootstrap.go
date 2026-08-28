package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// runBootstrap implements `wavehouse bootstrap [dir]`: write the
// starter settings directory — all four files, every key at its default —
// so an operator has a complete, valid directory to point WH_SETTINGS_DIR at
// and edit from. The directory resolves exactly as it does for `wavehouse
// validate`: the argument, falling back to WH_SETTINGS_DIR, and a usage
// error with neither — so the two commands are interchangeable on the same
// path (in the container images the env var is preset, so a bare
// `bootstrap` seeds /app/settings). The server never does this on its
// own: a missing directory at boot is a refused start, not an invitation to
// invent one (the `initdb` contract). Refuses a non-empty directory. Exit
// codes: 0 written, 1 failed, 2 usage.
func runBootstrap(args []string) int {
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), `usage: wavehouse bootstrap [dir]

Write a starter settings directory (%s) with every key at its
default into dir, creating it if needed. With no dir argument, the directory
comes from %s — the same resolution as 'wavehouse validate'. Refuses a
non-empty directory.

Exit codes: 0 written, 1 failed, 2 usage.
`, strings.Join(settings.Files(), ", "), config.EnvSettingsDir)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 1 {
		fmt.Fprintf(os.Stderr, "wavehouse bootstrap: expected at most one directory argument, got %d\n", fs.NArg())
		fs.Usage()
		return 2
	}
	dir := os.Getenv(config.EnvSettingsDir)
	if fs.NArg() == 1 {
		dir = fs.Arg(0)
	}
	if dir == "" {
		fmt.Fprintf(os.Stderr, "wavehouse bootstrap: no directory given and %s is not set\n", config.EnvSettingsDir)
		fs.Usage()
		return 2
	}
	if err := settings.WriteSeed(dir); err != nil {

		fmt.Fprintf(os.Stderr, "wavehouse bootstrap: %v\n", err)
		return 1
	}
	fmt.Printf("wrote %s to %s\n", strings.Join(settings.Files(), ", "), dir)
	return 0
}
