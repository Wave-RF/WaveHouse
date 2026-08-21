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

// runInitSettings implements `wavehouse init-settings <dir>`: write the
// starter settings directory — all four files, every key at its default —
// so an operator has a complete, valid directory to point WH_SETTINGS_DIR at
// and edit from. The server never does this on its own: a missing directory
// at boot is a refused start, not an invitation to invent one (the
// `initdb` contract). Refuses a non-empty directory. Exit codes: 0 written,
// 1 failed, 2 usage.
func runInitSettings(args []string) int {
	fs := flag.NewFlagSet("init-settings", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), `usage: wavehouse init-settings <dir>

Write a starter settings directory (%s) with every key at its
default into dir, creating it if needed. Refuses a non-empty directory. Point
%s at the result (or pass it to 'wavehouse validate').

Exit codes: 0 written, 1 failed, 2 usage.
`, strings.Join(settings.Files(), ", "), config.EnvSettingsDir)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "wavehouse init-settings: expected exactly one directory argument, got %d\n", fs.NArg())
		fs.Usage()
		return 2
	}
	dir := fs.Arg(0)
	if err := settings.WriteSeed(dir); err != nil {
		fmt.Fprintf(os.Stderr, "wavehouse init-settings: %v\n", err)
		return 1
	}
	fmt.Printf("wrote %s to %s\n", strings.Join(settings.Files(), ", "), dir)
	return 0
}
