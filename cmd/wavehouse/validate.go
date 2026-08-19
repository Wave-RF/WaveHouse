package main

import (
	"fmt"
	"os"

	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// runValidate implements `wavehouse validate [dir]`: validate a settings
// directory (roles.json, policies.json, pipes.json, config.json) without
// starting the server, so an operator or CI can gate a config change before it
// reaches a running instance. The directory comes from the argument, falling
// back to WH_SETTINGS_DIR. Exit codes: 0 valid (warnings allowed), 1 invalid,
// 2 usage.
func runValidate(args []string) int {
	if len(args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: wavehouse validate [dir]")
		return 2
	}
	dir := os.Getenv(settings.EnvDir)
	if len(args) == 1 {
		dir = args[0]
	}
	if dir == "" {
		fmt.Fprintf(os.Stderr, "usage: wavehouse validate [dir] (or set %s)\n", settings.EnvDir)
		return 2
	}

	findings := settings.Validate(dir)
	var errs, warns int
	for _, f := range findings {
		fmt.Println(f)
		if f.Severity == settings.SeverityError {
			errs++
		} else {
			warns++
		}
	}
	if errs > 0 {
		fmt.Printf("invalid: %d error(s), %d warning(s) in %s\n", errs, warns, dir)
		return 1
	}
	fmt.Printf("valid: %s (%d warning(s))\n", dir, warns)
	return 0
}
