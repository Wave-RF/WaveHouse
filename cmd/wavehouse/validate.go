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

// runValidate implements `wavehouse validate [dir]`: validate a settings
// directory (roles.json, policies.json, pipes.json, config.json) without
// starting the server, so an operator or CI can gate a config change before it
// reaches a running instance. The directory comes from the argument, falling
// back to WH_SETTINGS_DIR. Exit codes: 0 valid (warnings allowed), 1 invalid,
// 2 usage.
func runValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), `usage: wavehouse validate [dir]

Validate a settings directory (%s) without
starting the server. With no dir argument, the directory comes from %s.

Exit codes: 0 valid (warnings allowed), 1 invalid, 2 usage.
`, strings.Join(settings.Files(), ", "), config.EnvSettingsDir)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2 // fs already printed the flag error and usage
	}
	if fs.NArg() > 1 {
		fmt.Fprintf(os.Stderr, "wavehouse validate: expected at most one directory argument, got %d\n", fs.NArg())
		fs.Usage()
		return 2
	}
	dir := os.Getenv(config.EnvSettingsDir)
	if fs.NArg() == 1 {
		dir = fs.Arg(0)
	}
	if dir == "" {
		fmt.Fprintf(os.Stderr, "wavehouse validate: no directory given and %s is not set\n", config.EnvSettingsDir)
		fs.Usage()
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
