package main

import (
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/pkg/settings"
)

// runValidate implements `wavehouse validate [dir]`: validate a settings
// directory (roles.json, policies.json, pipes.json, config.json — or one
// folder per tenant, each holding those four) without starting the server, so
// an operator or CI can gate a config change before it reaches a running
// instance. The directory comes from the argument, falling back to
// WH_SETTINGS_DIR. Exit codes: 0 valid (warnings allowed), 1 invalid, 2 usage.
func runValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), `usage: wavehouse validate [dir]

Validate a settings directory (%s, or one
folder per tenant, each holding those files) without starting the server.
With no dir argument, the directory comes from %s.

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

	tree, findings := settings.Validate(dir)
	var errs, warns int
	configErrs := false
	for _, f := range findings {
		fmt.Println(f)
		if f.Severity == settings.SeverityError {
			errs++
			configErrs = configErrs || path.Base(f.File) == settings.FileConfig
		} else {
			warns++
		}
	}
	printClaims(tree)
	// The findings say what is wrong; the fix is this binary's to suggest.
	if configErrs {
		fmt.Println("hint: `wavehouse bootstrap <dir>` writes a complete starter config.json to compare against")
	}
	if errs > 0 {
		fmt.Printf("invalid: %d error(s), %d warning(s) in %s\n", errs, warns, dir)
		return 1
	}
	fmt.Printf("valid: %s (%d warning(s))\n", dir, warns)
	return 0
}

// printClaims lists, under the findings, the token claims each pipe binds
// (`pipes.json: pipes[0] "daily_totals" binds token claims jwt.sub`), one line
// per claim-scoped pipe, its file named as a finding names it (#938). A
// reviewer of a pipes.json needs to see that a pipe is scoped by the caller's
// token; it is a fact about the file, not a finding, so it counts as neither
// an error nor a warning, and boot and a reload do not report it.
func printClaims(tree *settings.Tree) {
	if tree == nil {
		return
	}
	for _, id := range slices.Sorted(maps.Keys(tree.Tenants)) {
		doc := tree.Tenants[id].Doc
		if doc == nil {
			continue
		}
		file := settings.FilePipes
		if tree.Nested {
			// path, not filepath: a finding's File spells the folder this way on every OS.
			file = path.Join(id.String(), file)
		}
		for i := range doc.Pipes {
			q := &doc.Pipes[i]
			claims := q.Claims()
			if len(claims) == 0 {
				continue
			}
			refs := make([]string, len(claims))
			for k, c := range claims {
				refs[k] = "jwt." + c
			}
			fmt.Printf("%s: pipes[%d] %q binds token claims %s\n", file, i, q.Name, strings.Join(refs, ", "))
		}
	}
}
