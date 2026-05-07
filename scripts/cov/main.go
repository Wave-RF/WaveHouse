// Coverage rendering + threshold gating for WaveHouse.
//
// Subcommands:
//
//	cov render <suite>    Render covdata under tmp/coverage/<suite>/data
//	                      to text + HTML + a per-package breakdown, then
//	                      gate against the suite's threshold.
//	cov merge             Merge whichever Go suites have covdata into
//	                      tmp/coverage/total/, render, gate against
//	                      threshold.total.
//	cov threshold <suite> Print the configured threshold for <suite>
//	                      (or "total"). Used by the SDK pipeline to
//	                      pass into vitest's --coverage.thresholds.
//
// All thresholds come from .testcoverage.yml — `threshold.total` is the
// canonical merged-coverage gate (the same field go-test-coverage reads
// natively); per-suite gates live under `suites:`. There is no env-var
// override path: edit the YAML.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	configPath = ".testcoverage.yml"
	root       = "tmp/coverage"
)

// Suites whose covdata participates in the merged total. SDK coverage
// is rendered separately by vitest (different toolchain, lcov format)
// and surfaced in the merge output as informational only.
var goSuites = []string{"unit", "integration", "e2e"}

type config struct {
	LocalPrefix string `yaml:"local-prefix"`
	Threshold   struct {
		Total int `yaml:"total"`
	} `yaml:"threshold"`
	Suites  map[string]int `yaml:"suites"`
	Exclude struct {
		Paths []string `yaml:"paths"`
	} `yaml:"exclude"`
}

const (
	cyan   = "\x1b[36m"
	green  = "\x1b[32m"
	yellow = "\x1b[33m"
	red    = "\x1b[31m"
	reset  = "\x1b[0m"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cfg, err := loadConfig()
	if err != nil {
		fatal("%v", err)
	}
	switch os.Args[1] {
	case "render":
		if len(os.Args) < 3 {
			usage()
		}
		if err := renderSuite(cfg, os.Args[2]); err != nil {
			fatal("%v", err)
		}
	case "merge":
		if err := merge(cfg); err != nil {
			fatal("%v", err)
		}
	case "threshold":
		if len(os.Args) < 3 {
			usage()
		}
		fmt.Println(thresholdFor(cfg, os.Args[2]))
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: cov render <suite> | merge | threshold <suite>")
	os.Exit(2)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "cov: "+format+"\n", args...)
	os.Exit(1)
}

func loadConfig() (*config, error) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", configPath, err)
	}
	var c config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", configPath, err)
	}
	return &c, nil
}

// thresholdFor resolves the gate for a name. "total" is the merged-coverage
// gate (threshold.total); everything else is a per-suite gate (suites.<name>).
func thresholdFor(c *config, suite string) int {
	if suite == "total" {
		return c.Threshold.Total
	}
	if v, ok := c.Suites[suite]; ok {
		return v
	}
	fatal("no threshold for %q in %s — add it under suites: (or threshold.total)", suite, configPath)
	return 0
}

func renderSuite(c *config, suite string) error {
	dir := filepath.Join(root, suite)
	dataDir := filepath.Join(dir, "data")
	if !hasCovdata(dataDir) {
		return fmt.Errorf("no covdata in %s — did make test-%s run?", dataDir, suite)
	}
	profile := filepath.Join(dir, "coverage.txt")
	htmlOut := filepath.Join(dir, "coverage.html")
	if err := sh("go", "tool", "covdata", "textfmt", "-i="+dataDir, "-o", profile); err != nil {
		return err
	}
	if err := sh("go", "tool", "cover", "-html="+profile, "-o", htmlOut); err != nil {
		return err
	}
	threshold := thresholdFor(c, suite)
	fmt.Printf("\n%s==> %s coverage: %s%s%s  (threshold: %d%%)%s\n\n",
		cyan, suite, yellow, totalPct(profile), reset, threshold, reset)
	printBreakdown(profile, c, threshold)
	fmt.Printf("\n  HTML: %s\n\n", htmlOut)

	if err := gate(profile, threshold); err != nil {
		fmt.Fprintf(os.Stderr, "%s==> %s gate FAILED (below %d%%)%s\n", red, suite, threshold, reset)
		return err
	}
	fmt.Printf("%s==> %s gate passed (≥ %d%%)%s\n", green, suite, threshold, reset)
	return nil
}

func merge(c *config) error {
	var dirs []string
	fmt.Printf("%s==> Merging coverage data...%s\n", cyan, reset)
	for _, s := range goSuites {
		d := filepath.Join(root, s, "data")
		if hasCovdata(d) {
			dirs = append(dirs, d)
			fmt.Printf("  %s✔%s %-13s %s\n", green, reset, s, d)
		} else {
			hint := "test-" + s
			if s == "unit" {
				hint = "test"
			}
			fmt.Printf("  %s✗%s %-13s (no covdata; run `make %s` to include)\n", yellow, reset, s, hint)
		}
	}
	if len(dirs) == 0 {
		return fmt.Errorf("no coverage data; run make test-all first")
	}

	totalDir := filepath.Join(root, "total")
	if err := os.MkdirAll(totalDir, 0o750); err != nil {
		return err
	}
	profile := filepath.Join(totalDir, "coverage.txt")
	htmlOut := filepath.Join(totalDir, "coverage.html")
	if err := sh("go", "tool", "covdata", "textfmt", "-i="+strings.Join(dirs, ","), "-o", profile); err != nil {
		return err
	}
	if err := sh("go", "tool", "cover", "-html="+profile, "-o", htmlOut); err != nil {
		return err
	}

	fmt.Printf("\n%s==> Per-suite breakdown:%s\n", cyan, reset)
	for _, s := range goSuites {
		fmt.Printf("  %s%-13s%s %s\n", cyan, s+":", reset, suitePct(s))
	}
	if pct := readSDKPct(); pct != "" {
		fmt.Printf("  %s%-13s%s %s%%  %s(separate gate; not in merge above)%s\n",
			cyan, "sdk:", reset, pct, yellow, reset)
	}

	threshold := c.Threshold.Total
	fmt.Println()
	printBreakdown(profile, c, threshold)
	fmt.Printf("\n  HTML: %s\n\n", htmlOut)
	fmt.Printf("%s==> Combined coverage gate (threshold %d%%):%s\n", cyan, threshold, reset)

	if err := gate(profile, threshold); err != nil {
		fmt.Fprintf(os.Stderr, "%s==> Total gate FAILED%s\n", red, reset)
		return err
	}
	fmt.Printf("%s==> Total gate passed%s\n", green, reset)
	return nil
}

// printBreakdown emits a per-package coverage table for `profile`, sorted
// ascending so the least-covered packages land at the top. Honors the
// YAML `local-prefix` (strips the module path for readability) and
// `exclude.paths` regexes (so the table matches what the gate evaluates).
func printBreakdown(profile string, c *config, threshold int) {
	type row struct {
		pkg            string
		total, covered int
	}
	pkgs := map[string]*row{}
	excludes := compileRegexes(c.Exclude.Paths)

	// #nosec G304,G703 — profile is filepath.Join(root, <suite>, "coverage.txt"),
	// rendered by this same tool. Not user input.
	raw, err := os.ReadFile(profile)
	if err != nil {
		return
	}
	for i, line := range strings.Split(string(raw), "\n") {
		if i == 0 || line == "" {
			continue // mode line / blank
		}
		// Profile line: <path>:<start>.<col>,<end>.<col> <stmts> <count>
		f := strings.Fields(line)
		if len(f) != 3 {
			continue
		}
		colon := strings.IndexByte(f[0], ':')
		if colon < 0 {
			continue
		}
		path := f[0][:colon]
		slash := strings.LastIndexByte(path, '/')
		if slash < 0 {
			continue
		}
		pkg := path[:slash]
		if c.LocalPrefix != "" && strings.HasPrefix(pkg, c.LocalPrefix+"/") {
			pkg = pkg[len(c.LocalPrefix)+1:]
		}
		// Match exclusions against pkg+"/" so a pattern like
		// ^internal/testutil/ matches a bare-directory pkg string.
		if matchesAny(excludes, pkg+"/") {
			continue
		}
		stmts, _ := strconv.Atoi(f[1])
		count, _ := strconv.Atoi(f[2])
		r, ok := pkgs[pkg]
		if !ok {
			r = &row{pkg: pkg}
			pkgs[pkg] = r
		}
		r.total += stmts
		if count > 0 {
			r.covered += stmts
		}
	}

	rows := make([]row, 0, len(pkgs))
	for _, r := range pkgs {
		if r.total > 0 {
			rows = append(rows, *r)
		}
	}
	if len(rows) == 0 {
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		return float64(rows[i].covered)/float64(rows[i].total) <
			float64(rows[j].covered)/float64(rows[j].total)
	})

	const limit = 15
	hidden := 0
	if len(rows) > limit {
		hidden = len(rows) - limit
		rows = rows[:limit]
	}

	fmt.Printf("  %-50s %10s\n", "Package", "Coverage")
	fmt.Printf("  %s %s\n", strings.Repeat("─", 50), strings.Repeat("─", 10))
	for _, r := range rows {
		pct := float64(r.covered) * 100.0 / float64(r.total)
		color, icon := green, "✓"
		switch {
		case pct < 50:
			color, icon = red, "✗"
		case pct < float64(threshold):
			color, icon = yellow, "⚠"
		}
		fmt.Printf("  %-50s %s%9.1f%%%s  %s%s%s\n", r.pkg, color, pct, reset, color, icon, reset)
	}
	if hidden > 0 {
		fmt.Printf("  %s… %d more (see HTML for full breakdown)%s\n", cyan, hidden, reset)
	}
}

// readSDKPct returns the SDK's top-level statements.pct from vitest's
// coverage-summary.json, or "" if the file is missing. We pluck a single
// field with a regex rather than parsing the full JSON because vitest
// emits the entire summary as one giant single-line record.
func readSDKPct() string {
	raw, err := os.ReadFile(filepath.Join(root, "sdk", "coverage-summary.json"))
	if err != nil {
		return ""
	}
	m := regexp.MustCompile(`"statements":\{[^}]*"pct":([0-9.]+)`).FindSubmatch(raw)
	if len(m) > 1 {
		return string(m[1])
	}
	return ""
}

// suitePct returns "go tool cover -func" total for a suite, or "n/a" if
// covdata is missing. Uses an existing coverage.txt when present, else
// renders a throwaway one (so `make cov` shows per-suite numbers without
// requiring you to have run `make test-<suite>` separately).
func suitePct(suite string) string {
	d := filepath.Join(root, suite, "data")
	if !hasCovdata(d) {
		return "n/a"
	}
	profile := filepath.Join(root, suite, "coverage.txt")
	if _, err := os.Stat(profile); err != nil {
		tmp, err := os.CreateTemp("", "cov-*.txt")
		if err != nil {
			return "?"
		}
		_ = tmp.Close()
		defer func() { _ = os.Remove(tmp.Name()) }()
		// #nosec G204,G702 — all args are constants or paths we computed
		// (suite covdata dir + a temp file we just created).
		if err := exec.CommandContext(context.Background(), "go", "tool", "covdata", "textfmt", "-i="+d, "-o", tmp.Name()).Run(); err != nil {
			return "?"
		}
		profile = tmp.Name()
	}
	return totalPct(profile)
}

// totalPct extracts the trailing total percentage from `go tool cover -func`.
func totalPct(profile string) string {
	// #nosec G204,G702 — profile is a coverage textfmt path we rendered ourselves.
	out, err := exec.CommandContext(context.Background(), "go", "tool", "cover", "-func="+profile).Output()
	if err != nil {
		return "?"
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 0 {
		return "?"
	}
	f := strings.Fields(lines[len(lines)-1])
	if len(f) == 0 {
		return "?"
	}
	return f[len(f)-1]
}

// gate runs go-test-coverage with the given total threshold. Exclusions
// (testutil, tests/) come from the same .testcoverage.yml.
func gate(profile string, threshold int) error {
	return sh("go", "tool", "go-test-coverage",
		"--config="+configPath,
		"--profile="+profile,
		"--threshold-total="+strconv.Itoa(threshold),
	)
}

// sh runs an external command with stdio wired through. Every call site
// passes "go" as the program and a fixed series of "tool", "<tool-name>",
// flag, … args; the only variable bits are paths we computed ourselves.
// #nosec G204,G702 — name and args are not user input.
func sh(name string, args ...string) error {
	cmd := exec.CommandContext(context.Background(), name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func hasCovdata(dir string) bool {
	matches, _ := filepath.Glob(filepath.Join(dir, "covmeta.*"))
	return len(matches) > 0
}

func compileRegexes(patterns []string) []*regexp.Regexp {
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		if p == "" {
			continue
		}
		if r, err := regexp.Compile(p); err == nil {
			out = append(out, r)
		}
	}
	return out
}

func matchesAny(rs []*regexp.Regexp, s string) bool {
	for _, r := range rs {
		if r.MatchString(s) {
			return true
		}
	}
	return false
}
