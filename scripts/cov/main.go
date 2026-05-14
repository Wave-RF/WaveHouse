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
		// Paths applies to every per-suite render AND to the merged total.
		// Use for paths that aren't meaningful coverage anywhere (e.g.,
		// internal/testutil/, tests/, scripts/).
		Paths []string `yaml:"paths"`
		// PerSuite applies on top of Paths but only when rendering the
		// named suite's profile — not when computing the merged total.
		// Use for files that legitimately can't be covered by one suite
		// (e.g., cmd/wavehouse/main.go is untestable by unit but exercised
		// by e2e); excluding from `unit` keeps that suite's gate clean
		// without hiding the file's e2e-derived coverage from the merged
		// total.
		PerSuite map[string][]string `yaml:"per-suite"`
	} `yaml:"exclude"`
}

// excludesFor returns the regex patterns that apply when rendering the
// given suite: global Paths plus that suite's PerSuite list (if any).
// Pass an empty suite name (or "total") to get only global excludes —
// what the merged-total render uses.
func (c *config) excludesFor(suite string) []string {
	out := append([]string(nil), c.Exclude.Paths...)
	if suite != "" && suite != "total" {
		out = append(out, c.Exclude.PerSuite[suite]...)
	}
	return out
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
	rows, total, covered, err := parseCoverage(profile, c, c.excludesFor(suite))
	if err != nil {
		return err
	}
	fmt.Printf("\n%s==> %s coverage: %s%s%s  (threshold: %d%%)%s\n\n",
		cyan, suite, yellow, formatPct(covered, total), reset, threshold, reset)
	printBreakdown(rows, threshold)
	fmt.Printf("\n  HTML: %s\n\n", htmlOut)

	if !meetsThreshold(covered, total, threshold) {
		fmt.Fprintf(os.Stderr, "%s==> %s gate FAILED (%s < %d%%)%s\n",
			red, suite, formatPct(covered, total), threshold, reset)
		return fmt.Errorf("%s gate failed: %s < %d%%", suite, formatPct(covered, total), threshold)
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
		fmt.Printf("  %s%-13s%s %s\n", cyan, s+":", reset, suitePct(c, s))
	}
	if pct := readSDKPct(); pct != "" {
		fmt.Printf("  %s%-13s%s %s%%  %s(separate gate; not in merge above)%s\n",
			cyan, "sdk:", reset, pct, yellow, reset)
	}

	threshold := c.Threshold.Total
	// Merged total uses only global excludes — per-suite excludes are
	// intentionally NOT applied here, so a file that's untestable by one
	// suite (e.g., cmd/wavehouse/main.go vs unit) still counts toward the
	// project-wide gate via the suite(s) that DO cover it (e2e).
	rows, total, covered, err := parseCoverage(profile, c, c.excludesFor(""))
	if err != nil {
		return err
	}
	fmt.Println()
	printBreakdown(rows, threshold)
	fmt.Printf("\n  HTML: %s\n\n", htmlOut)
	fmt.Printf("%s==> Combined coverage gate (threshold %d%%):%s\n", cyan, threshold, reset)

	if !meetsThreshold(covered, total, threshold) {
		fmt.Fprintf(os.Stderr, "%s==> Total gate FAILED (%s < %d%%)%s\n",
			red, formatPct(covered, total), threshold, reset)
		return fmt.Errorf("total gate failed: %s < %d%%", formatPct(covered, total), threshold)
	}
	fmt.Printf("%s==> Total gate passed (%s ≥ %d%%)%s\n",
		green, formatPct(covered, total), threshold, reset)
	return nil
}

type pkgRow struct {
	pkg            string
	total, covered int
}

// parseCoverage reads a textfmt coverage profile, groups statements by
// package, applies local-prefix stripping and the supplied exclude patterns,
// and returns the per-package rows plus aggregate totals. The aggregates
// match what go-test-coverage's gate evaluates, so callers can render
// headers that line up with its "Total test coverage" output.
//
// Callers control which excludes apply by passing the patterns directly
// (see config.excludesFor). This is the seam that makes per-suite excludes
// possible: a unit-only exclude lives in c.Exclude.PerSuite["unit"], the
// renderSuite call passes it in, and merge() omits it so the file still
// shows up in the merged total when e2e covers it.
func parseCoverage(profile string, c *config, excludePatterns []string) (rows []pkgRow, total, covered int, err error) {
	// #nosec G304,G703 — profile is a coverage.txt path we rendered ourselves.
	raw, err := os.ReadFile(profile)
	if err != nil {
		return nil, 0, 0, err
	}
	excludes := compileRegexes(excludePatterns)
	pkgs := map[string]*pkgRow{}
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
		shortPath := path
		if c.LocalPrefix != "" && strings.HasPrefix(shortPath, c.LocalPrefix+"/") {
			shortPath = shortPath[len(c.LocalPrefix)+1:]
		}
		// Match exclusions against either pkg+"/" (directory-level) or
		// shortPath (file-level). Directory patterns like ^internal/testutil/
		// match the former; single-file patterns like ^cmd/wavehouse/main\.go$
		// match the latter, useful for excluding binary entry points that
		// can't be unit-tested without dragging the suite gate.
		if matchesAny(excludes, pkg+"/") || matchesAny(excludes, shortPath) {
			continue
		}
		stmts, _ := strconv.Atoi(f[1])
		count, _ := strconv.Atoi(f[2])
		r, ok := pkgs[pkg]
		if !ok {
			r = &pkgRow{pkg: pkg}
			pkgs[pkg] = r
		}
		r.total += stmts
		total += stmts
		if count > 0 {
			r.covered += stmts
			covered += stmts
		}
	}
	rows = make([]pkgRow, 0, len(pkgs))
	for _, r := range pkgs {
		if r.total > 0 {
			rows = append(rows, *r)
		}
	}
	return rows, total, covered, nil
}

func formatPct(covered, total int) string {
	if total == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%%", float64(covered)*100.0/float64(total))
}

// printBreakdown emits a per-package coverage table from pre-parsed rows,
// sorted ascending so the least-covered packages land at the top.
func printBreakdown(rows []pkgRow, threshold int) {
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

// suitePct returns the post-exclusion coverage percentage for a suite, or
// "n/a" if covdata is missing. Uses an existing coverage.txt when present,
// else renders a throwaway one (so `make cov` shows per-suite numbers
// without requiring you to have run `make test-<suite>` separately).
func suitePct(c *config, suite string) string {
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
	_, total, covered, err := parseCoverage(profile, c, c.excludesFor(suite))
	if err != nil || total == 0 {
		return "?"
	}
	return formatPct(covered, total)
}

// meetsThreshold returns true iff covered/total * 100 ≥ threshold, using
// integer arithmetic to avoid floating-point rounding at the boundary
// (e.g., 69.99999% should fail the 70% gate; covered*100 >= total*threshold
// is exact). Empty profile (total == 0) is treated as passing — same
// semantics as `go test -cover` reporting "no test files" rather than 0%.
//
// Replaces the previous `go-test-coverage --threshold-total=N` subprocess
// call. That tool predated this PR's per-suite excludes (.testcoverage.yml
// `exclude.per-suite`) and re-parsed the YAML with global-only semantics,
// which made the gate disagree with our breakdown for any suite that had
// per-suite excludes. Doing the check inline guarantees gate and breakdown
// use the same exclude set, computed once.
func meetsThreshold(covered, total, threshold int) bool {
	if total == 0 {
		return true
	}
	return covered*100 >= total*threshold
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
