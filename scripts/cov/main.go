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
//	cov merge-all         Run merge; fail if no suite has data (i.e.
//	                      `make cov` ran before any test target).
//	cov report            Single consolidated summary: every suite + the
//	                      merged Go-total, each with its gate status + a
//	                      clickable HTML report path, then one aggregate
//	                      pass/fail. Backs `make cov`.
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
	"slices"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	configPath = ".testcoverage.yml"
	root       = "tmp/coverage"
)

// Go suites whose covdata participates in the merged Go total.
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
		// (e.g., the settings CLI entry points that e2e never runs);
		// excluding them from that suite keeps its gate clean without
		// hiding the file's other-suite coverage from the merged total.
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
	case "merge-all":
		// Fail if nothing has data, so a stray `make cov` (before any test
		// target) doesn't look like a passing gate.
		if !hasAnyCoverage() {
			fatal("no coverage data anywhere — run the test targets (e.g. `make test-all`) before `make cov`")
		}
		if err := merge(cfg); err != nil {
			fatal("%v", err)
		}
	case "report":
		if err := report(cfg); err != nil {
			fatal("%v", err)
		}
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: cov render <suite> | merge | merge-all | report")
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

// goSuiteCoverage renders a Go suite's covdata to txt + html and parses the
// post-exclusion coverage. No printing or gating — it's the seam shared by
// `render` (standalone: prints a per-package breakdown) and `report` (the
// consolidated summary). Returns the rendered HTML path so callers can link
// to the suite's drill-down report.
func goSuiteCoverage(c *config, suite string) (rows []pkgRow, total, covered int, htmlOut string, err error) {
	dir := filepath.Join(root, suite)
	dataDir := filepath.Join(dir, "data")
	if !hasCovdata(dataDir) {
		return nil, 0, 0, "", fmt.Errorf("no covdata in %s — did make test-%s run?", dataDir, suite)
	}
	profile := filepath.Join(dir, "coverage.txt")
	htmlOut = filepath.Join(dir, "coverage.html")
	if err = sh("go", "tool", "covdata", "textfmt", "-i="+dataDir, "-o", profile); err != nil {
		return nil, 0, 0, "", err
	}
	if err = sh("go", "tool", "cover", "-html="+profile, "-o", htmlOut); err != nil {
		return nil, 0, 0, "", err
	}
	rows, total, covered, err = parseCoverage(profile, c, c.excludesFor(suite))
	if err != nil {
		return nil, 0, 0, "", err
	}
	return rows, total, covered, htmlOut, nil
}

func renderSuite(c *config, suite string) error {
	rows, total, covered, htmlOut, err := goSuiteCoverage(c, suite)
	if err != nil {
		return err
	}
	threshold := thresholdFor(c, suite)
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

// hasAnyCoverage reports whether at least one suite has covdata to merge.
// merge-all uses it to tell "nothing ran at all" (fail, because the caller
// expected a gate) from a partial run.
func hasAnyCoverage() bool {
	for _, s := range goSuites {
		if hasCovdata(filepath.Join(root, s, "data")) {
			return true
		}
	}
	return false
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
		fmt.Printf("  %sno Go coverage data — skipping Go merge%s\n", yellow, reset)
		return nil
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

	threshold := c.Threshold.Total
	// Merged total uses only global excludes — per-suite excludes are
	// intentionally NOT applied here, so a file that's untestable by one
	// suite (e.g., cmd/wavehouse/validate.go vs e2e) still counts toward the
	// project-wide gate via the suite(s) that DO cover it (unit).
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

// presentGoSuites returns the Go suites that have covdata, in goSuites order.
func presentGoSuites() []string {
	var present []string
	for _, s := range goSuites {
		if hasCovdata(filepath.Join(root, s, "data")) {
			present = append(present, s)
		}
	}
	return present
}

// goTotalCoverage merges the given suites' covdata into tmp/coverage/total
// (txt + html) and parses the merged profile with global excludes only —
// the project-wide number gated by threshold.total. No printing/gating.
func goTotalCoverage(c *config, present []string) (rows []pkgRow, total, covered int, htmlOut string, err error) {
	dirs := make([]string, 0, len(present))
	for _, s := range present {
		dirs = append(dirs, filepath.Join(root, s, "data"))
	}
	totalDir := filepath.Join(root, "total")
	if err = os.MkdirAll(totalDir, 0o750); err != nil {
		return nil, 0, 0, "", err
	}
	profile := filepath.Join(totalDir, "coverage.txt")
	htmlOut = filepath.Join(totalDir, "coverage.html")
	if err = sh("go", "tool", "covdata", "textfmt", "-i="+strings.Join(dirs, ","), "-o", profile); err != nil {
		return nil, 0, 0, "", err
	}
	if err = sh("go", "tool", "cover", "-html="+profile, "-o", htmlOut); err != nil {
		return nil, 0, 0, "", err
	}
	rows, total, covered, err = parseCoverage(profile, c, c.excludesFor(""))
	if err != nil {
		return nil, 0, 0, "", err
	}
	return rows, total, covered, htmlOut, nil
}

// reportRow is one line in the consolidated `cov report` table.
type reportRow struct {
	name   string // suite name, or "Go total"
	pct    string // "85.1" / "n/a"
	gated  bool   // false → informational (never fails the build)
	thresh int
	pass   bool
	html   string // HTML report path (relative), or "" if none
	rule   bool   // draw a separator line above this row (group/total boundary)
}

// report renders ONE consolidated coverage summary — every suite plus the
// merged Go-total, each with its gate status and a clickable
// HTML report path — then a single aggregate pass/fail. It generates every
// artifact it needs (per-suite html, merged Go profile), so it
// stands alone at the end of `make ci` whether or not the per-suite test
// targets rendered inline. This backs `make cov`.
func report(c *config) error {
	if !hasAnyCoverage() {
		return fmt.Errorf("no coverage data anywhere — run the test targets (e.g. `make test-all`) before `make cov`")
	}

	var rows []reportRow

	// --- Go suites + merged Go total ---
	present := presentGoSuites()
	for _, s := range goSuites {
		if !slices.Contains(present, s) {
			rows = append(rows, reportRow{name: s, pct: "n/a"})
			continue
		}
		_, total, covered, html, err := goSuiteCoverage(c, s)
		if err != nil {
			return err
		}
		th := thresholdFor(c, s)
		rows = append(rows, reportRow{
			name: s, pct: formatPctBare(covered, total), gated: true, thresh: th,
			pass: meetsThreshold(covered, total, th), html: html,
		})
	}
	if len(present) > 0 {
		_, total, covered, html, err := goTotalCoverage(c, present)
		if err != nil {
			return err
		}
		th := c.Threshold.Total
		rows = append(rows, reportRow{
			name: "Go total", pct: formatPctBare(covered, total), gated: true, thresh: th,
			pass: meetsThreshold(covered, total, th), html: html, rule: true,
		})
	}

	printReport(rows)

	var failed []string
	for _, r := range rows {
		if r.gated && !r.pass {
			failed = append(failed, r.name)
		}
	}
	if len(failed) > 0 {
		fmt.Fprintf(os.Stderr, "%s==> Coverage gate FAILED: %s%s\n", red, strings.Join(failed, ", "), reset)
		return fmt.Errorf("coverage gate failed: %s", strings.Join(failed, ", "))
	}
	fmt.Printf("%s==> All coverage gates passed%s\n", green, reset)
	return nil
}

// printReport prints the consolidated suite table. Columns: suite, coverage,
// minimum (gate floor), a ✓/✗/· status glyph, and the HTML report path. Rows
// flagged with rule get a separator above them (the Go total).
func printReport(rows []reportRow) {
	fmt.Printf("\n%s==> Coverage — all suites%s\n\n", cyan, reset)
	fmt.Printf("  %-12s %7s  %-5s %s\n", "Suite", "Cover", "Min", "Report")
	rule := "  " + strings.Repeat("─", 78)
	fmt.Println(rule)
	for _, r := range rows {
		if r.rule {
			fmt.Println(rule)
		}
		cov := r.pct
		if cov != "n/a" {
			cov += "%"
		}
		var glyph, floor, color string
		switch {
		case !r.gated:
			glyph, floor, color = "·", "-", yellow
		case r.pass:
			glyph, floor, color = "✓", fmt.Sprintf(">=%d", r.thresh), green
		default:
			glyph, floor, color = "✗", fmt.Sprintf(">=%d", r.thresh), red
		}
		fmt.Printf("  %-12s %7s  %s%-5s %s%s  %s\n",
			r.name, cov, color, floor, glyph, reset, r.html)
	}
	fmt.Println()
}

// formatPctBare is formatPct without the trailing % ("85.1"), or "n/a".
func formatPctBare(covered, total int) string {
	if total == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", float64(covered)*100.0/float64(total))
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
		// match the former; single-file patterns like ^cmd/wavehouse/validate\.go$
		// match the latter, useful for excluding a file one suite never
		// reaches without dragging that suite's gate.
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
