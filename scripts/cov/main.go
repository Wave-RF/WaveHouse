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
//	cov ts-merge          Merge SDK vitest coverage (ts-unit + ts-e2e)
//	                      via nyc into tmp/coverage/ts-total/, render
//	                      HTML + summary, gate against suites.ts-total.
//	cov merge-all         Run merge + ts-merge; skip either side with no
//	                      data, but fail if BOTH are empty (i.e. `make cov`
//	                      ran before any test target).
//	cov report            Single consolidated summary: every suite + the
//	                      merged Go-total and ts-total, each with its gate
//	                      status + a clickable HTML report path, then one
//	                      aggregate pass/fail. Backs `make cov`.
//	cov threshold <suite> Print the configured threshold for <suite>
//	                      (or "total"). Used by the SDK pipeline to
//	                      pass into vitest's --coverage.thresholds.
//	cov badge             Emit the shields.io endpoint JSON for the merged
//	                      Go-total coverage (the number threshold.total
//	                      gates) to stdout — CI publishes it to the
//	                      `badges` branch for the README coverage badge.
//
// All thresholds come from .testcoverage.yml — `threshold.total` is the
// canonical merged-coverage gate (the same field go-test-coverage reads
// natively); per-suite gates live under `suites:`. There is no env-var
// override path: edit the YAML.
package main

import (
	"context"
	"encoding/json"
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

// Go suites whose covdata participates in the merged Go total. SDK
// coverage (ts-unit, ts-e2e) is rendered separately by vitest (different
// toolchain, Istanbul JSON format) and merged via `cov ts-merge` —
// see the ts-total path below.
var goSuites = []string{"unit", "integration", "e2e"}

// Go suites rendered and gated like goSuites but never merged into the Go
// total: they come from the nested clients/go module — see the go-sdk
// comment in .testcoverage.yml.
var standaloneGoSuites = []string{"go-sdk"}

// suiteModuleDir maps a non-root-module suite to the module directory its
// covdata came from. `go tool cover -html` resolves the profile's package
// paths through the module in its working directory, so it must run there.
var suiteModuleDir = map[string]string{"go-sdk": "clients/go"}

// TypeScript SDK suites (vitest). ts-unit comes from clients/ts; ts-e2e
// from tests/e2e/sdk run with --coverage. Both produce Istanbul-format
// coverage-final.json that `cov ts-merge` combines into ts-total.
var tsSuites = []string{"ts-unit", "ts-e2e"}

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
	case "ts-merge":
		if err := mergeTS(cfg); err != nil {
			fatal("%v", err)
		}
	case "merge-all":
		// Run both sides, skipping whichever has no data — but fail if
		// neither does, so a stray `make cov` (before any test target)
		// doesn't look like a passing gate.
		if !hasAnyCoverage() {
			fatal("no coverage data anywhere — run the test targets (e.g. `make test-all`) before `make cov`")
		}
		if err := merge(cfg); err != nil {
			fatal("%v", err)
		}
		if err := mergeTS(cfg); err != nil {
			fatal("%v", err)
		}
	case "report":
		if err := report(cfg); err != nil {
			fatal("%v", err)
		}
	case "badge":
		if err := badge(cfg); err != nil {
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
	fmt.Fprintln(os.Stderr, "usage: cov render <suite> | merge | ts-merge | merge-all | report | badge | threshold <suite>")
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
	if err = renderHTML(suite, profile, htmlOut); err != nil {
		return nil, 0, 0, "", err
	}
	rows, total, covered, err = parseCoverage(profile, c, c.excludesFor(suite))
	if err != nil {
		return nil, 0, 0, "", err
	}
	return rows, total, covered, htmlOut, nil
}

// renderHTML turns a textfmt profile into the clickable HTML report, running
// from the suite's own module (hence absolute paths) when it is a nested one.
func renderHTML(suite, profile, htmlOut string) error {
	dir, nested := suiteModuleDir[suite]
	if !nested {
		return sh("go", "tool", "cover", "-html="+profile, "-o", htmlOut)
	}
	absProfile, err := filepath.Abs(profile)
	if err != nil {
		return err
	}
	absHTML, err := filepath.Abs(htmlOut)
	if err != nil {
		return err
	}
	return shIn(dir, "go", "tool", "cover", "-html="+absProfile, "-o", absHTML)
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

// hasAnyCoverage reports whether at least one suite has data to merge — any
// Go covdata dir or any TS coverage-final.json. merge-all uses it to tell
// "one side legitimately absent" (skip, fine) from "nothing ran at all"
// (fail, because the caller expected a gate).
func hasAnyCoverage() bool {
	for _, s := range slices.Concat(goSuites, standaloneGoSuites) {
		if hasCovdata(filepath.Join(root, s, "data")) {
			return true
		}
	}
	for _, s := range tsSuites {
		if _, err := os.Stat(filepath.Join(root, s, "coverage-final.json")); err == nil {
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
	// Nested-module Go suites: gated on their own, never merged above.
	for _, s := range standaloneGoSuites {
		if pct := suitePct(c, s); pct != "n/a" {
			fmt.Printf("  %s%-13s%s %s  %s(separate gate; not in merge above)%s\n",
				cyan, s+":", reset, pct, yellow, reset)
		}
	}
	// Surface TS SDK coverage alongside the Go total — informational only,
	// not part of the Go merged number above. `make cov` is the gate.
	for _, s := range append(tsSuites, "ts-total") {
		if pct := readTSPct(s); pct != "" {
			fmt.Printf("  %s%-13s%s %s%%  %s(separate gate; not in merge above)%s\n",
				cyan, s+":", reset, pct, yellow, reset)
		}
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

// mergeTSArtifacts stages the present TS suites' coverage-final.json under a
// scratch dir and runs `nyc merge` + `nyc report` to produce the merged
// ts-total report (coverage-final.json plus the requested reporters, e.g.
// "html", "json-summary", "text"). It returns the suite names that were
// merged (nil if neither had data). No printing or gating — the seam shared
// by `ts-merge` (standalone) and `report` (consolidated summary).
//
// Layout under tmp/coverage/:
//
//	ts-unit/coverage-final.json    ← `make test-ts`
//	ts-e2e/coverage-final.json     ← `make test-e2e`
//	ts-merge-input/                ← scratch dir (both renamed json files)
//	ts-total/                      ← merged JSON + requested reports
func mergeTSArtifacts(reporters ...string) (merged []string, err error) {
	const (
		inputDir = "tmp/coverage/ts-merge-input"
		outDir   = "tmp/coverage/ts-total"
	)

	var inputs []string
	for _, name := range tsSuites { // ts-unit, ts-e2e
		path := filepath.Join(root, name, "coverage-final.json")
		if _, statErr := os.Stat(path); statErr != nil {
			continue
		}
		inputs = append(inputs, path)
		merged = append(merged, name)
	}
	if len(inputs) == 0 {
		return nil, nil
	}

	// Recreate the merge input dir so stale files from a previous run
	// don't get re-merged. Copy each suite's coverage-final.json under
	// a suite-named filename — nyc merges all *.json under input-dir.
	if err = os.RemoveAll(inputDir); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(inputDir, 0o750); err != nil {
		return nil, err
	}
	for _, p := range inputs {
		suite := filepath.Base(filepath.Dir(p)) // "ts-unit" or "ts-e2e"
		dst := filepath.Join(inputDir, suite+".json")
		if err = copyFile(p, dst); err != nil {
			return nil, fmt.Errorf("stage %s for merge: %w", suite, err)
		}
	}

	if err = os.RemoveAll(outDir); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(outDir, 0o750); err != nil {
		return nil, err
	}

	// nyc merge: combine every *.json under inputDir into one coverage-final.json.
	// Quiet — nyc prints a "coverage files … merged into …" line to stdout we
	// don't want above the report table.
	mergedJSON := filepath.Join(outDir, "coverage-final.json")
	if err = shQuiet("pnpm", "exec", "nyc", "merge", inputDir, mergedJSON); err != nil {
		return nil, fmt.Errorf("nyc merge: %w", err)
	}

	// nyc report: render the requested reporters from the merged JSON.
	// --temp-dir points at outDir (which now holds coverage-final.json);
	// --report-dir is the same directory so reports land alongside.
	args := []string{"exec", "nyc", "report", "--temp-dir=" + outDir, "--report-dir=" + outDir}
	for _, r := range reporters {
		args = append(args, "--reporter="+r)
	}
	if err = sh("pnpm", args...); err != nil {
		return nil, fmt.Errorf("nyc report: %w", err)
	}
	return merged, nil
}

// mergeTS combines the ts-unit + ts-e2e Istanbul coverage into one ts-total
// report (text + html + json-summary) and gates it against suites.ts-total.
// mergeTSArtifacts does the istanbul merge + re-render; this wraps it with
// the standalone command's per-suite log + threshold gate.
func mergeTS(c *config) error {
	fmt.Printf("%s==> Merging SDK coverage (ts-unit + ts-e2e)...%s\n", cyan, reset)
	merged, err := mergeTSArtifacts("text", "html", "json-summary")
	if err != nil {
		return err
	}
	for _, name := range tsSuites {
		if slices.Contains(merged, name) {
			fmt.Printf("  %s✔%s %-9s %s\n", green, reset, name,
				filepath.Join(root, name, "coverage-final.json"))
		} else {
			fmt.Printf("  %s✗%s %-9s (no coverage-final.json; run `make %s` to include)\n",
				yellow, reset, name, ternary(name == "ts-unit", "test-ts", "test-e2e"))
		}
	}
	if len(merged) == 0 {
		fmt.Printf("  %sno TS coverage data — skipping ts-merge (run `make test-ts` and/or `make test-e2e` to populate)%s\n", yellow, reset)
		return nil
	}

	pct := readTSPct("ts-total")
	if pct == "" {
		return fmt.Errorf("no ts-total summary written by nyc report")
	}
	threshold := thresholdFor(c, "ts-total")
	fmt.Printf("\n%s==> ts-total: %s%s%%%s  (threshold: %d%%)%s\n",
		cyan, yellow, pct, reset, threshold, reset)
	fmt.Printf("  HTML: %s/index.html\n\n", filepath.Join(root, "ts-total"))

	if !tsMeetsThreshold(pct, threshold) {
		fmt.Fprintf(os.Stderr, "%s==> ts-total gate FAILED (%s%% < %d%%)%s\n",
			red, pct, threshold, reset)
		return fmt.Errorf("ts-total gate failed: %s%% < %d%%", pct, threshold)
	}
	fmt.Printf("%s==> ts-total gate passed (%s%% ≥ %d%%)%s\n", green, pct, threshold, reset)
	return nil
}

// tsMeetsThreshold compares a vitest/nyc json-summary pct string ("47.08")
// against an integer floor, with a tiny epsilon so 40.00 clears a 40 gate.
func tsMeetsThreshold(pctStr string, threshold int) bool {
	f, err := strconv.ParseFloat(pctStr, 64)
	if err != nil {
		return false
	}
	return f+1e-9 >= float64(threshold)
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
	name   string // suite name, or "Go total" / "ts-total"
	pct    string // "85.1" / "n/a"
	gated  bool   // false → informational (never fails the build)
	thresh int
	pass   bool
	html   string // HTML report path (relative), or "" if none
	rule   bool   // draw a separator line above this row (group/total boundary)
}

// report renders ONE consolidated coverage summary — every suite plus the
// merged Go-total and ts-total, each with its gate status and a clickable
// HTML report path — then a single aggregate pass/fail. It generates every
// artifact it needs (per-suite html, merged Go profile, nyc ts-total), so it
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

	// --- Nested-module Go suites: own gate, not in the total above ---
	for i, s := range standaloneGoSuites {
		if !hasCovdata(filepath.Join(root, s, "data")) {
			rows = append(rows, reportRow{name: s, pct: "n/a", rule: i == 0})
			continue
		}
		_, total, covered, html, err := goSuiteCoverage(c, s)
		if err != nil {
			return err
		}
		th := thresholdFor(c, s)
		rows = append(rows, reportRow{
			name: s, pct: formatPctBare(covered, total), gated: true, thresh: th,
			pass: meetsThreshold(covered, total, th), html: html, rule: i == 0,
		})
	}

	// --- TS suites + merged ts-total ---
	merged, err := mergeTSArtifacts("html", "json-summary")
	if err != nil {
		return err
	}
	if len(merged) > 0 {
		for i, s := range tsSuites {
			th := thresholdFor(c, s)
			row := reportRow{name: s, pct: readTSPct(s), html: tsHTML(s), rule: i == 0}
			switch {
			case row.pct == "":
				row.pct = "n/a"
			case th > 0: // ts-e2e threshold is 0 = informational
				row.gated, row.thresh, row.pass = true, th, tsMeetsThreshold(row.pct, th)
			}
			rows = append(rows, row)
		}
		th := thresholdFor(c, "ts-total")
		pct := readTSPct("ts-total")
		rows = append(rows, reportRow{
			name: "ts-total", pct: pct, gated: true, thresh: th,
			pass: tsMeetsThreshold(pct, th), html: tsHTML("ts-total"), rule: true,
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
// flagged with rule get a separator above them (Go total, ts group, ts-total).
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

// badgeData is the shields.io endpoint schema (https://shields.io/endpoint).
// The README's coverage badge is an <img> pointing at img.shields.io/endpoint
// whose url= is this JSON, published to the `badges` branch by CI. Emitting it
// here means the badge always shows the exact number `make cov` gated.
type badgeData struct {
	SchemaVersion int    `json:"schemaVersion"`
	Label         string `json:"label"`
	Message       string `json:"message"`
	Color         string `json:"color"`
}

// badge writes the shields.io endpoint JSON for the merged Go-total coverage
// (global excludes only — the same number threshold.total gates) to stdout,
// and nothing else, so the caller can redirect it straight to a file. It reads
// the profile `make cov`/`cov report` already rendered to tmp/coverage/total.
func badge(c *config) error {
	profile := filepath.Join(root, "total", "coverage.txt")
	if _, err := os.Stat(profile); err != nil {
		return fmt.Errorf("no merged Go profile at %s — run `make cov` first", profile)
	}
	_, total, covered, err := parseCoverage(profile, c, c.excludesFor(""))
	if err != nil {
		return err
	}
	msg, color := "unknown", "lightgrey"
	if total > 0 {
		pct := float64(covered) * 100.0 / float64(total)
		msg = fmt.Sprintf("%.1f%%", pct)
		color = badgeColor(pct, c.Threshold.Total)
	}
	out, err := json.Marshal(badgeData{SchemaVersion: 1, Label: "coverage", Message: msg, Color: color})
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// badgeColor maps a coverage percentage to a shields.io color anchored on the
// configured gate: at/above the gate reads green, warming through yellow to
// red below it (so the badge color tracks the same line the build enforces).
func badgeColor(pct float64, gate int) string {
	g := float64(gate)
	switch {
	case pct >= g+10:
		return "brightgreen"
	case pct >= g:
		return "green"
	case pct >= g-10:
		return "yellowgreen"
	case pct >= g-20:
		return "yellow"
	case pct >= g-30:
		return "orange"
	default:
		return "red"
	}
}

// formatPctBare is formatPct without the trailing % ("85.1"), or "n/a".
func formatPctBare(covered, total int) string {
	if total == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", float64(covered)*100.0/float64(total))
}

// tsHTML is the vitest/nyc HTML report path for a TS suite.
func tsHTML(suite string) string { return filepath.Join(root, suite, "index.html") }

// ternary returns a if cond else b. Used inline to keep the merge log
// branching from sprawling into a 5-line if/else.
func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// copyFile streams src → dst, creating dst and overwriting if it exists.
// Used to stage coverage-final.json files under suite-prefixed names
// before nyc merge so the inputs land in one directory.
func copyFile(src, dst string) error {
	// #nosec G304 — src/dst are paths we compute inside the merge dirs.
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	// #nosec G304 — see above.
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	_, err = out.ReadFrom(in)
	return err
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

// readTSPct returns the top-level statements.pct from a vitest
// coverage-summary.json at tmp/coverage/<suite>/coverage-summary.json,
// or "" if the file is missing. We pluck a single field with a regex
// rather than parsing the full JSON because vitest emits the entire
// summary as one giant single-line record.
func readTSPct(suite string) string {
	// #nosec G304 — constant components, not user input.
	raw, err := os.ReadFile(filepath.Join(root, suite, "coverage-summary.json"))
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
func sh(name string, args ...string) error { return shIn("", name, args...) }

// shIn is sh with an explicit working directory ("" = inherit ours).
// #nosec G204,G702 — name and args are not user input.
func shIn(dir, name string, args ...string) error {
	cmd := exec.CommandContext(context.Background(), name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// shQuiet is sh with stdout discarded (stderr still wired through) — for
// tools that print chatter to stdout we don't want in the report, e.g.
// `nyc merge` announcing the merged-file path. Leaving cmd.Stdout nil
// connects the child's stdout to the null device (os/exec semantics).
// #nosec G204,G702 — name and args are not user input.
func shQuiet(name string, args ...string) error {
	cmd := exec.CommandContext(context.Background(), name, args...)
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
