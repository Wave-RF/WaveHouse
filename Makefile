# ==============================================================================
# WaveHouse Build System
# ==============================================================================

# --- Make Configuration -------------------------------------------------------
# Strict-mode bash for every recipe (errexit, nounset, pipefail).
#
# NOTE: .SHELLFLAGS was added in GNU Make 4.0. macOS ships Make 3.81 by
# default, which silently ignores this — `brew install make` and run `gmake`
# (or put it on PATH) for strict mode. The Makefile is written defensively to
# work either way; this is a belt-and-braces upgrade for those who have it.
SHELL       := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

# Warn on undefined variables — catches typos like $(BINAIRES) silently expanding to empty.
MAKEFLAGS += --warn-undefined-variables

.DEFAULT_GOAL := help

# --- Environment Detection ----------------------------------------------------
# CI=true is set by GitHub Actions, GitLab, CircleCI, Buildkite, etc.
# We use it to skip interactive prompts (e.g. clean-all confirm).
CI       ?=
NO_COLOR ?=

# --- Colors -------------------------------------------------------------------
# GitHub Actions, GitLab, etc. all render ANSI fine — only opt out via NO_COLOR.
# We bake the literal ESC byte (0x1B) into each variable via $(shell printf).
# This way `echo "$(CYAN)..."` outputs raw ESC sequences regardless of whether
# the shell's echo interprets backslash escapes (bash builtin echo doesn't,
# unless xpg_echo is set — which we can't reliably toggle on Make 3.81).
ifeq ($(strip $(NO_COLOR)),)
  ESC    := $(shell printf '\033')
  CYAN   := $(ESC)[36m
  GREEN  := $(ESC)[32m
  YELLOW := $(ESC)[33m
  RED    := $(ESC)[31m
  BOLD   := $(ESC)[1m
  RESET  := $(ESC)[0m
else
  CYAN   :=
  GREEN  :=
  YELLOW :=
  RED    :=
  BOLD   :=
  RESET  :=
endif

# --- System & Architecture ----------------------------------------------------
OS   := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

# --- Build Variables ----------------------------------------------------------
# Tunables (override via env or `make VAR=value target`):
#   V=1                set for verbose test output
#   TAGS="foo bar"     Go build/test tags (space-separated)
#   ARGS="-run TestX"  extra flags passed to `go test`
#   NO_COLOR=1         disable colored output
#   VERSION=v1.2.3     override version string embedded in binary
#
# Add binaries here as the project grows (e.g., wavehouse-api, wavehouse-worker).
BINARIES := wavehouse

TAGS ?=
ARGS ?=

# Version metadata is always embedded in the binary. Names match the package
# vars in cmd/wavehouse/main.go (Version / GitCommit / BuildTime), which the
# goreleaser config injects the same way for release artifacts.
VERSION    ?= $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION_LDFLAGS := -X main.Version=$(VERSION) -X main.GitCommit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)

# LDFLAGS is the strip toggle — empty by default (debug-friendly local builds);
# `make build-release` sets it to `-s -w` for stripped release-style output.
LDFLAGS ?=

ifdef V
  GOTESTSUM_FMT := standard-verbose
else
  # pkgname-and-test-fails: one line per package on success, full detail on
  # failure. Quiet enough for CI logs, informative when something breaks.
  GOTESTSUM_FMT := pkgname-and-test-fails
endif

# --- Tooling Paths ------------------------------------------------------------
# $(CURDIR) is Make's built-in for the working dir — no subshell needed.
LOCAL_BIN := $(CURDIR)/.bin/$(OS)_$(ARCH)

# Pinned via go.mod tool directives — no install step needed.
GOTESTSUM   := go tool gotestsum
GOFUMPT     := go tool gofumpt
GOIMPORTS   := go tool goimports
GOCOVER     := go tool go-test-coverage
GOVULNCHECK := go tool govulncheck
DEADCODE    := go tool deadcode
GODA        := go tool goda
# gsa imports encoding/json/v2 which is still gated behind a build experiment;
# the env var is required at every invocation because Go's build cache keys on
# experiment flags.
GSA         := GOEXPERIMENT=jsonv2 go tool gsa

# Externally-installed tools — version is encoded in the path so bumping the
# version invalidates the file rule and triggers a reinstall.
GOLANGCI_LINT_VERSION := v2.11.4
GOLANGCI_LINT         := $(LOCAL_BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)

# --- Coverage Directories -----------------------------------------------------
# Binary covdata dirs (one per test suite) → text + HTML rendered into
# matching output dirs. The `cov` target merges all available covdata into
# tmp/coverage/total/.
COV_UNIT  := tmp/covdata/unit
COV_INT   := tmp/covdata/integration
COV_E2E   := tmp/covdata/e2e

COV_OUT_UNIT  := tmp/coverage/unit
COV_OUT_INT   := tmp/coverage/integration
COV_OUT_E2E   := tmp/coverage/e2e
COV_OUT_TOTAL := tmp/coverage/total

# --- Coverage Thresholds ------------------------------------------------------
# Per-suite + total thresholds enforced after each test suite runs (and by
# `make cov` for the merged total). Override via env or CLI:
#   COV_THRESHOLD_UNIT=80 make test
COV_THRESHOLD_UNIT  ?= 70
COV_THRESHOLD_INT   ?= 30
COV_THRESHOLD_E2E   ?= 10
COV_THRESHOLD_TOTAL ?= 70

# Derived: coverage-instrumented binaries (e.g., wavehouse-cov).
COVER_BINARIES := $(addsuffix -cov,$(BINARIES))

# ==============================================================================
# Targets
# ==============================================================================

# (use double hashes and `@` to document make target sections for the help menu)
##@ General

.PHONY: help
help: ## Show this help menu
	@printf "Usage: $(BOLD)make$(RESET) $(CYAN)<target>$(RESET) [VAR=value]\n"
	@awk 'BEGIN { FS = "## " } \
		/^##@/ { printf "\n$(YELLOW)%s$(RESET)\n", substr($$0, 5); next } \
		/^[a-zA-Z0-9_-]+:.*## / { \
			tname = $$0; sub(/:.*/, "", tname); \
			printf "  $(CYAN)%-18s$(RESET) %s\n", tname, $$2 \
		}' $(MAKEFILE_LIST)
	@printf "\nTunable variables are documented at the top of the Makefile.\n"

.PHONY: dev
dev: ## Start dev environment (ClickHouse + hot-reload)
	@echo "$(CYAN)==> Starting Dev Environment...$(RESET)"
	@go run scripts/dev.go

##@ Code Quality

.PHONY: fmt
fmt: ## Check formatting (run `make fix` to apply)
	@OUT=$$($(GOFUMPT) -l .); \
	if [ -n "$$OUT" ]; then \
		echo "$(RED)==> Files not formatted:$(RESET)"; \
		echo "$$OUT"; \
		echo "Run $(CYAN)make fix$(RESET) to apply."; \
		exit 1; \
	fi
	@echo "$(GREEN)==> Formatting OK$(RESET)"

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run golangci-lint (run `make fix` to apply --fix)
	@echo "$(CYAN)==> Running linters...$(RESET)"
	@$(GOLANGCI_LINT) run ./...

.PHONY: vulncheck
vulncheck: ## Run govulncheck (V=1 for full call stacks)
	@echo "$(CYAN)==> Running govulncheck...$(RESET)"
ifdef V
	@$(GOVULNCHECK) ./...
else
	@$(GOVULNCHECK) -scan package ./...
endif

# tidy: read-only check via `go mod tidy -diff` (Go 1.23+). Prints the
# unified diff that would be applied and exits non-zero if anything is off,
# without touching go.mod / go.sum. Safe to run in parallel with fmt/lint.
.PHONY: tidy
tidy: ## Verify go.mod/go.sum are tidy (run `make fix` to apply)
	@echo "$(CYAN)==> Checking module tidiness...$(RESET)"
	@if ! go mod tidy -diff; then \
		echo "$(RED)==> go.mod/go.sum is not tidy.$(RESET)"; \
		echo "Run $(CYAN)make fix$(RESET) to apply."; \
		exit 1; \
	fi
	@echo "$(GREEN)==> Modules OK$(RESET)"

.PHONY: fix
fix: $(GOLANGCI_LINT) ## Apply all auto-fixes (tidy + gofumpt + goimports + lint --fix)
	@echo "$(CYAN)==> Applying all auto-fixes...$(RESET)"
	@go mod tidy
	@$(GOFUMPT) -w .
	@$(GOIMPORTS) -w .
	@$(GOLANGCI_LINT) run --fix ./...
	@echo "$(GREEN)==> Done$(RESET)"

# verify: aggregate of all static checks. The "is my code OK?" target —
# what contributors should run before pushing. Mirrors what CI's lint job
# checks (CI uses golangci-lint-action for the lint step for cache speed).
#
# Declared as direct prereqs (not recursive `$(MAKE)` calls) so all four
# checks can run concurrently with `make -j verify`.
.PHONY: verify
verify: tidy fmt vulncheck lint ## Run all static checks (parallel-safe: `make -j verify`)
	@echo "$(GREEN)==> All static checks passed$(RESET)"

##@ Build

# Static rule: knows how to build any binary in $(BINARIES) from ./cmd/<name>.
# `$@` expands to the target name. Adding a binary is just appending to BINARIES.
# Default build keeps debug symbols; `make build-release` strips them.
.PHONY: $(BINARIES)
$(BINARIES):
	@echo "$(CYAN)==> Building $@...$(RESET)"
	@START=$$(date +%s); \
	CGO_ENABLED=0 go build -tags="$(TAGS)" -ldflags="$(LDFLAGS) $(VERSION_LDFLAGS)" -o bin/$@ ./cmd/$@; \
	END=$$(date +%s); \
	SIZE=$$(ls -lh bin/$@ | awk '{print $$5}'); \
	printf "$(GREEN)✔$(RESET) bin/$@ ($(YELLOW)%ss$(RESET), $(YELLOW)%s$(RESET))\n" "$$((END - START))" "$$SIZE"

.PHONY: build
build: $(BINARIES) ## Compile all binaries (use `make -j build` for parallel)

.PHONY: build-release
build-release: LDFLAGS := -s -w
build-release: build ## Compile with stripped symbols (release-style)

# Static rule for coverage-instrumented variants:
# `wavehouse-cov` → builds ./cmd/wavehouse with -cover into bin/wavehouse-cov.
.PHONY: $(COVER_BINARIES)
$(COVER_BINARIES): %-cov:
	@echo "$(CYAN)==> Building $* (coverage instrumented)...$(RESET)"
	@START=$$(date +%s); \
	CGO_ENABLED=0 go build -cover -tags="$(TAGS)" -ldflags="$(LDFLAGS) $(VERSION_LDFLAGS)" -o bin/$@ ./cmd/$*; \
	END=$$(date +%s); \
	SIZE=$$(ls -lh bin/$@ | awk '{print $$5}'); \
	printf "$(GREEN)✔$(RESET) bin/$@ ($(YELLOW)%ss$(RESET), $(YELLOW)%s$(RESET))\n" "$$((END - START))" "$$SIZE"

.PHONY: build-cover
build-cover: $(COVER_BINARIES) ## Compile all binaries with coverage instrumentation

##@ Test

# Each test target runs its suite, writes covdata, renders text + HTML to its
# own coverage output dir, and gates against its per-suite threshold. The
# /testutil package is filtered out at `go list` time — it has no tests of
# its own and only serves as a helper for other packages' tests.

# Color + path env vars piped to scripts/coverage.sh's render-suite subcommand.
COVERAGE_ENV := \
	CYAN='$(CYAN)' GREEN='$(GREEN)' YELLOW='$(YELLOW)' RED='$(RED)' RESET='$(RESET)' \
	COV_UNIT='$(COV_UNIT)' COV_INT='$(COV_INT)' COV_E2E='$(COV_E2E)' \
	COV_OUT_UNIT='$(COV_OUT_UNIT)' COV_OUT_INT='$(COV_OUT_INT)' \
	COV_OUT_E2E='$(COV_OUT_E2E)' COV_OUT_TOTAL='$(COV_OUT_TOTAL)' \
	COV_THRESHOLD_UNIT='$(COV_THRESHOLD_UNIT)' \
	COV_THRESHOLD_INT='$(COV_THRESHOLD_INT)' \
	COV_THRESHOLD_E2E='$(COV_THRESHOLD_E2E)' \
	COV_THRESHOLD_TOTAL='$(COV_THRESHOLD_TOTAL)'

.PHONY: test
test: ## Run unit tests + render coverage + gate threshold
	@echo "$(CYAN)==> Running Unit Tests...$(RESET)"
	@rm -rf $(COV_UNIT) && mkdir -p $(COV_UNIT)
	@PKGS=$$(go list ./internal/... | grep -v /testutil); \
	GOCOVERDIR=$(CURDIR)/$(COV_UNIT) $(GOTESTSUM) --format $(GOTESTSUM_FMT) -- \
		-tags="$(TAGS)" -cover -race $$PKGS $(ARGS) \
		-args -test.gocoverdir=$(CURDIR)/$(COV_UNIT)
	@$(COVERAGE_ENV) scripts/coverage.sh render-suite unit

.PHONY: test-integration
test-integration: ## Run integration tests + render coverage + gate threshold (requires Docker)
	@echo "$(CYAN)==> Running Integration Tests...$(RESET)"
	@rm -rf $(COV_INT) && mkdir -p $(COV_INT)
	@PKGS=$$(go list ./tests/integration/... | grep -v /testutil); \
	GOCOVERDIR=$(CURDIR)/$(COV_INT) $(GOTESTSUM) --format $(GOTESTSUM_FMT) -- \
		-tags="integration $(TAGS)" -timeout 120s -coverpkg=./... -race -count=1 \
		$$PKGS $(ARGS) \
		-args -test.gocoverdir=$(CURDIR)/$(COV_INT)
	@$(COVERAGE_ENV) scripts/coverage.sh render-suite integration

# E2E: starts a coverage-instrumented binary, runs the SDK suite, then SIGINTs
# the binary so Go flushes coverage data to GOCOVERDIR.
.PHONY: test-e2e
test-e2e: build-cover ## Run E2E SDK suite (./tests/e2e/sdk) against instrumented binary
	# TODO: need to refactor this into go script in `tests/` like integration tests do
	@echo "$(CYAN)==> Running E2E Tests...$(RESET)"
	@rm -rf $(COV_E2E) && mkdir -p $(COV_E2E) tmp
	@echo "$(YELLOW)==> Starting instrumented binary...$(RESET)"
	@GOCOVERDIR=$(CURDIR)/$(COV_E2E) bin/wavehouse-cov & echo $$! > tmp/wavehouse.pid
	@sleep 2
	@echo "$(YELLOW)==> Running SDK E2E suite...$(RESET)"
	@cd tests/e2e/sdk && npm install --silent && npx vitest run \
		|| { kill -SIGINT $$(cat tmp/wavehouse.pid) 2>/dev/null || true; \
		     rm -f tmp/wavehouse.pid; exit 1; }
	@echo "$(YELLOW)==> Stopping binary (flushes coverage)...$(RESET)"
	@kill -SIGINT $$(cat tmp/wavehouse.pid) 2>/dev/null || true
	@wait $$(cat tmp/wavehouse.pid) 2>/dev/null || true
	@rm -f tmp/wavehouse.pid
	@$(COVERAGE_ENV) scripts/coverage.sh render-suite e2e

# test-all is intentionally a recipe-based aggregator (not declared prereqs)
# so the suites run sequentially even under `make -j N`. The suites are NOT
# parallel-safe — each binds ports / spins testcontainers / starts the binary.
.PHONY: test-all
test-all: ## Run all suites sequentially + merged total coverage + gate
	@$(MAKE) test
	@$(MAKE) test-integration
	@$(MAKE) test-e2e
	@$(MAKE) cov

##@ Coverage

# cov: aggregates whichever covdata exists across unit/integration/e2e,
# renders merged text + HTML to tmp/coverage/total/, prints per-suite
# breakdown, gates against COV_THRESHOLD_TOTAL. Does NOT run tests — call
# the test targets first (or run `make test-all` for the full pipeline).
.PHONY: cov
cov: ## Merge all available covdata + gate against total threshold
	@$(COVERAGE_ENV) scripts/coverage.sh merge-total

##@ CI

# ci: full local approximation of the pipeline. Use before opening a PR
# when you want to catch what CI would catch end-to-end.
.PHONY: ci
ci: ## Full pipeline: verify + build + unit + integration tests
	@printf "\n$(BOLD)$(YELLOW)━━━ 1/4  Static checks ━━━$(RESET)\n"
	@$(MAKE) verify
	@printf "\n$(BOLD)$(YELLOW)━━━ 2/4  Build ━━━$(RESET)\n"
	@$(MAKE) build
	@printf "\n$(BOLD)$(YELLOW)━━━ 3/4  Unit tests ━━━$(RESET)\n"
	@$(MAKE) test
	@printf "\n$(BOLD)$(YELLOW)━━━ 4/4  Integration tests ━━━$(RESET)\n"
	@$(MAKE) test-integration
	@printf "\n$(BOLD)$(GREEN)✔ All CI checks passed$(RESET)\n"

##@ Analysis

# Analysis tools are exploratory utilities run by humans investigating binary
# size, dependency weight, or unused code. They are intentionally not part of
# `verify` or `ci` — `deadcode` has false positives on reflection / HTTP
# routers, and the size/dep tools are too slow for a pre-push gate.

# audit-cgo: WaveHouse builds with CGO_ENABLED=0. Listed packages have pure-Go
# fallbacks today, but a new dep could quietly break that constraint — this
# audit surfaces every transitively-reachable package with C files so the drift
# is visible before a release-time cross-compile breaks.
.PHONY: audit-cgo
audit-cgo: ## Audit dependency tree for CGO files (informational)
	@echo "$(CYAN)==> Scanning dependency tree for packages with C files...$(RESET)"
	@printf "  WaveHouse builds with %sCGO_ENABLED=0%s — listed packages have pure-Go fallbacks\n" "$(YELLOW)" "$(RESET)"
	@echo  "  and their C code is never compiled. This audit catches new CGO deps."
	@echo
	@CGO_ENABLED=1 go list -deps -f '{{if .CgoFiles}}  ⚠ {{.ImportPath}}  ({{len .CgoFiles}} C files){{end}}' ./cmd/... 2>/dev/null || true
	@echo
	@echo "$(GREEN)==> CGO audit complete$(RESET)"

# deadcode: whole-program reachability analysis, complementary to
# golangci-lint's `unused` (which is locally scoped). False positives are
# common for HTTP routers, reflection-based dispatch, and init() registration
# — treat the output as a starting point, not a verdict.
.PHONY: deadcode
deadcode: ## Find unreachable functions
	@echo "$(CYAN)==> Searching for dead code...$(RESET)"
	@$(DEADCODE) -test ./...

# size: binary size analysis. Default `make build` keeps DWARF symbols, which
# gsa needs for accurate per-package attribution. The script also reports the
# strip-equivalent size (what `make build-release` would produce) and explains
# the gsa output's quirks (the "CGO" label is mislabeled type metadata, etc.).
.PHONY: size
size: build ## Binary size analysis → text + SVG + interactive HTML
	@CYAN='$(CYAN)' GREEN='$(GREEN)' YELLOW='$(YELLOW)' RED='$(RED)' RESET='$(RESET)' \
		scripts/size.sh

# dep-cut: surfaces packages with few dependents (low InDegree) that drag in
# heavy transitive weight — i.e. the best candidates to remove or replace.
# Override the default top-N with `make dep-cut LIMIT=50`.
LIMIT ?= 30
.PHONY: dep-cut
dep-cut: ## Top cuttable dependencies by transitive weight (LIMIT=N to override)
	@CYAN='$(CYAN)' YELLOW='$(YELLOW)' RESET='$(RESET)' \
		LIMIT='$(LIMIT)' scripts/dep-cut.sh

# binary-analysis: one command for "what's in my binary, and what's wrong with
# it." Runs in dep-order: build → size → audit-cgo → deadcode.
.PHONY: binary-analysis
binary-analysis: size audit-cgo deadcode ## Combined: size + audit-cgo + deadcode
	@echo
	@echo "$(GREEN)==> Binary analysis complete$(RESET)"
	@printf "  Cuttable dependencies: %smake dep-cut%s\n" "$(CYAN)" "$(RESET)"

##@ Cleanup

.PHONY: clean
clean: ## Remove build artifacts (bin/, tmp/, dist/)
	@echo "$(YELLOW)==> Cleaning build artifacts...$(RESET)"
	@rm -rf bin/ tmp/ dist/

.PHONY: clean-all
clean-all: clean ## Remove everything: bin/, tmp/, dist/, .bin/ tools, data/
	@echo "$(YELLOW)==> Cleaning tools and data...$(RESET)"
	@rm -rf .bin/ data/

##@ Tooling

# tools: bootstrap a fresh clone. Pre-fetches Go modules and installs pinned
# external binaries to .bin/. Optional — individual targets auto-install what
# they need lazily via the file-target rules below — but useful when you want
# everything ready up front (offline prep, CI image baking, etc.).
.PHONY: tools
tools: $(GOLANGCI_LINT) ## Install pinned tools and download Go modules
	@echo "$(CYAN)==> Downloading Go modules...$(RESET)"
	@go mod download
	@echo "$(GREEN)==> Tools in $(LOCAL_BIN); modules cached$(RESET)"

# File-target rules: only run when the versioned binary is missing. Bumping
# the *_VERSION variable changes the path and re-triggers the install.
#
# TODO: replace `curl | sh` with a SHA256-verified release tarball download.
# The upstream installer pipes a fetched script directly to shell, which is the
# same supply-chain risk class we reject in workflow files. See releases at
# https://github.com/golangci/golangci-lint/releases for binary tarballs +
# checksums.txt.
$(GOLANGCI_LINT):
	@echo "$(YELLOW)==> Downloading golangci-lint $(GOLANGCI_LINT_VERSION) for $(OS)_$(ARCH)...$(RESET)"
	@mkdir -p $(LOCAL_BIN)
	@curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b $(LOCAL_BIN) $(GOLANGCI_LINT_VERSION)
	@mv $(LOCAL_BIN)/golangci-lint $@
	@echo "$(GREEN)==> Installed: $@$(RESET)"
