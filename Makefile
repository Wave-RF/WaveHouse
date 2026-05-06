# ==============================================================================
# WaveHouse Build System
# ==============================================================================

# --- Make Configuration -------------------------------------------------------
# Pin recipes to bash. /bin/sh is dash on Debian/Ubuntu and a thin bash on
# macOS — pinning avoids subtle portability bugs (arrays, $'...', [[ ]], etc.).
# Recipes that need strict-mode safety set it themselves; the scripts under
# scripts/ all start with `set -euo pipefail`.
SHELL := /usr/bin/env bash

# Warn on undefined variables — catches typos like $(BINAIRES) silently expanding to empty.
MAKEFLAGS += --warn-undefined-variables

# Suppress "Entering directory '...'" / "Leaving directory '...'" around every
# $(MAKE) recursion. The `ci` target alone makes 4 recursive calls; without
# this, output gets noisy fast (especially under --output-sync where each
# pair brackets a target's whole log block).
MAKEFLAGS += --no-print-directory

ifdef CI
  MAKEFLAGS += --output-sync=target
endif

# Delete the target file if its recipe fails non-zero. Default Make leaves
# a partial file behind, which then satisfies the dependency check on the
# next run and silently ships stale/broken output. Most of our recipes are
# .PHONY (so this is a no-op for them), but the file-target rules — like
# the golangci-lint installer at $(GOLANGCI_LINT) — benefit, and any future
# file-target rule gets the safety property for free.
.DELETE_ON_ERROR:

.DEFAULT_GOAL := help

# --- Environment Detection ----------------------------------------------------
# NO_COLOR is read by the colors block below; CI is read by scripts/size.sh
# to suppress the browser auto-open. Both are set by GitHub Actions et al.
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
#   V=1                          verbose test output
#   TAGS="foo bar"               Go build/test tags (space-separated)
#   ARGS="-run TestX"            extra flags passed to `go test`
#   NO_COLOR=1                   disable colored output
#   VERSION=v1.2.3               override version string embedded in binary
#   LDFLAGS="-s -w"              extra ldflags (e.g. force-strip a local build)
#   LIMIT=50                     top-N for `make dep-cut`
#   COV_THRESHOLD_<SUITE>=NN     per-suite coverage gate; see Coverage Thresholds
#                                section below for defaults and full list
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
# One path per suite. Internal layout (managed by scripts/coverage.sh):
#   $(COV_X)/data/         binary covdata (covmeta.* / covcounters.*)
#   $(COV_X)/coverage.txt  rendered textfmt profile
#   $(COV_X)/coverage.html rendered HTML report
COV_UNIT  := tmp/coverage/unit
COV_INT   := tmp/coverage/integration
COV_E2E   := tmp/coverage/e2e
COV_TOTAL := tmp/coverage/total

# --- Coverage Thresholds ------------------------------------------------------
# Per-suite + total thresholds enforced after each test suite runs (and by
# `make cov` for the merged total). Override via env or CLI:
#   COV_THRESHOLD_UNIT=80 make test
COV_THRESHOLD_UNIT        ?= 70
COV_THRESHOLD_INTEGRATION ?= 30
COV_THRESHOLD_E2E         ?= 10
COV_THRESHOLD_TOTAL       ?= 70

# Derived: coverage-instrumented and release-stripped binaries.
COVER_BINARIES   := $(addsuffix -cov,$(BINARIES))
RELEASE_BINARIES := $(addsuffix -release,$(BINARIES))

# --- Exported to recipes ------------------------------------------------------
# All variables consumed by scripts/* live here. `export` in Make is global —
# applying these per-section would imply scoping that doesn't exist.
export VERSION_LDFLAGS LDFLAGS TAGS
export GOTESTSUM_FMT
export COV_THRESHOLD_UNIT COV_THRESHOLD_INTEGRATION COV_THRESHOLD_E2E COV_THRESHOLD_TOTAL

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
	@if ! OUT=$$($(GOFUMPT) -l .); then \
		echo "$(RED)==> gofumpt failed$(RESET)"; \
		exit 1; \
	fi; \
	if [ -n "$$OUT" ]; then \
		echo "$(RED)==> Files not formatted:$(RESET)"; \
		echo "$$OUT"; \
		echo "Run $(CYAN)make fix$(RESET) to apply."; \
		exit 1; \
	fi
	@echo "$(GREEN)==> Formatting OK$(RESET)"

.PHONY: lint
lint: $(GOLANGCI_LINT) go-mod-download ## Run golangci-lint (run `make fix` to apply --fix)
	@echo "$(CYAN)==> Running linters...$(RESET)"
	@$(GOLANGCI_LINT) run ./...

.PHONY: vulncheck
vulncheck: go-mod-download ## Run govulncheck (V=1 for full call stacks)
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

.PHONY: verify
verify: tidy fmt vulncheck lint ## Run all static checks (parallel-safe: `make -j verify`)
	@echo "$(GREEN)==> All static checks passed$(RESET)"

##@ Build

# All three variants dispatch to scripts/build.sh, which knows how to handle
# debug / release / cover and prints a uniform "✔ <output> (<time>s, <size>)"
# status line. Per-binary parallelism is preserved via the $(BINARIES) /
# $(RELEASE_BINARIES) / $(COVER_BINARIES) target lists — `make -j build`
# would dispatch them concurrently if BINARIES grows.
#
# VERSION_LDFLAGS / LDFLAGS / TAGS are exported to recipes near the top.
#
# go-mod-download is a no-doc intermediate target — every Go-toolchain target
# (build/test/lint variants) declares it as a prereq so `make -j` doesn't
# kick off N parallel `go mod download` calls racing on the module cache.
# Symmetric with install-sdk for the Node side.
.PHONY: go-mod-download
go-mod-download:
	@go mod download

.PHONY: $(BINARIES)
$(BINARIES): go-mod-download
	@scripts/build.sh $@ debug

.PHONY: build
build: $(BINARIES) ## Compile all binaries with debug symbols → bin/<name>

# Hidden alias — `build` is debug by default, but `build-debug` reads more
# obviously when paired with `build-release` in scripts or contributor docs.
.PHONY: build-debug
build-debug: build

# Release variants build alongside (not overwriting) the debug binaries, so
# `make size` can compare the two without recompiling between targets.
.PHONY: $(RELEASE_BINARIES)
$(RELEASE_BINARIES): %-release: go-mod-download
	@scripts/build.sh $* release

.PHONY: build-release
build-release: $(RELEASE_BINARIES) ## Compile all binaries stripped → bin/<name>-release

# Coverage-instrumented variants (`wavehouse-cov`): used by E2E to capture
# coverage from the running binary.
.PHONY: $(COVER_BINARIES)
$(COVER_BINARIES): %-cov: go-mod-download
	@scripts/build.sh $* cover

.PHONY: build-cover
build-cover: $(COVER_BINARIES) ## Compile all binaries with coverage instrumentation → bin/<name>-cov

# --- TypeScript SDK build / install ------------------------------------------
# pnpm is the canonical package manager (migrated from npm). Locally available
# via PATH; CI installs/caches it via actions/cache + setup-node. Both the SDK
# (clients/ts/) and the E2E test harness (tests/e2e/sdk/) use pnpm.
PNPM        ?= pnpm
SDK_DIR     := clients/ts
E2E_SDK_DIR := tests/e2e/sdk

# install-sdk + install-e2e-sdk are intermediate prereqs — they have no doc
# comment so they don't show in `make help`. The user-facing targets below
# (build-sdk, test-sdk, test-e2e) depend on them.
.PHONY: install-sdk
install-sdk:
	@cd $(SDK_DIR) && $(PNPM) install --frozen-lockfile

.PHONY: install-e2e-sdk
install-e2e-sdk:
	@cd $(E2E_SDK_DIR) && $(PNPM) install --frozen-lockfile

.PHONY: build-sdk
build-sdk: install-sdk ## Build TypeScript SDK → clients/ts/dist/ (required by E2E imports)
	@echo "$(CYAN)==> Building SDK...$(RESET)"
	@cd $(SDK_DIR) && $(PNPM) build

# build-docker: build the WaveHouse Dockerfile. No run, no test — just
# proves the Dockerfile compiles. CI's e2e job formerly built this image too;
# now that test-e2e uses bin/wavehouse-cov locally, this target gives PRs a
# light Dockerfile-validity check independent of E2E.
.PHONY: build-docker
build-docker: ## Build WaveHouse Docker image (Dockerfile sanity check)
	@echo "$(CYAN)==> Building production Docker image...$(RESET)"
	@docker buildx build --load -t wavehouse:test -f deployments/Dockerfile .
	@echo "$(GREEN)==> Image built: wavehouse:test$(RESET)"

##@ Test

# Each test target hands off to scripts/test-suite.sh, which runs the suite,
# writes covdata to tmp/coverage/<suite>/data/, and gates against
# COV_THRESHOLD_<SUITE> via scripts/coverage.sh. Per-suite specifics (build
# tags, package globs, gotestsum format) live in test-suite.sh. Threshold +
# format env vars are exported to recipes near the top.

.PHONY: test
test: go-mod-download ## Run unit tests + render coverage + gate threshold
	@scripts/test-suite.sh unit $(ARGS)

# Hidden alias — symmetry with test-integration / test-e2e for muscle memory.
.PHONY: test-unit
test-unit: test

.PHONY: test-integration
test-integration: go-mod-download ## Run integration tests + render coverage + gate threshold (requires Docker)
	@scripts/test-suite.sh integration $(ARGS)

# test-sdk runs vitest unit tests inside clients/ts. Independent of any Go
# work — separate toolchain, separate runner pod in CI.
.PHONY: test-sdk
test-sdk: install-sdk ## Run SDK vitest unit tests
	@echo "$(CYAN)==> Running SDK Tests...$(RESET)"
	@cd $(SDK_DIR) && $(PNPM) test

# test-e2e starts ClickHouse via compose, runs bin/wavehouse-cov locally
# (with auth enabled) so coverage is captured natively, then runs the SDK
# vitest harness against it. Depends on:
#   - build-sdk: clients/ts/dist/ (E2E tests import @wavehouse/sdk via file: link)
#   - build-cover: bin/wavehouse-cov (the running binary; coverage flushes on SIGINT)
#   - install-e2e-sdk: tests/e2e/sdk/node_modules/
.PHONY: test-e2e
test-e2e: build-sdk build-cover install-e2e-sdk ## Run E2E SDK suite against cover binary + render coverage + gate
	@scripts/test-suite.sh e2e $(ARGS)

# Aggregator: recipe-based with $(MAKE) calls so suites run sequentially even
# under `make -j N`. The suites bind ports / spin testcontainers / start the
# release binary, so concurrent execution is unsafe.
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
	@scripts/coverage.sh merge $(COV_THRESHOLD_TOTAL)

##@ CI

# ci-parallel: hidden — the parallel-safe leaves. No `## ` doc comment so
# it stays out of `make help`; users invoke `make ci`, not this directly.
.PHONY: ci-parallel
ci-parallel: verify build build-cover build-sdk build-docker test test-sdk

.PHONY: ci
ci: ## Full pipeline — parallel checks, then sequential heavy suites + coverage
	@echo "$(CYAN)==> Phase 1: Parallel Build & Static Checks$(RESET)"
	@$(MAKE) -j 4 ci-parallel
	@echo "$(CYAN)==> Phase 2: Sequential Heavy Tests$(RESET)"
	@$(MAKE) test-integration
	@$(MAKE) test-e2e
	@$(MAKE) cov
	@echo "$(GREEN)$(BOLD)✔ All CI checks passed$(RESET)"

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
	@CGO_ENABLED=1 go list -deps -f '{{if .CgoFiles}}  ⚠ {{.ImportPath}}  ({{len .CgoFiles}} C files){{end}}' ./cmd/...
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
	@scripts/size.sh

# dep-cut: surfaces packages with few dependents (low InDegree) that drag in
# heavy transitive weight — i.e. the best candidates to remove or replace.
# Override the default top-N with `make dep-cut LIMIT=50`.
LIMIT ?= 30
.PHONY: dep-cut
dep-cut: ## Top cuttable dependencies by transitive weight (LIMIT=N to override)
	@LIMIT='$(LIMIT)' scripts/dep-cut.sh

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
	@rm -rf bin/ tmp/ dist/ clients/ts/dist/

.PHONY: clean-all
clean-all: clean ## Full reset — also wipes .bin/ tools AND data/ dev state
	@echo "$(YELLOW)==> Full reset (tools, dev data, docker)...$(RESET)"
	@rm -rf .bin/ data/ tests/e2e/sdk/.setup-state.json clients/ts/node_modules/ tests/e2e/sdk/node_modules/
	@docker compose -f tests/e2e/compose.yaml down -v --remove-orphans

##@ Tooling

# tools: bootstrap a fresh clone.
#   - Installs pinned external binaries to .bin/ (currently just golangci-lint).
#   - Downloads Go modules so go.mod tool deps are available offline.
#   - Installs SDK + E2E pnpm deps so test-sdk / test-e2e are runnable
#     without a separate manual setup step.
#
# Note: go.mod tool deps (gotestsum, gofumpt, etc.) are *downloaded* by
# `go mod download` but compile lazily on first `go tool <name>` invocation.
# Go's build cache makes subsequent invocations near-instant. If you need
# them pre-compiled (offline CI image baking), run them once with --help.
.PHONY: tools
tools: $(GOLANGCI_LINT) go-mod-download install-sdk install-e2e-sdk ## Install pinned tools, Go modules, and pnpm deps
	@echo "$(GREEN)==> Tools cached; Go modules + pnpm packages installed$(RESET)"
	@echo "    (go.mod tool deps compile on first \`go tool <name>\` invocation)"

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
