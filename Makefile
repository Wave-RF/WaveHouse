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

# Serialize recipe output per-target whenever make is running in parallel.
# Without this, `make -j N` interleaves stdout/stderr from concurrent
# recipes line-by-line, and a failure (e.g. `make[1]: *** [lint] Error 1`)
# scrolls off-screen behind whichever ✓ output happened to land last.
MAKEFLAGS += --output-sync=target

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

# air is the hot-reload runner used by `make dev`. We install it to .bin/
# rather than as a go.mod tool directive because air's transitive deps
# (hugo, godartsass, sass libs) are heavy enough to bloat go.sum on every
# `go mod download` — same exclusion principle as golangci-lint.
AIR_VERSION := v1.65.1
AIR         := $(LOCAL_BIN)/air-$(AIR_VERSION)

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
# Per-suite and total thresholds live in .testcoverage.yml

# Derived: coverage-instrumented and release-stripped binaries.
COVER_BINARIES   := $(addsuffix -cov,$(BINARIES))
RELEASE_BINARIES := $(addsuffix -release,$(BINARIES))

# --- Exported to recipes ------------------------------------------------------
# All variables consumed by scripts/* live here. `export` in Make is global —
# applying these per-section would imply scoping that doesn't exist.
export VERSION_LDFLAGS LDFLAGS TAGS
export GOTESTSUM_FMT

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

##@ Dev

# All dev / dependency targets share one Compose file. Aliasing the full
# `docker compose -f ...` invocation keeps recipes terse and lets us swap
# the path or compose binary in one place if it ever moves (e.g. to support
# `podman compose`).
DEV_COMPOSE_FILE := deployments/compose/dependencies.yaml
DEV_COMPOSE      := docker compose -f $(DEV_COMPOSE_FILE)

CONFIG_FILES = .config.local.yaml # .policy.local.yaml
# This strips the leading '.' and trailing '.local.yaml' to find the base name,
# then appends '.yaml' to find the source file.
# TODO: if we add a validate subcommand to the binary we could test that here too
$(CONFIG_FILES): .%.local.yaml: %.yaml
	@if [ ! -f $@ ]; then \
		echo "⚙️  Creating local config: $@ from $<..."; \
		cp $< $@; \
	fi

.PHONY: dev
dev: deps-up $(AIR) $(CONFIG_FILES) ## Hot-reload dev server: ClickHouse + WaveHouse via air on :8080
	@echo "$(CYAN)==> Starting WaveHouse with air hot-reload (Ctrl+C to stop)$(RESET)"
	@echo "    WaveHouse:  $(GREEN)http://localhost:8080$(RESET)  (CORS=*, auth disabled by default)"
	@echo "    ClickHouse: $(GREEN)http://localhost:8123$(RESET)  (HTTP), $(GREEN)localhost:9000$(RESET) (native)"
	WH_CONFIG=.config.local.yaml $(AIR) -c .air.toml

# Docs site dev/preview servers — long-running, blocking. Astro dev defaults
# to :4321; `wrangler dev` (preview) defaults to :8787, so both coexist with
# `make dev` on :8080.
.PHONY: dev-docs
dev-docs: install-docs-playwright ## Hot-reload docs site dev server (Astro on :4321)
	@cd $(DOCS_DIR) && $(PNPM) dev

.PHONY: preview-docs
preview-docs: install-docs-playwright ## Preview the production docs build locally (auto-builds if dist/ is missing)
	@# Skip the build if `dist/` already exists — running `make build-docs`
	@# and then `make preview-docs` should serve the existing artifact, not
	@# rebuild from scratch. Re-run `make build-docs` explicitly to refresh.
	@if [ ! -d $(DOCS_DIR)/dist ]; then \
	  echo "$(CYAN)==> No dist/ — building docs first...$(RESET)"; \
	  cd $(DOCS_DIR) && $(PNPM) build; \
	fi
	@cd $(DOCS_DIR) && $(PNPM) preview

# `up -d --wait` blocks until the compose healthcheck transitions to
# healthy, so callers can chain on success without a polling loop. The
# ClickHouse healthcheck (in $(DEV_COMPOSE_FILE)) probes /ping on :8123.
.PHONY: deps-up
deps-up: ## Start ClickHouse (idempotent; blocks until healthy)
	@echo "$(CYAN)==> Starting ClickHouse...$(RESET)"
	@$(DEV_COMPOSE) up -d --wait clickhouse

.PHONY: deps-down
deps-down: ## Stop ClickHouse (preserves data volume)
	@echo "$(YELLOW)==> Stopping ClickHouse...$(RESET)"
	@$(DEV_COMPOSE) down

.PHONY: deps-logs
deps-logs: ## Tail ClickHouse logs (Ctrl+C to detach; container keeps running)
	@$(DEV_COMPOSE) logs -f clickhouse

.PHONY: deps-shell
deps-shell: ## Open a clickhouse-client REPL on the running container
	@$(DEV_COMPOSE) exec clickhouse clickhouse-client

.PHONY: deps-wipe
deps-wipe: ## Stop ClickHouse AND destroy its data volume (DESTRUCTIVE — use to reset state)
	@echo "$(RED)==> Wiping ClickHouse (containers + volumes)...$(RESET)"
	@$(DEV_COMPOSE) down -v --remove-orphans

##@ Observability

.PHONY: obs-aspire
obs-aspire: ## Start local Aspire Dashboard (clean UI for Traces, Metrics, Logs)
	@scripts/otel/aspire.sh

.PHONY: obs-grafana
obs-grafana: ## Start local Grafana LGTM stack (advanced correlation & UI)
	@scripts/otel/grafana.sh

.PHONY: obs-front
obs-front: ## Start local OTel Front UI
	@scripts/otel/otel-front.sh

##@ Code Quality

# Dynamically find all directories containing Go files, safely ignoring hidden folders like .worktrees
GO_DIRS := $(shell go list -f '{{.Dir}}' ./...)

.PHONY: fmt
fmt: ## Check formatting (run `make fix` to apply)
	@if ! OUT=$$($(GOFUMPT) -l $(GO_DIRS)); then \
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
	@$(GOFUMPT) -w $(GO_DIRS)
	@$(GOIMPORTS) -w $(GO_DIRS)
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

# build-all: umbrella for "compile every artifact this repo produces" without
# running tests. Recursive `$(MAKE) -j 4` mirrors how `ci` forces parallelism
# on `ci-parallel` — typing `make build-all` gets parallel builds without
# requiring the user to remember `-j`.
.PHONY: build-all
build-all: ## Build all artifacts in parallel — Go binaries + SDK + docs site
	@echo "$(CYAN)==> Building all artifacts...$(RESET)"
	@$(MAKE) -j 4 build build-sdk build-docs
	@echo "$(GREEN)$(BOLD)✔ All artifacts built$(RESET)"

# --- TypeScript SDK build / install ------------------------------------------
# pnpm is the canonical package manager (migrated from npm). Locally available
# via PATH; CI installs/caches it via actions/cache + setup-node. Three pnpm
# projects share a single store: the SDK (clients/ts/), the E2E test harness
# (tests/e2e/sdk/), and the docs site (docs/).
PNPM        ?= pnpm
SDK_DIR     := clients/ts
E2E_SDK_DIR := tests/e2e/sdk
DOCS_DIR    := docs

# install-sdk + install-e2e-sdk + install-docs are intermediate prereqs — they
# have no doc comment so they don't show in `make help`. User-facing targets
# below (build-sdk, test-sdk, test-e2e, build-docs, dev-docs, preview-docs)
# depend on them.
.PHONY: install-sdk
install-sdk:
	@cd $(SDK_DIR) && $(PNPM) install --frozen-lockfile

.PHONY: install-docs
install-docs:
	@cd $(DOCS_DIR) && $(PNPM) install --frozen-lockfile

# Playwright Chromium (~130 MB) is required by `rehype-mermaid` (build-time
# diagram SSR) and `starlight-links-validator`. Kept out of `install-docs` so
# the top-level `make tools` bootstrap doesn't force every Go-only contributor
# to download Chromium. Wired into `build-docs` / `dev-docs` instead. The
# `--with-deps` apt step is $CI-gated since it needs sudo on Linux laptops;
# CI runners on minimal base images need it. Both steps are idempotent. See
# docs/src/content/docs/development.md for the full Linux-dev-machine story.
.PHONY: install-docs-playwright
install-docs-playwright: install-docs
	@cd $(DOCS_DIR) && $(PNPM) exec playwright install chromium $${CI:+--with-deps} >/dev/null

.PHONY: install-e2e-sdk
install-e2e-sdk:
	@cd $(E2E_SDK_DIR) && $(PNPM) install --frozen-lockfile

.PHONY: build-sdk
build-sdk: install-sdk ## Build TypeScript SDK → clients/ts/dist/ (required by E2E imports)
	@echo "$(CYAN)==> Building SDK...$(RESET)"
	@cd $(SDK_DIR) && $(PNPM) build

# Docs site (Astro + Starlight) lives in docs/. Markdown sources are the
# Starlight content collection itself — no separate convert step.
.PHONY: build-docs
build-docs: install-docs-playwright ## Build docs site for production → docs/dist/
	@echo "$(CYAN)==> Building docs site...$(RESET)"
	@cd $(DOCS_DIR) && $(PNPM) build

# Single-source-of-truth logo pipeline. Edit scripts/branding/mark.svg (the
# path) and run `make branding` to regenerate favicon.{svg,ico},
# apple-touch-icon.png, og.png, and the Starlight nav light/dark SVGs.
# Requires librsvg (rsvg-convert) + ImageMagick 7 (magick) — `brew install
# librsvg imagemagick`. Not part of `build-docs`: derived assets are
# committed so contributors don't need the raster toolchain to build docs.
.PHONY: branding
branding: ## Regenerate all logo/favicon/OG assets from scripts/branding/mark.svg
	@scripts/branding/generate.sh

##@ Test

# Each Go test target writes covdata to tmp/coverage/<suite>/data/ and
# hands off to .tools/cov for rendering + the threshold gate. Suite-specific
# coverage scope:
#   unit:         -cover (per-package — each test covers its own package)
#   integration:  -coverpkg=./... (cross-package — drives end-to-end paths)
#   e2e:          covdata flushed on SIGINT by the running cover binary,
#                 captured by the orchestrator
# Thresholds (per suite + total) live in .testcoverage.yml.

.PHONY: test-unit
test-unit: go-mod-download ## Run Go unit tests + render coverage + gate threshold
	@printf "$(CYAN)==> Running Unit Tests...$(RESET)\n"
	@rm -rf $(COV_UNIT)/data && mkdir -p $(COV_UNIT)/data
	@GOCOVERDIR="$(CURDIR)/$(COV_UNIT)/data" go tool gotestsum --format $(GOTESTSUM_FMT) -- \
		-tags="$(TAGS)" -cover -race -timeout 15s ./internal/... ./cmd/... $(ARGS) \
		-args -test.gocoverdir="$(CURDIR)/$(COV_UNIT)/data"
	@go run ./scripts/cov render unit

# Hidden alias: `make test` matches `go test ./...` muscle memory; test-unit
# is the explicit form.
.PHONY: test
test: test-unit

.PHONY: test-integration
test-integration: go-mod-download ## Run Go integration tests + render coverage + gate threshold (requires Docker)
	@printf "$(CYAN)==> Running Integration Tests...$(RESET)\n"
	@rm -rf $(COV_INT)/data && mkdir -p $(COV_INT)/data
	@GOCOVERDIR="$(CURDIR)/$(COV_INT)/data" go tool gotestsum --format $(GOTESTSUM_FMT) -- \
		-tags="integration $(TAGS)" -timeout 120s -coverpkg=./... -race -count=1 \
		./tests/integration/... $(ARGS) \
		-args -test.gocoverdir="$(CURDIR)/$(COV_INT)/data"
	@go run ./scripts/cov render integration

# test-sdk: vitest unit tests for the SDK. SDK_COVERAGE_DIR points vitest
# at tmp/coverage/sdk/ (see clients/ts/vitest.config.ts), and the threshold
# is read from .testcoverage.yml's `suites.sdk` and passed to vitest's
# inline gate. Independent of Go — separate toolchain.
.PHONY: test-sdk
test-sdk: install-sdk ## Run SDK vitest unit tests + render coverage + gate threshold
	@printf "$(CYAN)==> Running SDK Tests...$(RESET)\n"
	@rm -rf tmp/coverage/sdk && mkdir -p tmp/coverage/sdk
	@SDK_COVERAGE_DIR="$(CURDIR)/tmp/coverage/sdk" \
		pnpm --dir clients/ts exec vitest run --coverage \
		--coverage.thresholds.statements=$$(go run ./scripts/cov threshold sdk) $(ARGS)
	@printf "$(GREEN)==> sdk gate passed$(RESET)  HTML: tmp/coverage/sdk/index.html\n"

# test-e2e starts ClickHouse + bin/wavehouse-cov via the orchestrator under
# scripts/, then runs the SDK vitest harness against the live stack so both
# halves are exercised. covdata flushes on SIGINT into tmp/coverage/e2e/data/.
.PHONY: test-e2e
test-e2e: build-sdk build-cover install-e2e-sdk ## Run E2E SDK suite against cover binary + render coverage + gate
	@printf "$(CYAN)==> Running E2E Tests...$(RESET)\n"
	@rm -rf $(COV_E2E)/data && mkdir -p $(COV_E2E)/data tmp
	@go run ./scripts/orchestrator
	@go run ./scripts/cov render e2e

# Aggregator: recipe-based with $(MAKE) calls so suites run sequentially even
# under `make -j N`. The suites bind ports / spin testcontainers / start the
# release binary, so concurrent execution is unsafe.
.PHONY: test-all
test-all: ## Run all suites sequentially + merged Go coverage + gate
	@$(MAKE) test-unit
	@$(MAKE) test-sdk
	@$(MAKE) test-integration
	@$(MAKE) test-e2e
	@$(MAKE) cov

##@ Coverage

# cov: merges whichever Go suites have covdata into tmp/coverage/total/,
# prints a per-suite breakdown (plus SDK as informational), and gates
# against threshold.total in .testcoverage.yml. Does NOT run tests — call
# the test targets first (or run `make test-all` for the full pipeline).
.PHONY: cov
cov: ## Merge all available covdata + gate against total threshold
	@go run ./scripts/cov merge

##@ CI

# ci-parallel: hidden — the parallel-safe leaves. No `## ` doc comment so
# it stays out of `make help`; users invoke `make ci`, not this directly.
.PHONY: ci-parallel
ci-parallel: verify build build-cover build-sdk build-docs test test-sdk

.PHONY: ci
ci: ## Full pipeline — parallel checks, then sequential heavy suites + coverage
	@echo "$(CYAN)==> Phase 1: Parallel Build & Static Checks$(RESET)"
	@$(MAKE) -j 4 ci-parallel
	@echo "$(CYAN)==> Phase 2: Sequential Heavy Tests$(RESET)"
	@$(MAKE) test-integration
	@$(MAKE) test-e2e
	@$(MAKE) cov
	@# Marker file for the pre-push git hook: confirms `make ci` passed for the
	@# tree state being pushed. tmp/ is gitignored. Keyed by **tree SHA**
	@# (working dir snapshot at the moment ci ran), not commit SHA, so the
	@# common flow `make ci → git add → git commit → git push` doesn't
	@# spuriously invalidate the marker when the post-commit tree matches what
	@# ci just validated. Re-edit the tree → re-run ci.
	@#
	@# We compute the tree the same way `git add -A && git write-tree` would,
	@# but against a throwaway index seeded from HEAD — so the real index and
	@# working dir are untouched. (`git stash create` was the obvious choice but
	@# it ignores untracked files unless something is also tracked-and-changed,
	@# which gives the wrong answer for "new .go file + make ci + commit + push".)
	@mkdir -p tmp
	@tmp_idx=$$(mktemp); \
	  trap "rm -f '$$tmp_idx'" EXIT; \
	  git read-tree --reset HEAD --index-output="$$tmp_idx" >/dev/null 2>&1; \
	  GIT_INDEX_FILE="$$tmp_idx" git add -A >/dev/null 2>&1; \
	  tree_sha=$$(GIT_INDEX_FILE="$$tmp_idx" git write-tree); \
	  touch "tmp/ci-passed-tree-$$tree_sha"
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

# Tiered clean targets, each scoped to a single artifact class. Compose them
# explicitly (e.g. `make clean clean-test`) when you want a partial reset
# without nuking installed tools or pnpm deps. `clean-all` chains them.
#
#   clean        build outputs only       (bin/, dist/, clients/ts/dist/)
#   clean-test   test outputs only        (tmp/ — coverage, logs, NATS state)
#   clean-tools  installed deps           (.bin/, node_modules/ everywhere)
#   clean-all    everything above + data/ + docker volumes (full reset)

.PHONY: clean
clean: ## Remove build artifacts (bin/, dist/, clients/ts/dist/, docs/dist/)
	@echo "$(YELLOW)==> Cleaning build artifacts...$(RESET)"
	@rm -rf bin/ dist/ clients/ts/dist/ docs/dist/

.PHONY: clean-test
clean-test: ## Remove test artifacts (tmp/ — coverage data, logs, NATS state)
	@echo "$(YELLOW)==> Cleaning test artifacts...$(RESET)"
	@rm -rf tmp/

.PHONY: clean-tools
clean-tools: ## Remove installed tools and pnpm deps (.bin/, node_modules/)
	@echo "$(YELLOW)==> Cleaning installed tools and pnpm deps...$(RESET)"
	@rm -rf .bin/ clients/ts/node_modules/ tests/e2e/sdk/node_modules/ docs/node_modules/

.PHONY: clean-all
clean-all: clean clean-test clean-tools ## Full reset — clean + clean-test + clean-tools + dev data + docker volumes
	@echo "$(YELLOW)==> Full reset (dev state, docker)...$(RESET)"
	@rm -rf data/
	@$(DEV_COMPOSE) down -v --remove-orphans 2>/dev/null || true
	@docker compose -f tests/e2e/compose.yaml down -v --remove-orphans 2>/dev/null || true
	@# Clean up any orphaned standalone observability containers
	@docker rm -f aspire-dashboard otel-lgtm otel-front 2>/dev/null || true

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
tools: $(GOLANGCI_LINT) $(AIR) go-mod-download install-sdk install-e2e-sdk install-docs ## Install pinned tools, Go modules, pnpm deps, and git hooks
	@# Install team-wide git hooks via core.hooksPath. Idempotent — running
	@# `make tools` repeatedly just re-asserts the config. The .githooks/
	@# directory is committed; this line plumbs git to it. Users can opt out
	@# locally by unsetting the config (`git config --unset core.hooksPath`).
	@git config core.hooksPath .githooks
	@echo "$(GREEN)==> Tools cached; Go modules + pnpm packages installed; git hooks active (.githooks/)$(RESET)"
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

# air installs cleanly via `go install` (pure Go, no shell-piping). GOBIN
# pins the install location to our .bin/ rather than the user's $GOPATH/bin.
$(AIR):
	@echo "$(YELLOW)==> Installing air $(AIR_VERSION) for $(OS)_$(ARCH)...$(RESET)"
	@mkdir -p $(LOCAL_BIN)
	@GOBIN=$(LOCAL_BIN) go install github.com/air-verse/air@$(AIR_VERSION)
	@mv $(LOCAL_BIN)/air $@
	@echo "$(GREEN)==> Installed: $@$(RESET)"
