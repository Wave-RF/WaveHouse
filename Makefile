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

# --- Quiet check runner -------------------------------------------------------
# $(call run,Label,command(s),hint) — run a check quietly: capture its combined
# output and print a single green "✓ Label" on success (output discarded), or a
# red "✗ Label" + the indented output (+ optional fix hint) on failure, then
# stop. Keeps `make verify` scannable: a column of ✓ when green, full detail
# only for whatever broke. Honors NO_COLOR (the color vars go empty above).
#
# Always pass all THREE args (omit the hint with a trailing comma) so
# --warn-undefined-variables stays quiet on $(3). Args cross make → shell, so:
# no commas inside an argument (commas delimit $(call) args); Label and hint are
# printf %s args — keep them plain text (no backticks / $(...) / quotes, which
# the shell would evaluate). For a `-j` fan-out, --output-sync=target keeps each
# leaf's ✓/✗ line intact.
define run
@out=$$({ $(2) ; } 2>&1) \
  && printf "  $(GREEN)✓$(RESET) %s\n" "$(1)" \
  || { printf "  $(RED)✗ %s$(RESET)\n" "$(1)"; printf '%s\n' "$$out" | sed 's/^/      /'; $(if $(3),printf "      $(YELLOW)→ %s$(RESET)\n" "$(3)";) exit 1; }
endef

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
#   JOBS=4                       parallel slots for the -j fan-outs (default: CPU count)
#
# Add binaries here as the project grows (e.g., wavehouse-api, wavehouse-worker).
BINARIES := wavehouse

TAGS ?=
ARGS ?=

# JOBS: parallel slots for the recursive `$(MAKE) -j` fan-outs (verify, ci,
# build-all, tools, fix). Default = CPU count (getconf _NPROCESSORS_ONLN) — "use
# whatever the machine has": a 4-core box runs -j4, a 16-core box -j16. It's
# self-limiting both ways: never more than the cores present (small runners
# aren't oversubscribed), and make never starts more jobs than there are ready
# targets anyway (8 for verify, ~14 for ci), so the effective width is the
# smaller of cores and leaves. NOTE: a bare `make -j` with NO number means
# *unlimited*, not "all cores" — it ignores the CPU count and would oversubscribe
# a small runner, which is why we pass an explicit count here. Override:
# `make ci JOBS=4`. Falls back to 4 if getconf is unavailable.
DEFAULT_JOBS := $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
JOBS ?= $(DEFAULT_JOBS)

# Version metadata is always embedded in the binary. Names match the package
# vars in cmd/wavehouse/main.go (Version / GitCommit / BuildTime), which the
# goreleaser config injects the same way for release artifacts.
# `--match 'v[0-9]*'` keeps this on the SERVER's tag family. Without it,
# `git describe` returns whichever tag is nearest in history — so once
# `clients/ts/v0.1.0` exists, `make build` stamps the SDK's version into the
# server binary and out through /version, and goreleaser-validate.yml logs
# "current tag is not semver". `--always` still covers the no-match case.
VERSION    ?= $(shell git describe --tags --match 'v[0-9]*' --dirty --always 2>/dev/null || echo dev)
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

# misspell: curated common-typo corrector + US/UK locale enforcer. Installed
# standalone to .bin/ (pure Go, `go install` — same pattern as air) so it can
# lint Markdown/MDX prose. DISTINCT from the misspell analyzer bundled inside
# golangci-lint, which only inspects Go source; same maintained fork
# (github.com/golangci/misspell), two entry points. Drives `make lint-prose`.
MISSPELL_VERSION := v0.8.0
MISSPELL         := $(LOCAL_BIN)/misspell-$(MISSPELL_VERSION)

# shellcheck: shell-script linter — the CI workflow logic lives in scripts/
# now, so it gates like any other source. Haskell binary (no `go install`
# path): official release tarball, checksum-verified by
# scripts/install-shellcheck.sh. The version is pinned THERE (with the
# per-platform checksums); this variable only names the installed file.
SHELLCHECK_VERSION := v0.11.0
SHELLCHECK         := $(LOCAL_BIN)/shellcheck-$(SHELLCHECK_VERSION)

# actionlint: GitHub Actions workflow linter (also shellchecks the inline
# `run:` blocks via $(SHELLCHECK)). Pure Go — go-install pattern like air.
ACTIONLINT_VERSION := v1.7.12
ACTIONLINT         := $(LOCAL_BIN)/actionlint-$(ACTIONLINT_VERSION)

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

.PHONY: dev-ts
dev-ts: pnpm-install ## Watch-build SDK (tsup --watch)
	@$(PNPM) --filter $(SDK_NAME) run dev

# dev-docs runs docs/scripts/dev.mjs: a full `astro build` on every save,
# synced into docs/dist/ and served through `wrangler dev --live-reload` —
# so the Worker (md twins, pagefind search, llm outputs) behaves exactly like
# production while you edit, and the browser refreshes itself per build.
# Slower per change than Astro HMR; the raw dev server remains available as
# `pnpm --filter wavehouse-docs run start` when fidelity doesn't matter.
# Serves :4321, walking upward if that's taken (ports are machine-wide, so a
# dev server in another worktree or repo will claim it) — the script prints the
# port it settled on. DOCS_PORT=… moves the starting point.
.PHONY: dev-docs
dev-docs: install-playwright-docs build-ts ## Prod-faithful docs dev loop: rebuild-on-save + wrangler dev on :4321 (next free port if busy)
	@$(PNPM) --filter $(DOCS_FILTER) run dev

# preview-docs serves the production build through wrangler (Cloudflare Workers
# preview), building docs/dist/ first if it's missing.
.PHONY: preview-docs
preview-docs: install-playwright-docs ## Wrangler preview of the docs production build (auto-builds if dist/ missing)
	@if [ ! -d $(DOCS_DIR)/dist ]; then \
		echo "$(CYAN)==> No docs/dist — building first...$(RESET)"; \
		$(MAKE) build-docs; \
	fi
	@$(PNPM) --filter $(DOCS_FILTER) run preview

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

# fmt / lint / fix: one Biome binary (biome.json) scans the whole workspace
# (SDK + e2e + docs); Markdown is owned separately by markdownlint-cli2 (rules in
# .markdownlint.json, globs in .markdownlint-cli2.jsonc) — Biome only does
# JS/TS/JSON. gofumpt + golangci-lint own the Go side.
#
# Each tool is its OWN target (fmt-go/fmt-ts, lint-go/lint-ts/lint-md) so
# `make verify` can fan them all out in parallel as verify-parallel leaves —
# gofumpt no longer waits behind Biome, golangci no longer behind markdownlint.
# The public `fmt` / `lint` targets are thin aggregates over those leaves, for
# muscle-memory and standalone use (`make -j lint` parallelizes them too). Each
# leaf depends on pnpm-install (TS) so Biome/markdownlint never run before
# node_modules exists.
#
# fmt = format only (quick): `biome format` + gofumpt. lint = `biome check
# --error-on-warnings` (format + lint + organize-imports) + markdownlint +
# golangci. `biome check` is the read-only inverse of fix's `biome check --write`,
# so `make verify` gates precisely what `make fix` would change; --error-on-
# warnings makes warn-level rules hard-fail (matching gofumpt/golangci). NOTE:
# Biome demotes style nits to non-blocking "info" by default — bump a rule to
# "warn"/"error" in biome.json (as for useTemplate) to gate it. verify runs
# `biome check` (lint-ts) but NOT `biome format` (fmt-ts): check already verifies
# formatting, so running both would double the Biome work.
.PHONY: fmt
fmt: fmt-go fmt-ts ## Check formatting across Go (gofumpt) + TS (Biome). Run `make fix` to apply.

.PHONY: fmt-go
fmt-go:
	$(call run,gofumpt (Go fmt),$(GOFUMPT) -l $(GO_DIRS) | (! grep .),run make fix to apply formatting)

.PHONY: fmt-ts
fmt-ts: pnpm-install
	$(call run,Biome (format),$(PNPM) -s -w run format,run make fix to apply formatting)

.PHONY: lint
lint: lint-go lint-ts lint-md lint-prose ## Lint across Go (golangci-lint) + TS/JSON (Biome) + Markdown (markdownlint) + docs prose (misspell). Run `make fix` to apply --fix.

.PHONY: lint-go
lint-go: $(GOLANGCI_LINT) go-mod-download
	$(call run,golangci-lint,$(GOLANGCI_LINT) run ./... --allow-parallel-runners,run make fix to auto-fix what is fixable)

.PHONY: lint-ts
lint-ts: pnpm-install
	$(call run,Biome (lint + format + imports),$(PNPM) -s -w run check,run make fix to auto-fix what is fixable)

.PHONY: lint-md
lint-md: pnpm-install
	$(call run,markdownlint,$(PNPM) -s -w run lint:md,run make fix to auto-fix what is fixable)

# lint-prose: docs prose quality, owned by misspell — a curated common-typo +
# US-locale (UK → US) checker over the canonical docs-prose set (see DOCS_PROSE
# and scripts/docs-prose.sh — the Starlight content plus the root governance
# docs). Its word list is finite and maintained upstream, so it gates with
# ~zero false positives and no project dictionary to babysit. `-error` makes it
# exit non-zero on findings; `make fix` (fix-prose) auto-applies the
# corrections. Distinct domain from markdownlint (*style*) and Biome
# (JS/TS/JSON) — no overlap. (A full
# dictionary spell-checker, cspell, was trialled and dropped: on these jargon-
# dense docs it flagged ~64 legitimate terms and zero real typos — an unbounded
# dictionary tax for no signal. Catching novel typos is left to human/LLM
# review, which can judge a word in context; a finite checker can't.)
.PHONY: lint-prose
lint-prose: $(MISSPELL)
	$(call run,misspell (US spelling),$(MISSPELL) -locale US -source text -error $(DOCS_PROSE),run make fix to auto-correct)

# lint-sh: shellcheck over every tracked shell script (scripts/, hooks,
# docs tooling). -x follows `source`d files; -P SCRIPTDIR resolves
# `# shellcheck source=` directives relative to the sourcing script, not
# the cwd. Lazily expanded so the ls-files only runs when the target does.
SHELL_SOURCES = $(shell git ls-files '*.sh')
.PHONY: lint-sh
lint-sh: $(SHELLCHECK)
	$(call run,shellcheck,$(SHELLCHECK) -x -P SCRIPTDIR $(SHELL_SOURCES),)

# lint-gha: actionlint over .github/workflows/*.yml — expression/type
# errors, action-input mismatches, SHA-pin syntax, and shellcheck (via the
# pinned $(SHELLCHECK)) on inline run: blocks.
.PHONY: lint-gha
lint-gha: $(ACTIONLINT) $(SHELLCHECK)
	$(call run,actionlint (workflows),$(ACTIONLINT) -shellcheck $(SHELLCHECK),)

# test-classify-paths: assert scripts/classify-paths.sh (the shared change
# classifier behind CI's `changes` job and the local git hooks) against the
# canonical change shapes — fast, dependency-free, so the allowlists can't
# silently regress. A verify leaf so CI's lint job runs it.
# test-md-rules: fixtures for the repo-local markdownlint rules. They rewrite
# every .md/.mdx on every agent write, and they classify by line shape with no
# parse tree, so an unrecognized construct is corrupted rather than skipped —
# cheap fixtures are the only thing that catches the next shape regression.
.PHONY: test-md-rules
test-md-rules: pnpm-install
	$(call run,markdownlint rule tests,node --test scripts/markdownlint-rules/rules.test.mjs,)

.PHONY: test-classify-paths
test-classify-paths:
	$(call run,classify-paths test,scripts/classify-paths.test.sh,)

# test-release-channel: assert scripts/ci/release-channel.sh — the single rule
# mapping a release tag to its moving channel (`:latest` / `@latest` vs
# `:rc` / `@rc` …), shared by release.yml, publish-npm.yml and release.sh.
# Covers the fail-closed cases too: an unclassifiable tag must never resolve to
# `latest`. A verify leaf, same as test-classify-paths.
.PHONY: test-release-channel
test-release-channel:
	$(call run,release-channel test,scripts/ci/release-channel.test.sh,)

.PHONY: vulncheck
vulncheck: go-mod-download ## Run govulncheck (V=1 for full call stacks)
ifdef V
	@echo "$(CYAN)==> Running govulncheck (verbose)...$(RESET)"
	@$(GOVULNCHECK) ./...
else
	$(call run,vulncheck,$(GOVULNCHECK) -scan package ./...,)
endif

# tidy: read-only check via `go mod tidy -diff` (Go 1.23+). Prints the
# unified diff that would be applied and exits non-zero if anything is off,
# without touching go.mod / go.sum. Safe to run in parallel with fmt/lint.
.PHONY: tidy
tidy: ## Verify go.mod/go.sum are tidy (run `make fix` to apply)
	$(call run,go.mod tidy,go mod tidy -diff,run make fix to tidy go.mod and go.sum)

# fix: apply auto-fixes everywhere, fanned out into three tracks that touch
# disjoint files — Go (.go + go.mod/sum), TS/JS/JSON (Biome), Markdown — so they
# run in parallel safely. Two of the three are themselves serial chains, because
# inside a track every step rewrites the same files: Go is tidy → gofumpt →
# goimports → golangci --fix (the formatters must settle before lint --fix), and
# Markdown is fix-md → fix-prose (markdownlint and misspell both write .md/.mdx,
# so running them concurrently is a lost-update race).
.PHONY: fix
fix: ## Apply auto-fixes across Go (tidy + gofumpt + goimports + lint --fix) + TS/JSON (Biome) + Markdown (markdownlint) + docs prose (misspell)
	@$(MAKE) -j $(JOBS) fix-go fix-ts fix-docs
	@echo "$(GREEN)==> Done$(RESET)"

# fix-docs: the Markdown track, serial. A wrapper, so `make fix-md` and
# `make fix-prose` still stand on their own. It names pnpm-install even though
# fix-md already does: fix-md is reached through a SUB-make, whose prerequisites
# the parent cannot dedup against fix-ts's, so without this `make -j fix` can
# run two `pnpm install` processes against one node_modules.
.PHONY: fix-docs
fix-docs: pnpm-install
	@$(MAKE) fix-md
	@$(MAKE) fix-prose

.PHONY: fix-go
fix-go: $(GOLANGCI_LINT)
	@echo "$(CYAN)==> Applying Go auto-fixes (tidy + gofumpt + goimports + lint --fix)...$(RESET)"
	@go mod tidy
	@$(GOFUMPT) -w $(GO_DIRS)
	@$(GOIMPORTS) -w $(GO_DIRS)
	@$(GOLANGCI_LINT) run --fix ./... --allow-parallel-runners

.PHONY: fix-ts
fix-ts: pnpm-install
	@echo "$(CYAN)==> Applying Biome fixes (format + lint + imports)...$(RESET)"
	@$(PNPM) -w run fix

# fix-md: the generic markdownlint --fix pass reaches **/*.md only and never
# .mdx — the config globs .md, and the .mdx glob lives on `lint:md`, so even a
# bare `markdownlint-cli2 --fix` is safe.
#
# That is the root fix for a whole class of corruption:
# markdownlint parses CommonMark, MDX does not, and where the two disagree a
# generic autofix rewrites the inside of a code block — de-indenting YAML
# comments it reads as headings, autolinking bare URLs. Reporting on that
# disagreement is useful (lint-md still checks .mdx); acting on it is not.
#
# .mdx therefore gets exactly one STRUCTURAL fixer, our own
# scripts/fix-mdx-fences.mjs — misspell still corrects spelling there, since its
# curated list needs no parse. That fixer only ever inserts a blank line next to
# a JSX tag, so its worst failure is a render-neutral blank line rather than
# rewritten code.
#
# The md pass runs twice because it is not a fixpoint in one: WH001's insert
# carries the pre-fix text of the lines it joins, so another rule's fix for a
# joined line is dropped on the first pass.
.PHONY: fix-md
fix-md: pnpm-install
	@echo "$(CYAN)==> Applying MDX structure + markdownlint fixes...$(RESET)"
	@$(PNPM) -w run fix:md

# fix-prose: misspell autofix (common typos + UK → US) over the docs prose. Its
# corrections come from a curated list and are unambiguous, so applying them
# wholesale is safe.
.PHONY: fix-prose
fix-prose: $(MISSPELL)
	@echo "$(CYAN)==> Applying misspell fixes (common typos + US spelling)...$(RESET)"
	@$(MISSPELL) -locale US -source text -w $(DOCS_PROSE)

# verify: all static checks across the repo, split into a parallel-safe leaf
# list (verify-parallel) and a thin wrapper that fans it out under `-j`, exactly
# like ci/ci-parallel. A bare `make verify` self-parallelizes instead of running
# serially — and because every tool is its OWN leaf, the long pole is the single
# slowest tool, not the slowest *group* (e.g. golangci no longer drags Biome +
# markdownlint along behind it).
#
# Leaves (13): tidy, fmt-go (gofumpt), lint-go (golangci), vulncheck on the Go
# side; lint-ts (biome check) + lint-md (markdownlint) + lint-prose (misspell,
# docs spelling) + test-md-rules (node --test over the WH001/WH002 fixtures)
# for JS/TS + Markdown + prose; lint-sh (shellcheck), lint-gha (actionlint) and
# test-classify-paths for the tooling;
# check-docs (astro check — the only leaf that writes, to docs/.astro/, and
# nothing else touches it) and typecheck-ts (tsc --noEmit). It runs lint-ts
# (`biome check`) but NOT fmt-ts (`biome format`) — check already covers
# formatting. ci-parallel depends on verify-parallel (not verify), so phase 1
# shares one jobserver instead of nesting `make -j` inside `make -j`.
.PHONY: verify
verify: ## Run all static checks across the repo (Go + TS + docs, parallelized)
	@printf "$(CYAN)==> Static checks$(RESET) (Go + TS + docs, -j $(JOBS)):\n"
	@$(MAKE) -j $(JOBS) verify-parallel || { printf "$(RED)$(BOLD)✗ Static checks failed$(RESET) — fix the ✗ above (often $(CYAN)make fix$(RESET)).\n"; exit 1; }
	@scripts/ci-marker.sh write-verify
	@printf "$(GREEN)$(BOLD)✔ All static checks passed$(RESET)\n"

.PHONY: verify-parallel
verify-parallel: tidy fmt-go lint-go lint-ts lint-md lint-prose lint-sh lint-gha test-classify-paths test-md-rules test-release-channel vulncheck check-docs typecheck-ts

# typecheck-ts: tsc --noEmit on the SDK. Its own target (was inline in verify's
# recipe) so it can run as a parallel leaf of verify-parallel.
.PHONY: typecheck-ts
typecheck-ts: pnpm-install
	$(call run,typecheck-ts (tsc --noEmit),$(PNPM) -s --filter $(SDK_NAME) run typecheck,)


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
# Symmetric with pnpm-install for the Node side.
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
# running tests. Recursive `$(MAKE) -j $(JOBS)` mirrors how `ci` forces
# parallelism on `ci-parallel` — typing `make build-all` gets parallel builds
# without requiring the user to remember `-j`.
.PHONY: build-all
build-all: ## Build all artifacts in parallel — Go binaries + SDK + docs site
	@echo "$(CYAN)==> Building all artifacts...$(RESET)"
	@$(MAKE) -j $(JOBS) build build-ts build-docs
	@echo "$(GREEN)$(BOLD)✔ All artifacts built$(RESET)"

# build-ts: pnpm-driven SDK build → clients/ts/dist/ (ESM + CJS + .d.ts).
# Required by test-e2e (e2e tests import the built artifact) and by
# build-all. Standalone via `make build-ts`.
.PHONY: build-ts
build-ts: pnpm-install ## Build TypeScript SDK → clients/ts/dist/
	@$(PNPM) --filter $(SDK_NAME) run build

# check-docs: astro check — type-checks .astro/.mdx, content-collection frontmatter
# schemas, and config TS. Catches what `astro build` does NOT (the build strips
# types without checking). No browser needed. A prereq of both `verify` and
# `build-docs`, so make runs it once and serializes it before the build — Astro's
# content-sync writes docs/.astro/, so check and build must not run concurrently.
# (Link validation is separate: owned by starlight-links-validator at build.)
# build-ts prereq: the landing page imports @wavehouse/sdk (workspace package),
# which resolves to clients/ts/dist/ for both types and the browser bundle.
.PHONY: check-docs
check-docs: pnpm-install build-ts ## Type-check the docs (astro check — types + content schemas)
	$(call run,check-docs (astro check),NODE_OPTIONS=--no-deprecation $(PNPM) -s --filter $(DOCS_FILTER) run check,)

# build-docs: Astro site → docs/dist/. Pulls in Chromium (install-playwright-docs)
# because rehype-mermaid renders diagrams via headless Chrome at build time
# (starlight-links-validator runs here too, but needs no browser). Depends on
# check-docs so a type/content error fails fast before the (heavier) build,
# and the two never race on .astro.
.PHONY: build-docs
build-docs: check-docs install-playwright-docs ## Build docs site → docs/dist/
	@echo "$(CYAN)==> Building docs site...$(RESET)"
	@$(PNPM) --filter $(DOCS_FILTER) run build

# branding-docs: regenerate logo/favicon/OG assets from the brand SVG.
# Not a `build-docs` prereq — derived assets are committed so contributors
# don't need rsvg, ImageMagick, resvg, or usvg to build docs (only someone
# iterating on the mark does). The script self-locates via git, so it runs the
# same from the repo root.
.PHONY: branding-docs
branding-docs: ## Regenerate docs logo/favicon/OG assets from docs/src/assets/branding/mark.svg
	@docs/scripts/branding/generate.sh

# --- Node workspace: SDK + docs ----------------------------------------------
# pnpm is the canonical package manager (migrated from npm). Locally available
# via PATH; CI installs/caches it via actions/cache + setup-node. Three pnpm
# packages share one workspace + store: the SDK (clients/ts/, @wavehouse/sdk),
# the E2E harness (tests/e2e/sdk/, wavehouse-e2e), and the docs site
# (docs/, wavehouse-docs). One root `pnpm install` installs all three.
#
# Everything is driven directly via `pnpm --filter <pkg>` — no per-subproject
# Makefiles. The user-facing Node targets live inline in their natural verb
# sections (build-ts/dev-ts/test-ts/clean-ts; build-docs/dev-docs/preview-docs/
# branding-docs/clean-docs) and declare pnpm-install as a prereq so a fresh
# clone or a changed lockfile is handled lazily. `make tools` does the full
# bootstrap.
#
# Not exposed as targets: ts fmt/lint/fix/verify (Biome is workspace-wide — see
# Code Quality); ts typecheck (runs inline inside `verify` via `tsc --noEmit`,
# the way Go's golangci-lint implicitly type-checks); ts codegen (the SDK's own
# published CLI, clients/ts/src/cli/codegen.ts — not a dev build step).
PNPM        ?= pnpm
DOCS_DIR    := docs
SDK_NAME    := @wavehouse/sdk
DOCS_FILTER := wavehouse-docs

# The canonical docs-prose set, from the one script that defines it (AGENTS.md
# §DRY). lint-prose / fix-prose hand misspell this explicit file list rather
# than a directory, so it never reads a .ts content-config as text — the
# script's extension filter enforces that now, where a local `find -name` used
# to. That `find` covered only docs/src/content, so README, CONTRIBUTING,
# SECURITY, SUPPORT, CODE_OF_CONDUCT and the SDK readme were being rewritten by
# the on-save hook's misspell pass but never checked by this gate.
#
# Recursively expanded (`=`, not `:=`), so the git call fires only inside the
# lint-prose / fix-prose recipes — `make help` never pays for it.
#
# One asymmetry to know about: the script lists TRACKED files (`git ls-files`),
# while the hook gates on `docs-prose.sh is-match`, a pure path test. So a
# brand-new page that hasn't been `git add`ed is fixed on write but not seen by
# `make fix-prose`. It self-heals — pre-commit stages first, so `make verify`
# does see it — but the symptom is a commit that fails on spelling right after
# a clean `make fix`. Widening the script to `git ls-files -co` would also feed
# untracked drafts to the docs-reviewer, which is why it lists tracked only.
DOCS_PROSE   = $(shell bash scripts/docs-prose.sh all 2>/dev/null)

# pnpm-install: hidden internal target. Node targets depend on it to ensure
# workspace deps are present; on a warm tree `--frozen-lockfile` is a fast
# no-op. No doc string → hidden from `make help`. --reporter=silent drops the
# "Scope / Already up to date / Done in Xms" chatter so it doesn't clutter the
# verify checklist; fatal errors (e.g. a lockfile mismatch) still print.
.PHONY: pnpm-install
pnpm-install:
	@$(PNPM) install --frozen-lockfile --reporter=silent

# install-playwright-docs: hidden helper — fetch the Chromium build the docs
# site needs (rehype-mermaid build-time SSR is the build's only browser use;
# the manual docs/scripts/screenshot.mjs helper drives the same install). It's
# ~130 MB, so it's lazy: only the docs build/dev/preview targets pull it in,
# never plain pnpm-install, so Go-only contributors don't pay for it. The
# --with-deps apt step needs sudo and only helps on CI's minimal images, so
# gate it on $CI. Both steps are idempotent. No doc string → hidden.
.PHONY: install-playwright-docs
install-playwright-docs: pnpm-install
	@$(PNPM) --filter $(DOCS_FILTER) exec playwright install chromium $${CI:+--with-deps} >/dev/null

##@ Test

# Each Go test target writes covdata to tmp/coverage/<suite>/data/ and
# hands off to .tools/cov for rendering + the threshold gate. Suite-specific
# coverage scope:
#   unit:         -cover (per-package — each test covers its own package)
#   integration:  -coverpkg=./... (cross-package — drives end-to-end paths)
#   e2e:          covdata flushed on SIGINT by the running cover binary,
#                 captured by the orchestrator
# Thresholds (per suite + total) live in .testcoverage.yml.
#
# COV_DEFER: when set (ci / test-all pass COV_DEFER=1 to the suite sub-makes),
# the per-suite test targets still COLLECT coverage but skip their inline
# render + gate — `make cov` (scripts/cov report) then renders ONE consolidated
# report and applies every gate at the end, so a full run prints a single
# coverage block instead of a render after each suite. Unset (standalone
# `make test-e2e` etc.) renders + gates inline as before. Exported so the
# vitest configs can drop their console table under the same flag.
COV_DEFER ?=
export COV_DEFER

.PHONY: test-unit
test-unit: go-mod-download ## Run Go unit tests + render coverage + gate threshold
	@printf "$(CYAN)==> Running Unit Tests...$(RESET)\n"
	@rm -rf $(COV_UNIT)/data && mkdir -p $(COV_UNIT)/data
	@GOCOVERDIR="$(CURDIR)/$(COV_UNIT)/data" go tool gotestsum --format $(GOTESTSUM_FMT) -- \
		-tags="$(TAGS)" -cover -race -timeout 15s ./internal/... ./cmd/... $(ARGS) \
		-args -test.gocoverdir="$(CURDIR)/$(COV_UNIT)/data"
	@if [ -z "$(COV_DEFER)" ]; then go run ./scripts/cov render unit; fi

# Hidden alias: `make test` matches `go test ./...` muscle memory; test-unit
# is the explicit form.
.PHONY: test
test: test-unit

.PHONY: test-integration
test-integration: go-mod-download ## Run Go integration tests + render coverage + gate threshold (requires Docker)
	@printf "$(CYAN)==> Running Integration Tests...$(RESET)\n"
	@rm -rf $(COV_INT)/data && mkdir -p $(COV_INT)/data
	@GOCOVERDIR="$(CURDIR)/$(COV_INT)/data" go tool gotestsum --format $(GOTESTSUM_FMT) -- \
		-tags="integration $(TAGS)" -timeout 240s -coverpkg=./... -race -count=1 \
		./tests/integration/... $(ARGS) \
		-args -test.gocoverdir="$(CURDIR)/$(COV_INT)/data"
	@if [ -z "$(COV_DEFER)" ]; then go run ./scripts/cov render integration; fi

# test-e2e starts ClickHouse + bin/wavehouse-cov via the orchestrator under
# scripts/, then runs the SDK vitest harness against the live stack so both
# halves are exercised. Coverage is collected on both sides (Go covdata
# from the cover binary → tmp/coverage/e2e/data/; vitest v8 coverage of
# the SDK source → tmp/coverage/ts-e2e/) — same "always coverage" pattern
# as the Go test targets. `make cov` merges ts-unit + ts-e2e after.
#
.PHONY: test-e2e
test-e2e: build-ts build-cover ## Run E2E SDK suite against cover binary + render coverage + gate
	@printf "$(CYAN)==> Running E2E Tests...$(RESET)\n"
	@rm -rf $(COV_E2E)/data tmp/coverage/ts-e2e
	@mkdir -p $(COV_E2E)/data tmp/coverage/ts-e2e tmp
	@TS_E2E_COVERAGE_DIR="$(CURDIR)/tmp/coverage/ts-e2e" \
		go run ./scripts/orchestrator
	@if [ -z "$(COV_DEFER)" ]; then go run ./scripts/cov render e2e; fi

# test-ts: vitest unit tests for the SDK, always with v8 coverage. Standalone
# it also gates against suites.ts-unit (via vitest's --coverage.thresholds);
# under COV_DEFER it only collects, leaving the gate to `make cov` (cov report)
# so CI emits one consolidated coverage block. THRESHOLD is read live from
# .testcoverage.yml via scripts/cov; override with
# `make test-ts ARGS='--coverage.thresholds.statements=70'`.
.PHONY: test-ts
test-ts: pnpm-install ## Run SDK vitest unit tests + coverage + gate against suites.ts-unit
	@printf "$(CYAN)==> Running SDK unit tests...$(RESET)\n"
	@rm -rf tmp/coverage/ts-unit && mkdir -p tmp/coverage/ts-unit
	@TS_UNIT_COVERAGE_DIR="$(CURDIR)/tmp/coverage/ts-unit" \
		$(PNPM) --filter $(SDK_NAME) exec vitest run --coverage \
		$(if $(COV_DEFER),,--coverage.thresholds.statements=$$(go run ./scripts/cov threshold ts-unit)) $(ARGS)
	@if [ -z "$(COV_DEFER)" ]; then printf "$(GREEN)==> ts-unit gate passed$(RESET)  HTML: tmp/coverage/ts-unit/index.html\n"; fi

# Aggregator: recipe-based with $(MAKE) calls so suites run sequentially even
# under `make -j N`. The suites bind ports / spin testcontainers / start the
# release binary, so concurrent execution is unsafe.
.PHONY: test-all
test-all: ## Run all suites sequentially + one consolidated Go + TS coverage report + gates
	@$(MAKE) test-unit COV_DEFER=1
	@$(MAKE) test-ts COV_DEFER=1
	@$(MAKE) test-integration COV_DEFER=1
	@$(MAKE) test-e2e COV_DEFER=1
	@$(MAKE) cov

##@ Coverage

# cov: renders ONE consolidated coverage report (scripts/cov report) — every
# suite that has data plus the merged Go-total and ts-total, each with its
# gate status + a clickable HTML report path, then a single aggregate
# pass/fail. Generates whatever artifacts it needs, so it stands alone at the
# end of `make ci` / `make test-all` even when the suites ran under COV_DEFER
# (collect-only). Standalone `make cov` is "show me the numbers without
# re-running tests." Fails if NO suite has data (a stray `make cov`).
.PHONY: cov
# go-mod-download is not optional here even though `go run ./scripts/cov`
# would fetch what it needs on its own. CI's coverage job shares the
# unsuffixed gomod-v1 cache with every other ci.yml Go job (via
# .github/actions/setup-env), and all of them race to save it on a key
# rotation. `go run` populates only the modules that one program imports,
# so if this job won that race it would store a PARTIAL ~/go/pkg/mod under
# the shared key — which then exact-hits for every other job, forever,
# until the next rotation. Downloading the full graph first keeps the
# shared entry complete whoever wins. See #443.
cov: go-mod-download ## Consolidated coverage report (Go + TS) + gate against thresholds (auto-runs after test-all / ci)
	@go run ./scripts/cov report

##@ CI

# ci-parallel: hidden — the parallel-safe leaves. No `## ` doc comment so it
# stays out of `make help`; users invoke `make ci`, not this directly.
# Everything is listed explicitly (no subproject fan-out): `verify-parallel`
# already spans Go + TS (Biome + tsc), and the build/test leaves are few enough
# that an explicit list reads clearer than an abstraction. Depends on
# verify-parallel (the leaves), NOT verify (the `-j` wrapper), so phase 1 runs
# under one jobserver rather than nesting `make -j` inside `make -j`. The verify
# marker that standalone `make verify` writes is instead written by ci's own
# `ci-marker.sh write` below — it touches both the ci and verify markers.
.PHONY: ci-parallel
ci-parallel: verify-parallel build build-cover build-ts build-docs test test-ts

.PHONY: ci
ci: ## Full pipeline — parallel checks, then sequential heavy suites + coverage
	@echo "$(CYAN)==> Phase 1: Parallel Build & Static Checks$(RESET)"
	@$(MAKE) -j $(JOBS) ci-parallel COV_DEFER=1
	@echo "$(CYAN)==> Phase 2: Sequential Heavy Tests$(RESET)"
	@$(MAKE) test-integration COV_DEFER=1
	@$(MAKE) test-e2e COV_DEFER=1
	@$(MAKE) cov
	@scripts/ci-marker.sh write
	@echo "$(GREEN)$(BOLD)✔ All CI checks passed$(RESET)"

##@ Release

# Releases are cut by pushing ONE tag — no version bump, no commit, no release
# PR. Every component derives its version from the tag it was built at (the
# server via GoReleaser's ldflags, the Go SDK because a module's version simply
# IS its tag, @wavehouse/sdk because publish-npm.yml stamps package.json from
# the tag before publishing). The `main` ruleset forbids direct pushes anyway,
# so a bump commit would need its own reviewed PR before every release.
#
# scripts/release.sh holds the preflight checks — on main, clean tree, synced
# with origin, tag free on both sides, CI green on this exact commit — and
# prints what will be published before prompting. DRY_RUN=1 stops after the
# plan. Full walkthrough: docs/src/content/docs/development.md §Cutting a release.

# VERSION is `?=`-defaulted to a git-describe string for build stamping, so it
# is NEVER empty — a forgotten `VERSION=` would reach release.sh as something
# like `1064a4fe-dirty` and fail with a confusing semver error instead of a
# usage message. $(origin) is what distinguishes "passed on the command line"
# from "defaulted above".
define require_release_version
@[ "$(origin VERSION)" = "command line" ] || { \
	printf '$(RED)✗$(RESET) usage: make $@ VERSION=X.Y.Z  (bare semver, no leading v)\n' >&2; exit 1; }
endef

.PHONY: release-server
release-server: ## Tag a server release — binaries + container image (VERSION=X.Y.Z)
	$(require_release_version)
	@scripts/release.sh server "$(VERSION)"

.PHONY: release-sdk-ts
release-sdk-ts: ## Tag a TypeScript SDK release — npm (VERSION=X.Y.Z)
	$(require_release_version)
	@scripts/release.sh ts "$(VERSION)"

.PHONY: release-sdk-go
release-sdk-go: ## Tag a Go SDK release — go get (VERSION=X.Y.Z)
	$(require_release_version)
	@scripts/release.sh go "$(VERSION)"

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
clean: ## Remove build artifacts (bin/, dist/, clients/ts/dist/, docs/dist/, docs/.dev-dist/)
	@echo "$(YELLOW)==> Cleaning build artifacts...$(RESET)"
	@rm -rf bin/ dist/ clients/ts/dist/ docs/dist/ docs/.dev-dist/

.PHONY: clean-ts
clean-ts: ## Remove SDK build artifacts only (clients/ts/dist/)
	@echo "$(YELLOW)==> Cleaning SDK build artifacts...$(RESET)"
	@rm -rf clients/ts/dist/

.PHONY: clean-docs
clean-docs: ## Remove docs build artifacts only (docs/dist/, docs/.dev-dist/)
	@echo "$(YELLOW)==> Cleaning docs dist/...$(RESET)"
	@rm -rf $(DOCS_DIR)/dist/ $(DOCS_DIR)/.dev-dist/

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
#   - Installs pinned external binaries to .bin/ (golangci-lint, air, misspell).
#   - Downloads Go modules so go.mod tool deps are available offline.
#   - Installs SDK + E2E pnpm deps so test-ts / test-e2e are runnable
#     without a separate manual setup step.
#
# Note: go.mod tool deps (gotestsum, gofumpt, etc.) are *downloaded* by
# `go mod download` but compile lazily on first `go tool <name>` invocation.
# Go's build cache makes subsequent invocations near-instant. If you need
# them pre-compiled (offline CI image baking), run them once with --help.
.PHONY: tools
tools: ## Install pinned tools, Go modules, pnpm deps, and git hooks
	@# The installs are independent — fan them out under -j (golangci-lint
	@# download ∥ air ∥ misspell ∥ shellcheck ∥ actionlint ∥ go-mod-download
	@# ∥ pnpm-install). Go's module cache is concurrency-safe, so this is
	@# just faster on a cold clone.
	@$(MAKE) -j $(JOBS) $(GOLANGCI_LINT) $(AIR) $(MISSPELL) $(SHELLCHECK) $(ACTIONLINT) go-mod-download pnpm-install
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

# misspell installs cleanly via `go install` (pure Go), GOBIN-pinned to .bin/
# like air. cmd/misspell is the CLI entry point of the golangci fork — the same
# codebase golangci-lint vendors as a library for its Go-only misspell linter.
$(MISSPELL):
	@echo "$(YELLOW)==> Installing misspell $(MISSPELL_VERSION) for $(OS)_$(ARCH)...$(RESET)"
	@mkdir -p $(LOCAL_BIN)
	@GOBIN=$(LOCAL_BIN) go install github.com/golangci/misspell/cmd/misspell@$(MISSPELL_VERSION)
	@mv $(LOCAL_BIN)/misspell $@
	@echo "$(GREEN)==> Installed: $@$(RESET)"

# shellcheck is a Haskell binary — fetched as the official release tarball
# and checksum-verified; version + per-platform sha256 live in the script.
$(SHELLCHECK):
	@scripts/install-shellcheck.sh $@

$(ACTIONLINT):
	@echo "$(YELLOW)==> Installing actionlint $(ACTIONLINT_VERSION) for $(OS)_$(ARCH)...$(RESET)"
	@mkdir -p $(LOCAL_BIN)
	@GOBIN=$(LOCAL_BIN) go install github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	@mv $(LOCAL_BIN)/actionlint $@
	@echo "$(GREEN)==> Installed: $@$(RESET)"
