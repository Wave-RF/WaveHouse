# ==============================================================================
# WaveHouse TypeScript SDK (@wavehouse/sdk)
# ==============================================================================
# Self-contained Makefile for the SDK package. Run `make` from inside
# clients/ts/ to build, test, format, etc. The root Makefile exposes
# every target here as `make ts-<name>` via a pattern rule, mirroring
# how docs/Makefile is wired up.
#
# Note: pnpm workspaces handle dependency resolution at the repo root.
# `make install` from here runs the same `pnpm install --frozen-lockfile`
# that root would — pnpm walks up to find pnpm-workspace.yaml and installs
# every workspace package in one pass.

SHELL := /usr/bin/env bash
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-print-directory
.DELETE_ON_ERROR:
.DEFAULT_GOAL := help

# --- Environment Detection ----------------------------------------------------
CI       ?=
NO_COLOR ?=

# --- Colors -------------------------------------------------------------------
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

PNPM     ?= pnpm
SDK_NAME := @wavehouse/sdk

# Coverage threshold passed in by the root Makefile (`ts-test` derives
# it from .testcoverage.yml via Go's scripts/cov). Defaults to 0 so a
# standalone `make test-cov` here works for ad-hoc local runs.
THRESHOLD ?= 0

# ==============================================================================
# Targets
# ==============================================================================

##@ General

.PHONY: help
help: ## Show this help menu
	@printf "Usage: $(BOLD)make$(RESET) $(CYAN)<target>$(RESET)\n"
	@printf "       (run from clients/ts/ — root Makefile exposes the same targets as \`make ts-<name>\`)\n"
	@awk 'BEGIN { FS = "## " } \
		/^##@/ { printf "\n$(YELLOW)%s$(RESET)\n", substr($$0, 5); next } \
		/^[a-zA-Z0-9_-]+:.*## / { \
			tname = $$0; sub(/:.*/, "", tname); \
			printf "  $(CYAN)%-18s$(RESET) %s\n", tname, $$2 \
		}' $(MAKEFILE_LIST)

##@ Setup

.PHONY: install
install: ## Install workspace dependencies (frozen lockfile)
	@$(PNPM) install --frozen-lockfile

##@ Build

.PHONY: build
build: install ## Build SDK → dist/ (ESM + CJS + .d.ts)
	@printf "$(CYAN)==> Building SDK...$(RESET)\n"
	@$(PNPM) --filter $(SDK_NAME) run build

.PHONY: dev
dev: install ## Watch-build SDK (tsup --watch)
	@$(PNPM) --filter $(SDK_NAME) run dev

.PHONY: codegen
codegen: install ## Run SDK codegen
	@$(PNPM) --filter $(SDK_NAME) run codegen

##@ Test

.PHONY: typecheck
typecheck: install ## Run tsc --noEmit
	@$(PNPM) --filter $(SDK_NAME) run typecheck

# test: vitest unit tests, always with v8 coverage — matches Go's pattern
# where `make test` / `make test-integration` / `make test-e2e` always
# include coverage. For tight no-coverage dev iteration, run vitest
# directly: `pnpm --filter $(SDK_NAME) exec vitest run` (or :watch).
#
# THRESHOLD gates statement coverage; defaults to 0 (vacuous) when run
# standalone. The root Makefile's `ts-test` overrides it with the value
# from .testcoverage.yml (via scripts/cov) so CI gates real numbers.
.PHONY: test
test: install ## Run vitest unit tests with v8 coverage (THRESHOLD=<pct> to gate)
	@$(PNPM) --filter $(SDK_NAME) exec vitest run --coverage \
		--coverage.thresholds.statements=$(THRESHOLD)

##@ Code Quality

# fmt/lint/fix delegate to root scripts because Biome's config lives at
# the workspace root and its file globs cover SDK + e2e together — running
# them from anywhere in the workspace produces the same result.
.PHONY: fmt
fmt: install ## Check formatting (run `make fix` to apply)
	@printf "$(CYAN)==> Checking TypeScript formatting...$(RESET)\n"
	@$(PNPM) -w run format

.PHONY: lint
lint: install ## Run Biome lint
	@printf "$(CYAN)==> Running Biome lint...$(RESET)\n"
	@$(PNPM) -w run lint

.PHONY: fix
fix: install ## Apply Biome lint + format auto-fixes
	@printf "$(CYAN)==> Applying Biome fixes...$(RESET)\n"
	@$(PNPM) -w run fix
	@printf "$(GREEN)==> Done$(RESET)\n"

.PHONY: verify
verify: fmt lint ## Run all static checks (format + lint)
	@printf "$(GREEN)==> SDK static checks passed$(RESET)\n"

##@ Cleanup

.PHONY: clean
clean: ## Remove dist/
	@$(PNPM) --filter $(SDK_NAME) run clean
