# ==============================================================================
# WaveHouse Docs Site (Astro + Starlight)
# ==============================================================================
# Self-contained Makefile for the docs site. Run `make` from inside docs/ to
# drive Astro directly (install, dev server, build, preview).
#
# The root Makefile exposes every target here as `make docs-<name>` via a
# single pattern rule (`docs-%: ; @$(MAKE) -C docs $*`). Adding a target
# here makes it instantly available from root — no per-target wiring.

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

PNPM ?= pnpm

# ==============================================================================
# Targets
# ==============================================================================

##@ General

.PHONY: help
help: ## Show this help menu
	@printf "Usage: $(BOLD)make$(RESET) $(CYAN)<target>$(RESET)\n"
	@printf "       (run from docs/ — root Makefile exposes the same targets as \`make docs-<name>\`)\n"
	@awk 'BEGIN { FS = "## " } \
		/^##@/ { printf "\n$(YELLOW)%s$(RESET)\n", substr($$0, 5); next } \
		/^[a-zA-Z0-9_-]+:.*## / { \
			tname = $$0; sub(/:.*/, "", tname); \
			printf "  $(CYAN)%-18s$(RESET) %s\n", tname, $$2 \
		}' $(MAKEFILE_LIST)

##@ Setup

.PHONY: install
install: ## Install pnpm dependencies (frozen lockfile)
	@$(PNPM) install --frozen-lockfile

# Playwright Chromium (~130 MB) is required by `rehype-mermaid` (build-time
# diagram SSR) and `starlight-links-validator`. The `--with-deps` apt step
# needs sudo and is only useful on CI's minimal base images; gate on $CI so
# local laptops skip it. Both steps are idempotent.
#
# Hidden from help (no `##` annotation) — it's a transitive prereq of `dev` /
# `preview` / `build`, almost never invoked directly. Run explicitly with
# `make install-playwright` (or `make docs-install-playwright` from root)
# if you ever need to refresh the Chromium cache standalone.
.PHONY: install-playwright
install-playwright: install
	@$(PNPM) exec playwright install chromium $${CI:+--with-deps} >/dev/null

##@ Dev

.PHONY: dev
dev: install-playwright ## Hot-reload Astro dev server on :4321
	@$(PNPM) dev

.PHONY: preview
preview: install-playwright ## Preview the production build locally via wrangler (auto-builds if dist/ is missing)
	@# Skip the build if `dist/` already exists — running `make build` then
	@# `make preview` should serve the existing artifact, not rebuild. Re-run
	@# `make build` explicitly to refresh.
	@if [ ! -d dist ]; then \
	  echo "$(CYAN)==> No dist/ — building first...$(RESET)"; \
	  $(PNPM) build; \
	fi
	@$(PNPM) preview

##@ Build

.PHONY: build
build: install-playwright ## Build the docs site for production → dist/
	@echo "$(CYAN)==> Building docs site...$(RESET)"
	@$(PNPM) build

# Single-source-of-truth logo pipeline. Edit scripts/branding/mark.svg and run
# `make branding` (or `make docs-branding` from root) to regenerate favicon
# .{svg,ico}, apple-touch-icon.png, og.png, and the Starlight nav light/dark
# SVGs. Requires librsvg (rsvg-convert) + ImageMagick 7 (magick) — `brew install
# librsvg imagemagick`. Not a `build` prereq: derived assets are committed so
# contributors don't need the raster toolchain to build docs.
.PHONY: branding
branding: ## Regenerate logo/favicon/OG assets from scripts/branding/mark.svg
	@scripts/branding/generate.sh

# --- Common-verb contract ----------------------------------------------------
# The root Makefile's `subprojects-<verb>` fan-out expects every subproject to
# define `test`, `lint`, and `verify`. The docs site doesn't have automated
# tests / lints / type checks yet — these are no-ops for now. Replace with
# real recipes as the project grows (`astro check`, link validator standalone,
# accessibility checks, etc.). Hidden from help on purpose.
.PHONY: test lint verify
test lint verify:
	@:

##@ Cleanup

.PHONY: clean
clean: ## Remove build output (dist/)
	@echo "$(YELLOW)==> Cleaning docs dist/...$(RESET)"
	@rm -rf dist/

.PHONY: clean-tools
clean-tools: ## Remove node_modules/
	@echo "$(YELLOW)==> Cleaning docs node_modules/...$(RESET)"
	@rm -rf node_modules/
