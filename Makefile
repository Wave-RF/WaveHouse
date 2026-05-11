# ── Pinned tools (versions locked in go.mod tool directives) ─────
GOTESTSUM := go run gotest.tools/gotestsum
GOFUMPT   := go run mvdan.cc/gofumpt
GOIMPORTS := go run golang.org/x/tools/cmd/goimports

# ── External tool versions (for `make tools`) ─────────────────────
GOLANGCI_LINT_VERSION := v2.11.4

# Build tags (e.g., TAGS="scylla dynamodb")
TAGS ?=

# Analysis output row limit (e.g., LIMIT=50 make dep-cut)
LIMIT ?= 30

# ── Build Configuration ───────────────────────────────────────────
# Strip debugging symbols to reduce binary size (~30%).
# Use `make build-debug` for delve/profiling builds with full symbols.
LDFLAGS := -s -w

# Define the targets
BINARIES := wavehouse wavehouse-api wavehouse-worker

# Verbose test output: V=1 make test
ifdef V
  GOTESTSUM_FMT := standard-verbose
else
  GOTESTSUM_FMT := testdox
endif

# Colors
GREEN  := \033[32m
YELLOW := \033[33m
CYAN   := \033[36m
RED    := \033[31m
RESET  := \033[0m

.PHONY: help setup tools check-tools build build-debug dev \
        fmt fmt-check lint lint-fix fix \
        test test-integration test-all ci coverage coverage-enforce \
        test-sdk test-e2e test-e2e-dev test-e2e-setup test-everything \
        smoke-test mod-tidy-check \
        docker compose-standalone compose-clustered compose-deps deps-wipe \
        clean release-test \
        vulncheck security deadcode audit-cgo \
        size-report size-tree size-treemap dep-graph dep-why dep-cut binary-analysis \
        docs-install docs-convert docs-dev docs-build docs-preview

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(RESET) %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────────
setup: ## Download Go modules and cache tools
	@echo "$(GREEN)==> Downloading modules...$(RESET)"
	@go mod download
	@echo "$(GREEN)==> Done$(RESET)"

tools: ## Install external dev tools (golangci-lint, air, goreleaser)
	@echo "$(GREEN)==> Installing external tools...$(RESET)"
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "$(CYAN)  Installing golangci-lint $(GOLANGCI_LINT_VERSION)...$(RESET)"; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	else \
		echo "$(CYAN)  golangci-lint already installed: $$(golangci-lint version --short 2>/dev/null || echo 'unknown')$(RESET)"; \
	fi
	@if ! command -v air >/dev/null 2>&1; then \
		echo "$(CYAN)  Installing air (hot-reload)...$(RESET)"; \
		go install github.com/air-verse/air@latest; \
	else \
		echo "$(CYAN)  air already installed$(RESET)"; \
	fi
	@if ! command -v goreleaser >/dev/null 2>&1; then \
		echo "$(CYAN)  Installing goreleaser...$(RESET)"; \
		go install github.com/goreleaser/goreleaser/v2@latest; \
	else \
		echo "$(CYAN)  goreleaser already installed$(RESET)"; \
	fi
	@echo "$(GREEN)==> All tools installed$(RESET)"

check-tools: ## Verify all required tools are installed
	@echo "$(CYAN)Go tools (pinned in go.mod — no install needed):$(RESET)"
	@echo "  gotestsum:  $$($(GOTESTSUM) --version 2>/dev/null || echo 'available via go run')"
	@echo "  gofumpt:    $$($(GOFUMPT) -version 2>/dev/null || echo 'available via go run')"
	@echo "  goimports:  available via go run"
	@echo ""
	@echo "$(CYAN)External tools:$(RESET)"
	@printf "  golangci-lint: "; command -v golangci-lint >/dev/null 2>&1 && echo "$(GREEN)✔$(RESET) $$(golangci-lint version --short 2>/dev/null)" || echo "$(RED)✗ not installed (run 'make tools')$(RESET)"
	@printf "  air:           "; command -v air >/dev/null 2>&1 && echo "$(GREEN)✔$(RESET)" || echo "$(YELLOW)✗ optional — needed for 'make dev'$(RESET)"
	@printf "  goreleaser:    "; command -v goreleaser >/dev/null 2>&1 && echo "$(GREEN)✔$(RESET)" || echo "$(YELLOW)✗ optional — needed for 'make release-test'$(RESET)"
	@printf "  docker:        "; command -v docker >/dev/null 2>&1 && echo "$(GREEN)✔$(RESET)" || echo "$(YELLOW)✗ optional — needed for integration tests & Docker builds$(RESET)"

# ── Build ─────────────────────────────────────────────────────────
build: $(BINARIES) ## Compile all binaries (run `make -j3 build` for parallel)

# This is a "Pattern Rule". It tells Make how to build ANY of the binaries in the list.
$(BINARIES):
	@echo "$(GREEN)==> Building $@...$(RESET)"
	@START=$$(date +%s); \
	CGO_ENABLED=0 go build -tags="$(TAGS)" -ldflags="$(LDFLAGS)" -o bin/$@ ./cmd/$@; \
	END=$$(date +%s); \
	SIZE=$$(ls -lh bin/$@ | awk '{print $$5}'); \
	printf "$(GREEN)✔$(RESET) $(CYAN)%-25s$(RESET) built in %ss (Size: $(YELLOW)%s$(RESET))\n" "$@" "$$((END - START))" "$$SIZE"

build-debug: LDFLAGS= ## Compile all binaries with debug symbols (for delve/profiling)
build-debug: build

# ── Development ───────────────────────────────────────────────────
dev: ## Hot-reload dev server (starts ClickHouse via tests/compose.yaml + air)
	@command -v air >/dev/null 2>&1 || { \
		echo "$(RED)==> air not found$(RESET)"; \
		echo "Install: https://github.com/air-verse/air"; \
		echo "  brew install air                     (macOS)"; \
		echo "  go install github.com/air-verse/air@latest"; \
		exit 1; \
	}
	@echo "$(GREEN)==> Starting ClickHouse...$(RESET)"
	@docker compose -f tests/compose.yaml up -d clickhouse
	@echo "$(GREEN)==> Applying SQL fixtures...$(RESET)"
	@for f in tests/fixtures/*.sql; do \
		curl -sf http://localhost:8123 -d @"$$f" >/dev/null 2>&1 || true; \
	done
	@echo "$(GREEN)==> Starting air (hot-reload)...$(RESET)"
	@air -c .air.toml

# ── Formatting ────────────────────────────────────────────────────
fmt: ## Format code (gofumpt + goimports)
	@echo "$(GREEN)==> Formatting...$(RESET)"
	@$(GOFUMPT) -w .
	@$(GOIMPORTS) -w .

fmt-check: ## Verify code formatting (no changes, exits non-zero if unformatted)
	@OUTPUT=$$($(GOFUMPT) -l .); \
	if [ -n "$$OUTPUT" ]; then \
		echo "$(RED)==> Files not formatted:$(RESET)"; \
		echo "$$OUTPUT"; \
		echo "Run '$(CYAN)make fmt$(RESET)' and commit."; \
		exit 1; \
	fi
	@echo "$(GREEN)==> Formatting OK$(RESET)"

# ── Linting ───────────────────────────────────────────────────────
lint: ## Run golangci-lint
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "$(RED)==> golangci-lint not found$(RESET)"; \
		echo "Install: https://golangci-lint.run/welcome/install/"; \
		echo "  brew install golangci-lint          (macOS)"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"; \
		exit 1; \
	}
	@golangci-lint run ./...

lint-fix: ## Run golangci-lint with --fix to auto-correct issues
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "$(RED)==> golangci-lint not found$(RESET)"; \
		echo "Install: https://golangci-lint.run/welcome/install/"; \
		echo "  brew install golangci-lint          (macOS)"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"; \
		exit 1; \
	}
	@golangci-lint run --fix ./...

fix: ## Auto-format and auto-fix linters
	@$(MAKE) fmt
	@$(MAKE) lint-fix

# ── Testing ───────────────────────────────────────────────────────
# Verbose output:     V=1 make test
# Extra flags:        make test ARGS="-run TestFoo"
# Both:               V=1 make test ARGS="-run TestFoo"
test: ## Unit tests with race detector
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="$(TAGS)" ./internal/... -race $(ARGS)

test-integration: ## Integration tests (requires Docker)
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="integration $(TAGS)" -timeout 120s ./tests/... -race -count=1 $(ARGS)

test-all: ## Unit + integration tests
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="$(TAGS)" ./internal/... -race $(ARGS)
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="integration $(TAGS)" -timeout 120s ./tests/... -race -count=1 $(ARGS)

coverage: ## Unit test coverage → tmp/coverage/ and summary
	@mkdir -p tmp/coverage
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="$(TAGS)" ./internal/... -race -coverprofile=tmp/coverage/coverage.txt -covermode=atomic $(ARGS)
	@go tool cover -html=tmp/coverage/coverage.txt -o tmp/coverage/coverage.html
	@echo "$(GREEN)==> Coverage Summary:$(RESET)"
	@go tool cover -func=tmp/coverage/coverage.txt | tail -n 1 | awk '{print "  Total Coverage: $(CYAN)" $$3 "$(RESET)"}'
	@echo "$(YELLOW)==> Open tmp/coverage/coverage.html in your browser for line-by-line details$(RESET)"

coverage-enforce: coverage ## Fail if unit test coverage is below the 70% threshold in .testcoverage.yml
	@go run github.com/vladopajic/go-test-coverage/v2@v2.18.7 --config=.testcoverage.yml

smoke-test: ## Manual Bento insert+delete (requires running WaveHouse)
	@go run ./tests/cmd/bento_pub

# ── SDK & E2E Testing ─────────────────────────────────────────────
test-sdk: ## Run SDK unit tests (clients/ts)
	@cd clients/ts && npm test

test-e2e-setup: ## Install E2E test dependencies
	@cd tests/sdk && npm install

test-e2e: ## Run E2E integration tests via SDK (auto-starts Docker if needed)
	@cd tests/sdk && npm install --silent && npx vitest run

test-e2e-dev: ## Run E2E integration tests in watch mode
	@cd tests/sdk && npm install --silent && npx vitest

test-everything: test test-sdk test-integration test-e2e ## Run ALL tests: Go unit + SDK unit + Go integration + E2E

mod-tidy-check: ## Verify go.mod and go.sum are tidy
	@echo "$(GREEN)==> Checking module tidiness...$(RESET)"
	@go mod tidy
	@git diff --exit-code go.mod go.sum || { echo "$(RED)==> go.mod/go.sum is not tidy. Run 'go mod tidy' and commit.$(RESET)"; exit 1; }
	@echo "$(GREEN)==> Modules OK$(RESET)"

# ── CI ────────────────────────────────────────────────────────────
ci: ## Full CI check: tidy + fmt + lint + vulncheck + build + tests
	@echo "$(YELLOW)==> Running full CI check...$(RESET)"
	@echo ""
	@echo "$(GREEN)── Step 1/7: Module tidiness ──$(RESET)"
	@go mod tidy
	@git diff --exit-code go.mod go.sum || { echo "$(RED)==> go.mod/go.sum is not tidy.$(RESET)"; exit 1; }
	@echo ""
	@echo "$(GREEN)── Step 2/7: Formatting ──$(RESET)"
	@OUTPUT=$$($(GOFUMPT) -l .); test -z "$$OUTPUT" || { echo "$(YELLOW)Files not formatted:$(RESET)"; echo "$$OUTPUT"; echo "Run 'make fmt' and commit."; exit 1; }
	@echo ""
	@echo "$(GREEN)── Step 3/7: Linting ──$(RESET)"
	@command -v golangci-lint >/dev/null 2>&1 || { echo "$(RED)==> golangci-lint not found. Run 'make tools' first.$(RESET)"; exit 1; }
	@golangci-lint run ./...
	@echo ""
	@echo "$(GREEN)── Step 4/7: Vulnerability check ──$(RESET)"
	@go run golang.org/x/vuln/cmd/govulncheck@latest -scan package ./...
	@echo ""
	@echo "$(GREEN)── Step 5/7: Build ──$(RESET)"
	@CGO_ENABLED=0 $(MAKE) build
	@echo ""
	@echo "$(GREEN)── Step 6/7: Unit tests ──$(RESET)"
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="$(TAGS)" ./internal/... -race
	@echo ""
	@echo "$(GREEN)── Step 7/7: Integration tests ──$(RESET)"
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="integration $(TAGS)" -timeout 120s ./tests/... -race -count=1
	@echo ""
	@echo "$(GREEN)==> All CI checks passed! ✔$(RESET)"

# ── Docker ────────────────────────────────────────────────────────
docker: ## Build Docker image for your local machine's architecture
	@docker build -f deployments/Dockerfile -t wavehouse:latest .

compose-standalone: ## Start standalone via Docker Compose
	docker compose -f deployments/compose/standalone.yaml up -d

compose-clustered: ## Start clustered via Docker Compose
	docker compose -f deployments/compose/clustered.yaml up -d

compose-deps: ## Start infrastructure dependencies
	docker compose -f deployments/compose/dependencies.yaml up -d

deps-wipe: ## Destroy and recreate dependencies
	docker compose -f deployments/compose/dependencies.yaml down -v --remove-orphans
	docker compose -f deployments/compose/dependencies.yaml up -d --force-recreate

clean: ## Remove bin/, tmp/, data/, dist/
	@rm -rf bin/ tmp/ data/ dist/

# ── Releases ──────────────────────────────────────────────────────
release-test: ## Test cross-compiling all binaries via GoReleaser (doesn't publish)
	@command -v goreleaser >/dev/null 2>&1 || { echo "$(RED)==> goreleaser not found. Run 'make tools' to install.$(RESET)"; exit 1; }
	@echo "$(GREEN)==> Running GoReleaser in snapshot mode...$(RESET)"
	@goreleaser build --snapshot --clean --parallelism 2

# ── Security & Analysis ───────────────────────────────────────────
vulncheck: ## Run Go vulnerability check (summary; use V=1 for full call stacks)
	@echo "$(GREEN)==> Running govulncheck...$(RESET)"
ifdef V
	@go run golang.org/x/vuln/cmd/govulncheck@latest ./...
else
	@go run golang.org/x/vuln/cmd/govulncheck@latest -scan package ./...
endif

security: vulncheck ## Combined security scan (vulncheck + gosec via linter)
	@echo "$(GREEN)==> Running gosec via golangci-lint...$(RESET)"
	@command -v golangci-lint >/dev/null 2>&1 || { echo "$(RED)==> golangci-lint not found. Run 'make tools'.$(RESET)"; exit 1; }
	@golangci-lint run --enable gosec ./...
	@echo "$(GREEN)==> Security scan complete$(RESET)"

deadcode: ## Find unreachable functions and unused code
	@echo "$(GREEN)==> Searching for dead code...$(RESET)"
	@go run golang.org/x/tools/cmd/deadcode@latest -test ./...

audit-cgo: ## Audit which dependencies contain C code (informational)
	@echo "$(GREEN)==> Scanning dependency tree for packages with C files...$(RESET)"
	@echo "  $(CYAN)Note:$(RESET) WaveHouse builds with CGO_ENABLED=0. Packages listed below"
	@echo "  have pure-Go fallbacks and their C code is $(YELLOW)never compiled$(RESET)."
	@echo "  This audit exists to catch new CGO deps before they cause build issues."
	@echo ""
	@CGO_ENABLED=1 go list -deps -f '{{if .CgoFiles}}  ⚠ {{.ImportPath}}  ({{len .CgoFiles}} C files){{end}}' ./cmd/... 2>/dev/null || true
	@echo ""
	@echo "$(GREEN)==> CGO audit complete$(RESET)"
	@echo "  To verify pure-Go build: $(CYAN)CGO_ENABLED=0 go build ./...$(RESET)"

# ── Binary Size Analysis ─────────────────────────────────────────

size-report: build ## Show binary sizes for all targets
	@echo "$(GREEN)==> Binary sizes:$(RESET)"
	@for b in $(BINARIES); do \
		SIZE=$$(ls -lh bin/$$b | awk '{print $$5}'); \
		SIZE_BYTES=$$(ls -l bin/$$b | awk '{print $$5}'); \
		printf "  $(CYAN)%-25s$(RESET) %s (%s bytes)\n" "$$b" "$$SIZE" "$$SIZE_BYTES"; \
	done

size-tree: build-debug ## Top packages by size in the binary
	@echo "$(GREEN)==> Binary size by package (debug build for DWARF accuracy):$(RESET)"
	@echo ""
	@GOEXPERIMENT=jsonv2 go run github.com/Zxilly/go-size-analyzer/cmd/gsa@latest \
		--format text --hide-sections bin/wavehouse 2>/dev/null

size-treemap: build-debug ## Full binary size analysis → text + SVG + interactive HTML
	@echo "$(GREEN)==> Analyzing bin/wavehouse (debug build for DWARF accuracy)...$(RESET)"
	@echo "  Note: debug builds add ~30%% DWARF metadata but package proportions match production."
	@echo ""
	@mkdir -p tmp/analysis
	@GOEXPERIMENT=jsonv2 go run github.com/Zxilly/go-size-analyzer/cmd/gsa@latest \
		--format text --hide-sections bin/wavehouse 2>/dev/null
	@echo ""
	@GOEXPERIMENT=jsonv2 go run github.com/Zxilly/go-size-analyzer/cmd/gsa@latest \
		--format svg --output tmp/analysis/size-map.svg --hide-sections bin/wavehouse 2>/dev/null
	@echo "  $(CYAN)SVG  → tmp/analysis/size-map.svg$(RESET)"
	@GOEXPERIMENT=jsonv2 go run github.com/Zxilly/go-size-analyzer/cmd/gsa@latest \
		--format html --output tmp/analysis/size-map.html --hide-sections bin/wavehouse 2>/dev/null
	@echo "  $(CYAN)HTML → tmp/analysis/size-map.html (interactive treemap)$(RESET)"
	@if [ -z "$$CI" ]; then \
		if command -v open >/dev/null 2>&1; then open tmp/analysis/size-map.html; \
		elif command -v xdg-open >/dev/null 2>&1; then xdg-open tmp/analysis/size-map.html; \
		fi; \
	fi

dep-graph: ## Dependency graph → tmp/analysis/graph.svg (requires graphviz `dot`)
	@echo "$(GREEN)==> Generating dependency graph...$(RESET)"
	@mkdir -p tmp/analysis
	@go run github.com/loov/goda@latest graph -cluster -short "github.com/Wave-RF/WaveHouse/...:all" > tmp/analysis/graph.dot
	@if command -v dot >/dev/null 2>&1; then \
		dot -Tsvg tmp/analysis/graph.dot -o tmp/analysis/graph.svg 2>/dev/null; \
		echo "$(GREEN)==> tmp/analysis/graph.svg generated$(RESET)"; \
		if [ -z "$$CI" ]; then \
			if command -v open >/dev/null 2>&1; then open tmp/analysis/graph.svg; \
			elif command -v xdg-open >/dev/null 2>&1; then xdg-open tmp/analysis/graph.svg; \
			fi; \
		fi; \
	else \
		echo "$(YELLOW)==> tmp/analysis/graph.dot generated (install graphviz for SVG rendering)$(RESET)"; \
		echo "  $(CYAN)brew install graphviz$(RESET)  then re-run this target"; \
		echo "  or paste graph.dot into $(CYAN)https://dreampuf.github.io/GraphvizOnline/$(RESET)"; \
	fi

dep-why: ## Show why a module is included (usage: make dep-why MOD=github.com/aws/aws-sdk-go-v2)
	@if [ -z "$(MOD)" ]; then \
		echo "$(RED)==> Usage: make dep-why MOD=github.com/aws/aws-sdk-go-v2$(RESET)"; \
		exit 1; \
	fi
	@echo "$(GREEN)==> Why $(MOD) is required:$(RESET)"
	@go mod why -m $(MOD) 2>&1 || true
	@echo ""
	@echo "$(GREEN)==> Import chain:$(RESET)"
	@go mod graph | grep "$(MOD)" | head -20 || true

dep-cut: ## Top cuttable dependencies by transitive impact
	@echo "$(GREEN)==> Dependency cut analysis (top $(LIMIT), InDegree ≤ 3):$(RESET)"
	@echo "  Packages with few dependents that pull in the most transitive weight."
	@echo ""
	@go run github.com/loov/goda@latest cut ./...:all 2>/dev/null | \
		awk 'NR==1 { printf "  %-58s %4s %5s %10s\n", "Package", "Deps", "Pkgs", "Size"; next } \
		     $$2+0 <= 3 { name=$$1; gsub(/github\.com\//, "", name); printf "  %-58s %4s %5s %10s\n", name, $$2, $$3, $$4 }' | \
		head -n $$(($(LIMIT) + 1))
	@echo ""
	@echo "  $(CYAN)Full output: go run github.com/loov/goda@latest cut ./...:all$(RESET)"

binary-analysis: size-report deadcode audit-cgo ## Combined binary analysis (sizes + dead code + CGO audit)
	@echo ""
	@echo "$(GREEN)==> Binary analysis complete$(RESET)"
	@echo "  For interactive size explorer: $(CYAN)make size-treemap$(RESET)"
	@echo "  For package weight breakdown:  $(CYAN)make size-tree$(RESET)"
	@echo "  For dependency graph:          $(CYAN)make dep-graph$(RESET)"
	@echo "  For dependency cut analysis:   $(CYAN)make dep-cut$(RESET)"

# ── Documentation ────────────────────────────────────────────────
docs-install: ## Install docs site dependencies
	@cd site && pnpm install --frozen-lockfile

docs-convert: ## Convert /docs/ markdown to Starlight format
	@node site/scripts/convert-docs.mjs

docs-dev: docs-convert ## Start docs dev server with hot-reload
	@cd site && pnpm dev

docs-build: docs-convert ## Build docs site for production
	@cd site && pnpm build

docs-preview: docs-build ## Preview production build locally
	@cd site && pnpm preview
