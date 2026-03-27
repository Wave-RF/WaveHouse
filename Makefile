# ── Pinned tools (versions locked in go.mod tool directives) ─────
GOTESTSUM := go run gotest.tools/gotestsum
GOFUMPT   := go run mvdan.cc/gofumpt
GOIMPORTS := go run golang.org/x/tools/cmd/goimports

# Build tags (e.g., TAGS="scylla dynamodb")
TAGS ?=

# ── Build Configuration ───────────────────────────────────────────
# Force CGO disabled for all local builds to match GoReleaser exactly
export CGO_ENABLED=0
# Default: Strip debugging symbols to reduce binary size by 30%
LDFLAGS := -s -w
# If DEBUG=1 is passed, remove the stripping flags so debuggers work
ifdef DEBUG
	LDFLAGS :=
endif

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

.PHONY: help setup build dev fmt lint lint-fix fix test test-integration \
        test-all ci coverage smoke-test docker compose-standalone \
        compose-clustered compose-deps deps-wipe clean release-test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(RESET) %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────────
setup: ## Download Go modules and cache tools
	@echo "$(GREEN)==> Downloading modules...$(RESET)"
	@go mod download
	@echo "$(GREEN)==> Done$(RESET)"

# ── Build ─────────────────────────────────────────────────────────
build: $(BINARIES) ## Compile all binaries (run `make -j3 build` for parallel)

# This is a "Pattern Rule". It tells Make how to build ANY of the binaries in the list.
$(BINARIES):
	@echo "$(GREEN)==> Building $@...$(RESET)"
	@START=$$(date +%s); \
	go build -tags="$(TAGS)" -ldflags="$(LDFLAGS)" -o bin/$@ ./cmd/$@; \
	END=$$(date +%s); \
	SIZE=$$(ls -lh bin/$@ | awk '{print $$5}'); \
	printf "$(GREEN)✔$(RESET) $(CYAN)%-25s$(RESET) built in %ss (Size: $(YELLOW)%s$(RESET))\n" "$@" "$$((END - START))" "$$SIZE"

# ── Development ───────────────────────────────────────────────────
dev: ## Hot-reload dev server (requires air)
	@command -v air >/dev/null 2>&1 || { \
		echo "$(RED)==> air not found$(RESET)"; \
		echo "Install: https://github.com/air-verse/air"; \
		echo "  brew install air                     (macOS)"; \
		echo "  go install github.com/air-verse/air@latest"; \
		exit 1; \
	}
	@air -c .air.toml

# ── Formatting ────────────────────────────────────────────────────
fmt: ## Format code (gofumpt + goimports)
	@echo "$(GREEN)==> Formatting...$(RESET)"
	@$(GOFUMPT) -w .
	@$(GOIMPORTS) -w .

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

coverage: ## Unit test coverage → coverage.html and summary
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="$(TAGS)" ./internal/... -race -coverprofile=coverage.txt -covermode=atomic $(ARGS)
	@go tool cover -html=coverage.txt -o coverage.html
	@echo "$(GREEN)==> Coverage Summary:$(RESET)"
	@go tool cover -func=coverage.txt | tail -n 1 | awk '{print "  Total Coverage: $(CYAN)" $$3 "$(RESET)"}'
	@echo "$(YELLOW)==> Open coverage.html in your browser for line-by-line details$(RESET)"

smoke-test: ## Manual Bento insert+delete (requires running WaveHouse)
	@go run ./tests/cmd/bento_pub

# ── CI ────────────────────────────────────────────────────────────
ci: ## Full CI check: fmt + lint + tests (run before pushing)
	@echo "$(YELLOW)==> Running full CI check...$(RESET)"
	@echo "$(GREEN)==> Checking formatting...$(RESET)"
	@OUTPUT=$$($(GOFUMPT) -l .); test -z "$$OUTPUT" || { echo "$(YELLOW)Files not formatted:$(RESET)"; echo "$$OUTPUT"; echo "Run 'make fmt' and commit."; exit 1; }
	@echo "$(GREEN)==> Linting...$(RESET)"
	@command -v golangci-lint >/dev/null 2>&1 || { echo "$(RED)==> golangci-lint not found. See 'make lint' for install instructions.$(RESET)"; exit 1; }
	@golangci-lint run ./...
	@echo "$(GREEN)==> Unit tests...$(RESET)"
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="$(TAGS)" ./internal/... -race
	@echo "$(GREEN)==> Integration tests...$(RESET)"
	@$(GOTESTSUM) --format $(GOTESTSUM_FMT) -- -tags="integration $(TAGS)" -timeout 120s ./tests/... -race -count=1
	@echo "$(GREEN)==> All checks passed!$(RESET)"

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

clean: ## Remove bin/, tmp/, data/, coverage
	@rm -rf bin/ tmp/ data/ coverage.txt coverage.html

# ── Releases ──────────────────────────────────────────────────────
release-test: ## Test cross-compiling all binaries via GoReleaser (doesn't publish)
	@command -v goreleaser >/dev/null 2>&1 || { echo "$(RED)==> goreleaser not found. Run 'brew install goreleaser'.$(RESET)"; exit 1; }
	@echo "$(GREEN)==> Running GoReleaser in snapshot mode...$(RESET)"
	@# Limit parallelism so it doesn't freeze my laptop during testing...
	@goreleaser build --snapshot --clean --parallelism 2


# TODO: need to work on all the things below this point more
# https://www.datadoghq.com/blog/engineering/agent-go-binaries
# TODO: these need to re-build with LDFLAGS allowing debug symbols when running

# ── Advanced Tooling & Security ───────────────────────────────────
vulncheck: ## Run Go vulnerability check
	@echo "$(GREEN)==> Running govulncheck...$(RESET)"
	@go run golang.org/x/vuln/cmd/govulncheck@latest ./...

size-tree: ## Analyze binary size dependencies (requires building first)
	@echo "$(GREEN)==> Analyzing bin/wavehouse...$(RESET)"
	@go run github.com/loov/goda@latest weight -h ./...

# ── Advanced Tooling & Profiling ──────────────────────────────────
# These targets use `go run ...@latest` so they don't pollute the project's go.mod.

deadcode: ## Find unreachable functions and unused code
	@echo "$(GREEN)==> Searching for dead code...$(RESET)"
	@go run golang.org/x/tools/cmd/deadcode@latest -test ./...

size-treemap: build ## Generate an interactive SVG map of binary size
	@echo "$(GREEN)==> Generating SVG treemap for bin/wavehouse...$(RESET)"
	@command -v qs >/dev/null 2>&1 || { echo "Downloading go-size-analyzer..."; go install github.com/Zxilly/go-size-analyzer/cmd/gsa@latest; }
	@go run github.com/Zxilly/go-size-analyzer/cmd/gsa@latest --format svg --output size-map.svg bin/wavehouse
	@echo "$(YELLOW)==> Open size-map.svg in your web browser$(RESET)"

goda-aws: ## Trace exactly how the AWS SDK is getting imported TODO: doesn't work
	@echo "$(GREEN)==> Tracing path to AWS SDK...$(RESET)"
	@go run github.com/loov/goda@latest tree "reach(./..., github.com/aws/aws-sdk-go-v2...)"

goda-graph: ## Generate a Graphviz dot file of your internal dependencies
	@echo "$(GREEN)==> Generating dependency graph...$(RESET)"
	@go run github.com/loov/goda@latest graph "github.com/Wave-RF/WaveHouse/..." > graph.dot
	@echo "$(YELLOW)==> graph.dot generated. Render it using: dot -Tsvg graph.dot -o graph.svg$(RESET)"
	@echo "If you don't have Graphviz installed, paste the contents of graph.dot into https://dreampuf.github.io/GraphvizOnline/"

audit-cgo: ## Check all dependencies for hidden C code
	@echo "$(GREEN)==> Scanning all dependencies for C files...$(RESET)"
	@go list -deps -f '{{if .CgoFiles}}{{.ImportPath}} uses CGO: {{.CgoFiles}}{{end}}' ./...
