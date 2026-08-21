---
description: Render coverage HTML for a suite and surface drops below threshold
argument-hint: [unit|integration|e2e|go-sdk|sdk|merge|all] (default: merge whatever exists)
---

Generate the coverage report and surface anything below threshold from `.testcoverage.yml`.

Suite to run: $ARGUMENTS

Behavior:

- **no argument or "merge"**: just `make cov` (merges whatever Go + TS coverage exists under `tmp/coverage/` and gates against the thresholds)
- **unit**: `make test-unit` (gates per-suite + writes `tmp/coverage/unit/`)
- **integration**: `make test-integration` (requires Docker)
- **e2e**: `make test-e2e` (requires Docker; orchestrator + cover binary)
- **go-sdk**: `make test-go-sdk` (nested module `clients/go`; gates against `suites.go-sdk`, rendered separately and never merged into the Go total)
- **ts-unit**: `make test-ts` (SDK unit tests + coverage + gate against `suites.ts-unit`)
- **ts-e2e**: emitted as a side effect of `make test-e2e` (the orchestrator always passes `--coverage` to the e2e vitest run; informational only, no standalone gate)
- **ts-total**: `make cov` (runs `cov report` — one consolidated Go + TS summary with per-suite HTML links + all gates; fails if *no* suite has data)
- **all**: `make test-all` (every suite sequentially + `make cov`)

After the run completes:

1. Parse `tmp/coverage/<suite>/coverage.txt` (Go) or `tmp/coverage/sdk/index.html` (SDK) for per-package coverage.
2. Identify packages below the suite's threshold from `.testcoverage.yml`. Report as a sorted list.
3. Surface the suite total + delta vs. the threshold.
4. Print the path to the HTML report — don't auto-open (lets the user open it themselves if they want).

If the suite errors before producing covdata, report what failed without trying to render coverage.
