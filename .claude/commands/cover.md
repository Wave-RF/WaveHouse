---
description: Render coverage HTML for a suite and surface drops below threshold
argument-hint: [unit|integration|e2e|sdk|merge|all] (default: merge whatever exists)
---

Generate the coverage report and surface anything below threshold from `.testcoverage.yml`.

Suite to run: $ARGUMENTS

Behavior:
- **no argument or "merge"**: just `make cov` (merges whatever covdata exists in `tmp/coverage/*/data/` and gates against `threshold.total`)
- **unit**: `make test-unit` (gates per-suite + writes `tmp/coverage/unit/`)
- **integration**: `make test-integration` (requires Docker)
- **e2e**: `make test-e2e` (requires Docker; orchestrator + cover binary)
- **ts-unit**: `make ts-test` (SDK unit tests + coverage + gate)
- **ts-e2e**: emitted as a side effect of `make test-e2e` (the orchestrator always passes `--coverage` to the e2e vitest run; informational only, no standalone gate)
- **ts-total**: `make ts-cov` (merge of ts-unit + ts-e2e via `cov ts-merge` → gate against `suites.ts-total`)
- **all**: `make test-all` (all four suites sequentially + `make cov`)

After the run completes:
1. Parse `tmp/coverage/<suite>/coverage.txt` (Go) or `tmp/coverage/sdk/index.html` (SDK) for per-package coverage.
2. Identify packages below the suite's threshold from `.testcoverage.yml`. Report as a sorted list.
3. Surface the suite total + delta vs. the threshold.
4. Print the path to the HTML report — don't auto-open (lets the user open it themselves if they want).

If the suite errors before producing covdata, report what failed without trying to render coverage.
