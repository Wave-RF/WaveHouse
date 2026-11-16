---
description: Render coverage HTML for a suite and surface drops below threshold
argument-hint: [unit|integration|e2e|merge|all] (default: merge whatever exists)
---

Generate the coverage report and surface anything below threshold from `.testcoverage.yml`.

Suite to run: $ARGUMENTS

Behavior:

- **no argument or "merge"**: just `make cov` (merges whatever Go coverage exists under `tmp/coverage/` and gates against the thresholds)
- **unit**: `make test-unit` (gates per-suite + writes `tmp/coverage/unit/`)
- **integration**: `make test-integration` (requires Docker)
- **e2e**: `make test-e2e` (requires Docker; orchestrator + cover binary)
- **all**: `make test-all` (all three suites sequentially + `make cov`)

After the run completes:

1. Parse `tmp/coverage/<suite>/coverage.txt` for per-package coverage.
2. Identify packages below the suite's threshold from `.testcoverage.yml`. Report as a sorted list.
3. Surface the suite total + delta vs. the threshold.
4. Print the path to the HTML report — don't auto-open (lets the user open it themselves if they want).

If the suite errors before producing covdata, report what failed without trying to render coverage.
