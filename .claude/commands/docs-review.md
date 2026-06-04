---
description: Review docs prose for accuracy-vs-code, runnable examples, clarity & completeness (complements misspell/markdownlint/links-validator)
argument-hint: "[path/glob | all] (default: docs changed vs main)"
---

Run a documentation **prose** review — the accuracy / clarity / completeness layer that the deterministic tools (misspell, markdownlint, starlight-links-validator) can't cover.

Scope: $ARGUMENTS

Invoke the **`docs-reviewer`** subagent (fresh context, for objectivity) and pass it the scope above:

- **empty** → the docs changed on this branch vs `main`
- **a path or glob** (e.g. `docs/src/content/docs/api.md`) → just those files
- **`all`** → the whole docs site

The subagent reads `.github/prompts/docs-review.md`, cross-checks each doc claim against the code/config, and returns `[MUST]` / `[SHOULD]` / `[MAY]` findings. It is **advisory** — it surfaces findings, doesn't edit docs, doesn't post PR comments, and doesn't gate pushes.

Relay the subagent's findings, then ask which to act on and offer to apply the ones the user picks.
