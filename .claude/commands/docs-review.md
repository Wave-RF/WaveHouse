---
description: Review docs prose (accuracy-vs-code, runnable examples, clarity, completeness) + code↔docs sync. No-arg = the gating pre-push review; a path/`all` = advisory. Complements misspell/markdownlint/links-validator.
argument-hint: "[path/glob | all] (default: branch docs review — gates the push)"
---

Run a documentation review — the accuracy / runnable-examples / clarity / completeness / doc-sync layer the deterministic tools (misspell, markdownlint, starlight-links-validator) can't cover.

Scope: $ARGUMENTS

Invoke the **`docs-reviewer`** subagent (fresh context, for objectivity) and pass it the scope above:

- **empty** → **gating mode**: the branch's changes vs `main` (docs prose **and** code↔docs sync). This is the mandatory pre-push docs review — on `VERDICT: ship_it` the SubagentStop hook writes `tmp/docs-reviewer-passed-<HEAD-sha>`, which `.claude/hooks/agent-bash-gate.sh` requires before the push. It's only one of the gating reviewers, though — prefer `/prepush`, which runs them all in parallel.
- **a path or glob** (e.g. `docs/src/content/docs/api.md`) → **advisory**: just those files, no verdict, no marker.
- **`all`** → **advisory**: the whole docs-prose set (`scripts/docs-prose.sh all`), no verdict, no marker.

The subagent reads `.github/prompts/docs-review.md`, cross-checks each claim against the code/config, and returns `[MUST]` / `[SHOULD]` / `[MAY]` findings. It never edits docs and never posts PR comments.

Relay the subagent's findings. In **gating mode**, loop — fix the findings, commit, re-invoke `docs-reviewer` in fresh context — until `ship_it`. In **advisory mode**, ask which to act on and offer to apply the ones the user picks.
