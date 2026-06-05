---
description: Pre-push gate — run BOTH mandatory reviewers (code + docs) in parallel, loop to ship_it. Run before every PR-branch push.
argument-hint: "(no args — always the gating branch review)"
---

The mandatory pre-push self-review. A push to a PR branch is blocked until **both** review markers exist for HEAD (`.claude/hooks/agent-bash-gate.sh`), so run this before every push. **One reviewer is never enough** — `docs-reviewer` is a separate gate and runs even on code-only changes (its job is catching "code changed but the docs should have and didn't").

## Preconditions

1. **Commit your work.** Reviews and markers are keyed to HEAD/tree, so the tree must be settled first.
2. **`make ci` is green for the current tree** — run it the documented way (AGENTS.md → §Local-First Validation → Running `make ci`). The push also needs its `tmp/ci-passed-tree-<TREE>` marker.

## Run both reviewers in parallel

Launch **both** subagents **in one message** (two invocations, so they run concurrently), each in **fresh context**:

- **`pre-push-reviewer`** — code review of `main...HEAD` (correctness → security → performance → testing → doc/SDK-sync).
- **`docs-reviewer`** with **empty scope** — the gating docs review (prose accuracy + code↔docs sync). Pass *no* path: empty scope is what makes it emit a `VERDICT:` line and write its marker; a path or `all` is advisory only.

Each returns `[MUST]`/`[SHOULD]`/`[MAY]` findings and a `VERDICT:` line. On `VERDICT: ship_it` the `SubagentStop` hook (`.claude/hooks/review-marker.sh`) writes that reviewer's marker — `tmp/review-passed-<HEAD>` / `tmp/docs-review-passed-<HEAD>`.

## Loop until BOTH say ship_it

`ship_it` requires **zero findings at any severity** — a single `[MAY]` forces another round. So:

1. Address **every** finding from **both** reviewers.
2. Commit (HEAD changes → both prior markers are now stale for the new HEAD).
3. Re-run `make ci` **only if** a finding made you edit a tracked file.
4. Re-invoke **both** reviewers in fresh context (again in parallel) for the new HEAD.
5. Repeat until both return `ship_it`.

Only when both markers exist for HEAD will `git push` succeed. If a reviewer returns `block`, **stop and surface it to the user** — don't push past it. Never hand-write a marker and never `--no-verify` (AGENTS.md → §Don't bypass the gates).
