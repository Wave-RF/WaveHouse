---
description: Pre-push gate — judge which reviewers the change needs, run those in parallel, skip the rest (logged), loop to ship_it. Run before every PR-branch push.
argument-hint: "[all] (default: judge per change; 'all' forces every reviewer)"
---

The mandatory pre-push self-review. A push to a PR branch is blocked until a marker exists for HEAD from **every** reviewer in `scripts/pre-push-reviewers.sh` (`.claude/hooks/agent-bash-gate.sh`). You satisfy each marker one of two ways: **run** the reviewer (it writes its marker on `ship_it`), or **skip** it when it adds nothing to *this* change (a logged skip writes the marker). The goal: a real review where it matters, and no 10-minute review of a one-line typo.

## Preconditions

1. **Commit your work.** Reviews and markers are keyed to HEAD/tree, so the tree must be settled first.
2. **`make ci` is green for the current tree** (AGENTS.md → §Local-First Validation → Running `make ci`). The push also needs its `tmp/ci-passed-tree-<TREE>` marker.

## 1. See the change and the reviewer set

```bash
git diff --stat main...HEAD          # size + which files changed
git diff --name-only main...HEAD     # exact paths
scripts/pre-push-reviewers.sh        # the gating reviewers, one subagent name per line
```

## 2. Decide, per reviewer: run or skip?

Judge each reviewer against the **actual diff**. **Bias to running — skip only when you're confident the reviewer has nothing to do here.** A skip is your judgment on the record (it gets logged), so treat it as one. If `$ARGUMENTS` is `all`, skip nothing — run every reviewer (use this for a full sweep, or whenever you're unsure).

Rules of thumb for today's reviewers:

- **`pre-push-reviewer`** (code) — run for any code / config / CI / script / Dockerfile change. Skip only when the diff is purely documentation prose (`*.md` / `*.mdx`) or other non-code text.
- **`docs-reviewer`** (docs prose + code↔docs sync) — run when docs changed **or** when code changed that could need a doc update (new/changed API route, config key, event format, CLI flag, deployment, SDK surface, architecture — per AGENTS.md §Documentation Sync). Skip only when neither is true: a test-only change, an internal refactor with no public-surface or doc impact, a comment/whitespace-only edit.
- A genuinely **trivial** change (typo in a comment, rename a local var, reflow whitespace) can skip both — but read the diff and be sure it really is trivial.

## 3. Run the ones you keep — in parallel

Launch all kept reviewers **in one message** (one `Agent` call each → concurrent), each in **fresh context**, with **no scope argument**; `subagent_type` is the reviewer name. Each returns `[MUST]`/`[SHOULD]`/`[MAY]` findings and a `VERDICT:` line; on `ship_it` the `SubagentStop` hook (`review-marker.sh`) writes its marker.

## 4. Skip the rest — on the record

For each reviewer you're skipping, record it (this writes the marker the gate needs **and** logs the reason for audit):

```bash
scripts/skip-pre-push-review.sh <name> "<one-line reason>"
```

It prints a ⚠️ when the skip looks risky (e.g. skipping docs review when docs files changed) — heed it and run that reviewer instead if the warning is right. **Tell the user which reviewers you skipped and why.** Never hand-write a marker (`touch`/`Write`/`Edit`); the skip command is the only honest skip path (AGENTS.md → §Don't bypass the gates).

## 5. Loop until every marker is present

`ship_it` requires **zero findings at any severity** — a single `[MAY]` forces another round. So:

1. Address **every** finding from **every** reviewer you ran — don't drop one because it's outside another reviewer's lane; fix it or track it in an issue (AGENTS.md §Review Response).
2. Commit (HEAD changes → all prior markers, run *and* skipped, go stale for the new HEAD).
3. Re-run `make ci` **only if** a finding made you edit a tracked file.
4. Re-decide §2 for the new HEAD, then re-run the kept reviewers in fresh context (parallel) and re-skip the rest.
5. Repeat until a marker exists for HEAD from every listed reviewer.

Only then does `git push` succeed (and the gate prints any judgment-skips for the record). If a reviewer returns `block`, **stop and surface it to the user** — don't push past it. Never `--no-verify`.
