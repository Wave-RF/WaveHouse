---
name: pm-triage
description: Project-manager review for WaveHouse issues — triage dogfooding feedback logs, re-classify the backlog P0–P3 (launch-gated), sweep code TODOs, and reconcile issue/PR status into well-scoped, tracked GitHub issues on the Task Board (project #7). Use when the user wants to triage feedback or the backlog, file/update issues from a feedback doc or TODOs, re-prioritize, edit titles/bodies or add follow-up comments, or check that work is actually tracked. Invoke as `/pm-triage [feedback-doc <url|path> | backlog | todos | status | all]`. **safe-auto by default** (auto-apply low-risk changes; for risky ones — ask when you're watching, queue to `pending.md` when unattended); add `auto` (apply everything), `report-only` (write nothing), or `propose` (review even safe changes). `all` is the daily local routine (status+backlog+todos, incremental); state on a local-only orphan branch `pm-triage-state`. See `references/routine.md`.
---

# PM triage for WaveHouse

Turn raw inputs — dogfooding logs, the open-issue backlog, code TODOs, PR/issue status — into **well-scoped, correctly-prioritized, tracked** GitHub issues on the WaveHouse Task Board. You are the project manager: make issues addressable (enough context to pick up, not a spec; **never solve the issue in the issue**), prioritize against the launch, fold sprawl into epics, and keep tracking honest.

**Run modes:**

- **`safe-auto`** (default) — auto-apply the **safe tier** (set *missing* priorities, fix board Status to reality, file clearly-new issues with provenance, tick epic boxes for closed issues). For the **risky tier** (teammate priority changes, security/disclosure, closes/dupes, body edits, cross-repo writes): **ask** when a human's in the session, **queue** to the state branch's `pending.md` when unattended. Never apply risky changes unattended. Reads/writes the local state branch (`## Mode: all`).
- **`auto`** — apply everything, including the risky tier, without asking. For when you're watching and fully trust it.
- **`report-only`** — emit the full plan as output, **write nothing** (no GitHub writes, no state commit). The **dry-run / preview** mode.
- **`propose`** — fully interactive: present *everything* (even safe-tier changes) for approval before doing it. Use when you want eyes on every change.

## Modes (the argument)

- **`feedback-doc <url|path>`** — a dogfooding log (e.g. WaveHouse-Stats `WAVEHOUSE-FEEDBACK.md`, nas-observability `WHissues.md`, or a local notes file). Validate → file untracked → optionally write upstream links back into the source.
- **`backlog`** — re-classify every open issue P0–P3 (launch-gated) on the board.
- **`todos`** — sweep code `TODO/FIXME/HACK/XXX` for untracked-worth-tracking items.
- **`status`** — reconcile open issues **and PRs**: stale items, untracked work, un-updated threads.
- **`all`** — `status` + `backlog` + `todos` in one **incremental** pass (the routine scope), diffed against the state branch's last run. Excludes `feedback-doc` (needs an explicit doc); point the routine at known feedback logs separately if wanted. See `## Mode: all (the routine)`.
- No arg → ask which, or infer from what the user just referenced.

## Operating conventions (every mode)

1. **Priority lives on the board Priority field, not labels.** Never create `P0/P1` labels. Labels are semantic (`area/*`, `security`, `bug`, `documentation`, `enhancement`, `breaking-change`). See memory `feedback_priority_field_not_labels.md` and `reference_wavehouse_board.md` for the field/option IDs (and re-verify with `gh project field-list 7 --owner Wave-RF`).
2. **Comment, don't edit issue bodies** — unless the user explicitly says to edit. When you change a teammate's priority, leave a one-line note pointing at the launch triage (#149) so it isn't a silent change.
3. **Validate by code-read against the running dev commit.** Feedback items are claims about code at a `file:line`; confirm each against current source. Mark every item **valid / invalid / already-fixed / already-tracked**. Older logs especially: many items are already fixed — say so instead of re-filing.
4. **Tight bodies, context not solutions.** Use `references/issue-template.md` (Expected/Actual/Impact/Ref/Scope + provenance footer). One-line scope pointer at most; leave the fix to the PR.
5. **Fold clusters into an epic** with a checklist (the #194 SDK / #228 auth pattern), split into discrete issues when promoted to Ready. Avoids board spam and feature creep.
6. **Launch-gated priority** — `references/rubric.md`. Keep P0 small and sharp.
7. **Security/disclosure care.** A live data-exposure issue is only private while the repo is. Before the public flip, such an issue must be fixed+closed or moved to a GitHub Security Advisory — don't flip with an open issue describing a live vuln. Flag this, don't decide it unilaterally.
8. **Race-safe board writes.** A repo workflow auto-adds new issues to the board, so a direct `gh project item-add` right after `gh issue create` races it. Always **create first, then poll the board for the item id, then set fields** — `scripts/board.sh` does this. (See the gotcha in `reference_wavehouse_board.md`.)
9. **Cross-reference before filing.** Check open issues AND open PRs AND the `#194`/`#228` epic checklists. Don't duplicate; cross-link instead.

## Mode: feedback-doc

1. **Fetch.** If it's another repo (often private), use `gh api repos/<owner>/<repo>/contents/<file> --jq .content | base64 -d`. Note the doc's "running dev" commit and last-modified date.
2. **Validate** each item by code-read at its cited `file:line` against current source. Group by file to read each once. Mark valid / invalid / already-fixed / already-tracked.
3. **Cross-reference** every valid item to an existing issue/PR/epic-checklist or "untracked".
4. **Propose** (or, with `auto`, execute): file untracked-valid items with launch-gated priority; fold clusters into epics; cross-link related existing issues with a comment; for the dedupe/security ones note disclosure handling.
5. **Optional upstream-linking** (only if asked / if the source is a tracking doc): add an "Upstream" column + per-entry issue links back into the source doc so the other repo's agents can track status. Use a deterministic transform (see how `WAVEHOUSE-FEEDBACK.md` was wired), verify row/section counts, then push (their convention may allow direct-to-main with ADMIN — confirm before pushing to another repo).

## Mode: backlog

1. Pull open issues + current board priority (`scripts/board.sh open-by-priority`).
2. Assign each a launch-gated P0–P3 (`references/rubric.md`). Most large features / breaking refactors / multi-tenant work → P2/P3 even if a teammate set them P1; the point is to concentrate P0/P1 on true launch blockers.
3. Set only what changes (`board.sh set-priority`). Set missing priorities (e.g. fresh SDK-epic children).
4. Post **one** consolidated rationale comment on the launch tracker (#149), plus a one-liner on each downgraded teammate issue — not a comment per change.

## Mode: todos

1. `rg -n "TODO|FIXME|HACK|XXX" internal/ cmd/ clients/ Makefile --glob '!**/node_modules/**'` (+ non-Go).
2. Bucket: **untracked-worth-tracking** / **already-tracked** (map TODO→issue) / **stale** (already implemented → flag for removal) / **skip** (musings, micro-opts, false-positives). Read the few that need context (goroutine lifecycles, determinism, security).
3. Watch for self-acknowledged security/correctness TODOs (`// TODO: prevent table-enumeration`, non-deterministic keys, unbounded growth, "fatal on boot") — those usually warrant issues.
4. File the worthy ones; cite the exact TODO site + provenance.

## Mode: status

1. `gh pr list --state open` + `gh issue list --state open`; cross-check against the board.
2. Flag: open PRs with no tracking issue / stale (no update in N days) / out-of-date-with-main; issues with open PRs that should be linked; epic checkboxes whose issues closed (tick them); board items whose Status drifted from reality (closed issue not Done, etc.).
3. **Reconcile MERGED PRs against open tracking issues.** For each PR merged since `last_sha` (`git log --oneline <last_sha>..origin/main`, map commit→PR), check whether it completes a checklist item / precondition in an issue that is **still open** (#149, #268, the epics #194/#228/#294/#326/#327, …) and tick that box (SAFE tier). **Match by content, not `Closes #`:** many PRs — especially `ci:`/`docs:`/`chore:` — finish a tracked deliverable without a closing reference and without closing the issue, so there is *no* closed-issue or reference-graph signal (e.g. #308 provenance → #268's box; #283 runner-migration → #149 L25). Never dismiss the CI/docs/chore commits wholesale — they are often the tracked deliverable. Leave *partial-progress* boxes unticked (a single NO_LCP fix doesn't complete #248's "Lighthouse pass").
4. Propose the reconciling edits (comments, board status, checkbox ticks). Don't mass-comment.

## Mode: all (the routine)

`status` + `backlog` + `todos` in one pass, **incremental** against the local state branch, with the default `safe-auto` policy. This is the daily local routine (Claude desktop app, runs on your Mac — not cloud). Full procedure + the paste-ready routine prompt: `references/routine.md`.

**Durable state — a local-only orphan branch (not an issue, not a cache dir).** `scripts/state.sh` keeps state on an orphan branch `pm-triage-state` (shares no history with main; never pushed by default → private), in its own worktree at `<main>/.worktrees/pm-triage-state` (path is stable across all your worktrees). **Never switch branches in a working tree — navigate by path:**

- `state.sh ensure` → create-if-needed, echoes the worktree path `S`.
- `state.sh read` → `state.json` (`{ schema, last_run_at, last_sha, filed }`, `filed` = fingerprint→issue#); `{}` on first run = full sweep.
- Write `S/state.json` via `state.sh write` (preserve `schema`); write `S/pending.md` (risky items) and `S/runs.log` directly — `state.sh commit` stages all three.
- `state.sh commit "<summary>"` after a meaningful run → a versioned audit trail you can `git -C S log`.

**Read code without disturbing any tree.** `git fetch origin`, then do TODO sweeps / code-read validation against **`origin/main`** by ref (`git grep`, `git show`, `git diff <last_sha>..origin/main`) — don't assume or mutate a working tree, don't checkout.

**Write tiers (`safe-auto`).** Apply the SAFE tier; for the RISKY tier — **ask** if a human's in the session, else **queue** to `S/pending.md`. Never apply risky unattended:

| SAFE (auto) | RISKY (ask if watching · else queue to pending.md) |
|---|---|
| set a *missing* Priority; fix Status to reality; file a clearly-new issue (dedupe first, provenance, ≤8/run); tick epic boxes for closed issues | change a teammate-set priority; security/disclosure; close/dupe/merge-into-epic; edit a body or comment on a teammate's issue; any cross-repo write |

**Rails.** Dedupe before filing (conv. #9); `state.json.filed` (fingerprint→issue#) also stops re-filing across runs. Cap new issues at **8/run** (the rest are re-found next run). At start, surface any open `pending.md` items. The routine creates real GitHub issues + sets board fields as always — only its **own bookkeeping** moved to the local branch. Inherit the `.claude/settings.json` deny-list (never merge PRs, never `gh pr ready`).

## Scripts

- **`scripts/board.sh`** — race-safe board ops. Subcommands:
  - `file <P0|P1|P2|P3> <Backlog|Ready|InProgress|InReview|Done> "<labels>" "<title>"` (body on stdin) → creates the issue, waits out the auto-add workflow, sets Priority + Status, echoes the number.
  - `set-priority <issue#> <P0..P3>` · `set-status <issue#> <Status>` · `item-id <issue#>` · `open-by-priority`.
  - Field/option IDs are baked in (verified) and overridable by env (`WH_PROJECT_ID`, `WH_PRIORITY_FIELD`, …) if the board changes.
- **`scripts/state.sh`** — local-only routine state on the orphan branch `pm-triage-state` in its own worktree (private, never pushed, stable across worktrees, versioned audit trail; see `## Mode: all`). `ensure` (create-if-needed, echoes worktree path) · `path` · `read`/`write` (state.json) · `commit "<msg>"`.

When filing many issues at once, prefer: create them all (capture numbers), **then** one board pass that looks up item ids and sets fields — fewer auto-add races than per-issue.

## After acting (report)

Summarize: validated verdicts (valid/fixed/tracked counts), what was filed/edited/reclassified/commented, the **launch-blocking shortlist** (current P0/P1), priority changes that touch teammates, and anything needing the user's decision (disclosure handling, divergent priority calls, missing labels like `area/auth`). If a board field/option ID turned out stale, update `reference_wavehouse_board.md`.
