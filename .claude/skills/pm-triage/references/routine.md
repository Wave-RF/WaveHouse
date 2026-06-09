# Running `/pm-triage all` as a local routine

The scheduled form of pm-triage: a daily local pass that reconciles status, re-checks the
backlog, and sweeps new TODOs — applying safe changes itself and surfacing risky ones. Scope
`all`, default run-mode `safe-auto`. Runs on your Mac (only when it's on — that's fine), set up
as a routine in the **Claude desktop app**. It is **not** a cloud routine (this repo's gates,
`gh` auth, and `make ci` deps don't exist in the cloud) and **not** a launchd / `claude -p` job.

## State: a local-only orphan branch

State lives on an **orphan branch `pm-triage-state`** — shares no history with main (its own
thing), checked out in its own worktree at `<main>/.worktrees/pm-triage-state`, **never pushed
by default** (private; teammates can't see an unpushed branch). `scripts/state.sh` manages it.
Why this over the alternatives:

- **vs. a GitHub issue:** no API round-trips, no notifications, no teammate comments — the logic stays private.
- **vs. `.git/pm-triage/`:** a real branch is legible and idiomatic, and it's versioned.
- **vs. a plain gitignored dir:** untracked files are *per-worktree*, so a `.claude/pm-triage/` would fork across your 7 worktrees. The orphan worktree is one registered path, identical from everywhere.

You get a **versioned audit trail** — each meaningful run commits, so `git -C <state-wt> log`
shows when it changed what — and an easy **future sync** path: flip on `git push -u origin
pm-triage-state` to share across machines (left OFF for now).

**No branch-switching, ever.** The skill never `git checkout`s between main and the state
branch in one tree (that swaps your whole working dir and breaks on uncommitted work). It
navigates by **path**: file I/O in the state worktree, and code/TODO reads against `origin/main`
by ref (`git fetch`, then `git grep` / `git diff <last_sha>..origin/main`).

Files in the state worktree:

- `state.json` — `{ schema, last_run_at, last_sha, filed }`; `filed` is the fingerprint→issue# dedupe index, so it never re-files.
- `pending.md` — risky proposals awaiting you (human-editable; resolve them and the next run reconciles).
- `runs.log` — append-only, one line per run.

`state.json` goes through `state.sh write` (validated, atomic); `pending.md` and `runs.log` are written directly, and `state.sh commit` stages all three.

## safe-auto is the default (every invocation)

The safe tier just happens — no proposal step — whether you run it by hand or it fires on
schedule. Only the risky tier flexes:

- **You're watching** (manual `/pm-triage …`): it presents risky items and asks you right there.
- **Unattended** (the routine): it appends them to `pending.md` for you to review later.

| SAFE — auto | RISKY — ask if watching · else queue to `pending.md` |
|---|---|
| set a **missing** Priority; fix **Status** to reality; file a **clearly-new** issue (dedupe, provenance, ≤8/run); tick epic boxes for closed issues | change a teammate-set priority; security/disclosure; close/dupe/merge-into-epic; edit a body or comment on a teammate's issue; any cross-repo write |

(`auto` = don't even ask on risky; `report-only` = dry run, writes nothing; `propose` = review even the safe changes.)

## Cadence

Daily, weekday mornings. In the routine's schedule pick an off-`:00` minute (e.g. 9:07) so
you're not landing on the same instant as everyone else.

## Set it up in the Claude desktop app

1. Open the WaveHouse repo as a local project in the desktop app, so the routine has the repo,
   your keychain'd `gh`, and the `.claude/` skill.
2. Create a routine / scheduled task on a weekday-morning schedule and paste the prompt below.
3. **On the first run, verify:** it read the skill (`SKILL.md`), `gh auth status` is good, and it
   created the state worktree. Approve the `gh` / `git` / file tools it needs — or pre-grant them:

### Permissions (so an unattended run doesn't stop on a prompt)

Pre-grant the reads/writes in **`.claude/settings.local.json`** (gitignored, per-user — *not*
the shared `settings.json`). The shared `deny` list still blocks the dangerous ops, and `deny`
beats `allow`:

```jsonc
// .claude/settings.local.json  (gitignored — personal)
{
  "permissions": {
    "allow": [
      "Bash(gh issue create:*)", "Bash(gh issue edit:*)", "Bash(gh issue comment:*)",
      "Bash(gh issue list:*)", "Bash(gh issue view:*)",
      "Bash(gh pr list:*)", "Bash(gh pr view:*)",
      "Bash(gh project item-list:*)", "Bash(gh project item-edit:*)",
      "Bash(gh project field-list:*)", "Bash(gh project item-add:*)",
      "Bash(git fetch:*)", "Bash(git grep:*)", "Bash(git diff:*)", "Bash(git log:*)",
      "Bash(bash .claude/skills/pm-triage/scripts/board.sh:*)",
      "Bash(bash .claude/skills/pm-triage/scripts/state.sh:*)"
    ]
  }
}
```

(Rules are prefix matches on the exact command string; the script rules assume the
`bash .claude/skills/pm-triage/scripts/<name>.sh …` form the prompt uses.)

## Dry run first (writes nothing)

```text
/pm-triage all report-only
```

## The routine prompt (paste into the desktop routine)

> You are running the WaveHouse PM-triage daily routine, on the local machine, in the
> `Wave-RF/WaveHouse` repo. The procedure's source of truth is
> `.claude/skills/pm-triage/SKILL.md` — read and follow it. Scope: `all`. Run-mode: `safe-auto`,
> **UNATTENDED** (no human is watching — do NOT ask; queue risky items instead of applying them).
>
> 1. Resolve state: `bash .claude/skills/pm-triage/scripts/state.sh ensure` → state worktree
>    path `S`. Read `S/state.json` (`{schema,last_run_at,last_sha,filed}`) and `S/pending.md`.
> 2. `git fetch origin`. Do all code/TODO reads against `origin/main` by ref (`git grep`,
>    `git diff <last_sha>..origin/main`) — never checkout or switch branches.
> 3. Triage incrementally since `last_sha`/`last_run_at` (full sweep if state is empty):
>    **status** (open issues + PRs vs board #7), **backlog** (launch-gated P0–P3 per
>    `references/rubric.md` for new/changed issues + any missing a priority), **todos**
>    (new/changed `TODO|FIXME|HACK|XXX` since `last_sha`).
> 4. Apply the SAFE tier automatically (set *missing* priorities; fix board Status to reality;
>    file clearly-new issues — dedupe against `filed` + live issues/PRs first, provenance footer,
>    ≤8/run; tick epic boxes for closed issues). Use `scripts/board.sh` for board writes.
> 5. Queue the RISKY tier to `S/pending.md` (teammate priority changes, security/disclosure,
>    closes/dupes, body edits, cross-repo) — do NOT apply unattended.
> 6. Update state: write `S/state.json` (keep `schema`; `last_run_at`=now, `last_sha`=`git rev-parse
>    origin/main`; extend `filed` with anything you filed), refresh `S/pending.md` (drop items you can
>    see were resolved), append a line to `S/runs.log`, then
>    `bash .claude/skills/pm-triage/scripts/state.sh commit "<one-line summary>"`.
> 7. If nothing changed, still write `last_run_at`/`last_sha` but skip the commit.
>
> Follow every operating convention in SKILL.md; respect `.claude/settings.json`'s deny-list
> (never merge PRs, never `gh pr ready`). End with a short summary: counts (filed /
> priority-set / status-fixed), the launch-blocking shortlist (P0/P1), and how many items are in
> `pending.md` awaiting you.
