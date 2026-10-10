---
title: "Claude Code & AI agents"
description: "Working with Claude Code in the WaveHouse repo — what ships in .claude/ and .githooks/, how enforcement is layered, and recommended user-level setup."
sidebar:
  order: 12
---

WaveHouse ships minimal team-wide [Claude Code](https://claude.com/claude-code) configuration. The repo commits only what's distinctly useful to every contributor; cosmetic and personal choices stay at the user level.

If you're new to Claude Code itself, the [official docs](https://code.claude.com/docs) cover the basics. This page is WaveHouse-specific.

## Quick setup

1. **Install Claude Code**: follow the [official install guide](https://code.claude.com/docs/en/quickstart). On macOS: `brew install --cask claude-code`.

2. **Authenticate**: log in to your Max subscription — Claude Code prompts you on first run.

3. **Bootstrap the repo**: `make tools`. This installs Go tools, pnpm deps, and **also configures git hooks** (`git config core.hooksPath .githooks`). Without this step, the team's pre-commit / pre-push gates won't fire.

4. **Optional — worktrunk**: install [worktrunk](https://worktrunk.dev) for parallel-agent worktree management. The team's project hooks live in `.config/wt.toml`.

## How enforcement is layered

| Layer | Lives in | Applies to | Purpose |
| ----- | -------- | ---------- | ------- |
| **Git hooks** | `.githooks/` (installed by `make tools`) | Humans + Claude uniformly | Hard enforcement: `make verify` on commit; before push, `make ci` for a code change or `make verify` for a docs-only one (classifier-gated) |
| **Claude Code agent gate** | `.claude/hooks/agent-bash-gate.sh` (PreToolUse Bash) + `.claude/settings.json` deny rules | Agents only | Catches accidental violations of [Agent PR Discipline](#agent-pr-discipline): drafts only, no human reviewer adds, a marker (from a review or a logged skip) from every reviewer in `scripts/pre-push-reviewers.sh` required for the commit each refspec publishes (its tip, or HEAD with no refspec) when `main` doesn't have it yet, judged in the worktree the push runs in; PR title linted via `scripts/lint-pr-title.sh` on `gh pr create` / `gh pr new` / `gh pr edit --title`; the `gh api` REST and GraphQL forms of these, and of a merge, checked too |
| **Claude Code ergonomic hooks** | `.claude/hooks/gofumpt-on-save.sh` + `.claude/hooks/markdown-on-save.sh` (PostToolUse Edit/Write/MultiEdit), `.claude/hooks/review-marker.sh` (SubagentStop) | Claude only | gofumpt / markdown-on-save: auto-format on file edits (humans get this from their IDE). review-marker: on a reviewer's `VERDICT: ship_it`, writes that reviewer's `tmp/<name>-passed-<sha>` marker for the commit its `REVIEWED:` line names |
| **Claude Code skills / agents / commands** | `.claude/skills/`, `.claude/agents/`, `.claude/commands/` | Claude only (when relevant) | Workflow guidance and on-demand helpers — not gates |

Git hooks are the source of truth for "must pass before merge." `.claude/` layers agent-specific gates and ergonomic hooks on top; it doesn't substitute for the universal gates.

## Git hooks (`.githooks/`)

Two scripts, both committed to the repo:

| Hook | Behavior |
| ---- | -------- |
| `pre-commit` | Runs `make verify` (the static checks listed in [Development → Makefile Targets](/development#makefile-targets), ~30s) — **blocks on failure**. Skipped if `make ci` or `make verify` already ran for the current tree state (cached via `scripts/ci-marker.sh`). A worktree with no `Makefile` has nothing to verify and commits; a missing or failing `scripts/ci-marker.sh` is reported and `make verify` runs. |
| `pre-push` | Scales the bar to the change set (same classifier CI uses, `scripts/classify-paths.sh`): a **code** change requires the `make ci` marker (`tmp/ci-passed-tree-<TREE-sha>`, from `make ci` or `make ci-remote`); a **docs/prose-only** push requires only the `make verify` marker (`tmp/verify-passed-tree-<TREE-sha>`) — CI skips the Go/SDK suites for those too. **Blocks** if the required marker is absent. Fail-closed: an unclassifiable push falls back to requiring `make ci`. A pushed commit with no `Makefile` goes through, having no code to validate; one with a `Makefile`, pushed from a worktree without `scripts/ci-marker.sh`, is blocked. |

`--no-verify` is for intentional WIP / draft pushes. Agents should not use it — policy in AGENTS.md §"Agent PR Discipline", not regex-enforced.

Both markers are tree-keyed so commit-then-push works without a re-run when the tree is unchanged. `make ci` / `make verify` skip the marker write when `$CI` is set (CI runners don't push). Shared logic lives in `scripts/ci-marker.sh`.

`make tools` sets `core.hooksPath` to the relative `.githooks`, which git resolves in each worktree: each worktree runs its own branch's hooks, and one whose branch has no `.githooks/` runs none. An absolute `core.hooksPath` runs one checkout's hooks in every worktree, which is why the hooks handle a worktree with no code. `make test-githooks` (part of `make verify`) commits and pushes through the real hooks from scratch worktrees with and without code.

## What's in `.claude/` and `.config/`

| Path | Purpose |
| ---- | ------- |
| `.claude/settings.json` | Team-wide: `deny` permissions (force-push / git reset --hard / filter-branch / update-ref -d, gh pr merge / ready / approve / request-changes, gh repo/release delete, gh secret delete, gh workflow disable, rm -rf / sudo rm), all four hooks wired, each through a guard that runs its script when it's there and fails closed when it isn't (see [When a hook script is missing](#when-a-hook-script-is-missing)) |
| `.claude/hooks/gofumpt-on-save.sh` | PostToolUse Edit/Write/MultiEdit: auto-formats `.go` files |
| `.claude/hooks/markdown-on-save.sh` | PostToolUse Edit/Write/MultiEdit: repairs MDX fence structure in `.mdx`, applies markdownlint `--fix` (WH001 unwrapping) in `.md` only, and runs misspell over docs prose in both |
| `.claude/hooks/agent-bash-gate.sh` | PreToolUse Bash: catches accidental Agent PR Discipline violations (drafts only, no human reviewer adds, a marker (from a review or a logged skip) from every reviewer in `scripts/pre-push-reviewers.sh` required for the commit each refspec publishes (its tip, or HEAD with no refspec) when `origin/main` (local `main` if there's no `origin/main`) doesn't have it yet; PR title linted via `scripts/lint-pr-title.sh` on `gh pr create` / `gh pr new` / `gh pr edit --title`; the `gh api` REST and GraphQL forms of these, and of a merge, checked too, reading the command line only: a field or body read from stdin (`-F field=@-`, `--input -`) is read from a heredoc or here-string the `gh` command reads, and one read from a file (`-F field=@file`, `--input file`) or piped in goes unseen). A gh command only mentioned in a quoted string, a heredoc body or a comment doesn't count, unless a shell runs it; a `$(…)` counts where the shell expands one (a `${…}` default, arithmetic and the body of an unquoted heredoc included). A push is judged where it runs (the tool call's `cwd`, a `cd` or `pushd` before it, `git -C`), and a marker counts from any worktree of the repository. It blocks a push it can't follow (a computed path or refspec, `GIT_DIR` or `GIT_WORK_TREE` set earlier on the line, `--all` or a glob refspec, a push behind a wrapper or in a command substitution, one in an unquoted heredoc's body included, code handed to `bash -c`, `eval` or a heredoc or here-string read by a shell, inside a `$(…)` or not) and one that follows a HEAD-moving git command on the same line, run directly or not. It reads only the command line, so it catches the common accidental forms, not deliberate evasion; it doesn't follow a script that pushes, a shell behind a wrapper or one it doesn't know (`fish -c`), code piped into a shell, `git push --tags`, git run by path or through an alias, a `$(` split by a backslash-newline inside an unquoted heredoc body, or a `$(…)` inside single quotes within a double-quoted `${x:-…}` default (the shell still runs it). A line it can't parse, such as one with an apostrophe in a comment inside `$(…)`, is refused when it mentions a push or a `gh pr` / `gh api` command. It follows 64 pieces of code a shell runs; a `gh pr` / `gh api` command left beyond them, quoted or not, is refused unjudged |
| `.claude/hooks/review-marker.sh` | SubagentStop: on a reviewer's `VERDICT: ship_it`, writes its `tmp/<name>-passed-<sha>` marker for the commit named on the report's `REVIEWED:` line, into the worktree on that commit, and only if that worktree's HEAD stayed on it for the whole review (its reflog against the subagent transcript's first timestamp). The reviewer set comes from `scripts/pre-push-reviewers.sh`; filters by `agent_type` in-script. Reads the report from `.last_assistant_message`, or, when that has no `VERDICT:` line or says `ship_it` without `REVIEWED:`, from the hand-back message in the subagent's transcript; a final `iterate` or `block` stands. An abbreviated sha counts if it names exactly one commit. Logs every decision to `tmp/review-marker.log` |
| `.claude/hooks/review-gate.test.sh` | Test for the two review hooks: feeds them synthetic events in a scratch repository with sibling worktrees (`make test-review-gate`, part of `make verify`) |
| `.claude/hooks/hook-commands.test.sh` | Test for the hook commands in `.claude/settings.json`: runs each one under `sh -c` as Claude Code does, with its script present, missing and not executable (`make test-hook-commands`, part of `make verify`) |
| `.claude/commands/cover.md` | `/cover [suite]` — suite dispatch + coverage threshold analysis |
| `.claude/commands/docs-review.md` | `/docs-review [path\|all]` — launches the `docs-reviewer` subagent. No-arg = the gating pre-push docs review; a path/`all` = advisory |
| `.claude/commands/prepush.md` | `/prepush [all]` — the pre-push gate: reads `scripts/pre-push-reviewers.sh`, runs the reviewers the change needs in parallel (fresh context), skips the rest on the record (`scripts/skip-pre-push-review.sh`), loops to `ship_it`. `all` forces the full set |
| `.claude/agents/pre-push-reviewer.md` | `pre-push-reviewer` subagent — canonical pre-push **code** review (one of the parallel pre-push reviewers in `scripts/pre-push-reviewers.sh`); also used for auditing others' PRs locally |
| `.claude/agents/docs-reviewer.md` | `docs-reviewer` subagent — docs-prose + code↔docs-sync review; **mandatory pre-push gate** (writes `tmp/docs-reviewer-passed-<sha>` for the commit on its `REVIEWED:` line on ship_it, in parallel with the other pre-push reviewers); advisory only for ad-hoc path/`all`. Scope via `scripts/docs-prose.sh` |
| `.claude/skills/pr-sync-with-main/SKILL.md` | "Fix this stale PR" workflow — merge origin/main, never rebase or force-push |
| `.claude/skills/pr-review-locally/SKILL.md` | "Review PR <N> locally" workflow — `wt switch pr:<N>` + the relevant reviewers (code, docs, …) in parallel, no PR comments |
| `.claude/skills/pm-triage/SKILL.md` | PM-review workflow — triage feedback / backlog / code TODOs and reconcile issue & PR status into well-scoped, tracked Task Board (project #7) issues; invoked as `/pm-triage` |
| `.claude/settings.local.json` | **Your personal overrides** — gitignored; put model choice, status line, allow lists, etc. here |
| `.config/wt.toml` | Worktrunk project hooks (post-start, pre-merge, pre-remove) |

Notably absent: no `.mcp.json`, no committed status line, no `permissions.allow` list. See [GitHub access](#github-access-gh-cli-vs-mcp) and [Permission posture](#permission-posture) below.

## Slash commands

| Command | What it does |
| ------- | ------------ |
| `/cover [suite]` | Renders coverage for a suite (unit / integration / e2e / sdk / all / merge) and surfaces drops below threshold |
| `/docs-review [path\|all]` | Runs the `docs-reviewer` subagent — accuracy vs code, runnable examples, clarity, completeness, + code↔docs sync. No-arg gates the push (writes the docs marker on ship_it); a path/`all` is advisory. Complements misspell / markdownlint / links-validator |
| `/prepush [all]` | The mandatory pre-push self-review — judges which reviewers in `scripts/pre-push-reviewers.sh` the change needs, runs those in parallel (fresh context), skips the rest on the record (logged), and loops until each reaches `ship_it`. `all` forces the full set |

To add a command: drop a `.md` file in `.claude/commands/`. Filename becomes the slash command. Frontmatter: `description` and `argument-hint`; body is the prompt with `$ARGUMENTS`.

## Subagents

| Subagent | When to use |
| -------- | ----------- |
| `pre-push-reviewer` | **Mandatory before pushing to a PR branch** (enforced by `.claude/hooks/agent-bash-gate.sh`), run in parallel with the other reviewers in `scripts/pre-push-reviewers.sh` — all must reach `ship_it`. Also used for auditing someone else's PR after `wt switch pr:<N>`. Runs the canonical `.github/prompts/pr-review.md` workflow against the local branch in fresh context. Fetches PR comments + CI status + linked-issue acceptance criteria when on a PR branch. Returns `[MUST]`/`[SHOULD]`/`[MAY]` findings + parseable `REVIEWED: <sha>` and `VERDICT: ship_it\|iterate\|block` lines that drive the `tmp/pre-push-reviewer-passed-<sha>` marker. |
| `docs-reviewer` | **Mandatory before pushing to a PR branch** — runs in parallel with the other pre-push reviewers (all enforced by `.claude/hooks/agent-bash-gate.sh`). Reviews docs **prose** (accuracy-vs-code, runnable examples, clarity, completeness) **and code↔docs sync** (code that changed but whose docs didn't), using `.github/prompts/docs-review.md` over the `scripts/docs-prose.sh` denylist set (Starlight site + governance docs incl. the SDK readme). Default (branch) scope emits `REVIEWED: <sha>` and `VERDICT: ship_it\|iterate\|block` → writes `tmp/docs-reviewer-passed-<sha>`; a path/`all` is advisory (no marker). Posts no PR comments, never edits docs. Complements misspell / markdownlint / starlight-links-validator, never duplicates them. |

Invoke via the `Agent` tool with `subagent_type: pre-push-reviewer`, or via `/agents`.

To add a subagent: drop a `.md` file in `.claude/agents/`. Frontmatter: `description` (used by main Claude to decide when to delegate), `tools`, `model`. Body is the system prompt. To make a reviewer a **mandatory pre-push gate** (the way `pre-push-reviewer` and `docs-reviewer` are), also add its name to `scripts/pre-push-reviewers.sh` — the single source of truth the hooks and `/prepush` read, so no hook needs editing. The subagent itself must pin the commit with `git -C <worktree> rev-parse HEAD` before reading anything, where `<worktree>` is the worktree its prompt names, else its current directory, and end its report with the parseable `REVIEWED: <that sha>` and `VERDICT: ship_it|iterate|block` lines; without both, its `ship_it` writes no marker (`tmp/review-marker.log` says why). See AGENTS.md §"Adding a pre-push reviewer".

## Skills

Skills load automatically into Claude's context when conversation patterns match their `description`.

| Skill | Triggers on |
| ----- | ----------- |
| `pr-sync-with-main` | When a PR shows "out-of-date with base branch", or a user asks to "fix the PR" / "sync with main". Documents the merge-not-rebase procedure and the WaveHouse-specific reason long-lived branches need it. |
| `pr-review-locally` | When a user asks to "review PR <N>", "audit PR <N>", "look at PR <N>" — pulls the PR down via `wt switch pr:<N>` (or `gh pr checkout`), runs the relevant reviewers (code, docs, …) in parallel in fresh context, surfaces their combined findings without commenting on the PR. |
| `pm-triage` | When a user asks to triage dogfooding feedback, re-prioritize the backlog (P0–P3), sweep code TODOs, or check that work is tracked — runs a PM-style review against the Task Board (project #7) and proposes well-scoped issues. Invoked as `/pm-triage`. |

To add a skill: create `.claude/skills/<name>/SKILL.md` with frontmatter `name` + `description` and the workflow body. Description quality matters — that's what Claude matches against to load the skill.

## Agent PR Discipline

Agents (Claude Code etc.) have additional gating beyond what humans face — enforced by `.claude/hooks/agent-bash-gate.sh` (PreToolUse Bash) + deny rules in `.claude/settings.json`. Humans keep full git/gh affordances; agents have these extra constraints:

- **Drafts only.** `gh pr create` (or its alias `gh pr new`) must include `--draft`. Only humans transition draft → ready (`gh pr ready` is blocked), approve (`gh pr review --approve` is blocked), or request changes (`gh pr review --request-changes` is blocked). The gate also lints the PR title on `gh pr create` / `gh pr new` / `gh pr edit --title` via `scripts/lint-pr-title.sh` — the same rule as the required `CI` check's `PR title` job, so a malformed title is caught before the PR exists (fail-open when the title is missing or computed).
- **The API forms count too.** Through `gh api`, opening a PR (`…/pulls`) needs `-F draft=true` and a title that passes the lint, and a review with `event=APPROVE` or `event=REQUEST_CHANGES` is blocked. Through GraphQL, `createPullRequest` needs `draft: true`, and `markPullRequestReadyForReview`, an approving or changes-requesting `addPullRequestReview` / `submitPullRequestReview`, and `requestReviews` are blocked. A merge through the API (`PUT …/pulls/<N>/merge`, `mergePullRequest`, `enablePullRequestAutoMerge`) is blocked. `gh pr merge` itself is left to the `deny` rule in `.claude/settings.json`, because a person merges with it from shell mode (`!`). Each `gh api` call is judged by its own endpoint (its first argument that isn't a flag) and its own fields, so a comment or review-thread reply whose body names an endpoint doesn't count, and neither does another call on the same line. Code a shell runs (a `$(…)` wherever it is, the body of an unquoted heredoc included, `bash -c '…'` with or without options before `-c`, `eval`, a heredoc a shell reads) is judged the same way, and so is a GraphQL query computed by a heredoc inside `$(…)`. `gh -R <repo> pr …` and `gh pr -R <repo> …` (or `--repo`) count as `gh pr …`. Flags are read as gh reads them, so a short-flag cluster counts (`-fr alice` requests a reviewer) and a flag's value doesn't (`-b "-a thing"` is a body). The gate reads the command line only: a field or body gh reads from stdin (`-F field=@-`, `--input -`) is read from a heredoc or here-string the `gh` command itself reads, and one read from a file (`-F field=@file`, `--input file`) or piped in goes unseen.
- **No human reviewer assignment.** `gh pr edit --add-reviewer / --add-assignee`, `--reviewer` / `--assignee` (or `-r` / `-a`, alone or in a short-flag cluster such as `-fr`) on `gh pr create` / `gh pr new`, and any `gh api` write to `…/requested_reviewers` are blocked. GitHub assigns reviewers natively — the `required_reviewers` ruleset rule requests the `@Wave-RF/wavehouse-admins` team and the team's code-review assignment picks the member; humans handle the rest.
- **Bot re-triggers via comments.** Agents CAN mention bots in PR comments to re-trigger reviews — `@coderabbitai review`, etc. This goes through `gh pr comment` (allowed), not the reviewer API.
- **Pre-push review required on PR branches.** Before `git push` to a branch with commits ahead of `main`, the agent runs the reviewers in `scripts/pre-push-reviewers.sh` (today: `pre-push-reviewer` for code, `docs-reviewer` for docs prose + code↔docs sync) that the change needs — in fresh context, in parallel — and **skips** any with nothing to do via `scripts/skip-pre-push-review.sh <name> "<reason>"` (a logged skip that satisfies the marker, so a docs typo doesn't pay for a full code review). `/prepush` does all of this. `ship_it` requires zero findings at any severity — any `[MUST]` / `[SHOULD]` / `[MAY]` forces iterate; fix and re-invoke (always fresh context) until clean. The push gate requires a marker — from a `ship_it` or a logged skip — from every listed reviewer for the commit each refspec publishes (its tip, or HEAD with no refspec), judged in the worktree the push runs in, and echoes the skips at push time. A `ship_it` marks only the commit the reviewer read, so don't commit in a worktree while its reviewers run.
- **Don't bypass.** `--no-verify` and hand-writing markers are policy violations, not regex-blocked. An agent that wants to bypass can edit the gate itself — trust the policy in AGENTS.md §"Agent PR Discipline". Markers come from `make ci`, the `review-marker.sh` hook, and `scripts/skip-pre-push-review.sh` (logged skips); nothing else.
- **PR reviews on others' PRs stay local.** Use the `pr-review-locally` skill for local-only audits — the reviewers' findings go to you, not the PR.

Full ruleset and rationale: AGENTS.md §"Agent PR Discipline".

### When a hook script is missing

Each hook in `.claude/settings.json` names its script under `$CLAUDE_PROJECT_DIR`, which the Claude Code docs define as ["the project root where the session started"](https://code.claude.com/docs/en/hooks#reference-scripts-by-path), and which stays put when the session enters a worktree. If that checkout is removed while the session runs, or a script goes missing or loses its executable bit, a bare path would make the shell exit 127, and Claude Code treats any exit code but 2 as a [non-blocking error](https://code.claude.com/docs/en/hooks#other-exit-codes): the push gate would stop gating without a word. So each command is a short guard that `exec`s the script when it's there and executable, unchanged, and otherwise acts by event:

| Event | Script | With the script missing |
| ----- | ------ | ----------------------- |
| PreToolUse (Bash) | `agent-bash-gate.sh` | Blocks (exit 2) a command that contains the words `git` and `push`; `gh` and `pr` with one of `create`, `new`, `edit`, `ready`, `review` or `merge`; or `gh` and `api` with `pulls` or `graphql`, which covers every call to the pull-request or GraphQL API, reads included. It also blocks every command when there is no `jq` to read it with. Other commands go through, so the session can still work |
| SubagentStop | `review-marker.sh` | Reports it on stderr with exit 1. Exit 2 [would keep the subagent running](https://code.claude.com/docs/en/hooks#exit-code-2-behavior-per-event). A reviewer's `ship_it` writes no marker, so the push gate blocks for lack of one |
| PostToolUse | `gofumpt-on-save.sh`, `markdown-on-save.sh` | Reports it with exit 2, which shows Claude the message; the edit has already happened |

Every message names the missing script and says to restart Claude Code from a live checkout. The PreToolUse match is crude on purpose: like the gate, it guards against accidents, not deliberate workarounds, and it errs toward blocking.

## Worktrunk integration

We use [worktrunk](https://worktrunk.dev) as the recommended worktree manager. It creates real `git worktree add` directories, so the committed `.claude/` config Just Works in every worktree.

Project hooks in `.config/wt.toml`:

| Hook | Command | Why |
| ---- | ------- | --- |
| `post-start` | `wt step copy-ignored`, then `make tools` (an ordered `[[post-start]]` pipeline) | Seeds the gitignored caches listed in `.worktreeinclude` (`node_modules/`, `.bin/`, build outputs) from the primary checkout, then bootstraps tools, modules, pnpm deps, **and git hooks**. The install runs after the copy, so it decides the final `node_modules` layout, not the copy |
| `pre-merge` | `make verify` | Fast pre-merge gate (same as pre-commit) |
| `pre-remove` | `git status --short` | Surfaces uncommitted work before tearing down |

Common workflow:

```bash
wt switch -c feat/new-thing       # fresh worktree off origin/main
wt switch -x claude -c feat/x     # spawn an agent in a new worktree
wt list                           # status of all worktrees
wt merge                          # squash + rebase + merge + clean up
wt remove feat/new-thing          # tear down (warns on uncommitted work)
```

User-specific worktrunk config goes in `~/.config/worktrunk/config.toml`; the committed `.config/wt.toml` has team-wide hooks only.

## GitHub access: gh CLI vs MCP

**We use `gh` CLI as the canonical GitHub access path**, not a GitHub MCP server. Reasons:

- `gh` is already a hard dev requirement
- Works identically in Claude Code, terminal, and shell scripts
- No extra auth / approval / npx cold-start

The GitHub MCP server is useful for cross-repo code search and bulk graph queries, but neither is a daily WaveHouse pattern. If you want it, add at user level:

```jsonc title="~/.claude.json"
// ~/.claude.json — your user-level config
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": { "GITHUB_PERSONAL_ACCESS_TOKEN": "${GITHUB_TOKEN}" }
    }
  }
}
```

Then `export GITHUB_TOKEN=$(gh auth token)` in your shell rc.

## Other useful MCP servers (user-level, optional)

None of these are committed at project level — pick what you actually use.

- **[Grafana MCP](https://github.com/grafana/mcp-grafana)** — for querying Prometheus / Loki / Tempo / Pyroscope from Claude when debugging observability work. Useful if you touch `internal/observability/`.
- **ClickHouse MCP** (community) — direct schema introspection + query against your local `make dev` ClickHouse. Useful for `internal/discovery/` and ingest work, but `make deps-shell` (clickhouse-client REPL) is often enough.

`make obs-grafana` brings up a local Grafana LGTM stack (see [Deployment → Local Observability Stack](/deployment#local-observability-stack)), so Grafana MCP pointed at it is already the natural choice for trace / log inspection — no heavyweight cluster to stand up first.

## Permission posture

`.claude/settings.json` is structured around the reality that most contributors run Claude Code in `bypassPermissions` (yolo). In that mode:

- ✅ `permissions.deny` **still fires** — the only programmatic guardrail
- ❌ `permissions.allow` / `permissions.ask` are moot (everything's already allowed)
- ✅ Hooks (Claude Code AND git) **still fire** — the behavioral layer

So the file is `deny`-only by design. Personal allow / ask lists go in `.claude/settings.local.json` (gitignored).

`defaultMode` is intentionally not set in committed config. Each user picks their preferred mode.

The deny list blocks:

| Blocked | Why |
| ------- | --- |
| `git push --force`, `-f`, `--force-with-lease` | Force-pushing is destructive; also loses inline review-comment anchors |
| `git reset --hard origin:*` | Throws away local work |
| `git filter-branch`, `git update-ref -d` | History-rewriting / ref destruction |
| `gh pr merge`, `gh repo delete`, `gh release delete` | Irreversible / shared-state |
| `gh pr ready`, `gh pr review --approve` / `-a` / `--request-changes` / `-r` | [Agent PR Discipline](#agent-pr-discipline) — draft→ready and review verdicts are human actions |
| `gh workflow disable`, `gh secret delete` | Operational footguns |
| `rm -rf /`, `rm -rf ~`, `rm -rf $HOME`, `sudo rm` | Filesystem destruction |

## Status line, output style, model

Not committed at project level. Personal preference — put in `.claude/settings.local.json`:

```jsonc title=".claude/settings.local.json"
{
  "statusLine": { "type": "command", "command": "~/.config/claude/statusline.sh" },
  "outputStyle": "default",
  "model": "claude-opus-4-7"
}
```

## Daily workflow

1. Write code (gofumpt-on-save formats Go files as you go; markdown-on-save does the same for `.md`/`.mdx`).
2. `git commit` → pre-commit hook runs `make verify` (or skips if `make ci` already validated this tree). Fix anything that fails.
3. `git push` → the pre-push hook blocks until the tree is validated for what changed — `make ci` for a code change, `make verify` for a docs/prose-only one (run it, fix, retry). For agents, the agent-bash-gate hook also requires a marker for the pushed commit from every reviewer in `scripts/pre-push-reviewers.sh` whenever `main` doesn't have that commit yet — including the first push, before the PR exists. Run `/prepush` → it judges which reviewers the change needs, runs those in parallel (on Ship it each marker auto-writes), and skips the rest on the record (`skip-pre-push-review.sh` writes their markers + logs why) → push succeeds. On Iterate/Block, fix, re-invoke (fresh context each time), repeat.
4. Open the PR with `gh pr create --draft` (agents required to use `--draft`; humans flip to ready when ready).
5. CI workflows fire on the new HEAD. Address review comments per AGENTS.md §Review Response.

Helpers along the way:

- Stale PR ("out-of-date with base branch") → ask Claude to "fix the PR" (loads `pr-sync-with-main` skill — merges main, doesn't rebase or force-push).
- Reviewing someone else's PR → "review PR 120 locally" / "audit PR 120" (loads `pr-review-locally` skill — `wt switch pr:120` + the relevant reviewers in parallel, no PR comments).
- Re-trigger a bot reviewer → ask Claude to "ping coderabbit again" — Claude posts the appropriate `@<bot>` comment on the PR.
- Coverage check on a specific suite → `/cover unit` (or `integration`, `e2e`, `sdk`, `all`).

**Memory**: Claude Code maintains per-project memory in `~/.claude/projects/<slug>/memory/`. `AGENTS.md` is the SHARED source of truth; memory is for personal observations / preferences that don't belong in committed config.

## Extending

The `.claude/` layout is intentionally small. Add things when they earn their keep:

- New slash commands as recurring workflows emerge
- New subagents for specialized one-shot tasks
- New skills for context-loaded workflow patterns
- New Claude Code hooks for edit-time UX gaps (NOT for enforcement — gates go in `.githooks/` so they apply to humans too)
- Adjust `.githooks/pre-commit` / `pre-push` if the gates feel wrong

If you build something useful, commit it and update this doc.

## Reference

- [AGENTS.md](https://github.com/Wave-RF/WaveHouse/blob/main/AGENTS.md) — project conventions, architecture, code style, doc-sync rules, SDK-sync rules, branch maintenance. Source of truth for all AI agent work.
- [Claude Code docs](https://code.claude.com/docs) — official Claude Code reference.
- [worktrunk.dev](https://worktrunk.dev) — worktree manager.
- `.github/prompts/pr-review.md` — the canonical review prompt that `pre-push-reviewer` runs locally.
