---
title: "Claude Code & AI agents"
description: "Working with Claude Code in the WaveHouse repo — what ships in .claude/ and .githooks/, how enforcement is layered, and recommended user-level setup."
sidebar:
  order: 12
---

WaveHouse ships minimal team-wide [Claude Code](https://claude.com/claude-code) config: only what's useful to every contributor. Cosmetic and personal choices stay at the user level.

New to Claude Code? The [official docs](https://code.claude.com/docs) cover basics; this page is WaveHouse-specific.

## Quick setup

1. **Install Claude Code**: [official install guide](https://code.claude.com/docs/en/quickstart), or macOS `brew install --cask claude-code`.

2. **Authenticate**: log in to your Max subscription on first run.

3. **Bootstrap the repo**: `make tools` installs Go tools and pnpm deps and sets git hooks (`git config core.hooksPath .githooks`) for the pre-commit/pre-push gates.

4. **Optional — worktrunk**: [worktrunk](https://worktrunk.dev) manages parallel-agent worktrees via `.config/wt.toml`.

## How enforcement is layered

| Layer | Lives in | Applies to | Purpose |
| ----- | -------- | ---------- | ------- |
| **Git hooks** | `.githooks/` (installed by `make tools`) | Humans + Claude uniformly | Hard enforcement: `make verify` on commit; before push, `make ci` for code changes or `make verify` for docs-only (classifier-gated) |
| **Claude Code agent gate** | `.claude/hooks/agent-bash-gate.sh` (PreToolUse Bash) + `.claude/settings.json` deny rules | Agents only | Enforces [Agent PR Discipline](#agent-pr-discipline): drafts only, no human reviewer adds; a marker from every `scripts/pre-push-reviewers.sh` reviewer for pushes ahead of `main`; `scripts/lint-pr-title.sh` on `gh pr create` / `gh pr edit --title` |
| **Claude Code ergonomic hooks** | `.claude/hooks/gofumpt-on-save.sh` (PostToolUse Edit/Write/MultiEdit), `.claude/hooks/review-marker.sh` (SubagentStop) | Claude only | gofumpt: auto-format on edits. review-marker: writes `tmp/<name>-passed-<HEAD-sha>` on `VERDICT: ship_it` |
| **Claude Code skills / agents / commands** | `.claude/skills/`, `.claude/agents/`, `.claude/commands/` | Claude only (when relevant) | Workflow guidance and helpers — not gates |

Git hooks are the source of truth for "must pass before merge"; `.claude/` adds agent-specific gates and ergonomics, never substitutes for them.

## Git hooks (`.githooks/`)

Two committed scripts:

| Hook | Behavior |
| ---- | -------- |
| `pre-commit` | Runs `make verify` (tidy, fmt, vulncheck, lint; ~30s). **Blocks on failure**. Skipped if `make ci`/`make verify` already ran for this tree state (cached via `scripts/ci-marker.sh`). |
| `pre-push` | Uses `scripts/classify-paths.sh`: **code** changes require the `make ci` marker (`tmp/ci-passed-tree-<TREE-sha>`); **docs/prose-only** pushes require the `make verify` marker (`tmp/verify-passed-tree-<TREE-sha>`). CI also skips Go/SDK suites for docs. **Blocks** if markers are absent; unclassifiable pushes default to requiring `make ci`. |

Use `--no-verify` only for intentional WIP/drafts. Agents must not use it (see AGENTS.md §"Agent PR Discipline").

Tree-keyed markers allow commit-then-push without re-running if the tree is unchanged. `make ci`/`verify` skip marker writes when `$CI` is set. Shared logic: `scripts/ci-marker.sh`.

## What's in `.claude/` and `.config/`

| Path | Purpose |
| ---- | ------- |
| `.claude/settings.json` | Team-wide: `deny` permissions (force-push, git reset --hard, filter-branch, update-ref -d, gh pr merge/ready/approve/request-changes, gh repo/release delete, gh secret delete, gh workflow disable, rm -rf / sudo rm); all three hooks wired |
| `.claude/hooks/gofumpt-on-save.sh` | PostToolUse Edit/Write/MultiEdit: auto-formats `.go` files |
| `.claude/hooks/agent-bash-gate.sh` | PreToolUse Bash: blocks Agent PR Discipline violations (drafts only; markers from all `scripts/pre-push-reviewers.sh` reviewers for pushes ahead of `main`; titles linted by `scripts/lint-pr-title.sh` on `gh pr create`/`edit --title`) |
| `.claude/hooks/review-marker.sh` | SubagentStop: writes `tmp/<name>-passed-<HEAD-sha>` on `VERDICT: ship_it` (reviewer set from `scripts/pre-push-reviewers.sh`). Filters by `agent_type`; reads the `.last_assistant_message` string, not structured `tool_response` |
| `.claude/commands/cover.md` | `/cover [suite]` — suite dispatch and coverage threshold analysis |
| `.claude/commands/docs-review.md` | `/docs-review [path\|all]` — launches `docs-reviewer` subagent. No-arg = gating pre-push review; path/`all` = advisory |
| `.claude/commands/prepush.md` | `/prepush [all]` — pre-push gate: reads `scripts/pre-push-reviewers.sh`, runs required reviewers in parallel (fresh context), skips others via `scripts/skip-pre-push-review.sh`, loops to `ship_it`. `all` forces full set |
| `.claude/agents/pre-push-reviewer.md` | `pre-push-reviewer` subagent — canonical pre-push code review (via `scripts/pre-push-reviewers.sh`); also used for local PR auditing |
| `.claude/agents/docs-reviewer.md` | `docs-reviewer` subagent — docs-prose and code↔docs-sync review; mandatory pre-push gate (writes `tmp/docs-reviewer-passed-<HEAD-sha>` on ship_it); advisory for ad-hoc paths. Scope via `scripts/docs-prose.sh` |
| `.claude/skills/pr-sync-with-main/SKILL.md` | "Fix this stale PR" workflow — merge origin/main; no rebase or force-push |
| `.claude/skills/pr-review-locally/SKILL.md` | "Review PR <N> locally" workflow — `wt switch pr:<N>` + parallel reviewers (code, docs, etc.); no PR comments |
| `.claude/skills/pm-triage/SKILL.md` | PM-review workflow (`/pm-triage`) — triage feedback/backlog/TODOs and reconcile issue/PR status into Task Board (project #7) issues |
| `.claude/skills/integration-astro-view-transitions/` | Vendored PostHog skill (v1.21.1) — docs-site analytics patterns: web snippet, ClientRouter view-transitions guard, user identify |
| `.claude/settings.local.json` | Personal overrides (gitignored): model choice, status line, allow lists, etc. |
| `.config/wt.toml` | Worktrunk project hooks (post-start, pre-merge, pre-remove) |

Absent: `.mcp.json`, a committed status line, a `permissions.allow` list — see [GitHub access](#github-access-gh-cli-vs-mcp) and [Permission posture](#permission-posture).

## Slash commands

| Command | What it does |
| ------- | ------------ |
| `/cover [suite]` | Renders coverage for a suite (unit / integration / e2e / sdk / all / merge) and surfaces drops below threshold |
| `/docs-review [path\|all]` | Runs `docs-reviewer` subagent for accuracy, examples, clarity, completeness, and code↔docs sync. No-arg gates the push (writes docs marker on ship_it); path/`all` is advisory. Complements misspell / markdownlint / links-validator |
| `/prepush [all]` | Mandatory pre-push review: runs required `scripts/pre-push-reviewers.sh` in parallel until each reaches `ship_it`. `all` forces the full set |

To add commands, place a `.md` file in `.claude/commands/`. Filename is the command. Frontmatter requires `description` and `argument-hint`; body is the prompt using `$ARGUMENTS`.

## Subagents

| Subagent | When to use |
| -------- | ----------- |
| `pre-push-reviewer` | **Mandatory before pushing to PR branches** (enforced by `.claude/hooks/agent-bash-gate.sh`), run in parallel via `scripts/pre-push-reviewers.sh`; all must reach `ship_it`. Also used for auditing others' PRs after `wt switch pr:<N>`. Runs `.github/prompts/pr-review.md` in fresh context, fetching PR comments, CI status, and linked-issue criteria. Returns `[MUST]`/`[SHOULD]`/`[MAY]` findings plus a parseable `VERDICT: ship_it\|iterate\|block` driving the `tmp/pre-push-reviewer-passed-<HEAD-sha>` marker. |
| `docs-reviewer` | **Mandatory before pushing to PR branches** (enforced by `.claude/hooks/agent-bash-gate.sh`), run in parallel via `scripts/pre-push-reviewers.sh`. Reviews prose accuracy, runnable examples, clarity, completeness, and code↔docs sync via `.github/prompts/docs-review.md` over the `scripts/docs-prose.sh` denylist (Starlight site + governance docs including SDK readme). Branch scope emits `VERDICT: ship_it\|iterate\|block` $\rightarrow$ `tmp/docs-reviewer-passed-<HEAD-sha>`; path/`all` is advisory. Never comments on PRs or edits docs. Complements misspell, markdownlint, and starlight-links-validator. |

Invoke via the `Agent` tool with `subagent_type: pre-push-reviewer`, or via `/agents`.

To add one: a `.md` file in `.claude/agents/` with frontmatter (`description`, `tools`, `model`) and a system-prompt body. For a **mandatory pre-push gate**, add its name to `scripts/pre-push-reviewers.sh` (single source of truth for hooks and `/prepush`) — AGENTS.md §"Adding a pre-push reviewer".

## Skills

Skills load into Claude's context when conversation patterns match their `description`.

| Skill | Triggers on |
| ----- | ----------- |
| `pr-sync-with-main` | PR "out-of-date with base branch", or "fix the PR"/"sync with main". Documents the merge-not-rebase procedure and why WaveHouse has long-lived branches. |
| `pr-review-locally` | Requests to "review", "audit", or "look at PR <N>". Pulls it via `wt switch pr:<N>` (or `gh pr checkout`), runs reviewers (code, docs, …) in parallel contexts, surfaces combined findings, comments on nothing. |
| `pm-triage` | Requests to triage dogfooding feedback, re-prioritize backlog (P0–P3), sweep TODOs, or check tracking. Runs PM review against Task Board (project #7), proposes scoped issues. `/pm-triage`. |
| `integration-astro-view-transitions` | PostHog Astro docs work (snippet patterns, view-transitions guard, identify). Vendored, wizard-installed, edited for markdownlint; live setup (relay host, committed fallback) lives in `PostHog.astro` and the CHANGELOG. |

To add one: `.claude/skills/<name>/SKILL.md` with frontmatter `name` + `description` and the workflow body. Matching quality follows description quality.

## Agent PR Discipline

Agents get extra gating from `.claude/hooks/agent-bash-gate.sh` (PreToolUse Bash) and `.claude/settings.json` deny rules. Humans keep full git/gh affordances; agents don't:

- **Drafts only.** `gh pr create` must include `--draft`. Only humans may flip draft → ready (`gh pr ready`), approve (`gh pr review --approve`), or request changes (`gh pr review --request-changes`). The gate lints titles with `scripts/lint-pr-title.sh` on `gh pr create` / `gh pr edit --title`, mirroring the CI `PR title` job (fail-open if no quoted title parses).
- **No human reviewer assignment.** `gh pr edit --add-reviewer / --add-assignee` and `POST /requested_reviewers` are blocked; the `required_reviewers` ruleset requests `@Wave-RF/wavehouse-admins`, and humans handle the rest.
- **Bot re-triggers.** Agents may mention bots (e.g., `@coderabbitai review`) via `gh pr comment`.
- **Pre-push reviews.** Before `git push` on a branch ahead of `main`, agents run every reviewer in `scripts/pre-push-reviewers.sh` (currently `pre-push-reviewer` for code, `docs-reviewer` for docs) in parallel, in fresh context; `/prepush` automates it. Log skips for irrelevant changes with `scripts/skip-pre-push-review.sh <name> "<reason>"`. `ship_it` needs zero findings — any `[MUST]` / `[SHOULD]` / `[MAY]` means iterate until clean. The push gate wants a marker from every listed reviewer (`ship_it` or logged skip) and echoes skips at push time.
- **No bypass.** `--no-verify` or forged markers violate AGENTS.md §"Agent PR Discipline". Valid markers come only from `make ci`, the `review-marker.sh` hook, and `scripts/skip-pre-push-review.sh`.
- **Local reviews.** Use the `pr-review-locally` skill for audits; findings remain local and are not posted to the PR.

Full ruleset: AGENTS.md §"Agent PR Discipline".

## Worktrunk integration

We recommend [worktrunk](https://worktrunk.dev): it uses `git worktree add`, so committed `.claude/` configs work in every worktree.

Project hooks in `.config/wt.toml`:

| Hook | Command | Why |
| ---- | ------- | --- |
| `post-start` | `make tools` | Bootstraps tools, modules, pnpm deps, and git hooks |
| `pre-merge` | `make verify` | Fast pre-merge gate (same as pre-commit) |
| `pre-remove` | `git status --short` | Surfaces uncommitted work before teardown |

Common workflow:

```bash
wt switch -c feat/new-thing       # fresh worktree off origin/main
wt switch -x claude -c feat/x     # spawn an agent in a new worktree
wt list                           # status of all worktrees
wt merge                          # squash + rebase + merge + clean up
wt remove feat/new-thing          # tear down (warns on uncommitted work)
```

User config: `~/.config/worktrunk/config.toml`; committed `.config/wt.toml` holds team-wide hooks only.

## GitHub access: gh CLI vs MCP

**Use `gh` CLI as the canonical GitHub access path**, not a GitHub MCP server, because:

- `gh` is a hard dev requirement
- Works identically in Claude Code, terminal, and shell scripts
- No extra auth, approval, or npx cold-start

The GitHub MCP server suits cross-repo search and bulk graph queries — neither a daily WaveHouse pattern. To add it at user level:

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

Not committed at project level; pick what you use.

- **[Grafana MCP](https://github.com/grafana/mcp-grafana)** — query Prometheus / Loki / Tempo / Pyroscope from Claude for observability work in `internal/observability/`.
- **ClickHouse MCP** (community) — schema introspection and queries against local `make dev` ClickHouse; useful for `internal/discovery/` and ingest, though `make deps-shell` usually suffices.

Once [issue #121](https://github.com/Wave-RF/WaveHouse/issues/121) adds a SigNoz dev stack via `make dev-obs`, Grafana MCP becomes ideal for trace/log inspection.

## Permission posture

`.claude/settings.json` assumes most contributors use `bypassPermissions` mode:

- ✅ `permissions.deny`: The only programmatic guardrail; still fires.
- ❌ `permissions.allow` / `permissions.ask`: Moot (everything is allowed).
- ✅ Hooks (Claude Code and git): Still fire as the behavioral layer.

`deny`-only by design. Personal allow/ask lists go in `.claude/settings.local.json` (gitignored); `defaultMode` is omitted so users choose.

The deny list blocks:

| Blocked | Why |
| ------- | --- |
| `git push --force`, `-f`, `--force-with-lease` | Destructive; loses inline review-comment anchors |
| `git reset --hard origin:*` | Discards local work |
| `git filter-branch`, `git update-ref -d` | History-rewriting / ref destruction |
| `gh pr merge`, `gh repo delete`, `gh release delete` | Irreversible / shared-state |
| `gh pr ready`, `gh pr review --approve` / `-a` / `--request-changes` / `-r` | [Agent PR Discipline](#agent-pr-discipline) — draft→ready and reviews are human actions |
| `gh workflow disable`, `gh secret delete` | Operational footguns |
| `rm -rf /`, `rm -rf ~`, `rm -rf $HOME`, `sudo rm` | Filesystem destruction |

## Status line, output style, model

Personal preference; keep in `.claude/settings.local.json` (not committed):

```jsonc title=".claude/settings.local.json"
{
  "statusLine": { "type": "command", "command": "~/.config/claude/statusline.sh" },
  "outputStyle": "default",
  "model": "claude-opus-4-7"
}
```

## Daily workflow

1. Write code (gofumpt-on-save formats Go files).
2. `git commit`: pre-commit hook runs `make verify` (unless `make ci` already validated the tree). Fix failures.
3. `git push`: the pre-push hook blocks until `make ci` (code) or `make verify` (docs/prose) passes. For agents on a branch ahead of `main`, `agent-bash-gate` also requires markers from every reviewer in `scripts/pre-push-reviewers.sh`. `/prepush` picks and runs them in parallel (Ship it auto-writes markers; `skip-pre-push-review.sh` logs skips); on Iterate/Block, fix and re-invoke.
4. Open PR via `gh pr create --draft` (agents must use `--draft`; humans flip to ready later).
5. CI workflows fire on HEAD. Address comments per AGENTS.md §Review Response.

Helpers:

- Stale PR: "fix the PR" (`pr-sync-with-main` skill merges main; no rebase/force-push).
- Reviewing others: "review PR 120 locally" / "audit PR 120" (`pr-review-locally` skill runs `wt switch pr:120` + reviewers in parallel, no comments).
- Re-trigger bot reviewer: "ping coderabbit again" posts a `@<bot>` comment.
- Coverage check: `/cover unit`, `integration`, `e2e`, `sdk`, or `all`.

**Memory**: per-project memory lives in `~/.claude/projects/<slug>/memory/`. `AGENTS.md` is the shared source of truth; memory holds personal notes not in committed config.

## Extending

Keep `.claude/` small. Add only as needed:

- Slash commands for recurring workflows
- Subagents for specialized one-shot tasks
- Skills for context-loaded patterns
- Claude Code hooks for edit-time UX (not enforcement; use `.githooks/` for human-applicable gates)
- Adjust `.githooks/pre-commit` or `pre-push` if gates are incorrect

Commit useful additions and update this doc.

## Reference

- [AGENTS.md](https://github.com/Wave-RF/WaveHouse/blob/main/AGENTS.md) — Source of truth for AI agent work: conventions, architecture, style, doc/SDK-sync rules, branch maintenance.
- [Claude Code docs](https://code.claude.com/docs) — Official reference.
- [worktrunk.dev](https://worktrunk.dev) — Worktree manager.
- `.github/prompts/pr-review.md` — Canonical `pre-push-reviewer` local prompt.
