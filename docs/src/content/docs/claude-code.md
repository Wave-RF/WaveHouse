---
title: "Claude Code & AI agents"
description: "Working with Claude Code in the WaveHouse repo — what ships in .claude/ and .githooks/, how enforcement is layered, and recommended user-level setup."
sidebar:
  order: 10
---

WaveHouse ships minimal team-wide [Claude Code](https://claude.com/claude-code) configuration. The repo commits only what's distinctly useful to every contributor; cosmetic and personal choices stay at the user level.

If you're new to Claude Code itself, the [official docs](https://code.claude.com/docs) cover the basics. This page is WaveHouse-specific.

## Quick setup

1. **Install Claude Code**: follow the [official install guide](https://code.claude.com/docs/en/quickstart). On macOS: `brew install --cask claude-code`.

2. **Authenticate**: run `claude setup-token` once. The CI Claude review workflow uses `CLAUDE_CODE_OAUTH_TOKEN`; locally you just need to be logged in to your Max subscription.

3. **Bootstrap the repo**: `make tools`. This installs Go tools, pnpm deps, and **also configures git hooks** (`git config core.hooksPath .githooks`). Without this step, the team's pre-commit / pre-push gates won't fire.

4. **Optional — worktrunk**: install [worktrunk](https://worktrunk.dev) for parallel-agent worktree management. The team's project hooks live in `.config/wt.toml`.

## How enforcement is layered

The team has two distinct gate layers, plus optional Claude-Code-only ergonomics:

| Layer | Lives in | Applies to | Purpose |
| ----- | -------- | ---------- | ------- |
| **Git hooks** | `.githooks/` (installed by `make tools`) | Humans + Claude uniformly | Hard enforcement: `make verify` on commit, `make ci` passed before push |
| **Claude Code hook** | `.claude/hooks/gofumpt-on-save.sh` (wired in `.claude/settings.json`) | Claude only | UX: auto-format on file edits (humans get this from their IDE) |
| **Claude Code skills / agents / commands** | `.claude/skills/`, `.claude/agents/`, `.claude/commands/` | Claude only (when relevant) | Workflow guidance and on-demand helpers — not gates |

Git hooks are the source of truth for "must pass before merge." Claude Code config layers on agentic affordances; it doesn't substitute for the gates.

## Git hooks (`.githooks/`)

Two scripts, both committed to the repo:

| Hook | Behavior |
| ---- | -------- |
| `pre-commit` | Runs `make verify` (tidy + fmt + vulncheck + lint, ~30s) — **blocks on failure**. Then emits informational stderr nudges for likely doc-sync and SDK-sync misses (see AGENTS.md §Documentation Sync and §SDK Sync). Nudges don't block. |
| `pre-push` | Checks for `tmp/ci-passed-<HEAD-sha>` marker. The `make ci` target writes this marker on success. **Blocks** if absent — Claude (or you) runs `make ci`, sees output, fixes failures, retries push. |

**Humans** can bypass with `git commit --no-verify` / `git push --no-verify` for intentional WIP / draft pushes. **Agents cannot** — `--no-verify` and direct marker writes are blocked (see [Agent PR Discipline](#agent-pr-discipline)).

The marker invalidates on every commit (HEAD SHA changes), so `make ci` re-runs after each new commit before pushing. That's the AGENTS.md rule made literal.

## What's in `.claude/` and `.config/`

| Path | Purpose |
| ---- | ------- |
| `.claude/settings.json` | Team-wide: `deny` permissions (force-push, gh pr merge / ready / approve, --no-verify, the obvious marker-write idioms, secrets), `worktree.baseRef: "fresh"` + symlinkDirectories, all four hooks wired |
| `.claude/hooks/gofumpt-on-save.sh` | PostToolUse Edit/Write/MultiEdit: auto-formats `.go` files |
| `.claude/hooks/agent-bash-gate.sh` | PreToolUse Bash: enforces Agent PR Discipline (drafts only, no human reviewer adds, no `--no-verify`, marker required on PR pushes) |
| `.claude/hooks/review-marker.sh` | PostToolUse Agent: writes `tmp/review-passed-<HEAD-sha>` when `pre-push-reviewer` returns `VERDICT: ship_it` |
| `.claude/commands/cover.md` | `/cover [suite]` — suite dispatch + coverage threshold analysis |
| `.claude/agents/pre-push-reviewer.md` | `pre-push-reviewer` subagent — canonical pre-push review, also used for auditing others' PRs locally |
| `.claude/skills/pr-sync-with-main/SKILL.md` | "Fix this stale PR" workflow — merge origin/main, never rebase or force-push |
| `.claude/skills/pr-review-locally/SKILL.md` | "Review PR <N> locally" workflow — `wt switch pr:<N>` + `pre-push-reviewer`, no PR comments |
| `.claude/settings.local.json` | **Your personal overrides** — gitignored; put model choice, status line, allow lists, etc. here |
| `.config/wt.toml` | Worktrunk project hooks (post-start, pre-merge, pre-remove) |

Notably absent: no `.mcp.json`, no committed status line, no `permissions.allow` list. See [GitHub access](#github-access-gh-cli-vs-mcp) and [Permission posture](#permission-posture) below.

## The one slash command

| Command | What it does |
| ------- | ------------ |
| `/cover [suite]` | Renders coverage for a suite (unit / integration / e2e / sdk / all / merge) and surfaces drops below threshold |

To add a command: drop a `.md` file in `.claude/commands/`. Filename becomes the slash command. Frontmatter: `description` and `argument-hint`; body is the prompt with `$ARGUMENTS`.

## Subagents

| Subagent | When to use |
| -------- | ----------- |
| `pre-push-reviewer` | **Mandatory before pushing to a PR branch** (enforced by `.claude/hooks/agent-bash-gate.sh`). Also used for auditing someone else's PR after `wt switch pr:<N>`. Runs the canonical `.github/prompts/pr-review.md` workflow against the local branch in fresh context. Fetches PR comments + CI status + linked-issue acceptance criteria when on a PR branch. Returns `[MUST]`/`[SHOULD]`/`[MAY]` findings + a parseable `VERDICT: ship_it\|iterate\|block` line that drives the pre-push marker. |

Invoke via the `Agent` tool with `subagent_type: pre-push-reviewer`, or via `/agents`.

To add a subagent: drop a `.md` file in `.claude/agents/`. Frontmatter: `description` (used by main Claude to decide when to delegate), `tools`, `model`. Body is the system prompt.

## Skills

Skills load automatically into Claude's context when conversation patterns match their `description`.

| Skill | Triggers on |
| ----- | ----------- |
| `pr-sync-with-main` | When a PR shows "out-of-date with base branch", or a user asks to "fix the PR" / "sync with main". Documents the merge-not-rebase procedure and the WaveHouse-specific reason long-lived branches need it. |
| `pr-review-locally` | When a user asks to "review PR <N>", "audit PR <N>", "look at PR <N>" — pulls the PR down via `wt switch pr:<N>` (or `gh pr checkout`), runs `pre-push-reviewer` in fresh context, surfaces findings without commenting on the PR. Documents the local-only vs CI-claude-review-comment-mode distinction. |

To add a skill: create `.claude/skills/<name>/SKILL.md` with frontmatter `name` + `description` and the workflow body. Description quality matters — that's what Claude matches against to load the skill.

## Agent PR Discipline

Agents (Claude Code etc.) have additional gating beyond what humans face — enforced by `.claude/hooks/agent-bash-gate.sh` (PreToolUse Bash) + deny rules in `.claude/settings.json`. Humans keep full git/gh affordances; agents have these extra constraints:

- **Drafts only.** `gh pr create` must include `--draft`. Only humans transition draft → ready (`gh pr ready` is blocked), approve (`gh pr review --approve` is blocked), or request changes (`gh pr review --request-changes` is blocked).
- **No human reviewer assignment.** `gh pr edit --add-reviewer / --add-assignee` and `POST /requested_reviewers` are blocked. The `housekeeping.yml` workflow auto-assigns; humans handle the rest.
- **Bot re-triggers via comments.** Agents CAN mention bots in PR comments to re-trigger reviews — `@coderabbitai review`, `@gemini-code-assist /gemini review`, `@claude` or `/review`, etc. This goes through `gh pr comment` (allowed), not the reviewer API.
- **Pre-push review required on PR branches.** Before `git push` to a branch with an open PR, the agent must invoke `pre-push-reviewer` (fresh context). `ship_it` requires zero findings at any severity — any `[MUST]` / `[SHOULD]` / `[MAY]` forces iterate. On iterate, fix the findings and re-invoke (always fresh context) — loop until clean.
- **No bypass.** `--no-verify` is blocked on `git push` / `git commit` for agents. The obvious marker-write idioms (`Bash(touch tmp/ci-passed:*)`, `Write`/`Edit` on `tmp/ci-passed-*` and `tmp/review-passed-*`) are denied at the permission layer. Everything else relies on the honest-agent rule in AGENTS.md §"Agent PR Discipline": markers come from `make ci` and the `review-marker.sh` hook — nowhere else, by any means. Bash can write a file by a dozen paths and regex enforcement is a porous game of whack-a-mole; the rule is documented, not regex-enforced.
- **PR reviews on others' PRs stay local by default.** Use `pr-review-locally` skill for local-only audits. To make the bot comment on the PR remotely, fire the CI workflow: `gh workflow run "Claude PR review" -f pr_number=<N>` — that's the canonical bot-comment path.

Full ruleset and rationale: AGENTS.md §"Agent PR Discipline".

## Worktrunk integration

We use [worktrunk](https://worktrunk.dev) as the recommended worktree manager. It creates real `git worktree add` directories, so the committed `.claude/` config Just Works in every worktree.

Project hooks in `.config/wt.toml`:

| Hook | Command | Why |
| ---- | ------- | --- |
| `post-start` | `make tools` | Bootstraps the new worktree: tools, modules, pnpm deps, **and git hooks** |
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

```jsonc
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

When [issue #121](https://github.com/Wave-RF/WaveHouse/issues/121) lands a SigNoz dev stack with `make dev-obs`, Grafana MCP pointed at that dev environment will become a natural choice for trace / log inspection.

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
| `gh pr merge`, `gh repo delete`, `gh release delete` | Irreversible / shared-state |
| `gh workflow disable`, `gh secret delete` | Operational footguns |
| `make deps-wipe`, `make clean-all` | Wipes data / docker volumes / installed tools |
| `rm -rf /`, `rm -rf $HOME`, `sudo rm` | Filesystem destruction |
| `Read(./.env)`, `Read(./.env.*)`, `Read(./secrets/**)` | Secrets shouldn't enter Claude's context |

## Status line, output style, model

Not committed at project level. Personal preference — put in `.claude/settings.local.json`:

```jsonc
{
  "statusLine": { "type": "command", "command": "~/.config/claude/statusline.sh" },
  "outputStyle": "default",
  "model": "claude-opus-4-7"
}
```

## Daily workflow

1. Write code (gofumpt-on-save formats Go files as you go).
2. `git commit` → pre-commit hook runs `make verify` + sync nudges. Fix anything that fails.
3. `git push` (first time on a feature branch) → pre-push hook blocks until `make ci` passed for HEAD. Run `make ci`, fix, retry push.
4. Open the PR with `gh pr create --draft` (agents required to use `--draft`; humans flip to ready when ready).
5. **Subsequent pushes** → agent-bash-gate hook ALSO requires `pre-push-reviewer` subagent to have run (fresh context) and returned `VERDICT: ship_it`. Invoke the subagent → on Ship it, marker auto-writes → push succeeds. On Iterate/Block, fix, re-invoke (fresh context each time), repeat.
6. CI workflows fire on the new HEAD. Address review comments per AGENTS.md §Review Response.

Helpers along the way:

- Stale PR ("out-of-date with base branch") → ask Claude to "fix the PR" (loads `pr-sync-with-main` skill — merges main, doesn't rebase or force-push).
- Reviewing someone else's PR → "review PR 120 locally" / "audit PR 120" (loads `pr-review-locally` skill — `wt switch pr:120` + `pre-push-reviewer`, no PR comments).
- Re-trigger a bot reviewer → ask Claude to "ping coderabbit again" / "re-request claude review" — Claude posts the appropriate `@<bot>` comment on the PR.
- Coverage check on a specific suite → `/cover unit` (or `integration`, `e2e`, `sdk`, `all`).
- Make the bot comment on a PR remotely → ask Claude to "fire the CI claude-review on PR 120" — `gh workflow run "Claude PR review" -f pr_number=120`.

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
- `.github/prompts/pr-review.md` — the prompt the CI Claude review runs (and what `pre-push-reviewer` mirrors locally).
- `.gemini/styleguide.md` — Gemini Code Assist review style.
