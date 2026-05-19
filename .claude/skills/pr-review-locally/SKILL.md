---
name: pr-review-locally
description: Use when a user wants to review someone else's open PR locally without commenting on the PR. Triggers on phrases like "review PR <N>", "look at PR <N>", "audit PR <N>", "pull down PR <N> and review", "check out PR <N> for me". Covers worktrunk's `wt switch pr:<N>` syntax (gh-CLI-backed), the `gh pr checkout` fallback, and how to optionally fire the CI claude-review for the bot to comment on the PR remotely. Pairs with the pre-push-reviewer subagent.
---

# Reviewing someone else's PR locally

For "review PR 120 locally" / "audit this PR" / "pull down PR <N> and tell me what you think" — pull the PR's content into a worktree, run the `pre-push-reviewer` subagent against it in fresh context, surface findings to the user. Don't comment on the PR.

## Procedure

### 1. Get the PR into a local worktree

Worktrunk supports `pr:N` syntax when `gh` CLI is installed:

```bash
wt switch pr:120                # creates a worktree on PR 120's branch + switches into it
```

Fallback if worktrunk isn't installed or `pr:` syntax fails:

```bash
gh pr checkout 120              # in-place checkout (no worktree)
```

Verify after switching:

```bash
git rev-parse HEAD              # should match the PR's head SHA
gh pr view --json number,state,headRefName --jq .   # confirm we're on PR 120's branch
```

### 2. Invoke the `pre-push-reviewer` subagent

Use the `Agent` tool:

```
Agent({
  subagent_type: "pre-push-reviewer",
  description: "Review PR <N> locally",
  prompt: "Review the current branch (PR <N>) against main. Use the canonical
           .github/prompts/pr-review.md workflow — full PR diff vs merge-base,
           latest commit, all open PR comments and reviews, CI status. Return
           [MUST]/[SHOULD]/[MAY] findings and a parseable verdict line."
})
```

The subagent runs in **fresh context** — no contamination from your current session's assumptions. That's the whole point: the review should be cold-eyed.

### 3. Surface findings to the user

Present the subagent's output. Don't auto-fix anything — the PR belongs to someone else. The user decides what to do with the findings.

If the user asks "should I approve?", that's their call — you can summarize the verdict (`Ship it` / `Iterate` / `Block`) and highlight the highest-severity findings, but the actual approval decision is theirs.

## Two modes

### Local-only (default)

The above procedure. Findings stay in your local session. Nothing is posted to the PR. Use for "I want a thorough read of this PR before I review it manually," "what would Claude flag here?", or "audit this PR for me."

### Make the bot comment on the PR remotely

If the user explicitly asks for the bot to post on the PR (not just local feedback), the mechanism is the **CI claude-review workflow** — NOT manual comments from the local session. Trigger it:

```bash
gh workflow run "Claude PR review" -f pr_number=<N>
```

That fires `.github/workflows/claude-review.yml` against PR `<N>`. `gh` picks up the repo from the current worktree, matching the convention used elsewhere in AGENTS.md. The workflow:

- Runs the same `.github/prompts/pr-review.md` prompt
- Posts inline review comments at line-level
- Edits a sticky verdict summary comment (one per PR)
- Gates on trust (HEAD's author / committer must have ≥read on the repo)

This is the same path as posting `@claude` or `/review` as a PR comment — both end up firing the workflow.

## What NOT to do

- ❌ **Don't run the reviewer locally and then post comments on the PR manually.** The CI workflow exists for that and handles trust gating + sticky-comment editing properly. Manual comments fragment the dialog and don't match the bot's conventions.
- ❌ **Don't `--add-reviewer` yourself or others to the PR.** Blocked by `.claude/hooks/agent-bash-gate.sh` anyway. Bot reviewers re-trigger via comment mentions; humans add themselves.
- ❌ **Don't switch back to your own branch before presenting findings.** Stay in the PR's worktree until you've reported to the user.
- ❌ **Don't approve, request-changes, or merge the PR.** Approval is humans-only (`gh pr review --approve` is denied). Merge is denied. Request-changes is denied. Surface findings, let the human reviewer act.

## Cleaning up

After the review, the user might want to switch back. If they used `wt switch`, the worktree persists — they can `wt switch <main-branch>` to return, or `wt remove <pr-branch>` to tear down. If they used `gh pr checkout`, their previous branch state was modified; `git switch -` returns to it. Either way, don't tear down the worktree without asking — the user might want to keep poking at the PR.

## If the PR can't be checked out

Common failures:

- **Auth**: `gh auth status` to confirm you're authenticated; `gh auth login` if not.
- **Closed/merged PR**: `gh pr view 120 --json state` — if not `OPEN`, you're reviewing historical state, which is fine but worth telling the user.
- **Permission denied**: PR may be from a fork you can't push to. Reviewing the PR head SHA still works (`git fetch origin pull/120/head:pr-120` then `git checkout pr-120`).

Surface failures to the user with a clear next-step rather than silently bailing.
