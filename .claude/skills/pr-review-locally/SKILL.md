---
name: pr-review-locally
description: Use when a user wants to review someone else's open PR locally without commenting on the PR. Triggers on phrases like "review PR <N>", "look at PR <N>", "audit PR <N>", "pull down PR <N> and review", "check out PR <N> for me". Covers worktrunk's `wt switch pr:<N>` syntax (gh-CLI-backed) and the `gh pr checkout` fallback. Runs the relevant reviewer subagents listed in scripts/pre-push-reviewers.sh (code, docs, …) in parallel.
---

# Reviewing someone else's PR locally

For "review PR 120 locally" / "audit this PR" / "pull down PR <N> and tell me what you think" — pull the PR's content into a worktree, run the relevant reviewer subagents against it in parallel in fresh context, surface their combined findings to the user. Don't comment on the PR.

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

### 2. Run the relevant reviewers — in parallel

List the gating reviewers and decide which apply to *this* PR's diff (same run/skip judgment as `/prepush` → "Decide, per reviewer", but lean toward running — an audit favors thoroughness and there's no push-loop cost):

```bash
scripts/pre-push-reviewers.sh        # the reviewer set, one subagent name per line
git diff --stat main...HEAD          # what the PR changes — guides which reviewers are relevant
```

Launch the relevant ones **in a single message** (one `Agent` call each → concurrent), each in **fresh context**; `subagent_type` is the reviewer name. For example:

```js
// all in one message:
Agent({ subagent_type: "pre-push-reviewer", description: "Review PR <N> (code)",
        prompt: "Review the current branch (PR <N>) vs main using .github/prompts/pr-review.md — full diff vs merge-base, latest commit, open PR comments + reviews, CI status. Return [MUST]/[SHOULD]/[MAY] findings + a verdict line." })
Agent({ subagent_type: "docs-reviewer", description: "Review PR <N> (docs)",
        prompt: "Review the current branch (PR <N>) — docs prose + code↔docs sync vs main, default branch scope. Return [MUST]/[SHOULD]/[MAY] findings + a verdict line." })
// …plus any other reviewer scripts/pre-push-reviewers.sh lists that's relevant to this PR
```

Fresh context is the whole point — cold-eyed, uncontaminated by your session. No markers matter here: this is an audit of someone else's branch, not your push (don't run `skip-pre-push-review.sh` — that's only for satisfying *your own* push gate). If a reviewer's `ship_it` happens to write a marker, it's harmless.

### 3. Surface findings to the user

Present the subagents' combined output, grouped by reviewer. Don't auto-fix anything — the PR belongs to someone else. The user decides what to do with the findings.

If the user asks "should I approve?", that's their call — you can summarize each reviewer's verdict (`Ship it` / `Iterate` / `Block`) and highlight the highest-severity findings, but the actual approval decision is theirs.

## Findings stay local

This is a local-only audit — findings stay in your session and nothing is posted to the PR. Use it for "I want a thorough read of this PR before I review it manually," "what would the reviewer flag here?", or "audit this PR for me." There is no bot-comment path from this skill; surface the findings to the user and let them decide what to act on.

## What NOT to do

- ❌ **Don't run the reviewer locally and then post comments on the PR manually.** This is a local-only audit — surface findings to the user. The PR belongs to someone else; the human reviewer decides what to post.
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
