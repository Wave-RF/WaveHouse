---
name: pr-sync-with-main
description: Use when a PR shows "This branch is out-of-date with the base branch" on GitHub, when a user asks to "fix the PR", "sync with main", "merge main into this branch", or whenever a feature branch needs to absorb upstream main commits. Covers the exact procedure (merge, never rebase, never force-push) plus the WaveHouse-specific reason this matters.
---

# Syncing a PR branch with main

When the GitHub UI says "This branch is out-of-date with the base branch" — or a teammate asks to "fix the PR" / "sync with main" — follow this workflow exactly.

## Procedure

```bash
# Working tree must be clean before merge
git status

# Fetch latest main without touching the working tree
git fetch origin main

# Merge into the current branch (the default merge commit message is fine)
git merge origin/main --no-edit

# If conflicts: STOP, surface to the user. Don't auto-resolve.

# Push the merge commit normally — no force, no force-with-lease
git push
```

After the merge:

- `make ci` for the merge commit is invalidated (HEAD SHA changed). Re-run `make ci` before pushing — the pre-push hook will block you otherwise.
- CI will fire on the merge commit. Verify it passes.
- The "out-of-date" banner on the PR clears once GitHub sees the merged base.

## Why merge, not rebase

WaveHouse has historically lost `pull_request` event firing on long-lived branches (symptom: only `pull_request_target` checks appear, regular CI doesn't trigger). The recovery is **merging origin/main into the branch**, not:

- ❌ `git rebase origin/main` — changes commit SHAs, requires force-push
- ❌ `git push --force` / `--force-with-lease` — blocked by `.claude/settings.json` deny rules anyway; also loses inline review-comment anchors
- ❌ Closing + reopening the PR — doesn't fix the underlying event-firing issue
- ❌ Pushing an empty commit (`git commit --allow-empty`) — superstitious
- ❌ Toggling draft ↔ ready — same; doesn't help

The merge approach preserves everything: inline review-thread anchors, the PR's review history, and re-triggers the standard `pull_request` events.

## If `git merge origin/main` fails with conflicts

**Stop and ask the user.** Don't make autonomous conflict-resolution decisions — the conflict might involve subtle semantic differences (two parallel changes to `internal/api/router.go`, for example, that look mechanically resolvable but break runtime behavior).

Surface the conflicting files and the conflict markers. Let the user decide how to resolve, or escalate to a teammate.

## If the user reports the merge "didn't fix the PR"

The merge is the correct action; if GitHub still shows out-of-date, possible causes:

- The push didn't actually land (check `git log origin/<branch>` vs local HEAD)
- GitHub's cache is stale (typically clears within a minute)
- A protected ref or rule is blocking the push

Don't reach for force-push as a fix. Verify the push succeeded and wait briefly.
