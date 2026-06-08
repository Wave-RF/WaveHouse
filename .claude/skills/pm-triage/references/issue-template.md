# Issue body template

Tight enough for an agent to pick up; **not a spec, and never solve it in the issue** — the fix belongs in the PR. One-line scope pointer at most.

```markdown
**Area:** <area · type> — <one-word class: bug / footgun / gap / security / friction / docs> · found via <source>

**Expected:** <one line — the contract the consumer assumed>
**Actual:** <what really happens, with the `path/file.go:line` ref>
**Impact:** <why it matters / who it bites — concrete>

<optional one line — Scope or Note: the smallest useful pointer, e.g. "reject at policy.Validate()" — NOT a design; or a maintainer TODO that already flags it>

Related: #X, #Y

---
_From <source> (e.g. WaveHouse-Stats `WAVEHOUSE-FEEDBACK.md` dogfooding / Eric's notes / codebase TODO sweep); validated by code-read against `<dev-commit>` on <date>._
```

## Labels (semantic only — priority is a board field)

- **`area/*`** per the `internal/` package: `area/api area/ingest area/query area/cache area/dedupe area/pipes area/policy area/observability area/docs area/sdk area/infra`.
- Type/flags: `bug`, `documentation`, `enhancement`, `breaking-change`, `security`, `polish`, `chore`.
- **No `P0`/`P1` labels.** Gaps to know: there is **no** `area/auth`, `area/config`, or `area/streaming` label yet — use the nearest (`area/api` + `area/policy` for auth; `area/api` for streaming). Flag the gap in the report if it recurs.

## Folding into epics

A cluster of related backlog items → **one epic** with a checklist (the `#194` SDK / `#228` auth pattern), split into discrete issues when promoted to Ready. The checklist items can carry `file:line` refs. When you promote a backlog checklist into real issues later, edit the epic body to link them (`- [ ] #NNN — …`) so closing each ticks its box.

## Provenance & cross-repo

- Always stamp the dev commit you validated against and the date.
- If the source is another repo's tracking doc and the user wants two-way tracking, add an **Upstream** column / per-entry issue links back into that doc (deterministic transform, verify counts, confirm before pushing to another repo's main).
