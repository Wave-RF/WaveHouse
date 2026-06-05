# Launch-gated priority rubric (P0–P3)

**Anchor:** the imminent public launch — `v0.1.0-alpha.1` cut **and** the repo public-flip, which happen together (tracked in **#149**). Re-anchor when the milestone moves.

- **P0 — blocks the flip.** Must be fixed before the repo goes public / the tag is cut. Data-exposure / fail-open security bugs, correctness that contradicts a claim the README/docs make, the release mechanics themselves. **Keep this set small and sharp** — "drop everything." (e.g. the `SELECT *` column-allowlist leak #223.)
- **P1 — first-alpha quality.** Wanted in, or right after, the first alpha: explicit #149 preconditions; correctness/observability footguns early adopters will hit; a cheap guard against a fail-open even if full enforcement comes later. Not a hard flip-blocker, but should land around launch. (e.g. silent-no-dedupe #219, the policy `_in` fail-open guard #224, the `time_range`/DateTime64 bug #238.)
- **P2 — blocks broader adoption.** DX / SDK / docs / perf / hardening that external adopters hit but that doesn't block the flip or the alpha. The default home for "real, but not now." Most large features and breaking refactors land here even if a teammate set them P1.
- **P3 — backlog / nice-to-have.** Future features, speculative work, clustered/multi-tenant-only concerns, micro-optimizations, big product-surface epics (CLI / Admin UI / MCP).

## Applying it

- Priority goes on the **board Priority custom field**, not labels.
- Translating a dogfooding log: those use a consumer "stable-release" scale (their P2 = "blocks widespread adoption"). Re-anchor to launch-gated — a consumer-P1 feature is often launch-P2.
- When you **lower** a teammate's existing priority, leave a one-line comment pointing at the launch triage (#149) so it isn't a silent change. Up-grades and missing-priority sets need no per-issue comment.
- The launch-blocking shortlist (P0 + P1) should stay short enough to read at a glance — if it's growing, you're probably mis-rating P2s as P1.
