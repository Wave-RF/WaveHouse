# Support

WaveHouse is in **alpha**. We're a small team building publicly while shipping the gateway toward GA. This page sets honest expectations for what we can (and can't) commit to during this phase.

## Where to ask

| Topic                                           | Where                                                                                                                                                                                                                              |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Security vulnerabilities**                    | See [SECURITY.md](SECURITY.md) — email `security@wave-rf.com`. **Do not** open a public issue.                                                                                                                                     |
| **Bug reports**                                 | Open a [bug report](https://github.com/Wave-RF/WaveHouse/issues/new?template=bug_report.md).                                                                                                                                       |
| **Feature requests**                            | Open a [feature request](https://github.com/Wave-RF/WaveHouse/issues/new?template=feature_request.md).                                                                                                                             |
| **Usage questions** ("how do I…")               | Open a thread in [GitHub Discussions → Q&A](https://github.com/Wave-RF/WaveHouse/discussions/categories/q-a). Ideas / show-and-tell / general chat live in the [other Discussion categories](https://github.com/Wave-RF/WaveHouse/discussions). Please don't open a bug-report issue for a usage question — we'll redirect.    |
| **Real-time chat**                              | No Discord or Slack today.                                                                                                                                                                                                         |
| **Commercial / enterprise interest**            | Email `hello@wave-rf.com`. WaveHouse is and will remain Apache-2.0-licensed open source; Wave RF (the parent company) is the operator behind it.                                                                                          |

## Response cadence (alpha)

- **Best-effort, no SLA.** Expect **1–2 business days** for an initial response on most issues during the alpha. Quieter weeks (holidays, freezes, releases) may stretch to a week.
- **Security reports get priority** — see [SECURITY.md](SECURITY.md) for the 48-hour acknowledgement and 5-business-day initial-assessment targets.
- **No auto-close.** If a thread sits without a response longer than a week, that's a miss on our end. Feel free to bump it.

## In scope during alpha

- Bug reports against the latest tagged release or `main` HEAD.
- Reproducible regressions vs. the previous tag.
- Documentation gaps or wrong examples (especially `getting-started.md`, `api.md`, `configuration.md`).
- Configuration questions where the docs disagree with reality.

## Out of scope during alpha

- **Older releases.** We patch the latest release; older releases are best-effort.
- **Non-ClickHouse backends.** WaveHouse is ClickHouse-specific by design — see [Why WaveHouse?](docs/src/content/docs/why-wavehouse.md).

## Code of conduct

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md). Be kind; assume good faith; do the work.
