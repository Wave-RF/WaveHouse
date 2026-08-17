# Security Policy

## Supported Versions

| Version        | Supported   |
| -------------- | ----------- |
| Latest release | Yes         |
| Older releases | Best effort |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Instead, report vulnerabilities by emailing **<security@wave-rf.com>** with:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will acknowledge receipt within 48 hours and aim to provide an initial assessment within 5 business days.

## Security Considerations

WaveHouse handles data and enforces strict isolation:

- **JWT validation**: The JWT middleware always runs (there is no on/off switch). Signing supports either an HMAC shared secret or a remote JWKS endpoint (`auth.jwks_url`). Accepted signing algorithms are restricted to the configured verifier's family — HMAC accepts only `HS256/384/512`, JWKS only the asymmetric set (`RS*`/`ES*`/`PS*`/`EdDSA`) — and the token's `alg` header is validated before any key material is used, so `alg: none` and algorithm-confusion attacks (re-signing with `HS256` against a JWKS deployment's public key) are rejected. A request with no token, or an invalid/expired one, falls back to the policy `default_role`; elevated access requires a valid token, and a denied request that carried a bad token fails loud (`401`) rather than as a bare `403`.
- **Role-based access control**: Roles are extracted from a configurable JWT claim path. Non-admin roles have per-table, per-column policies enforced on ingest, query, and the live SSE stream; row-level rules split by path — insert `check` constraints are enforced (and auto-injected) on ingest, while select `filter` predicates apply to structured queries and the live SSE stream (the stream's in-memory row-filter comparison has a documented fail-closed boundary — see the [access-control docs](https://wavehouse.dev/access-control#where-each-rule-is-enforced)); the admin role (`policy.admin_role`, `"admin"` by default, exact case-sensitive match) bypasses them. The configured non-JWT operator key (`auth.operator_key`) likewise bypasses per-role policy — a matching request is authorized as a full-access platform operator without a JWT; treat it as an admin secret. A request presenting a *non-matching* operator key is logged at `WARN` and counted by `wavehouse_auth_operator_key_failures_total`, so probing of that credential is observable and alertable.
- **Input validation**: JSON payloads are validated against ClickHouse schemas before processing.
- **Query passthrough**: Raw SQL via `POST /v1/ops/query` is restricted to the admin role — the same `RequireAdmin` gate as the rest of `/v1/ops/*`. A request with no/invalid token resolves to the `default_role`, which in a production config is not the admin role (setting `default_role` equal to the admin role is a loudly-warned, dev-only escape hatch), so it cannot reach this endpoint — the one exception is a request presenting the configured `auth.operator_key`, which reaches the whole `/v1/ops/*` surface (including this endpoint) without a JWT and even under a deleted policy, so treat that key as an admin secret. Raw SQL has no per-statement scope check (a full SQL parser would be needed to authorize predicates), so the role gate is the entire authorization story. Non-admin callers use structured queries (`POST /v1/query?table={table}`, validated against schema with permission injection) or named pipes (`GET/POST /v1/pipes/{name}`); raw-SQL grants to non-admin roles via the policy engine are no longer supported (the `raw_sql` field on policies has been removed).
- **Supply chain**: Third-party GitHub Actions are pinned to full commit SHAs (enforced by the repository's Actions settings — `sha_pinning_required`). `govulncheck` runs on every push/PR. Dependabot opens weekly grouped PRs for Go modules, GitHub Actions, and the npm packages — one grouped PR covering the docs site, TS SDK, and E2E tests via the root pnpm workspace. Released artifacts ship signed [Sigstore](https://www.sigstore.dev/) build-provenance attestations — verify the container image with `gh attestation verify oci://ghcr.io/wave-rf/wavehouse:<tag> --repo Wave-RF/WaveHouse`, a downloaded release-binary archive with `gh attestation verify <file> --repo Wave-RF/WaveHouse`, and the `@wavehouse/sdk` package via its npm provenance badge or `npm audit signatures`. (Provenance covers the published binaries and image, not `go install`, which compiles from source.)

## Disclosure Policy

We follow [coordinated disclosure](https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure). Once a fix is available, we will:

1. Release a patched version
2. Publish a security advisory on GitHub
3. Credit the reporter (unless they prefer anonymity)
