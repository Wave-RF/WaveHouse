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

- **JWT validation**: The JWT middleware always runs (there is no on/off switch). Signing supports either an HMAC shared secret or a remote JWKS endpoint (`auth.jwks_url`). A request with no token, or an invalid/expired one, falls back to the policy `default_role`; elevated access requires a valid token, and a denied request that carried a bad token fails loud (`401`) rather than as a bare `403`.
- **Role-based access control**: Roles are extracted from a configurable JWT claim path. Non-admin roles have per-table, per-column, row-level policies enforced on ingest and query; the admin role (`policy.admin_role`, `"admin"` by default, exact case-sensitive match) bypasses them.
- **Input validation**: JSON payloads are validated against ClickHouse schemas before processing.
- **Query passthrough**: Raw SQL via `POST /v1/admin/query` is restricted to the admin role — the same `RequireAdmin` gate as the rest of `/v1/admin/*`. A request with no/invalid token resolves to the `default_role`, which in a production config is not the admin role (setting `default_role` equal to the admin role is a loudly-warned, dev-only escape hatch), so it cannot reach this endpoint. Raw SQL has no per-statement scope check (a full SQL parser would be needed to authorize predicates), so the role gate is the entire authorization story. Non-admin callers use structured queries (`POST /v1/query?table={table}`, validated against schema with permission injection) or named pipes (`GET/POST /v1/pipes/{name}`); raw-SQL grants to non-admin roles via the policy engine are no longer supported (the `raw_sql` field on policies has been removed).
- **Supply chain**: Third-party GitHub Actions are pinned to full commit SHAs. `govulncheck` runs on every push/PR. Dependabot opens weekly grouped PRs for Go modules and Actions.

## Disclosure Policy

We follow [coordinated disclosure](https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure). Once a fix is available, we will:

1. Release a patched version
2. Publish a security advisory on GitHub
3. Credit the reporter (unless they prefer anonymity)
