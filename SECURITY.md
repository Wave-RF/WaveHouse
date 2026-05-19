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

- **JWT validation**: When auth is enabled, all `/v1/*` endpoints require valid JWTs. Signing supports either an HMAC shared secret or a remote JWKS endpoint (`auth.jwks_url`).
- **Role-based access control**: Roles are extracted from a configurable JWT claim path. Non-admin/service roles have per-table, per-column, row-level policies enforced on ingest and query.
- **Input validation**: JSON payloads are validated against ClickHouse schemas before processing.
- **Query passthrough**: Raw SQL via `POST /v1/admin/query` is restricted to the `admin` / `service` role — the same gate as the rest of `/v1/admin/*`. Raw SQL has no per-statement scope check (a full SQL parser would be needed to authorize predicates), so the role gate is the entire authorization story; service tokens already hold admin-scoped powers across the admin tree, so carving out a tighter gate for raw SQL alone would be inconsistency without a real authorization win. Non-admin callers use structured queries (`POST /v1/tables/{table}/query`, validated against schema with permission injection) or named pipes (`GET/POST /v1/pipes/{name}`); raw-SQL grants to non-admin roles via the policy engine are no longer supported (the `raw_sql` field on policies has been removed).
- **Supply chain**: Third-party GitHub Actions are pinned to full commit SHAs. `govulncheck` runs on every push/PR. Dependabot opens weekly grouped PRs for Go modules and Actions.

## Disclosure Policy

We follow [coordinated disclosure](https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure). Once a fix is available, we will:

1. Release a patched version
2. Publish a security advisory on GitHub
3. Credit the reporter (unless they prefer anonymity)
