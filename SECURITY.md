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

BeachHouse handles multi-tenant data and enforces strict isolation:

- **Tenant isolation**: `tenant_id` is exclusively sourced from JWT claims, never from user-supplied request data.
- **JWT validation**: All `/v1/*` endpoints require valid HMAC-signed JWTs.
- **Query scoping**: ClickHouse queries are automatically scoped to the authenticated tenant.
- **Input validation**: JSON payloads are validated before processing. The schema flattener rejects malformed input.

## Disclosure Policy

We follow [coordinated disclosure](https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure). Once a fix is available, we will:

1. Release a patched version
2. Publish a security advisory on GitHub
3. Credit the reporter (unless they prefer anonymity)
