# SigNoz dashboards (WaveHouse)

Version-controlled SigNoz dashboard definitions for the local observability stack.

| File | What it shows |
|---|---|
| `wavehouse-overview.json` | HTTP request rate / status / route, request latency p50–p99, span call rate by operation, OTLP intake at the collector |
| `wavehouse-runtime-internals.json` | Go runtime (goroutines, memory, allocations), embedded NATS (connections, msg/s), ingest pipeline (events/s, DLQ drops), HTTP payload sizes |

Format: SigNoz query-builder **v5** schema (the `version` field). Each file is exactly the `.data.data` object the SigNoz dashboards API returns.

## Loading them

SigNoz OSS doesn't provision dashboards from disk and disables self-registration, so loading can't be folded into `docker compose up` — run it once after you've created your SigNoz account in the UI.

Auth uses `SIGNOZ_TOKEN`, the JWT the SPA itself uses:

1. Sign in at `http://localhost:3301` — create your admin account on first visit.
2. DevTools → Application → Local Storage → `http://localhost:3301` → copy the value of `AUTH_TOKEN`.
3. `export SIGNOZ_TOKEN='eyJ...'`.
4. Run `make signoz-dashboards` from the repo root (or `../load-dashboards.sh` from this directory).

> SigNoz v0.122.0 moved username/password login to `/api/v2/sessions/email_password` and requires an org UUID that isn't externally discoverable, so token auth is the only supported path — but the token is one DevTools click away.

`load-dashboards.sh` upserts by title: existing dashboards are updated in place (so bookmarked URLs survive), new ones are created. Re-run it whenever these JSON files change. Override the target with `SIGNOZ_URL=http://localhost:3301` (the default). Requires `curl` and `jq`.

## Editing a dashboard

Easiest path: edit it in the SigNoz UI, then export it back —

```bash
curl -s -H "Authorization: Bearer $SIGNOZ_TOKEN" \
  http://localhost:3301/api/v1/dashboards/<id> | jq '.data.data' > wavehouse-overview.json
```

— and commit the result.
