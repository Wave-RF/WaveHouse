# SigNoz dashboards (WaveHouse)

Version-controlled SigNoz dashboard definitions for the local observability stack.

| File | What it shows |
|---|---|
| `wavehouse-overview.json` | HTTP request rate / status / route, request latency p50–p99, span call rate by operation, OTLP intake at the collector |
| `wavehouse-runtime-internals.json` | Go runtime (goroutines, memory, allocations), embedded NATS (connections, msg/s), ingest pipeline (events/s, DLQ drops), HTTP payload sizes |

Format: SigNoz query-builder **v5** schema (the `version` field). Each file is exactly the `.data.data` object the SigNoz dashboards API returns.

## Loading them

SigNoz OSS doesn't provision dashboards from disk, so after the stack is up and you've created your account in the UI:

```bash
SIGNOZ_EMAIL=you@example.com SIGNOZ_PASSWORD='…' ../load-dashboards.sh
```

`../load-dashboards.sh` upserts by title (existing dashboards are updated in place), so re-run it whenever these files change. See its header for auth options (`SIGNOZ_TOKEN`, `SIGNOZ_URL`).

## Editing a dashboard

Easiest path: edit it in the SigNoz UI, then export it back —

```bash
curl -s -H "Authorization: Bearer $SIGNOZ_TOKEN" \
  http://localhost:3301/api/v1/dashboards/<id> | jq '.data.data' > wavehouse-overview.json
```

— and commit the result.
