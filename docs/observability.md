# Observability Guide
WaveHouse uses a full OpenTelemetry (OTel) stack to provide deep, real-time visibility into the system's asynchronous architecture. We use SigNoz as our local telemetry dashboard to visualize traces, metrics, and logs.

## Important: Startup Order
WaveHouse and SigNoz both rely on ClickHouse. To prevent Docker port conflicts (specifically a "Turf War" over port 9000), you must start the WaveHouse dependencies before starting the observability stack. The SigNoz Docker Compose file is configured to keep its internal ClickHouse completely isolated from your host machine, ensuring it doesn't conflict with WaveHouse's primary application database.

### Prerequisite: Installing SigNoz
Because SigNoz has a complex internal architecture (requiring ClickHouse, Zookeeper, and specific database migrations), we rely on their official Docker stack rather than rebuilding it from scratch. 

To set this up alongside WaveHouse without port conflicts:

**1. Clone the official repository (outside your project)**
```bash
cd ~
git clone -b main [https://github.com/SigNoz/signoz.git](https://github.com/SigNoz/signoz.git)
```
**2. Copy the necessary files into WaveHouse**
```bash
mkdir -p ~/BeachHouse/observability
cp ~/signoz/deploy/docker/docker-compose.yaml ~/BeachHouse/observability/
cp ~/signoz/deploy/docker/otel-collector-config.yaml ~/BeachHouse/observability/
cp -r ~/signoz/deploy/common ~/BeachHouse/
```

**3. Resolve Port Conflicts**
By default, WaveHouse and SigNoz will fight over port 8080 (API) and port 9000 (ClickHouse). To give WaveHouse priority, edit observability/docker-compose.yaml and modify the signoz service:
```YAML
signoz:
    # ...
    ports:
      - "3301:8080" # Shift the UI to port 3301
    environment:
      - SIGNOZ_PORT=8080
      - FRONTEND_API_ENDPOINT=http://localhost:3301 # Tell the UI where it moved
      # ...
```
(Ensure the clickhouse and zookeeper ports in this file are either commented out or mapped to different host ports so they remain internal to the Docker network).

## Quick Start (Telemetry Enabled)
Note that we haven't dockerized Signoz here yet for development.

```bash
# 1. Start WaveHouse's core dependencies (ClickHouse & NATS)
make compose-up

# 2. Start the SigNoz Observability Stack
cd observability
docker compose up -d
cd ..

# 3. Start WaveHouse with telemetry explicitly enabled
export WH_OBSERVABILITY_ENABLED=true 
make dev
```

Once running, the SigNoz dashboard is available at: http://localhost:3301

To verify data is flowing:

1. Generate some traffic: curl -s -X POST http://localhost:8080/v1/ingest/clicks \ -H "Content-Type: application/json" \ -d '{"page": "/home", "button": "signup", "score": 42.5}'
2. Open SigNoz (http://localhost:3301).
3. Navigate to the Traces tab and look for the wavehouse-standalone service.

## How Observability Works in WaveHouse
WaveHouse is highly concurrent, meaning a single HTTP request triggers downstream events in message queues, background workers, and WebSocket broadcasts. Our telemetry pipeline links these disconnected pieces into a single narrative.

### 1. Distributed Tracing (OpenTelemetry)
Tracing is the backbone of WaveHouse's observability.
-The Context Thread: When an ingest request hits the API, a unique Trace ID is generated. This ID is passed through standard Go contexts.
- The NATS Boundary: NATS JetStream only accepts raw byte payloads. To prevent "orphaned" traces, WaveHouse manually extracts the OpenTelemetry msgCtx and injects it into the NATS headers.
- The Fan-Out: When the Broadcast Hub or the Bento Worker pulls a message from NATS, they extract the Trace ID from the envelope. This ensures that downstream actions (like batching to ClickHouse or pushing to a WebSocket) appear as child spans of the original HTTP request in the SigNoz UI.

### 2. Structured Logging (slog)
WaveHouse uses Go's standard log/slog for zero-allocation, machine-readable JSON logging.
- Logs automatically include relevant metadata like table_name, component, and subject.
- Because logs are structured, collectors can easily index them alongside trace data without complex regex parsing.

### 3. Metrics
The OpenTelemetry pipeline automatically tracks standard RED (Rate, Errors, Duration) metrics. You can view these in the SigNoz dashboard to monitor overall system health, API latency, and worker batch flush times.

## The Docker Setup (signoz.yaml)
To keep the WaveHouse repository clean, we do not vendor the entire SigNoz source code. Instead, deployments/compose/signoz.yaml pulls pre-built, official images directly from Docker Hub.

The stack consists of four lightweight containers:
1. signoz-clickhouse: A dedicated datastore purely for telemetry data. (Host ports are hidden to prevent conflicts).
2. signoz-query-service: The backend API for the SigNoz dashboard.
3. signoz-ui: The frontend dashboard available on port 3301.
4. signoz-otel-collector: The OpenTelemetry receiver. It listens on port 4317 (gRPC), which is where the WaveHouse API and Worker send their span data. It uses the configuration defined in deployments/compose/signoz-config/otel-collector-config.yaml.

### Teardown
To stop the observability stack and clean up its containers, run:

```bash
docker compose -f deployments/compose/signoz.yaml down
```