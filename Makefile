.PHONY: build dev test test-integration lint docker compose-standalone compose-cluster compose-deps clean

build:
	go build -o bin/wavehouse ./cmd/wavehouse
	go build -o bin/wavehouse-api ./cmd/wavehouse-api
	go build -o bin/wavehouse-worker ./cmd/wavehouse-worker

dev:
	air -c .air.toml

test:
	go test ./internal/...

test-integration:
	go test ./tests/... -tags=integration -v -timeout 120s

lint:
	golangci-lint run ./...

docker:
	docker build -f deployments/docker/Dockerfile.wavehouse -t wavehouse:latest .
	docker build -f deployments/docker/Dockerfile.wavehouse-api -t wavehouse-api:latest .
	docker build -f deployments/docker/Dockerfile.wavehouse-worker -t wavehouse-worker:latest .

compose-standalone:
	docker compose -f deployments/compose/standalone.yaml up -d

compose-cluster:
	docker compose -f deployments/compose/cluster.yaml up -d

compose-deps:
	docker compose -f deployments/compose/dependencies.yaml up -d

deps-wipe:
	docker compose -f deployments/compose/dependencies.yaml down -v --remove-orphans
	docker compose -f deployments/compose/dependencies.yaml up -d --force-recreate

clean:
	rm -rf bin/ tmp/ data/
