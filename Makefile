.PHONY: build dev test test-integration lint docker compose-standalone compose-cluster compose-deps clean

build:
	go build -o bin/beachhouse ./cmd/beachhouse
	go build -o bin/beachhouse-api ./cmd/beachhouse-api
	go build -o bin/beachhouse-worker ./cmd/beachhouse-worker

dev:
	air -c .air.toml

test:
	go test ./internal/...

test-integration:
	go test ./tests/... -tags=integration -v -timeout 120s

lint:
	golangci-lint run ./...

docker:
	docker build -f deployments/docker/Dockerfile.beachhouse -t beachhouse:latest .
	docker build -f deployments/docker/Dockerfile.beachhouse-api -t beachhouse-api:latest .
	docker build -f deployments/docker/Dockerfile.beachhouse-worker -t beachhouse-worker:latest .

compose-standalone:
	docker compose -f deployments/compose/standalone.yaml up -d

compose-cluster:
	docker compose -f deployments/compose/cluster.yaml up -d

compose-deps:
	docker compose -f deployments/compose/dependencies.yaml up -d

clean:
	rm -rf bin/ tmp/ data/
