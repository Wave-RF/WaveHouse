//go:build integration

package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Global variables for your tests to use
var (
	chAddr   string
	natsAddr string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	fmt.Println("🚀 Starting shared Testcontainers...")

	// 1. Start ClickHouse
	chReq := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		Env:          map[string]string{"CLICKHOUSE_PASSWORD": "test"},
		WaitingFor:   wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: chReq,
		Started:          true,
	})
	if err != nil {
		fmt.Printf("Failed to start ClickHouse: %v\n", err)
		os.Exit(1)
	}

	// Get Host/Port and set global variable
	host, _ := chContainer.Host(ctx)
	port, _ := chContainer.MappedPort(ctx, "9000")
	chAddr = fmt.Sprintf("%s:%s", host, port.Port())

	// 2. Start NATS (Implementation omitted for brevity, but same pattern as above)
	// natsContainer, err := ...
	// natsAddr = ...

	fmt.Println("✅ Containers ready. Running tests...")

	// 3. Run all the tests in this package!
	code := m.Run()

	// 4. Teardown
	fmt.Println("🛑 Tearing down containers...")
	chContainer.Terminate(ctx)
	// natsContainer.Terminate(ctx)

	// Exit with the result of the tests (0 = pass, 1 = fail)
	os.Exit(code)
}
