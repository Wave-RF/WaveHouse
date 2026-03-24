//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestClickHouseContainer(t *testing.T) {
	ctx := context.Background()

	chReq := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		WaitingFor:   wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: chReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer chContainer.Terminate(ctx)

	host, err := chContainer.Host(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, host)

	port, err := chContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)
	assert.NotEmpty(t, port)

	t.Logf("ClickHouse available at %s:%s", host, port.Port())
}

func TestNATSContainer(t *testing.T) {
	ctx := context.Background()

	natsReq := testcontainers.ContainerRequest{
		Image:        "nats:latest",
		Cmd:          []string{"--jetstream"},
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForListeningPort("4222/tcp").WithStartupTimeout(30 * time.Second),
	}
	natsContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: natsReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer natsContainer.Terminate(ctx)

	host, err := natsContainer.Host(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, host)

	t.Logf("NATS available at %s", host)
}
