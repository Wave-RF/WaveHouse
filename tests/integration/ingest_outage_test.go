//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// TestIngest_ClickHouseOutage_RetriedNotDeadLettered stops a real ClickHouse
// under a running ingest worker: the events published during the outage must
// not reach the DLQ, and once ClickHouse is back they must land, all of them.
// Its own container and broker, like the boot-resilience test: the shared env
// assumes ClickHouse stays up.
func TestIngest_ClickHouseOutage_RetriedNotDeadLettered(t *testing.T) {
	ctx := context.Background()

	ch, err := startClickHouse(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if ch.conn != nil {
			_ = ch.conn.Close()
		}
		_ = ch.container.Terminate(context.Background())
	})
	const table = "outage_events"
	require.NoError(t, ch.conn.Exec(ctx, "CREATE TABLE "+table+" (id UInt32) ENGINE = MergeTree ORDER BY id"))

	broker, err := mq.NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = broker.Close() })
	require.NoError(t, broker.SetMaxBytes(ctx, tenant.Default, 64<<20))

	// The worker resolves its target per flush, so a restart that moves the
	// mapped HTTP port is followed the way a settings reload would be.
	var chURL atomic.Pointer[string]
	setURL := func() { u := ch.httpURL(); chURL.Store(&u) }
	setURL()
	target := func(tenant.ID) chconn.Target {
		return chconn.Target{URL: *chURL.Load(), Username: testCHUser, Password: testCHPassword, Database: testCHDatabase}
	}
	stop, _, err := ingest.StartIngestWorker(ctx, broker, &testutil.MockCache{}, target, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = stop(stopCtx)
	})

	stopTimeout := 10 * time.Second
	require.NoError(t, ch.container.Stop(ctx, &stopTimeout))

	const rows = 3
	for i := range rows {
		payload, err := json.Marshal(ingest.EventMessage{
			TableName:         table,
			ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Format:            ingest.FormatJSONCompactEachRow,
			Columns:           []string{"id"},
			Row:               json.RawMessage(fmt.Sprintf("[%d]", i)),
		})
		require.NoError(t, err)
		require.NoError(t, broker.Publish(ctx, mq.Topic{Tenant: tenant.Default, Table: table}, payload))
	}

	parked := func() uint64 {
		c, err := broker.DeadLetterCounts(ctx, tenant.Default, "")
		require.NoError(t, err)
		return c.Total
	}
	// Longer than a batch wait (5s) plus several retries against the down
	// server: before this change every row would have been parked by now.
	assert.Never(t, func() bool { return parked() > 0 }, 12*time.Second, 250*time.Millisecond,
		"an unavailable ClickHouse must not dead-letter rows")

	require.NoError(t, ch.container.Start(ctx))
	httpPort, err := ch.container.MappedPort(ctx, "8123")
	require.NoError(t, err)
	ch.httpPort = httpPort.Port()
	setURL()
	require.NoError(t, refreshChAddr(ctx, ch))
	_ = ch.conn.Close()
	ch.conn, err = openDriver(ch.nativeAddr())
	require.NoError(t, err)
	require.NoError(t, waitForNativeReady(ctx, ch.conn, 60*time.Second))

	require.Eventually(t, func() bool {
		var n uint64
		if err := ch.conn.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&n); err != nil {
			return false
		}
		return n == rows
	}, 90*time.Second, 500*time.Millisecond, "every row published during the outage lands once ClickHouse is back")
	assert.Zero(t, parked(), "and none of them was parked")
}
