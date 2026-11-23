//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/internal/testutil/storedir"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// TestReplay_RowsWaitingForClickHouse is #1007's repro on each broker, under
// the real ingest worker. A tenant's ClickHouse cannot be reached while rows
// are accepted into one table. After more than the replay window a replay
// from before the first row still sends them: they are waiting to be written,
// whatever their age. Once ClickHouse is back and they are inserted, a replay
// right after the insert sends them too, although they were accepted longer
// ago than the window, and they leave the broker only when the window has
// passed since the insert.
func TestReplay_RowsWaitingForClickHouse(t *testing.T) {
	t.Parallel()
	const window = 4 * time.Second
	ctx := context.Background()
	brokers := map[string]func(t *testing.T) mq.Broker{
		// The window is the tenant's, as its stream.gap_window_minutes is.
		"embedded": func(t *testing.T) mq.Broker {
			broker, err := mq.NewEmbedded(storedir.New(t))
			require.NoError(t, err)
			t.Cleanup(func() { _ = broker.Close() })
			require.NoError(t, broker.SetReplayWindow(ctx, tenant.Default, window))
			require.NoError(t, broker.SetMaxBytes(ctx, tenant.Default, 64<<20))
			return broker
		},
		// The window is the operator's, the history stream's max_age.
		"nats": func(t *testing.T) mq.Broker {
			natsURL := startNATS(t)
			op, err := natstest.Connect(natsURL)
			require.NoError(t, err)
			t.Cleanup(op.Close)
			require.NoError(t, op.ApplyShipped(ctx))
			history, err := op.JetStream().Stream(ctx, "WH_HISTORY")
			require.NoError(t, err)
			cfg := history.CachedInfo().Config
			cfg.MaxAge, cfg.Duplicates = window, min(cfg.Duplicates, window)
			_, err = op.JetStream().UpdateStream(ctx, cfg)
			require.NoError(t, err)
			return shardBroker(t, natsURL)
		},
	}
	for name, open := range brokers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			broker := open(t)
			inserts := &insertLog{}
			clickhouse := inserts.server(t, name, nil)
			// Nothing listens on port 1: the worker's inserts are refused, as
			// they are while ClickHouse is down.
			var chURL atomic.Pointer[string]
			down := "http://127.0.0.1:1"
			chURL.Store(&down)
			limits := func(tenant.ID) (ingest.Limits, bool) {
				return ingest.Limits{MaxBatchBytes: 262144, MaxBatchWait: 100 * time.Millisecond, InsertTimeout: 5 * time.Second, RetryBackoffMax: time.Second}, true
			}
			stop, failed, err := ingest.StartIngestWorker(ctx, broker, &testutil.MockCache{},
				func(tenant.ID) chconn.Target { return chconn.Target{URL: *chURL.Load(), Database: "db"} }, nil, limits, ingest.ConsumerSettings{})
			require.NoError(t, err)
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = stop(stopCtx)
			})

			const table, rows = "waiting", 3
			topic := mq.Topic{Tenant: tenant.Default, Table: table}
			since := time.Now().Add(-time.Second)
			for i := range rows {
				require.NoError(t, broker.Publish(ctx, topic, seqEnvelope(t, table, i)))
			}
			// replayed is the rows a replay from before the first one sends,
			// each once however often it was sent, in order.
			replayed := func() ([]int, error) {
				var seqs []int
				err := broker.ReplaySince(ctx, topic, since, func(data []byte) bool {
					var msg ingest.EventMessage
					var row []int
					if json.Unmarshal(data, &msg) == nil && json.Unmarshal(msg.Row, &row) == nil && len(row) == 1 && !slices.Contains(seqs, row[0]) {
						seqs = append(seqs, row[0])
					}
					return true
				})
				slices.Sort(seqs)
				return seqs, err
			}

			time.Sleep(window + time.Second)
			require.Zero(t, inserts.count(), "nothing can have been inserted")
			got, err := replayed()
			require.NoError(t, err)
			assert.Equal(t, []int{0, 1, 2}, got, "waiting for ClickHouse for longer than the window, and replayed all the same")

			up := clickhouse.URL
			chURL.Store(&up)
			require.Eventually(t, func() bool { return inserts.count() == rows }, 30*time.Second, 20*time.Millisecond, "the rows never reached ClickHouse")
			inserted := time.Now()
			got, err = replayed()
			require.NoError(t, err)
			assert.Equal(t, []int{0, 1, 2}, got, "just inserted: replayable for the window from the insert, not from when they were accepted")

			require.Eventually(t, func() bool {
				got, err := replayed()
				return err == nil && len(got) == 0
			}, window+20*time.Second, 100*time.Millisecond, "the rows never left the broker")
			assert.GreaterOrEqual(t, time.Since(inserted), window-500*time.Millisecond, "and not before the window had passed since the insert")
			select {
			case err := <-failed:
				t.Fatalf("the worker ended: %v", err)
			default:
			}
		})
	}
}
