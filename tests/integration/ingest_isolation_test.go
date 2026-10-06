//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
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

// A CHECK constraint is a refusal chtypes does not make yet (#727), so a row
// that violates one reaches the worker and fails its batch's INSERT. These
// tests run the worker against the shared ClickHouse with the server's own
// async_insert default, which is on for 26.8: the batch INSERT is async, and
// an async flush merges every concurrent INSERT of the same statement into one
// block, which a constraint fails as a whole.

const checkColumns = "id UInt32, n Int32, CONSTRAINT c CHECK n > 0"

// isolationQueue is an ingest.Queue that hands the test the worker's delivery
// handler and records what the worker parks.
type isolationQueue struct {
	*testutil.MockPublisher
	handler chan func(*mq.Message)
}

func (q *isolationQueue) CreateConsumer(context.Context, mq.ConsumerConfig) (mq.Consumer, error) {
	return q, nil
}

func (q *isolationQueue) Consume(h func(*mq.Message), _ int) (func(), <-chan error, error) {
	q.handler <- h
	return func() {}, make(chan error), nil
}

// isolationWorker is one ingest worker writing to the shared ClickHouse.
type isolationWorker struct {
	queue   *isolationQueue
	deliver func(*mq.Message)
}

func startIsolationWorker(t *testing.T) isolationWorker {
	t.Helper()
	ch := env(t).ch
	q := &isolationQueue{MockPublisher: &testutil.MockPublisher{}, handler: make(chan func(*mq.Message), 1)}
	target := func(tenant.ID) chconn.Target {
		return chconn.Target{URL: ch.httpURL(), Username: testCHUser, Password: testCHPassword, Database: testCHDatabase}
	}
	stop, _, err := ingest.StartIngestWorker(context.Background(), q, &testutil.MockCache{}, target, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = stop(ctx)
	})
	return isolationWorker{queue: q, deliver: <-q.handler}
}

// checkRows builds one message per id, its n 0 (violating the constraint) for
// the ids in bad and 1 otherwise.
func checkRows(t *testing.T, table string, ids []int, bad ...int) map[int]*testutil.MockMessage {
	t.Helper()
	out := make(map[int]*testutil.MockMessage, len(ids))
	for _, id := range ids {
		n := 1
		if slices.Contains(bad, id) {
			n = 0
		}
		data, err := json.Marshal(ingest.EventMessage{
			TableName:         table,
			ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Format:            ingest.FormatJSONCompactEachRow,
			Columns:           []string{"id", "n"},
			Row:               json.RawMessage(fmt.Sprintf("[%d,%d]", id, n)),
		})
		require.NoError(t, err)
		out[id] = &testutil.MockMessage{MsgTopic: mq.Topic{Tenant: tenant.Default, Table: table}, MsgData: data}
	}
	return out
}

func idRange(from, n int) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = from + i
	}
	return ids
}

// requireServerAsyncInsert fails unless the shared ClickHouse defaults to
// async inserts, so these tests cannot pass by silently testing synchronous
// ones after an image bump.
func requireServerAsyncInsert(t *testing.T) {
	t.Helper()
	var on uint64
	require.NoError(t, env(t).chConn.QueryRow(context.Background(), "SELECT toUInt64(getSetting('async_insert'))").Scan(&on))
	require.Equal(t, uint64(1), on, "the server's async_insert default")
}

// requireSettled waits until every message is acked — inserted, or parked
// and then acked — and requires that none was handed back for a retry.
func requireSettled(t *testing.T, msgs map[int]*testutil.MockMessage) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, m := range msgs {
			if !m.DoubleAcked.Load() {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "every message is acked")
	for id, m := range msgs {
		assert.False(t, m.Naked.Load(), "row %d was handed back for a retry", id)
	}
}

// parkedIDs reads the id of each row a worker parked, requiring each parking
// to carry the constraint's refusal.
func parkedIDs(t *testing.T, w isolationWorker, table string) []int {
	t.Helper()
	var ids []int
	for _, p := range w.queue.Published() {
		require.True(t, p.DeadLetter)
		assert.Equal(t, table, p.Headers.Get("X-DLQ-Table"))
		assert.Contains(t, p.Headers.Get("X-DLQ-Error"), "Code: 469")
		assert.Contains(t, p.Headers.Get("X-DLQ-Error"), "VIOLATED_CONSTRAINT")
		var envelope ingest.EventMessage
		require.NoError(t, json.Unmarshal(p.Data, &envelope))
		var row [2]int
		require.NoError(t, json.Unmarshal(envelope.Row, &row))
		ids = append(ids, row[0])
	}
	slices.Sort(ids)
	return ids
}

// storedIDs is every id in table, in order, duplicates kept.
func storedIDs(t *testing.T, table string) []int {
	t.Helper()
	rows, err := env(t).chConn.Query(context.Background(), "SELECT id FROM "+table+" ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id uint32
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, int(id))
	}
	require.NoError(t, rows.Err())
	return ids
}

func without(ids []int, drop ...int) []int {
	return slices.DeleteFunc(slices.Clone(ids), func(id int) bool { return slices.Contains(drop, id) })
}

// TestIngest_CheckConstraint_IsolatesTheBadRow: one row of a batch violates a
// CHECK constraint. The async batch INSERT fails whole; isolation stores every
// other row once and parks exactly the bad one with the server's refusal, and
// every message is acked.
func TestIngest_CheckConstraint_IsolatesTheBadRow(t *testing.T) {
	requireServerAsyncInsert(t)
	table := createTable(t, checkColumns, "ORDER BY id")
	w := startIsolationWorker(t)

	ids := idRange(1, 20)
	const bad = 11
	msgs := checkRows(t, table, ids, bad)
	for _, id := range ids {
		w.deliver(msgs[id].Message())
	}

	requireSettled(t, msgs)
	assert.Equal(t, []int{bad}, parkedIDs(t, w, table), "exactly the violating row is parked")
	assert.Equal(t, without(ids, bad), storedIDs(t, table), "every other row is stored, once")
}

// TestIngest_CheckConstraint_ConcurrentWriterParksOnlyItsOwnRow: two workers
// write one table at once, one batch all good and the other with a violating
// row. Their batch INSERTs can share an async flush, so the good batch can
// fail too; its isolation must then store every row of it rather than park
// one that met the other worker's bad row in a shared flush.
func TestIngest_CheckConstraint_ConcurrentWriterParksOnlyItsOwnRow(t *testing.T) {
	requireServerAsyncInsert(t)
	table := createTable(t, checkColumns, "ORDER BY id")
	good, mixed := startIsolationWorker(t), startIsolationWorker(t)

	goodIDs, mixedIDs := idRange(1000, 20), idRange(2000, 20)
	const bad = 2010
	goodMsgs, mixedMsgs := checkRows(t, table, goodIDs), checkRows(t, table, mixedIDs, bad)
	var wg sync.WaitGroup
	wg.Go(func() {
		for _, id := range goodIDs {
			good.deliver(goodMsgs[id].Message())
		}
	})
	wg.Go(func() {
		for _, id := range mixedIDs {
			mixed.deliver(mixedMsgs[id].Message())
		}
	})
	wg.Wait()

	requireSettled(t, goodMsgs)
	requireSettled(t, mixedMsgs)
	assert.Empty(t, parkedIDs(t, good, table), "the good batch parks nothing")
	assert.Equal(t, []int{bad}, parkedIDs(t, mixed, table), "the other parks exactly its violating row")
	assert.Equal(t, append(slices.Clone(goodIDs), without(mixedIDs, bad)...), storedIDs(t, table), "every good row of both is stored, once")
}
