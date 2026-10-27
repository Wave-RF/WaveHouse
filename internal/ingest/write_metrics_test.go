package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// writeReading is what one collection of the worker's write metrics shows,
// each instrument summed by the attribute that tells its series apart.
type writeReading struct {
	rows       map[string]int64 // by outcome
	retries    int64
	poison     int64
	isolations int64
	inserts    map[string]uint64 // by "mode/result"
	batchRows  int64             // rows summed over every bulk INSERT
	batchCount uint64
}

// swapWriteMetrics points the worker's write, retry and poison instruments at
// a meter of the test's own for the rest of the test. They bind to the first
// global provider, so a later run in the same process would otherwise record
// into a dead reader.
func swapWriteMetrics(t *testing.T) func() writeReading {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	m := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	wm, err := newWriteMetrics(m)
	require.NoError(t, err)
	retries, err := newRetryCounter(m)
	require.NoError(t, err)
	poison, err := newPoisonCounter(m)
	require.NoError(t, err)
	savedWM, savedRetries, savedPoison := workerMetrics, retryCounter, poisonCounter
	workerMetrics, retryCounter, poisonCounter = wm, retries, poison
	t.Cleanup(func() { workerMetrics, retryCounter, poisonCounter = savedWM, savedRetries, savedPoison })

	return func() writeReading {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		r := writeReading{rows: map[string]int64{}, inserts: map[string]uint64{}}
		attr := func(set attribute.Set, key string) string {
			v, _ := set.Value(attribute.Key(key))
			return v.AsString()
		}
		for _, sm := range rm.ScopeMetrics {
			for _, md := range sm.Metrics {
				switch data := md.Data.(type) {
				case metricdata.Sum[int64]:
					for _, dp := range data.DataPoints {
						switch md.Name {
						case "wavehouse_ingest_rows_total":
							assert.Equal(t, "events", attr(dp.Attributes, "table"))
							r.rows[attr(dp.Attributes, "outcome")] += dp.Value
						case "wavehouse_ingest_retries_total":
							r.retries += dp.Value
						case "wavehouse_ingest_poison_total":
							r.poison += dp.Value
						case "wavehouse_ingest_isolations_total":
							r.isolations += dp.Value
						}
					}
				case metricdata.Histogram[float64]:
					for _, dp := range data.DataPoints {
						r.inserts[attr(dp.Attributes, "mode")+"/"+attr(dp.Attributes, "result")] += dp.Count
					}
				case metricdata.Histogram[int64]:
					for _, dp := range data.DataPoints {
						r.batchRows += dp.Sum
						r.batchCount += dp.Count
					}
				}
			}
		}
		return r
	}
}

// poisonRowRT fails every bulk INSERT as rejected, and every single-row INSERT
// whose row carries true.
func poisonRowRT() *testutil.MockRoundTripper {
	return &testutil.MockRoundTripper{
		Fn: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			s := string(body)
			if strings.Count(s, "\n") > 1 || strings.Contains(s, "true") {
				return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewBufferString("Code: 60. bad row"))}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("OK"))}, nil
		},
	}
}

// TestWriteMetrics_RecordedAndReconciled drives each way the worker settles a
// delivery and checks the write metrics record it (#895). Every delivery is
// counted exactly once across wavehouse_ingest_rows_total,
// wavehouse_ingest_retries_total and wavehouse_ingest_poison_total, so the
// three reconcile against what the worker received.
func TestWriteMetrics_RecordedAndReconciled(t *testing.T) {
	read := swapWriteMetrics(t)
	ctx := context.Background()
	delivered := 0
	msg := func(row map[string]any) *testutil.MockMessage {
		delivered++
		return newIngestMsg(t, "events", "", row)
	}

	// A successful batch: one bulk INSERT of two rows, both inserted.
	w, _, _, wait := newTestWorker(okRoundTripper())
	w.flushTable(ctx, "events", parseAll(t, w, msg(map[string]any{"id": 1}), msg(map[string]any{"id": 2})))
	wait()
	got := read()
	assert.Equal(t, map[string]int64{rowsInserted: 2}, got.rows)
	assert.Equal(t, map[string]uint64{"batch/ok": 1}, got.inserts)
	assert.Equal(t, uint64(1), got.batchCount)
	assert.Equal(t, int64(2), got.batchRows)

	// An isolation fallback that parks one row: the bulk INSERT is rejected,
	// two rows insert alone and the third is dead-lettered.
	w, _, _, wait = newTestWorker(poisonRowRT())
	w.flushTable(ctx, "events", parseAll(t, w,
		msg(map[string]any{"id": 3, "poison": false}),
		msg(map[string]any{"id": 4, "poison": true}),
		msg(map[string]any{"id": 5, "poison": false})))
	wait()
	got = read()
	assert.Equal(t, int64(1), got.isolations)
	assert.Equal(t, map[string]int64{rowsInserted: 4, rowsDeadLettered: 1}, got.rows)
	assert.Equal(t, map[string]uint64{"batch/ok": 1, "batch/rejected": 1, "isolated/ok": 2, "isolated/rejected": 1}, got.inserts)
	assert.Equal(t, uint64(2), got.batchCount, "an isolated INSERT is not a batch")
	assert.Equal(t, int64(5), got.batchRows)

	// A rejected row with the DLQ off for the table: left unacked.
	w, _, _, wait = newTestWorker(poisonRowRT())
	w.dlqEnabled = func(tenant.ID, string) bool { return false }
	w.flushTable(ctx, "events", parseAll(t, w,
		msg(map[string]any{"id": 6, "poison": false}),
		msg(map[string]any{"id": 7, "poison": true})))
	wait()
	got = read()
	assert.Equal(t, int64(2), got.isolations)
	assert.Equal(t, map[string]int64{rowsInserted: 5, rowsDeadLettered: 1, rowsLeftUnacked: 1}, got.rows)

	// An unavailable ClickHouse: the batch goes back for a delayed retry, and
	// the INSERT is recorded under the failure's class.
	refused := &testutil.MockRoundTripper{Fn: func(*http.Request) (*http.Response, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
	}}
	w, _, _, wait = newTestWorker(refused)
	w.flushTable(ctx, "events", parseAll(t, w,
		msg(map[string]any{"id": 8}), msg(map[string]any{"id": 9}), msg(map[string]any{"id": 10})))
	wait()
	got = read()
	assert.Equal(t, int64(3), got.retries)
	assert.Equal(t, uint64(1), got.inserts["batch/unavailable"])
	assert.Equal(t, map[string]int64{rowsInserted: 5, rowsDeadLettered: 1, rowsLeftUnacked: 1}, got.rows,
		"a row handed back for a retry is counted there, not here")

	// An envelope the worker cannot read: parked and counted as poison.
	w, _, _, _ = newTestWorker(okRoundTripper())
	delivered++
	_, ok := w.parseMsg(ctx, (&testutil.MockMessage{
		MsgTopic: mq.Topic{Tenant: tenant.Default, Table: "events"},
		MsgData:  v1Envelope(t, "events", map[string]any{"id": 11}),
	}).Message())
	require.False(t, ok)
	w.ackWg.Wait()

	got = read()
	var settled int64
	for _, n := range got.rows {
		settled += n
	}
	assert.Equal(t, int64(delivered), settled+got.retries+got.poison,
		"every delivery is counted once across rows, retries and poison")
}

// TestWriteMetrics_NoConnectionAndFailedPark covers the outcomes no INSERT
// succeeds for: a tenant with no ClickHouse connection, whose batch is parked
// or left by its DLQ switch and which sends and times nothing, and a rejected
// row whose DLQ publish fails, which stays in the stream.
func TestWriteMetrics_NoConnectionAndFailedPark(t *testing.T) {
	read := swapWriteMetrics(t)
	ctx := context.Background()
	noTarget := func(tenant.ID) chconn.Target { return chconn.Target{} }
	rows := func() []*testutil.MockMessage {
		return []*testutil.MockMessage{
			newIngestMsg(t, "events", "", map[string]any{"id": 1}),
			newIngestMsg(t, "events", "", map[string]any{"id": 2}),
		}
	}

	// No connection, DLQ on: the batch is parked whole.
	w, _, _, wait := newTestWorker(okRoundTripper())
	w.target = noTarget
	w.flushTable(ctx, "events", parseAll(t, w, rows()...))
	wait()
	assert.Equal(t, map[string]int64{rowsDeadLettered: 2}, read().rows)

	// No connection, DLQ off: the batch is left for redelivery.
	w, _, _, wait = newTestWorker(okRoundTripper())
	w.target = noTarget
	w.dlqEnabled = func(tenant.ID, string) bool { return false }
	w.flushTable(ctx, "events", parseAll(t, w, rows()...))
	wait()
	assert.Equal(t, map[string]int64{rowsDeadLettered: 2, rowsLeftUnacked: 2}, read().rows)

	// An INSERT with no connection makes no request, so it records no sample.
	w, _, _, _ = newTestWorker(okRoundTripper())
	w.target = noTarget
	msgs := parseAll(t, w, rows()...)
	require.ErrorIs(t, w.insert(ctx, "events", msgs[0].columns, msgs, modeBatch), errNoTarget)
	got := read()
	assert.Empty(t, got.inserts)
	assert.Zero(t, got.batchCount)

	// A rejected row whose DLQ publish fails is left unacked, not dead-lettered.
	w, pub, _, wait := newTestWorker(poisonRowRT())
	pub.Err = errors.New("jetstream unavailable")
	w.flushTable(ctx, "events", parseAll(t, w,
		newIngestMsg(t, "events", "", map[string]any{"id": 3, "poison": false}),
		newIngestMsg(t, "events", "", map[string]any{"id": 4, "poison": true})))
	wait()
	assert.Equal(t, map[string]int64{rowsInserted: 1, rowsDeadLettered: 2, rowsLeftUnacked: 3}, read().rows)
}

// TestWriteMetrics_InsertedBeforeAck pins that a row ClickHouse took is
// counted inserted even when its ack then fails: the row is in ClickHouse,
// and a redelivery is written and counted again (deployment.md, Ingest writes).
func TestWriteMetrics_InsertedBeforeAck(t *testing.T) {
	read := swapWriteMetrics(t)
	w, _, _, wait := newTestWorker(okRoundTripper())
	m := newIngestMsg(t, "events", "", map[string]any{"id": 1})
	m.DoubleAckErr = errors.New("ack timed out")
	w.flushTable(context.Background(), "events", parseAll(t, w, m))
	wait()
	assert.Equal(t, map[string]int64{rowsInserted: 1}, read().rows)
}
