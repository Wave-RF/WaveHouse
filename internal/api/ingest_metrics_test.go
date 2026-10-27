package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/Wave-RF/WaveHouse/pkg/policy"
)

// TestIngestRecords_EveryResult: each record POST /v1/ingest reads is counted
// once under its result, in single and batch bodies, and a refusal before any
// record is read counts nothing (#896). Not parallel: it swaps the counter,
// which binds to the first global provider.
func TestIngestRecords_EveryResult(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := newIngestRecordsCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	require.NoError(t, err)
	saved := ingestRecords
	ingestRecords = counter
	t.Cleanup(func() { ingestRecords = saved })
	// The counter is cumulative, so each case reads what it added.
	totals := func() map[string]int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		out := map[string]int64{}
		for _, sm := range rm.ScopeMetrics {
			for _, md := range sm.Metrics {
				for _, dp := range md.Data.(metricdata.Sum[int64]).DataPoints {
					table, _ := dp.Attributes.Value(attribute.Key("table"))
					assert.Equal(t, "clicks", table.AsString())
					result, _ := dp.Attributes.Value(attribute.Key("result"))
					out[result.AsString()] += dp.Value
				}
			}
		}
		return out
	}

	plain := func(t *testing.T, pub *testutil.MockPublisher) *IngestHandler {
		return NewIngestHandler(fixedRegistry(testRegistry(t)), pub)
	}
	deduped := func(dd *testutil.MockDeduplicator, requireID bool) func(*testing.T, *testutil.MockPublisher) *IngestHandler {
		return func(t *testing.T, pub *testutil.MockPublisher) *IngestHandler {
			return dedupHandler(t, pub, dd, requireID)
		}
	}
	held := testutil.NewMockDeduplicator()
	held.Hold(dedupe.Key{Table: "clicks", ID: "e1"})
	down := testutil.NewMockDeduplicator()
	down.Err = fmt.Errorf("%w: throttled", dedupe.ErrUnavailable)
	writer := func(t *testing.T, pub *testutil.MockPublisher) *IngestHandler {
		h := plain(t, pub)
		h.PolicySource = staticPolicy(&policy.Policy{Tables: map[string]policy.TablePolicy{
			"clicks": {"writer": {Insert: &policy.InsertPermissions{AllowColumns: []string{"page"}}}},
		}})
		return h
	}
	asWriter := func(r *http.Request) *http.Request { return r.WithContext(auth.WithRole(r.Context(), "writer")) }

	tests := []struct {
		name    string
		handler func(*testing.T, *testutil.MockPublisher) *IngestHandler
		pubErr  error
		after   int // publishes that succeed before pubErr
		req     func(*testing.T) *http.Request
		want    map[string]int64
	}{
		{"queued", plain, nil, 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "clicks", map[string]any{"page": "/a"})
		}, map[string]int64{resultQueued: 1}},
		{"unparsable single", plain, nil, 0, func(t *testing.T) *http.Request {
			return rawIngestRequest(t, "clicks", "application/json", `{"page":`)
		}, map[string]int64{resultInvalidBody: 1}},
		{"schema", plain, nil, 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "clicks", map[string]any{"page": "/a", "nope": 1})
		}, map[string]int64{resultSchema: 1}},
		{"forbidden column", writer, nil, 0, func(t *testing.T) *http.Request {
			return asWriter(ingestRequest(t, "clicks", map[string]any{"page": "/a", "button": "b"}))
		}, map[string]int64{resultForbidden: 1}},
		{"missing id", deduped(testutil.NewMockDeduplicator(), true), nil, 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "clicks", map[string]any{"page": "/a"})
		}, map[string]int64{resultMissingID: 1}},
		{"in flight", deduped(held, false), nil, 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "clicks", map[string]any{"page": "/a", "event_id": "e1"})
		}, map[string]int64{resultInFlight: 1}},
		{"dedupe unavailable", deduped(down, false), nil, 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "clicks", map[string]any{"page": "/a", "event_id": "e2"})
		}, map[string]int64{resultDedupeUnavailable: 1}},
		{"queue full", plain, fmt.Errorf("%w: maximum bytes exceeded", mq.ErrQueueFull), 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "clicks", map[string]any{"page": "/a"})
		}, map[string]int64{resultQueueFull: 1}},
		{"queue unavailable", plain, fmt.Errorf("%w: no answer", mq.ErrUnavailable), 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "clicks", map[string]any{"page": "/a"})
		}, map[string]int64{resultQueueUnavailable: 1}},
		{"publish failed", plain, errors.New("nats: bad subject"), 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "clicks", map[string]any{"page": "/a"})
		}, map[string]int64{resultPublishFailed: 1}},
		{"mixed batch", deduped(testutil.NewMockDeduplicator(), false), nil, 0, func(t *testing.T) *http.Request {
			return ndjsonRequest(t, "clicks",
				jsonLine(t, map[string]any{"page": "/a", "event_id": "b1"}),
				jsonLine(t, map[string]any{"page": "/a", "event_id": "b1"}),
				jsonLine(t, map[string]any{"page": "/a", "nope": 1}),
				"{ not json",
				jsonLine(t, map[string]any{"page": "/b"}),
			)
		}, map[string]int64{resultQueued: 2, resultDuplicate: 1, resultSchema: 1, resultInvalidBody: 1}},
		{"batch cut short by a full queue", plain, fmt.Errorf("%w: maximum bytes exceeded", mq.ErrQueueFull), 1, func(t *testing.T) *http.Request {
			return ndjsonRequest(t, "clicks",
				jsonLine(t, map[string]any{"page": "/a"}),
				jsonLine(t, map[string]any{"page": "/b"}),
				jsonLine(t, map[string]any{"page": "/c"}),
			)
		}, map[string]int64{resultQueued: 1, resultQueueFull: 2}},
		{"array that breaks off", plain, nil, 0, func(t *testing.T) *http.Request {
			return rawIngestRequest(t, "clicks", "application/json", `[{"page":"/a"}, {bad]`)
		}, map[string]int64{resultInvalidBody: 2}},
		{"array whose first element breaks it", plain, nil, 0, func(t *testing.T) *http.Request {
			return rawIngestRequest(t, "clicks", "application/json", `[{bad]`)
		}, map[string]int64{resultInvalidBody: 1}},
		{"array cut off before its end", plain, nil, 0, func(t *testing.T) *http.Request {
			return rawIngestRequest(t, "clicks", "application/json", `[{"page":"/a"}`)
		}, map[string]int64{resultInvalidBody: 1}},
		{"client gone before the publish failed", plain, errors.New("nats: connection closed"), 0, func(t *testing.T) *http.Request {
			c, cancel := context.WithCancel(context.Background())
			cancel()
			return ingestRequest(t, "clicks", map[string]any{"page": "/a"}).WithContext(c)
		}, map[string]int64{resultRecordsCanceled: 1}},
		{"unknown table counts nothing", plain, nil, 0, func(t *testing.T) *http.Request {
			return ingestRequest(t, "nope", map[string]any{"page": "/a"})
		}, map[string]int64{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := totals()
			pub := &testutil.MockPublisher{Err: tt.pubErr, ErrAfter: tt.after}
			tt.handler(t, pub).Handle(httptest.NewRecorder(), withTenant(tt.req(t)))
			added := map[string]int64{}
			for result, n := range totals() {
				if d := n - before[result]; d != 0 {
					added[result] = d
				}
			}
			assert.Equal(t, tt.want, added)
		})
	}
}
