package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parkedMsg is a message as the ingest worker would hand it to the DLQ,
// parked under tenant id's table.
func parkedMsg(id tenant.ID, table string) *mq.Message {
	return (&testutil.MockMessage{
		MsgTopic: mq.Topic{Tenant: id, Table: table},
		MsgData:  []byte(`{"table_name":"` + table + `"}`),
	}).Message()
}

// dlqStats serves GET /v1/ops/dlq/stats with query through handler.
func dlqStats(t *testing.T, handler *DLQHandler, query string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/ops/dlq/stats"+query, nil)
	rec := httptest.NewRecorder()
	handler.Stats(rec, req)
	return rec
}

// A tenant with no dead-letter queue — never given one on this data
// directory, or an id nobody uses — is a 404 that names it, not an empty
// count that would read as "nothing parked" for a typo.
func TestDLQStats_NoQueueIs404(t *testing.T) {
	// The embedded MQ opens a served tenant's queue at boot, so a queue's
	// absence comes from a mock.
	stats := &testutil.MockDeadLetterStats{Err: mq.ErrNoDeadLetterQueue}

	rec := dlqStats(t, NewDLQHandler(stats), "?tenant=acmee")
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "no dead-letter queue for tenant: acmee")
	testutil.AssertJSONErrorResponse(t, rec)
	assert.Equal(t, tenant.ID("acmee"), stats.Tenant)
}

func TestDLQStats_ReturnsCorrectCounts(t *testing.T) {
	emb := testutil.NewEmbeddedMQ(t, 1024*1024)

	ctx := context.Background()

	// Park messages on the dead-letter queue.
	for i := 0; i < 3; i++ {
		require.NoError(t, emb.DeadLetter(ctx, parkedMsg(tenant.Default, "events")))
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, emb.DeadLetter(ctx, parkedMsg(tenant.Default, "users")))
	}

	rec := dlqStats(t, NewDLQHandler(emb), "")

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["tables"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(3), tables["events"])
	assert.Equal(t, float64(2), tables["users"])
	assert.Equal(t, float64(5), resp["total"])
}

func TestDLQStats_EmptyBeforeAnyFailure(t *testing.T) {
	rec := dlqStats(t, NewDLQHandler(testutil.NewEmbeddedMQ(t, 1024*1024)), "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"tables":{},"total":0}`, rec.Body.String())
}

func TestDLQStats_SingleTable(t *testing.T) {
	emb := testutil.NewEmbeddedMQ(t, 1024*1024)

	ctx := context.Background()

	require.NoError(t, emb.DeadLetter(ctx, parkedMsg(tenant.Default, "orders")))

	rec := dlqStats(t, NewDLQHandler(emb), "")

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables := resp["tables"].(map[string]any)
	assert.Equal(t, float64(1), tables["orders"])
	assert.Equal(t, float64(1), resp["total"])
}

func TestDLQStats_BrokerFailureIsAnError(t *testing.T) {
	rec := dlqStats(t, NewDLQHandler(&testutil.MockDeadLetterStats{Err: errors.New("broker unavailable")}), "")
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "a failed read is not an empty queue")
}

func TestDLQStats_PassesTheTableFilter(t *testing.T) {
	emb := testutil.NewEmbeddedMQ(t, 1024*1024)

	ctx := context.Background()
	require.NoError(t, emb.DeadLetter(ctx, parkedMsg(tenant.Default, "default.orders")))
	require.NoError(t, emb.DeadLetter(ctx, parkedMsg(tenant.Default, "users")))

	rec := dlqStats(t, NewDLQHandler(emb), "?table=default.orders")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, map[string]any{"default.orders": float64(1)}, resp["tables"])
	assert.Equal(t, float64(2), resp["total"])
}

// ?tenant= reads that tenant's queue alone, and no parameter reads tenant 0's
// — the ops-read convention. The handler asks the MQ, not the settings, so a
// tenant the settings no longer serve (here, none at all) is read by name
// for as long as its queue is kept.
func TestDLQStats_ReadsTheNamedTenantsQueue(t *testing.T) {
	emb := testutil.NewEmbeddedMQ(t, 1024*1024, tenant.Default, "acme")
	ctx := context.Background()
	require.NoError(t, emb.DeadLetter(ctx, parkedMsg(tenant.Default, "events")))
	for range 2 {
		require.NoError(t, emb.DeadLetter(ctx, parkedMsg("acme", "events")))
	}
	handler := NewDLQHandler(emb)

	rec := dlqStats(t, handler, "?tenant=acme")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"tables":{"events":2},"total":2}`, rec.Body.String())

	rec = dlqStats(t, handler, "?tenant=acme&table=users")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"tables":{},"total":2}`, rec.Body.String())

	rec = dlqStats(t, handler, "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"tables":{"events":1},"total":1}`, rec.Body.String(), "no parameter reads tenant 0")

	rec = dlqStats(t, handler, "?tenant=globex")
	assert.Equal(t, http.StatusNotFound, rec.Code, "a tenant with no queue")
}

// The parameter is read strictly, like every ops read's (opsTenant): a
// query that misparses must not fall back to tenant 0's counts.
func TestDLQStats_RefusesAMalformedTenant(t *testing.T) {
	for _, query := range []string{"?tenant=a.b", "?tenant=", "?tenant=a&tenant=b", "?tenant=acme;x=1"} {
		stats := &testutil.MockDeadLetterStats{}
		rec := dlqStats(t, NewDLQHandler(stats), query)
		assert.Equal(t, http.StatusBadRequest, rec.Code, query)
		assert.Empty(t, stats.Tenant, "%s: nothing is read", query)
	}
}
