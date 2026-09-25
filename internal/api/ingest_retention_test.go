package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
)

// A finite retention must outlast the queue's duplicate window, or an id
// re-sent after it expires is claimed again and then dropped by the queue as
// a copy of the first publish.
func TestIngest_MinDedupeRetentionCoversTheDuplicateWindow(t *testing.T) {
	t.Parallel()
	assert.GreaterOrEqual(t, settings.MinDedupeRetention, mq.EmbeddedDuplicateWindow)
}

// dedupeConfig is fullConfig with dedupe switched on and the given dedupe
// block's retention settings.
func dedupeConfig(retention, tables string) string {
	return strings.Replace(fullConfig(100),
		`"dedupe": {"enabled": false, "id_field": "event_id", "require_id": false, "retention": "0"}`,
		`"dedupe": {"enabled": true, "id_field": "event_id", "require_id": false, "retention": "`+retention+`", "tables": `+tables+`}`, 1)
}

// Each record is committed with its table's retention from the adopted
// settings, and a reload changes it for the next request: the retention is
// read per record, like id_field, not fixed when the store was opened.
func TestIngest_Dedup_CommitsWithTheAdoptedRetention(t *testing.T) {
	t.Parallel()
	dir := writeSettingsFixture(t, dedupeConfig("720h", `{"users": {"retention": "0"}}`))
	tenants, findings := settings.Open(dir)
	require.NotNil(t, tenants, "findings: %v", findings)
	store, _ := tenants.For(tenant.Default)

	reg := testutil.NewTestSchemaRegistry(t, []*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{{Name: "event_id", Type: "String"}}},
		{Name: "users", Columns: []discovery.Column{{Name: "event_id", Type: "String"}}},
	})
	dedup := testutil.NewMockDeduplicator()
	h := NewIngestHandler(fixedRegistry(reg), &testutil.MockPublisher{})
	h.Dedup = staticDedup(dedup)
	h.DedupeSettings = (*settings.Store).DedupeFor
	ingest := func(table, id string) {
		t.Helper()
		w := httptest.NewRecorder()
		req := ingestRequest(t, table, map[string]any{"event_id": id})
		h.Handle(w, req.WithContext(WithStore(req.Context(), store)))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	}

	ingest("clicks", "e1")
	ingest("users", "e1")
	assert.Equal(t, 720*time.Hour, dedup.Retention(dedupe.Key{Table: "clicks", ID: "e1"}))
	assert.Equal(t, time.Duration(0), dedup.Retention(dedupe.Key{Table: "users", ID: "e1"}), "the table keeps ids forever")

	require.NoError(t, os.WriteFile(filepath.Join(dir, settings.FileConfig), []byte(dedupeConfig("24h", `{}`)), 0o600))
	_, adopted := tenants.Reload("test")
	require.True(t, adopted)
	ingest("clicks", "e2")
	ingest("users", "e2")
	assert.Equal(t, 24*time.Hour, dedup.Retention(dedupe.Key{Table: "clicks", ID: "e2"}))
	assert.Equal(t, 24*time.Hour, dedup.Retention(dedupe.Key{Table: "users", ID: "e2"}), "the override is gone")
	assert.Equal(t, 720*time.Hour, dedup.Retention(dedupe.Key{Table: "clicks", ID: "e1"}), "ids committed before the change keep theirs")
}

// A reload that lands mid-window splits the window's commit by retention, so
// every record keeps the retention of the snapshot it was prepared under.
func TestIngest_Dedup_ReloadMidWindowCommitsEachRetention(t *testing.T) {
	t.Parallel()
	dedup := testutil.NewMockDeduplicator()
	h := NewIngestHandler(fixedRegistry(testRegistry(t)), &testutil.MockPublisher{})
	h.Dedup = staticDedup(dedup)
	var calls atomic.Int32
	h.DedupeSettings = func(*settings.Store, string) settings.Dedupe {
		if calls.Add(1) <= 2 {
			return settings.Dedupe{Enabled: true, IDField: "event_id", Retention: time.Hour}
		}
		return settings.Dedupe{Enabled: true, IDField: "event_id", Retention: 2 * time.Hour}
	}

	w := httptest.NewRecorder()
	h.Handle(w, withTenant(ndjsonRequest(t, "clicks",
		`{"page": "/", "event_id": "a"}`, `{"page": "/", "event_id": "b"}`, `{"page": "/", "event_id": "c"}`)))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, 2, dedup.Commits, "one Commit per retention")
	assert.Equal(t, time.Hour, dedup.Retention(dedupe.Key{Table: "clicks", ID: "a"}))
	assert.Equal(t, time.Hour, dedup.Retention(dedupe.Key{Table: "clicks", ID: "b"}))
	assert.Equal(t, 2*time.Hour, dedup.Retention(dedupe.Key{Table: "clicks", ID: "c"}))
}
