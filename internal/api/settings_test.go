package api

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsHandler_Get(t *testing.T) {
	t.Parallel()
	h := NewSettingsHandler(settings.NewMemoryStore(settings.Defaults()))
	rec := httptest.NewRecorder()
	h.Get(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/admin/settings", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{
		"values": {"dedupe": {"id_field": "event_id", "require_id": false}},
		"sources": [{"section": "dedupe", "source": "default"}]
	}`, rec.Body.String())
}

func TestSettingsHandler_PutDedupe(t *testing.T) {
	t.Parallel()
	store := settings.NewMemoryStore(settings.Defaults())
	h := NewSettingsHandler(store)

	rec := httptest.NewRecorder()
	h.PutDedupe(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/v1/admin/settings/dedupe",
		strings.NewReader(`{"id_field": "view_id", "require_id": true}`)))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"ok": true}`, rec.Body.String())
	assert.Equal(t, settings.Dedupe{IDField: "view_id", RequireID: true}, store.Get().Dedupe)

	// GET now reports the override as the source.
	rec = httptest.NewRecorder()
	h.Get(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/admin/settings", nil))
	assert.Contains(t, rec.Body.String(), `"source":"db"`)
}

func TestSettingsHandler_PutDedupe_InvalidJSON(t *testing.T) {
	t.Parallel()
	store := settings.NewMemoryStore(settings.Defaults())
	h := NewSettingsHandler(store)

	rec := httptest.NewRecorder()
	h.PutDedupe(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/v1/admin/settings/dedupe",
		strings.NewReader(`{`)))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, settings.Defaults(), store.Get(), "a rejected body changes nothing")
}

func TestSettingsHandler_PutDedupe_UnknownFieldRejected(t *testing.T) {
	t.Parallel()
	store := settings.NewMemoryStore(settings.Defaults())
	h := NewSettingsHandler(store)

	// A typo'd key must fail loudly, not silently half-apply the section.
	rec := httptest.NewRecorder()
	h.PutDedupe(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/v1/admin/settings/dedupe",
		strings.NewReader(`{"id_field": "view_id", "requier_id": true}`)))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "unknown field")
	assert.Equal(t, settings.Defaults(), store.Get())
}

func TestSettingsHandler_PutDedupe_ValidationError(t *testing.T) {
	t.Parallel()
	store := settings.NewMemoryStore(settings.Defaults())
	h := NewSettingsHandler(store)

	rec := httptest.NewRecorder()
	h.PutDedupe(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/v1/admin/settings/dedupe",
		strings.NewReader(`{"id_field": "", "require_id": true}`)))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "dedupe.id_field")
	assert.Equal(t, settings.Defaults(), store.Get())
}

func TestSettingsHandler_PutDedupe_BodyTooLarge(t *testing.T) {
	t.Parallel()
	h := NewSettingsHandler(settings.NewMemoryStore(settings.Defaults()))
	h.maxRequestBytes = 8

	rec := httptest.NewRecorder()
	h.PutDedupe(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/v1/admin/settings/dedupe",
		strings.NewReader(`{"id_field": "way-over-the-tiny-test-cap"}`)))

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func TestSettingsHandler_PutDedupe_IfMatch(t *testing.T) {
	t.Parallel()
	store := settings.NewMemoryStore(settings.Defaults())
	h := NewSettingsHandler(store)

	put := func(ifMatch, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/v1/admin/settings/dedupe", strings.NewReader(body))
		if ifMatch != "" {
			req.Header.Set("If-Match", ifMatch)
		}
		rec := httptest.NewRecorder()
		h.PutDedupe(rec, req)
		return rec
	}

	// "No override yet" (0) creates the first override.
	require.Equal(t, http.StatusOK, put(`0`, `{"id_field": "view_id"}`).Code)

	// A second writer who also read "no override yet" conflicts, and the
	// store keeps the first write (ETag-style quotes are accepted).
	rec := put(`"0"`, `{"id_field": "org_id"}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "revision conflict")
	assert.Equal(t, "view_id", store.Get().Dedupe.IDField)

	// Retrying with the revision from a fresh read succeeds.
	rev := store.States()[0].Revision
	require.Equal(t, http.StatusOK, put(strconv.FormatUint(rev, 10), `{"id_field": "org_id"}`).Code)
	assert.Equal(t, "org_id", store.Get().Dedupe.IDField)

	// Garbage If-Match is a 400, never a silent unconditional write.
	rec = put(`not-a-revision`, `{"id_field": "nope"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "org_id", store.Get().Dedupe.IDField)
}

func TestSettingsHandler_ResetDedupe(t *testing.T) {
	t.Parallel()
	store := settings.NewMemoryStore(settings.Values{Dedupe: settings.Dedupe{IDField: "view_id", RequireID: true}})
	h := NewSettingsHandler(store)

	rec := httptest.NewRecorder()
	h.ResetDedupe(rec, httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/v1/admin/settings/dedupe", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"ok": true}`, rec.Body.String())
	assert.Equal(t, settings.Defaults(), store.Get())
}
