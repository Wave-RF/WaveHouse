package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersion_Handle(t *testing.T) {
	t.Parallel()
	h := NewVersionHandler("v1.2.3", "abc1234", "2026-06-02T12:00:00Z")

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/version", nil)
	h.Handle(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))

	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "v1.2.3", resp["version"])
	assert.Equal(t, "abc1234", resp["git_commit"])
	assert.Equal(t, "2026-06-02T12:00:00Z", resp["build_time"])
	assert.Equal(t, runtime.Version(), resp["go_version"])
}
