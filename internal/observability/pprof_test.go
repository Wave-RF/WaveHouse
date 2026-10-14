package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPprofHandler_ServesTheProfiles(t *testing.T) {
	t.Parallel()
	h := PprofHandler()
	tests := []struct {
		path string
		want string // in the body; "" checks the status alone
	}{
		{"/debug/pprof/", "goroutine"},
		{"/debug/pprof/cmdline", ""},
		{"/debug/pprof/symbol", "num_symbols"},
		{"/debug/pprof/goroutine?debug=1", "goroutine profile"},
		{"/debug/pprof/heap", ""},
		{"/debug/pprof/allocs", ""},
		{"/debug/pprof/threadcreate?debug=1", "threadcreate profile"},
		{"/debug/pprof/block", ""},
		{"/debug/pprof/mutex", ""},
		{"/debug/pprof/trace?seconds=0.01", ""},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.path, nil))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

// Only /debug/pprof/: the handler is a mux of its own, not a catch-all.
func TestPprofHandler_NothingElse(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/", "/metrics", "/v1/health", "/debug/vars"} {
		rec := httptest.NewRecorder()
		PprofHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		assert.Equal(t, http.StatusNotFound, rec.Code, path)
	}
	rec := httptest.NewRecorder()
	PprofHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/debug/pprof/nosuch", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
