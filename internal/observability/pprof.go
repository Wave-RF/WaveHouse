package observability

import (
	"net/http"
	"net/http/pprof"
)

// PprofHandler serves net/http/pprof's endpoints under /debug/pprof/ on a mux
// of its own. The package's import also registers them on
// http.DefaultServeMux, which nothing in WaveHouse serves; the handler is
// mounted only on the profiling listener, never on the API router.
func PprofHandler() http.Handler {
	mux := http.NewServeMux()
	// Index also serves each named profile: /debug/pprof/heap, goroutine,
	// allocs, block, mutex, threadcreate.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
