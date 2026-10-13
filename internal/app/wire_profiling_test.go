package app

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/config"
)

// The profiling listener is wired only when asked for, and pprof is never on
// the API router either way.
func TestNew_ProfilingListenerOnlyWhenEnabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		cfg := testConfig(t, writeSettings(t, nil))
		cfg.Profiling = config.Profiling{Enabled: enabled, Addr: closedAddr(t)}
		a := newApp(t, cfg, Options{})
		assert.Equal(t, enabled, slices.Contains(componentNames(a), "profiling server"), "enabled=%v", enabled)
		for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/cmdline"} {
			assert.Equal(t, http.StatusNotFound, get(t, a.Handler(), path).Code, "API router serves %s (enabled=%v)", path, enabled)
		}
	}
}

func getURL(t *testing.T, url string) (int, string, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), err
}

// Serial: it takes the process's one CPU profile. The listener serves pprof
// on profiling.addr, and a CPU profile in flight ends as the drain begins
// rather than holding the stop to server.shutdown_timeout.
func TestWireProfiling_ServesAndEndsAProfileAtTheStop(t *testing.T) {
	addr := closedAddr(t)
	a := &App{cfg: &config.Config{
		Server:    config.Server{ShutdownTimeout: 60},
		Profiling: config.Profiling{Enabled: true, Addr: addr},
	}}
	a.wireProfiling()
	require.Len(t, a.components, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ran := make(chan error, 1)
	go func() { ran <- a.components[0].run(ctx) }()

	base := "http://" + addr + "/debug/pprof/"
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		code, body, err := getURL(t, base)
		if assert.NoError(c, err) {
			assert.Equal(c, http.StatusOK, code)
			assert.Contains(c, body, "goroutine")
		}
	}, 5*time.Second, 20*time.Millisecond)

	type result struct {
		code int
		body string
		err  error
	}
	profiled := make(chan result, 1)
	go func() {
		code, body, err := getURL(t, base+"profile?seconds=60")
		profiled <- result{code, body, err}
	}()
	// In flight once the handler's goroutine shows in a goroutine dump.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, body, err := getURL(t, base+"goroutine?debug=2")
		if assert.NoError(c, err) {
			assert.True(c, strings.Contains(body, "net/http/pprof.Profile"), "no profile in flight yet")
		}
	}, 5*time.Second, 20*time.Millisecond)

	stopped := time.Now()
	cancel()
	select {
	case err := <-ran:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the stop waited on the CPU profile")
	}
	assert.Less(t, time.Since(stopped), 10*time.Second)
	select {
	case r := <-profiled:
		require.NoError(t, r.err)
		assert.Equal(t, http.StatusOK, r.code)
		assert.NotEmpty(t, r.body, "the profile ended with what it had")
	case <-time.After(5 * time.Second):
		t.Fatal("the CPU profile did not end")
	}
}

// A listener that cannot bind is logged, not fatal: the process serves on.
func TestWireProfiling_BindFailureIsNotFatal(t *testing.T) {
	t.Parallel()
	a := &App{cfg: &config.Config{
		Server:    config.Server{ShutdownTimeout: 1},
		Profiling: config.Profiling{Enabled: true, Addr: "256.0.0.1:6060"},
	}}
	a.wireProfiling()
	require.Len(t, a.components, 1)
	assert.NoError(t, a.components[0].run(t.Context()))
}
