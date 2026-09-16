package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// run reads the whole boot config from the environment here (no config
// file), so these tests set every WH_* variable they need and, because
// config.Load refuses any WH_* variable it doesn't bind, clear whatever the
// developer's shell exports under that prefix first. t.Setenv registers the
// restore; the unset is ours. They mutate process state (env, the default
// logger), so none of them run in parallel.
func hermeticEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if name, val, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "WH_") {
			t.Setenv(name, val)
			require.NoError(t, os.Unsetenv(name))
		}
	}
	saved := slog.Default()
	t.Cleanup(func() { slog.SetDefault(saved) })
	t.Setenv(config.EnvConfig, filepath.Join(t.TempDir(), "absent.yaml"))
}

// closedAddr is a 127.0.0.1 address nothing listens on, so the boot-time
// schema discovery against it fails fast whatever is on the developer's :9000.
func closedAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// seedSettings writes the embedded seed with clickhouse.addr pointed at a
// closed port.
func seedSettings(t *testing.T) string {
	t.Helper()
	files, err := settings.Seed()
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(files[settings.FileConfig], &doc))
	var ch map[string]any
	require.NoError(t, json.Unmarshal(doc["clickhouse"], &ch))
	ch["addr"] = closedAddr(t)
	doc["clickhouse"], err = json.Marshal(ch)
	require.NoError(t, err)
	files[settings.FileConfig], err = json.Marshal(doc)
	require.NoError(t, err)
	dir := t.TempDir()
	for name, data := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
	}
	return dir
}

func TestRun_BootsAndStopsOnCancel(t *testing.T) {
	hermeticEnv(t)
	t.Setenv(config.EnvSettingsDir, seedSettings(t))
	t.Setenv("WH_DATA_DIR", t.TempDir())
	_, port, err := net.SplitHostPort(closedAddr(t))
	require.NoError(t, err)
	t.Setenv("WH_SERVER_PORT", port)
	t.Setenv(config.EnvLogLevel, "warn")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan int, 1)
	go func() { done <- run(ctx) }()

	url := "http://127.0.0.1:" + port + "/version"
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK && strings.Contains(string(body), Version)
	}, 10*time.Second, 20*time.Millisecond, "server never answered /version")

	cancel()
	select {
	case code := <-done:
		assert.Equal(t, 0, code, "a signal-cancelled run exits 0")
	case <-time.After(15 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

func TestRun_RefusesToBoot(t *testing.T) {
	tests := []struct {
		name string
		env  func(t *testing.T)
	}{
		{
			name: "config file with an undeclared key",
			env: func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "config.yaml")
				require.NoError(t, os.WriteFile(path, []byte("bogus: 1\n"), 0o600))
				t.Setenv(config.EnvConfig, path)
			},
		},
		{
			name: "data_dir is a regular file",
			env: func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "not-a-dir")
				require.NoError(t, os.WriteFile(file, nil, 0o600))
				t.Setenv("WH_DATA_DIR", file)
			},
		},
		{
			name: "settings directory is empty",
			env: func(t *testing.T) {
				t.Setenv(config.EnvSettingsDir, t.TempDir())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hermeticEnv(t)
			t.Setenv(config.EnvSettingsDir, seedSettings(t))
			t.Setenv("WH_DATA_DIR", t.TempDir())
			t.Setenv("WH_SERVER_PORT", strconv.Itoa(8080))
			tt.env(t)
			assert.Equal(t, 1, run(t.Context()))
		})
	}
}

func TestStopOnSignals_SecondSignalExits(t *testing.T) {
	saved := slog.Default()
	t.Cleanup(func() { slog.SetDefault(saved) })
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	sigs := make(chan os.Signal, 2)
	cancelled := make(chan struct{})
	exited := make(chan int, 1)
	go stopOnSignals(sigs, func() { close(cancelled) }, func(code int) { exited <- code })

	sigs <- syscall.SIGTERM
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the first signal did not cancel the run context")
	}
	select {
	case code := <-exited:
		t.Fatalf("exited %d on the first signal; it should begin the graceful stop", code)
	case <-time.After(50 * time.Millisecond):
	}

	sigs <- syscall.SIGINT
	select {
	case code := <-exited:
		assert.Equal(t, 1, code, "a second signal abandons the stop with a non-zero exit")
	case <-time.After(5 * time.Second):
		t.Fatal("the second signal did not exit")
	}
}
