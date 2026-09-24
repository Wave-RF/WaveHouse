//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// TestNestedDirectory_PerTenantPoolsAndDiscovery boots the real wiring over
// a nested settings directory whose two tenants point at the one ClickHouse
// through two databases — two tuples, so two pools and two schema
// registries (#583 story 6) — and checks each tenant discovers its own
// tables and none of the other's, that the ops routes address a tenant by
// ?tenant=, that /livez turns 200 once a tenant has discovered and /readyz
// finds a pool that answers, and that a tenant's structured query runs
// against its own database.
func TestNestedDirectory_PerTenantPoolsAndDiscovery(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	const operatorKey = "it-operator-key"

	// One database per tenant, each with a table of its own shape.
	databases := map[tenant.ID]string{"acme": "it_acme_" + t.Name(), "globex": "it_globex_" + t.Name()}
	columns := map[tenant.ID]string{"acme": "id String, page String", "globex": "id String, amount Float64"}
	for id, db := range databases {
		db = strings.ToLower(strings.NewReplacer("/", "_", " ", "_", "-", "_").Replace(db))
		databases[id] = db
		require.NoError(t, e.chConn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db))
		t.Cleanup(func() { _ = e.chConn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
		require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.events (%s) ENGINE = MergeTree() ORDER BY id", db, columns[id])))
		require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf("INSERT INTO %s.events (id) VALUES ('1')", db)))
	}

	root := t.TempDir()
	for id, db := range databases {
		files, err := tenantSettings(e.ch, db)
		require.NoError(t, err)
		require.NoError(t, writeSettingsFiles(filepath.Join(root, id.String()), files))
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &config.Config{
		DataDir:    t.TempDir(),
		Server:     config.Server{ShutdownTimeout: 10},
		ClickHouse: config.ClickHouse{Password: testCHPassword},
		Auth:       config.Auth{OperatorKey: operatorKey},
		Cache:      config.Cache{L1MaxCost: 1 << 20},
		Settings:   config.Settings{Dir: root},
	}
	a, err := app.New(ctx, app.Options{Config: cfg, Listener: ln})
	require.NoError(t, err)
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		assert.NoError(t, <-runDone)
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		assert.NoError(t, a.Close(closeCtx))
	})
	baseURL := "http://" + ln.Addr().String()
	// A nested directory's tenants discover in the background: /livez turns
	// 200 once the first has.
	require.NoError(t, waitForLive(ctx, baseURL, 30*time.Second))

	do := func(method, path string, headers map[string]string, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, strings.NewReader(body))
		require.NoError(t, err)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(b)
	}
	operator := map[string]string{"X-Operator-Key": operatorKey}

	// Each tenant's registry discovers its own database and no other's;
	// every tenant's has loaded once its refresh route answers 200.
	for id, cols := range map[tenant.ID][]string{"acme": {"id", "page"}, "globex": {"id", "amount"}} {
		require.Eventually(t, func() bool {
			status, _ := do(http.MethodGet, "/v1/ops/schema?tenant="+id.String(), operator, "")
			return status == http.StatusOK
		}, 30*time.Second, 200*time.Millisecond, "tenant %s never discovered its schema", id)
		status, body := do(http.MethodGet, "/v1/ops/schema?table=events&tenant="+id.String(), operator, "")
		require.Equal(t, http.StatusOK, status, body)
		var schema struct {
			Columns []struct{ Name string } `json:"columns"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &schema))
		var names []string
		for _, c := range schema.Columns {
			names = append(names, c.Name)
		}
		assert.Equal(t, cols, names, "tenant %s reads its own database's table", id)
	}

	status, body := do(http.MethodGet, "/readyz", nil, "")
	assert.Equal(t, http.StatusOK, status, body)

	// A structured query runs against the tenant's own database: globex's
	// events has no page column, so the same query is a 400 there.
	query := `{"columns": ["page"]}`
	status, body = do(http.MethodPost, "/v1/query?table=events", map[string]string{tenant.Header: "acme", "Content-Type": "application/json"}, query)
	assert.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"page"`)
	status, body = do(http.MethodPost, "/v1/query?table=events", map[string]string{tenant.Header: "globex", "Content-Type": "application/json"}, query)
	assert.Equal(t, http.StatusBadRequest, status, body)

	// The raw-SQL proxy runs against the ?tenant='s database too.
	status, body = do(http.MethodPost, "/v1/ops/query?tenant=globex", operator, `{"sql": "SELECT amount FROM events"}`)
	assert.Equal(t, http.StatusOK, status, body)
	status, body = do(http.MethodPost, "/v1/ops/query?tenant=acme", operator, `{"sql": "SELECT amount FROM events"}`)
	assert.Equal(t, http.StatusBadRequest, status, body)
}
