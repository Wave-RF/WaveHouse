//go:build integration

package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/ingest"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

const natsOperatorKey = "it-nats-operator-key"

// natsProcess is one WaveHouse process booted on mq.backend: nats.
type natsProcess struct {
	app     *app.App
	id      string // its instance_id, the holder its leases name
	baseURL string
	runDone chan error
	stop    context.CancelFunc // ends Run; runDone then says how
}

// natsProcesses numbers the processes the tests boot, for their instance_id.
var natsProcesses atomic.Int32

// bootNATSProcess boots the real wiring with roles over the nested settings
// directory root, on the NATS at natsURL as the shipped wavehouse user with
// its leases in the shipped bucket, and runs it until the test ends (or until
// it fails on its own: runDone).
func bootNATSProcess(t *testing.T, natsURL, root string, roles ...config.Role) *natsProcess {
	t.Helper()
	ctx := context.Background()
	pw := filepath.Join(t.TempDir(), "nats-password")
	require.NoError(t, os.WriteFile(pw, []byte(natstest.Password(natstest.WaveHouseUser)+"\n"), 0o600))
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &config.Config{
		DataDir:    t.TempDir(),
		Server:     config.Server{Port: ln.Addr().(*net.TCPAddr).Port, ShutdownTimeout: 10},
		ClickHouse: config.ClickHouse{Password: testCHPassword},
		Auth:       config.Auth{OperatorKey: natsOperatorKey},
		MQ: config.MQ{Backend: config.MQNATS, NATS: config.MQNATSConfig{
			URLs: []string{natsURL}, User: natstest.WaveHouseUser, PasswordFile: pw,
			SubjectPrefix: "wh", Partitions: 4, IngestConsumer: "wh-ingest",
			ConnectTimeout: 5 * time.Second, PublishTimeout: 5 * time.Second, TopologyWait: 30 * time.Second,
		}},
		Cache:      config.Cache{Backend: config.CacheLocal, L1MaxCost: 1 << 20},
		Dedupe:     config.Dedupe{Backend: config.DedupePebble},
		Coord:      config.Coord{Backend: config.CoordNATS},
		Roles:      roles,
		InstanceID: fmt.Sprintf("proc-%d", natsProcesses.Add(1)),
		Settings:   config.Settings{Dir: root},
	}
	require.NoError(t, cfg.Validate(), "the split boots on a shared queue")
	a, err := app.New(ctx, app.Options{Config: cfg, Listener: ln})
	require.NoError(t, err)
	runCtx, stop := context.WithCancel(ctx)
	p := &natsProcess{app: a, id: cfg.InstanceID, baseURL: "http://" + ln.Addr().String(), runDone: make(chan error, 1), stop: stop}
	go func() { p.runDone <- a.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		assert.NoError(t, a.Close(closeCtx))
	})
	require.NoError(t, waitForLive(ctx, p.baseURL, 30*time.Second))
	return p
}

func (p *natsProcess) do(t *testing.T, method, path string, headers map[string]string, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, p.baseURL+path, strings.NewReader(body))
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

// sse opens GET /v1/stream on p for tenant id with query, and returns its
// lines as they arrive, once the stream is open.
func (p *natsProcess) sse(t *testing.T, id tenant.ID, query string) <-chan string {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/v1/stream?"+query, nil)
	require.NoError(t, err)
	req.Header.Set(tenant.Header, id.String())
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed by the reader below, on cancel
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	lines := make(chan string, 256)
	connected := make(chan struct{})
	go func() {
		defer func() { _ = resp.Body.Close() }()
		defer close(lines)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if sc.Text() == ": connected" {
				close(connected)
				continue
			}
			lines <- sc.Text()
		}
	}()
	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream never opened")
	}
	return lines
}

// awaitEvent reads lines until a data line contains want.
func awaitEvent(t *testing.T, lines <-chan string, want string) {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			require.True(t, ok, "the stream ended before an event with %q", want)
			if strings.HasPrefix(line, "data:") && strings.Contains(line, want) {
				return
			}
		case <-deadline:
			t.Fatalf("no event with %q within 30s", want)
		}
	}
}

// TestNATSBackend_EndToEnd runs WaveHouse on mq.backend: nats against a
// real NATS set up the way an operator would: the Helm values' accounts and
// permissions, then the nack manifests (deployments/nats), applied before
// WaveHouse starts publishing, as the deployment guide says. Two processes
// share it, a split the embedded MQ cannot serve: A runs every role, B runs
// api and ingest. Over a nested directory with two tenants it shows ingest
// reaching each tenant's own ClickHouse database exactly once whichever
// worker takes the row, live SSE events reaching the API process that did not
// ingest them (every hub reads the history), SSE replay from the history,
// dead-letter counts kept per tenant on the one shared dead-letter stream, and
// the operator deleting the ingest durable ending every worker, and so every
// process.
func TestNATSBackend_EndToEnd(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	natsURL := startNATS(t)
	op, err := natstest.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(op.Close)
	require.NoError(t, op.ApplyShipped(ctx))

	databases := map[tenant.ID]string{}
	root := t.TempDir()
	for _, id := range []tenant.ID{"acme", "globex"} {
		db := "it_nats_" + id.String()
		databases[id] = db
		require.NoError(t, e.chConn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db))
		t.Cleanup(func() { _ = e.chConn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
		require.NoError(t, e.chConn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.events (id String, page String) ENGINE = MergeTree() ORDER BY id", db)))
		files, err := tenantSettings(e.ch, db)
		require.NoError(t, err)
		require.NoError(t, writeSettingsFiles(filepath.Join(root, id.String()), files))
	}

	a := bootNATSProcess(t, natsURL, root, config.AllRoles()...)
	b := bootNATSProcess(t, natsURL, root, config.RoleAPI, config.RoleIngest)
	operator := map[string]string{"X-Operator-Key": natsOperatorKey}
	for _, p := range []*natsProcess{a, b} {
		for id := range databases {
			require.Eventually(t, func() bool {
				status, _ := p.do(t, http.MethodGet, "/v1/ops/schema?table=events&tenant="+id.String(), operator, "")
				return status == http.StatusOK
			}, 30*time.Second, 200*time.Millisecond, "tenant %s never discovered its schema", id)
		}
	}

	since := time.Now().UTC()
	liveA := a.sse(t, "acme", "table=events")
	liveB := b.sse(t, "acme", "table=events")
	for id, row := range map[tenant.ID]string{"acme": `{"id":"a1","page":"home"}`, "globex": `{"id":"g1","page":"cart"}`} {
		status, body := a.do(t, http.MethodPost, "/v1/ingest?table=events", map[string]string{tenant.Header: id.String(), "Content-Type": "application/json"}, row)
		require.Equal(t, http.StatusOK, status, body)
	}

	// Every API process's hub sees every event, whichever took the publish.
	awaitEvent(t, liveA, `"a1"`)
	awaitEvent(t, liveB, `"a1"`)

	// Each row reaches its own tenant's database, once, and no other's.
	count := func(db, id string) uint64 {
		var n uint64
		require.NoError(t, e.chConn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.events WHERE id = '%s'", db, id)).Scan(&n))
		return n
	}
	require.Eventually(t, func() bool {
		return count(databases["acme"], "a1") == 1 && count(databases["globex"], "g1") == 1
	}, 30*time.Second, 250*time.Millisecond, "each tenant's row reaches its own ClickHouse")
	assert.Zero(t, count(databases["acme"], "g1"))
	assert.Zero(t, count(databases["globex"], "a1"))

	// A reconnect replays from the history stream.
	replay := b.sse(t, "acme", "table=events&since="+url.QueryEscape(since.Format(time.RFC3339Nano)))
	awaitEvent(t, replay, `"a1"`)

	// A row no insert can take is parked on the shared dead-letter stream,
	// and counted for its tenant alone.
	payload, err := json.Marshal(ingest.EventMessage{
		TableName:         "missing",
		ReceivedTimestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Format:            ingest.FormatJSONCompactEachRow,
		Columns:           []string{"id"},
		Row:               json.RawMessage(`["x"]`),
	})
	require.NoError(t, err)
	require.NoError(t, a.app.MQ().Publish(ctx, mq.Topic{Tenant: "globex", Table: "missing"}, payload))
	dlq := func(p *natsProcess, id tenant.ID) (total float64, tables map[string]any) {
		status, body := p.do(t, http.MethodGet, "/v1/ops/dlq/stats?tenant="+id.String(), operator, "")
		require.Equal(t, http.StatusOK, status, body)
		var stats struct {
			Total  float64        `json:"total"`
			Tables map[string]any `json:"tables"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &stats))
		return stats.Total, stats.Tables
	}
	require.Eventually(t, func() bool {
		_, tables := dlq(b, "globex")
		_, ok := tables["missing"]
		return ok
	}, 30*time.Second, 250*time.Millisecond, "the unwritable row is parked under its tenant")
	total, tables := dlq(a, "acme")
	assert.Zero(t, total, "another tenant's parked rows are not counted for acme")
	assert.Empty(t, tables)

	// The operator deleting the durable ends every worker, and with it every
	// process: nothing can write what the API would go on accepting.
	require.NoError(t, op.DeleteDurable(ctx, "wh-ingest"))
	for name, p := range map[string]*natsProcess{"A": a, "B": b} {
		select {
		case err := <-p.runDone:
			require.ErrorIs(t, err, mq.ErrDeliveryEnded, "process %s", name)
			assert.True(t, strings.HasPrefix(err.Error(), "ingest worker: "), "process %s: %v", name, err)
		case <-time.After(30 * time.Second):
			t.Fatalf("process %s kept running without its durable", name)
		}
	}
}
