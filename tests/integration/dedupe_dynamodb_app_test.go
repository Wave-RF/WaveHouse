//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/app"
	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/settings"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// TestDynamoDBDedupe_TwoInstancesShareSeenIDs boots two apps the way two
// pods run — each its own data_dir, embedded queue and ingest worker — with
// dedupe.backend dynamodb over one table on dynamodb-local, and checks an id
// ingested through either is a duplicate through the other, that ClickHouse
// holds each id once, and that dedupe.lease reaches ingest as the in-flight
// answer's Retry-After.
func TestDynamoDBDedupe_TwoInstancesShareSeenIDs(t *testing.T) {
	e := env(t)
	ctx := context.Background()
	// The SDK's default chain, as in production; never the developer's files.
	none := filepath.Join(t.TempDir(), "none")
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local", "AWS_SESSION_TOKEN": "",
		"AWS_PROFILE": "", "AWS_CONFIG_FILE": none, "AWS_SHARED_CREDENTIALS_FILE": none,
		"AWS_EC2_METADATA_DISABLED": "true",
	} {
		t.Setenv(k, v)
	}

	chTable := createTable(t, "event_id String, n UInt32", "ORDER BY event_id")
	ddbTable := newDynamoTable()
	const lease = 7 * time.Second

	boot := func(name string) string {
		t.Helper()
		files, err := tenantSettings(e.ch, testCHDatabase)
		require.NoError(t, err)
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(files[settings.FileConfig], &doc))
		doc["dedupe"] = json.RawMessage(`{"enabled": true, "id_field": "event_id", "require_id": true, "tables": {}}`)
		files[settings.FileConfig], err = json.Marshal(doc)
		require.NoError(t, err)
		dir := filepath.Join(t.TempDir(), name)
		require.NoError(t, writeSettingsFiles(dir, files))

		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg := &config.Config{
			DataDir:    t.TempDir(),
			Server:     config.Server{ShutdownTimeout: 10},
			ClickHouse: config.ClickHouse{Password: testCHPassword},
			MQ:         config.MQ{Backend: config.MQEmbedded},
			Cache:      config.Cache{Backend: config.CacheLocal, L1MaxCost: 1 << 20},
			Dedupe: config.Dedupe{Backend: config.DedupeDynamoDB, Lease: lease, DynamoDB: config.DedupeDynamoDBConfig{
				Table: ddbTable, Region: "us-east-1", Endpoint: e.dynamoEndpoint,
				// dynamodb-local under a parallel suite is slower than the real thing.
				Timeout: 5 * time.Second, CreateTable: true,
			}},
			Coord:    config.Coord{Backend: config.CoordLocal},
			Settings: config.Settings{Dir: dir},
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
		require.NoError(t, waitForLive(ctx, baseURL, 30*time.Second))
		return baseURL
	}
	// Both create the table: the second finds it and leaves it as it is.
	podA, podB := boot("a"), boot("b")

	ingest := func(baseURL, id string, n int) (int, string, http.Header) {
		t.Helper()
		body := fmt.Sprintf(`{"event_id": %q, "n": %d}`, id, n)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/ingest?table="+url.QueryEscape(chTable), strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, strings.TrimSpace(string(b)), resp.Header
	}
	accepted := func(baseURL, id string, n int) {
		t.Helper()
		status, body, _ := ingest(baseURL, id, n)
		require.Equal(t, http.StatusOK, status, body)
		require.JSONEq(t, `{"ok": true}`, body)
	}
	duplicate := func(baseURL, id string, n int) {
		t.Helper()
		status, body, _ := ingest(baseURL, id, n)
		require.Equal(t, http.StatusOK, status, body)
		require.JSONEq(t, `{"duplicate": true}`, body)
	}

	accepted(podA, "e1", 1)
	duplicate(podB, "e1", 2)
	accepted(podB, "e2", 3)
	duplicate(podA, "e2", 4)
	duplicate(podA, "e1", 5)

	// A claim another process holds is in flight on both pods, for as long
	// as the configured lease says.
	peer := dynamoClient(t, ddbTable, dedupe.DynamoConfig{}).Tenant(tenant.Default)
	require.NoError(t, peer.Apply(true))
	claims, err := peer.Reserve(ctx, []dedupe.Key{{Table: chTable, ID: "e3"}}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, dedupe.Claimed, claims[0].Status)
	for _, pod := range []string{podA, podB} {
		status, body, header := ingest(pod, "e3", 6)
		require.Equal(t, http.StatusServiceUnavailable, status, body)
		assert.Equal(t, "7", header.Get("Retry-After"), "dedupe.lease, in seconds")
	}
	require.NoError(t, peer.Release(ctx, claims))
	accepted(podB, "e3", 7)
	duplicate(podA, "e3", 8)

	// Each pod's worker wrote only what its pod accepted: each id once.
	type row struct {
		ID string
		N  uint32
	}
	want := []row{{"e1", 1}, {"e2", 3}, {"e3", 7}}
	require.Eventually(t, func() bool {
		rows, err := e.chConn.Query(ctx, fmt.Sprintf("SELECT event_id, n FROM %s ORDER BY event_id", chTable))
		if err != nil {
			return false
		}
		defer func() { _ = rows.Close() }()
		var got []row
		for rows.Next() {
			var r row
			if rows.Scan(&r.ID, &r.N) != nil {
				return false
			}
			got = append(got, r)
		}
		return assert.ObjectsAreEqual(want, got)
	}, 30*time.Second, 500*time.Millisecond, "ClickHouse holds each id once, from the pod that accepted it")
}
