package app

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/config"
	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/dedupe/dedupetest"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// fakeDynamo answers the DynamoDB JSON protocol for one table, enough for
// boot's check, the dev create path, and a claim and its commit. Whether the
// table exists is the test's to switch.
type fakeDynamo struct {
	mu     sync.Mutex
	exists bool
	calls  []string
}

func (f *fakeDynamo) setExists(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exists = v
}

func (f *fakeDynamo) called(op string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == op {
			return true
		}
	}
	return false
}

func (f *fakeDynamo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	_, op, _ := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	f.mu.Lock()
	f.calls = append(f.calls, op)
	if op == "CreateTable" {
		f.exists = true
	}
	exists := f.exists
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	if !exists {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}`)
		return
	}
	body := `{}`
	switch op {
	case "DescribeTable", "CreateTable":
		body = `{"Table":{"TableName":"dedupe","TableStatus":"ACTIVE",` +
			`"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],` +
			`"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"B"}]}}`
	case "DescribeTimeToLive":
		body = `{"TimeToLiveDescription":{"AttributeName":"ex","TimeToLiveStatus":"ENABLED"}}`
	case "BatchWriteItem":
		body = `{"UnprocessedItems":{}}`
	}
	_, _ = io.WriteString(w, body)
}

// dynamoConfig points cfg's dedupe at a fake table, with credentials from the
// environment as the SDK's default chain reads them — and nothing from the
// developer's own AWS files.
func dynamoConfig(t *testing.T, cfg *config.Config, exists bool) *fakeDynamo {
	t.Helper()
	fake := &fakeDynamo{exists: exists}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	none := filepath.Join(t.TempDir(), "none")
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local", "AWS_SESSION_TOKEN": "",
		"AWS_PROFILE": "", "AWS_CONFIG_FILE": none, "AWS_SHARED_CREDENTIALS_FILE": none,
		"AWS_EC2_METADATA_DISABLED": "true",
	} {
		t.Setenv(k, v)
	}
	cfg.Dedupe = config.Dedupe{Backend: config.DedupeDynamoDB, DynamoDB: config.DedupeDynamoDBConfig{
		Table: "dedupe", Region: "us-east-1", Endpoint: srv.URL, MaxAttempts: 1,
	}}
	return fake
}

var dedupeOn = map[string]any{"dedupe": map[string]any{"enabled": true, "id_field": "event_id", "require_id": false, "retention": "0", "tables": map[string]any{}}}

func TestNew_DynamoDBDedupe(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, dedupeOn))
	fake := dynamoConfig(t, cfg, true)
	a := newApp(t, cfg, Options{})

	assert.True(t, fake.called("DescribeTable"), "boot checks the table")
	assert.False(t, fake.called("CreateTable"), "and never creates it without create_table")
	store := a.dedup.For(tenant.Default)
	require.True(t, store.Open())
	dup, err := dedupetest.Mark(t.Context(), store, eventKey)
	require.NoError(t, err)
	assert.False(t, dup)
	assert.True(t, fake.called("PutItem"), "the claim went to the table")
	assert.True(t, fake.called("BatchWriteItem"), "and so did its commit")
	assert.Nil(t, a.dedupeStats, "no Pebble instance, so no Pebble gauges")
	assert.NoDirExists(t, filepath.Join(cfg.DataDir, "pebble"))
}

func TestNew_DynamoDBDedupeCreatesTheTableOnlyWhenAsked(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, dedupeOn))
	fake := dynamoConfig(t, cfg, false)
	cfg.Dedupe.DynamoDB.CreateTable = true
	a := newApp(t, cfg, Options{})
	assert.True(t, fake.called("CreateTable"))
	assert.True(t, fake.called("UpdateTimeToLive"))
	assert.True(t, a.dedup.For(tenant.Default).Open())
}

// A table that fails the check follows the registry's rule for the shape,
// as a Pebble instance that cannot open does.
func TestNew_DynamoDBDedupeTableMissing(t *testing.T) {
	t.Run("flat refuses boot", func(t *testing.T) {
		for name, patch := range map[string]map[string]any{"dedupe on": dedupeOn, "dedupe off": nil} {
			t.Run(name, func(t *testing.T) {
				guardGlobals(t)
				cfg := testConfig(t, writeSettings(t, patch))
				dynamoConfig(t, cfg, false)
				_, err := New(t.Context(), Options{Config: cfg})
				require.ErrorContains(t, err, "dedupe open")
				require.ErrorContains(t, err, "ResourceNotFoundException")
			})
		}
	})
	t.Run("nested fails closed until a reload passes the check", func(t *testing.T) {
		root := writeNestedSettings(t, map[string]map[string]any{"acme": dedupeOn, "globex": nil})
		cfg := testConfig(t, root)
		fake := dynamoConfig(t, cfg, false)
		a := newApp(t, cfg, Options{})

		acme := a.dedup.For("acme")
		assert.False(t, acme.Open())
		_, err := dedupetest.Mark(t.Context(), acme, eventKey)
		require.ErrorIs(t, err, dedupe.ErrUnavailable, "switched on, table missing: ingest fails closed")
		_, err = dedupetest.Mark(t.Context(), a.dedup.For("globex"), eventKey)
		require.ErrorIs(t, err, dedupe.ErrDisabled)

		fake.setExists(true)
		a.tenants.Reload("test")
		assert.True(t, acme.Open(), "the reload checked again and opened the store")
		_, err = dedupetest.Mark(context.Background(), acme, eventKey)
		require.NoError(t, err)
	})
}

// A nested directory has no watcher, so a table that comes good is picked up
// by the background retry, not only by a reload someone has to send.
func TestRun_DynamoDBDedupeRetriesTheTableCheck(t *testing.T) {
	root := writeNestedSettings(t, map[string]map[string]any{"acme": dedupeOn})
	cfg := testConfig(t, root)
	fake := dynamoConfig(t, cfg, false)
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	a := newApp(t, cfg, Options{Listener: ln})
	acme := a.dedup.For("acme")
	require.False(t, acme.Open())

	_, stop := runApp(t, a, ln)
	fake.setExists(true)
	require.Eventually(t, acme.Open, 10*time.Second, 50*time.Millisecond, "the retry opened the store without a reload")
	require.NoError(t, stop())
}

// No region anywhere is a certain config error: refused at boot in either
// shape rather than failing every check afterwards.
func TestNew_DynamoDBDedupeRefusesNoRegion(t *testing.T) {
	for name, dir := range map[string]func(*testing.T) string{
		"flat":   func(t *testing.T) string { return writeSettings(t, dedupeOn) },
		"nested": func(t *testing.T) string { return writeNestedSettings(t, map[string]map[string]any{"acme": dedupeOn}) },
	} {
		t.Run(name, func(t *testing.T) {
			guardGlobals(t)
			cfg := testConfig(t, dir(t))
			dynamoConfig(t, cfg, true)
			cfg.Dedupe.DynamoDB.Region = ""
			t.Setenv("AWS_REGION", "")
			t.Setenv("AWS_DEFAULT_REGION", "")
			_, err := New(t.Context(), Options{Config: cfg})
			require.ErrorContains(t, err, "dynamodb region is not set")
		})
	}
}
