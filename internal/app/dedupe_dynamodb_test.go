package app

import (
	"bytes"
	"context"
	"io"
	"log/slog"
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
// table exists, whether every call is throttled, and whether the endpoint
// hangs (every call, or one op alone), are the test's to switch.
type fakeDynamo struct {
	mu        sync.Mutex
	exists    bool
	throttles bool
	hangs     bool
	hangOn    string // hang calls of this op alone, once set; "" hangs none this way
	calls     []string
}

func (f *fakeDynamo) setThrottles(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.throttles = v
}

func (f *fakeDynamo) setExists(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exists = v
}

func (f *fakeDynamo) setHangs(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hangs = v
}

func (f *fakeDynamo) setHangOn(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hangOn = op
}

func (f *fakeDynamo) called(op string) bool { return f.count(op) > 0 }

func (f *fakeDynamo) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == op {
			n++
		}
	}
	return n
}

func (f *fakeDynamo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Drained before any hang below: with the body unread, an SDK write
	// deadline or the client giving up never reaches this handler, since the
	// connection looks like it's still waiting for us to consume it.
	_, _ = io.Copy(io.Discard, r.Body)
	_, op, _ := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	f.mu.Lock()
	f.calls = append(f.calls, op)
	if op == "CreateTable" {
		f.exists = true
	}
	exists, throttled, hang := f.exists, f.throttles, f.hangs || op == f.hangOn
	f.mu.Unlock()
	if hang {
		<-r.Context().Done()
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	if throttled {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#ThrottlingException","message":"Rate exceeded"}`)
		return
	}
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
			`"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}}`
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

var dedupeOn = map[string]any{"dedupe": map[string]any{"enabled": true, "id_field": "event_id", "require_id": false, "tables": map[string]any{}}}

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

// A misconfigured table refuses boot only over a flat directory in which a
// tenant has dedupe on; every other failure boots and fails closed.
func TestNew_DynamoDBDedupeTableMissing(t *testing.T) {
	t.Run("flat with dedupe on refuses boot", func(t *testing.T) {
		guardGlobals(t)
		cfg := testConfig(t, writeSettings(t, dedupeOn))
		dynamoConfig(t, cfg, false)
		_, err := New(t.Context(), Options{Config: cfg})
		require.ErrorContains(t, err, "dedupe open")
		require.ErrorContains(t, err, "ResourceNotFoundException")
		require.NotErrorIs(t, err, dedupe.ErrUnavailable)
	})
	t.Run("flat with dedupe off boots, and fails closed once it is on", func(t *testing.T) {
		dir := writeSettings(t, nil)
		cfg := testConfig(t, dir)
		dynamoConfig(t, cfg, false)
		logs := bootLogged(t)
		a, err := New(t.Context(), Options{Config: cfg})
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, a.Close(context.Background())) })
		assert.Contains(t, logs.String(), `level=ERROR msg="dedupe: dynamodb table is misconfigured`)

		store := a.dedup.For(tenant.Default)
		_, err = dedupetest.Mark(t.Context(), store, eventKey)
		require.ErrorIs(t, err, dedupe.ErrDisabled)
		rewriteSettings(t, dir, dedupeOn)
		a.tenants.Reload("test")
		_, err = dedupetest.Mark(t.Context(), store, eventKey)
		require.ErrorIs(t, err, dedupe.ErrUnavailable, "switched on by a reload while the table is missing: closed, not un-deduped")
	})
	t.Run("nested fails closed", func(t *testing.T) {
		root := writeNestedSettings(t, map[string]map[string]any{"acme": dedupeOn, "globex": nil})
		cfg := testConfig(t, root)
		dynamoConfig(t, cfg, false)
		a := newApp(t, cfg, Options{})

		acme := a.dedup.For("acme")
		assert.False(t, acme.Open())
		_, err := dedupetest.Mark(t.Context(), acme, eventKey)
		require.ErrorIs(t, err, dedupe.ErrUnavailable, "switched on, table missing: ingest fails closed")
		_, err = dedupetest.Mark(t.Context(), a.dedup.For("globex"), eventKey)
		require.ErrorIs(t, err, dedupe.ErrDisabled)
	})
}

// A transient failure (a throttle) never refuses boot, even over a flat
// directory with dedupe on: the tenant fails closed until the background
// retry's check passes.
func TestRun_DynamoDBDedupeFlatThrottledRecovers(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, dedupeOn))
	fake := dynamoConfig(t, cfg, true)
	fake.setThrottles(true)
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	a := newApp(t, cfg, Options{Listener: ln})
	store := a.dedup.For(tenant.Default)
	require.False(t, store.Open())
	_, err = dedupetest.Mark(t.Context(), store, eventKey)
	require.ErrorIs(t, err, dedupe.ErrUnavailable, "switched on, table throttled: ingest fails closed")

	_, stop := runApp(t, a, ln)
	fake.setThrottles(false)
	require.Eventually(t, store.Open, 10*time.Second, 50*time.Millisecond, "the retry opened the store")
	_, err = dedupetest.Mark(context.Background(), store, eventKey)
	require.NoError(t, err)
	require.NoError(t, stop())
}

// With create_table on, an endpoint that fails transiently (dynamodb-local
// still starting) boots too, and the retry creates the table once it answers.
func TestRun_DynamoDBDedupeFlatCreateTableRetries(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, dedupeOn))
	fake := dynamoConfig(t, cfg, false)
	cfg.Dedupe.DynamoDB.CreateTable = true
	fake.setThrottles(true)
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	a := newApp(t, cfg, Options{Listener: ln})
	store := a.dedup.For(tenant.Default)
	require.False(t, store.Open())

	_, stop := runApp(t, a, ln)
	fake.setThrottles(false)
	require.Eventually(t, store.Open, 10*time.Second, 50*time.Millisecond, "the retry created the table and opened the store")
	assert.True(t, fake.called("CreateTable"))
	require.NoError(t, stop())
}

// bootLogged sends the default logger to a buffer for the rest of the test,
// for a boot that logs what it tolerated.
func bootLogged(t *testing.T) *lockedBuffer {
	t.Helper()
	guardGlobals(t)
	buf := &lockedBuffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	return buf
}

// lockedBuffer is a bytes.Buffer safe for the background retry's logging.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// The reload hook runs under the lock that serializes reloads, so it never
// calls DynamoDB: against a table that hangs, a reload returns at once, and a
// tenant it switches on fails closed rather than publishing un-deduped.
func TestReload_DynamoDBDedupeMakesNoTableCall(t *testing.T) {
	root := writeNestedSettings(t, map[string]map[string]any{"acme": nil})
	cfg := testConfig(t, root)
	fake := dynamoConfig(t, cfg, false)
	a := newApp(t, cfg, Options{})
	fake.setHangs(true)
	before := fake.count("DescribeTable")

	rewriteSettings(t, filepath.Join(root, "acme"), dedupeOn)
	start := time.Now()
	a.tenants.Reload("test")
	assert.Less(t, time.Since(start), time.Second, "a check would wait out its 2.5s deadline")
	assert.Equal(t, before, fake.count("DescribeTable"), "the reload made no table call")

	acme := a.dedup.For("acme")
	assert.False(t, acme.Open())
	_, err := dedupetest.Mark(t.Context(), acme, eventKey)
	require.ErrorIs(t, err, dedupe.ErrUnavailable, "switched on while the table fails: closed, not ErrDisabled")
}

// A reload wakes the background retry rather than running the check itself.
// The retry's first timed attempt is a second after it starts, and a timer
// never fires early, so an open sooner than that is the reload's doing.
func TestRun_DynamoDBDedupeReloadWakesTheRetry(t *testing.T) {
	root := writeNestedSettings(t, map[string]map[string]any{"acme": dedupeOn})
	cfg := testConfig(t, root)
	fake := dynamoConfig(t, cfg, false)
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	a := newApp(t, cfg, Options{Listener: ln})
	acme := a.dedup.For("acme")
	require.False(t, acme.Open())

	start := time.Now()
	_, stop := runApp(t, a, ln)
	fake.setExists(true)
	a.tenants.Reload("test")
	require.Eventually(t, acme.Open, 5*time.Second, 5*time.Millisecond)
	assert.Less(t, time.Since(start), time.Second, "opened before the first timed retry")
	_, err = dedupetest.Mark(context.Background(), acme, eventKey)
	require.NoError(t, err)
	require.NoError(t, stop())
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

// A reload must not wait behind a tenant's own in-flight DynamoDB call when
// nothing changes for that tenant: Managed.Apply's no-op fast path settles
// under a read lock alone, so it never contends with a Commit already
// holding one and returns long before the commit does.
func TestReload_DynamoDBDedupeDoesNotWaitOnInFlightCommit(t *testing.T) {
	cfg := testConfig(t, writeSettings(t, dedupeOn))
	fake := dynamoConfig(t, cfg, true)
	fake.setHangOn("BatchWriteItem")
	a := newApp(t, cfg, Options{})

	store := a.dedup.For(tenant.Default)
	require.True(t, store.Open())
	claims, err := store.Reserve(context.Background(), []dedupe.Key{eventKey}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, dedupe.Claimed, claims[0].Status)

	commitCtx, cancelCommit := context.WithCancel(context.Background())
	defer cancelCommit()
	commitDone := make(chan error, 1)
	go func() { commitDone <- store.Commit(commitCtx, claims, 0) }()
	require.Eventually(t, func() bool { return fake.called("BatchWriteItem") }, time.Second, time.Millisecond,
		"commit reached the table and is now hanging on it")

	start := time.Now()
	a.tenants.Reload("test")
	assert.Less(t, time.Since(start), 500*time.Millisecond,
		"a reload that changes nothing for this tenant waited on its in-flight commit")

	cancelCommit()
	<-commitDone // let the hung call finish (canceled) before the app closes
}
