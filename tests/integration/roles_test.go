//go:build integration

package tests

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// The binary under test is built once per run, with coverage when the suite
// collects it: a child inherits GOCOVERDIR and writes its counters there.
// TestMain starts the build alongside the containers, so it overlaps their
// startup and stays outside -timeout, which counts from m.Run; a cold cover
// build is most of a minute on a CI runner. binaryDir is removed by TestMain
// after the run (removeRolesBinary).
var (
	binaryBuilt = make(chan struct{})
	binaryDir   string
	binaryPath  string
	errBinary   error
)

// buildWavehouseBinary builds the binary and closes binaryBuilt. TestMain
// calls it once.
func buildWavehouseBinary() {
	defer close(binaryBuilt)
	dir, err := os.MkdirTemp("", "wh-roles-bin-")
	if err != nil {
		errBinary = err
		return
	}
	binaryDir = dir
	binaryPath = filepath.Join(dir, "wavehouse")
	args := []string{"build", "-o", binaryPath}
	if os.Getenv("GOCOVERDIR") != "" {
		args = append(args, "-cover", "-coverpkg=./...")
	}
	_, file, _, _ := runtime.Caller(0)
	cmd := exec.Command("go", append(args, "./cmd/wavehouse")...) //nolint:gosec // G204: fixed arguments
	cmd.Dir = filepath.Join(filepath.Dir(file), "..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		errBinary = fmt.Errorf("go build: %w\n%s", err, out)
	}
}

// removeRolesBinary deletes the binary buildWavehouseBinary built, if any.
func removeRolesBinary() {
	<-binaryBuilt
	if binaryDir != "" {
		_ = os.RemoveAll(binaryDir)
	}
}

func wavehouseBinary(t *testing.T) string {
	t.Helper()
	<-binaryBuilt
	require.NoError(t, errBinary)
	return binaryPath
}

// whProcess is one wavehouse process, configured by WH_* variables alone,
// as a Deployment would be.
type whProcess struct {
	name    string
	baseURL string
	cmd     *exec.Cmd
	log     *lockedWriter
	exited  chan struct{}
}

// freePort returns a port that was free a moment ago.
func freePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

// childEnv is the parent's environment without any WH_* variable (boot
// refuses an unbound one) or AWS setting, plus vars.
func childEnv(vars map[string]string) []string {
	var out []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "WH_") || strings.HasPrefix(kv, "AWS_") {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range vars {
		out = append(out, k+"="+v)
	}
	return out
}

// startProcess starts the binary as instance name with vars. It is stopped
// at cleanup, and its log printed if the test failed.
func startProcess(t *testing.T, name string, vars map[string]string) *whProcess {
	t.Helper()
	port := freePort(t)
	env := map[string]string{
		"WH_SERVER_PORT": strconv.Itoa(port),
		"WH_INSTANCE_ID": name,
		"WH_CONFIG":      filepath.Join(t.TempDir(), "absent.yaml"),
		"WH_DATA_DIR":    t.TempDir(),
	}
	for k, v := range vars {
		env[k] = v
	}
	cmd := exec.Command(wavehouseBinary(t)) //nolint:gosec // G204: the binary this test built
	cmd.Env = childEnv(env)
	w := &lockedWriter{w: &bytes.Buffer{}}
	cmd.Stdout, cmd.Stderr = w, w
	p := &whProcess{name: name, baseURL: "http://127.0.0.1:" + strconv.Itoa(port), cmd: cmd, log: w, exited: make(chan struct{})}
	require.NoError(t, cmd.Start())
	go func() { _ = cmd.Wait(); close(p.exited) }()
	t.Cleanup(func() {
		p.stop()
		if t.Failed() {
			t.Logf("---- %s log ----\n%s", name, w.String())
		}
	})
	return p
}

// stop sends SIGTERM and waits, killing the process after 15s.
func (p *whProcess) stop() {
	select {
	case <-p.exited:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.exited
	}
}

// kill ends the process at once: no resign, no drain.
func (p *whProcess) kill(t *testing.T) {
	t.Helper()
	require.NoError(t, p.cmd.Process.Kill())
	<-p.exited
}

// awaitLive waits for /livez, failing at once if the process exits.
func (p *whProcess) awaitLive(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.exited:
			t.Fatalf("%s exited before it was live:\n%s", p.name, p.log.String())
		default:
		}
		if status, _, _, _ := p.request(http.MethodGet, "/livez", nil, ""); status == http.StatusOK {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s never went live:\n%s", p.name, p.log.String())
}

// awaitExit waits for the process to exit on its own and returns its log.
func (p *whProcess) awaitExit(t *testing.T, within time.Duration) string {
	t.Helper()
	select {
	case <-p.exited:
	case <-time.After(within):
		t.Fatalf("%s still running after %s:\n%s", p.name, within, p.log.String())
	}
	return p.log.String()
}

func (p *whProcess) request(method, path string, headers map[string]string, body string) (int, http.Header, string, error) {
	req, err := http.NewRequestWithContext(context.Background(), method, p.baseURL+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, "", err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(b), err
}

// ingest posts body to p for table.
func (p *whProcess) ingest(t *testing.T, table, contentType, body string) (int, string) {
	t.Helper()
	status, _, resp, err := p.request(http.MethodPost, "/v1/ingest?table="+url.QueryEscape(table), map[string]string{"Content-Type": contentType}, body)
	require.NoError(t, err)
	return status, strings.TrimSpace(resp)
}

// query runs a select-all structured query on p: status, X-Cache, body.
func (p *whProcess) query(t *testing.T, table string) (int, string, string) {
	t.Helper()
	status, h, body, err := p.request(http.MethodPost, "/v1/query?table="+url.QueryEscape(table), map[string]string{"Content-Type": "application/json"}, `{"select_all":true}`)
	require.NoError(t, err)
	return status, h.Get("X-Cache"), body
}

// sse opens GET /v1/stream on p for table and returns its lines as they
// arrive, once the stream is open.
func (p *whProcess) sse(t *testing.T, table string) <-chan string {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/v1/stream?table="+url.QueryEscape(table), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed by the reader below, on cancel
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	lines := make(chan string, 1024)
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
		t.Fatalf("%s: the stream never opened", p.name)
	}
	return lines
}

type lockedWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

func (l *lockedWriter) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.String()
}

// TestRoles_SeparateProcesses runs the split #613's workstream C2 describes, as
// separate OS processes of the real binary that share nothing but the
// backends: A and E serve the API (roles=api), B and C write the queue to
// ClickHouse (roles=ingest), D sweeps (roles=sweeper). The queue is an
// operator-provisioned NATS (the shipped Helm values and nack manifests,
// the coordination bucket included), the cache Redis, dedupe a
// dynamodb-local table, over the suite's ClickHouse. It checks that ingest
// through the API lands in ClickHouse exactly once, that B's and C's
// inserts invalidate what A cached, that an SSE client on either API
// process sees every event, that a duplicate sent to both API processes is
// accepted once, and that killing D moves the sweeper lease to its
// replacement within about the lease duration.
func TestRoles_SeparateProcesses(t *testing.T) {
	t.Parallel()
	e := env(t)
	ctx := context.Background()
	natsURL := startNATS(t)
	op, err := natstest.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(op.Close)
	require.NoError(t, op.ApplyShipped(ctx))
	_, redisAddr := startRedis(t)

	table := createTable(t, "event_id String, page String", "ORDER BY event_id")
	files, err := tenantSettings(e.ch, testCHDatabase)
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(files[settings.FileConfig], &doc))
	doc["dedupe"] = json.RawMessage(`{"enabled": true, "id_field": "event_id", "require_id": true, "retention": "0", "tables": {}}`)
	files[settings.FileConfig], err = json.Marshal(doc)
	require.NoError(t, err)
	settingsDir := filepath.Join(t.TempDir(), "settings")
	require.NoError(t, writeSettingsFiles(settingsDir, files))

	pw := filepath.Join(t.TempDir(), "nats-password")
	require.NoError(t, os.WriteFile(pw, []byte(natstest.Password(natstest.WaveHouseUser)+"\n"), 0o600))
	none := filepath.Join(t.TempDir(), "none")
	shared := map[string]string{
		"WH_SETTINGS_DIR":      settingsDir,
		"WH_CH_PASSWORD":       testCHPassword,
		"WH_AUTH_OPERATOR_KEY": natsOperatorKey,

		"WH_MQ_BACKEND":            "nats",
		"WH_MQ_NATS_URLS":          natsURL,
		"WH_MQ_NATS_USER":          natstest.WaveHouseUser,
		"WH_MQ_NATS_PASSWORD_FILE": pw,
		"WH_MQ_NATS_PARTITIONS":    "4",
		"WH_COORD_BACKEND":         "nats",

		"WH_CACHE_BACKEND":                "redis",
		"WH_CACHE_REDIS_ADDRS":            redisAddr,
		"WH_CACHE_REDIS_KEY_PREFIX":       fmt.Sprintf("roles%d", cachePrefixes.Add(1)),
		"WH_CACHE_REDIS_TIMEOUT":          "2s",
		"WH_DEDUPE_BACKEND":               "dynamodb",
		"WH_DEDUPE_DYNAMODB_TABLE":        newDynamoTable(),
		"WH_DEDUPE_DYNAMODB_REGION":       "us-east-1",
		"WH_DEDUPE_DYNAMODB_ENDPOINT":     e.dynamoEndpoint,
		"WH_DEDUPE_DYNAMODB_TIMEOUT":      "5s",
		"WH_DEDUPE_DYNAMODB_CREATE_TABLE": "true",
		// The SDK's default chain, as in production; never the developer's files.
		"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local",
		"AWS_CONFIG_FILE": none, "AWS_SHARED_CREDENTIALS_FILE": none, "AWS_EC2_METADATA_DISABLED": "true",
	}
	with := func(roles string) map[string]string {
		vars := map[string]string{"WH_ROLES": roles}
		for k, v := range shared {
			vars[k] = v
		}
		return vars
	}

	// A first: it creates the dedupe table, which E then finds.
	a := startProcess(t, "api-a", with("api"))
	a.awaitLive(t)
	procs := []*whProcess{
		startProcess(t, "api-e", with("api")),
		startProcess(t, "ingest-b", with("ingest")),
		startProcess(t, "ingest-c", with("ingest")),
		startProcess(t, "sweeper-d", with("sweeper")),
	}
	for _, p := range procs {
		p.awaitLive(t)
	}
	apiE := procs[0]

	// Every API process sees every event: the hub is per process (#613).
	liveA, liveE := a.sse(t, table), apiE.sse(t, table)

	// A fills the cache, then a batch through A is written by B or C, the
	// only processes with a worker, and A's next answer is a miss carrying it:
	// an invalidation, not an expiry, since the fill is younger than the
	// shortest TTL.
	status, xc, body := a.query(t, table)
	require.Equal(t, http.StatusOK, status, body)
	require.Equal(t, "MISS", xc)
	require.Eventually(t, func() bool { _, x, _ := a.query(t, table); return x == "HIT" }, 10*time.Second, 100*time.Millisecond, "A's fill never reached the shared cache")
	filled := time.Now()

	const n = 50
	var batch strings.Builder
	for i := range n {
		fmt.Fprintf(&batch, `{"event_id":"e%03d","page":"p%d"}`+"\n", i, i%7)
	}
	status, resp := a.ingest(t, table, "application/x-ndjson", batch.String())
	require.Equal(t, http.StatusOK, status, resp)

	count := func() (uint64, uint64) {
		var c, u uint64
		require.NoError(t, e.chConn.QueryRow(ctx, "SELECT count(), uniqExact(event_id) FROM "+table).Scan(&c, &u))
		return c, u
	}
	require.Eventually(t, func() bool { c, _ := count(); return c >= n }, 60*time.Second, 200*time.Millisecond, "the batch never reached ClickHouse")
	var invalidated bool
	require.Eventually(t, func() bool {
		st, x, b := a.query(t, table)
		invalidated = st == http.StatusOK && x == "MISS" && strings.Contains(b, "e049")
		return invalidated || x == "HIT" && time.Since(filled) > minCacheTTL
	}, 30*time.Second, 100*time.Millisecond)
	require.True(t, invalidated, "A served its pre-insert fill: B's and C's inserts did not reach A's cache")
	assert.Less(t, time.Since(filled), 3*minCacheTTL)

	for i := range n {
		id := fmt.Sprintf("e%03d", i)
		awaitEvent(t, liveA, id)
		awaitEvent(t, liveE, id)
	}

	// Exactly once, and it stays so: nothing is redelivered and inserted again.
	c, u := count()
	assert.Equal(t, uint64(n), c)
	assert.Equal(t, uint64(n), u)
	assert.Never(t, func() bool { c, _ := count(); return c != n }, 5*time.Second, 500*time.Millisecond, "a row was inserted twice")

	// One id sent to both API processes at once is accepted by one of them.
	type answer struct {
		status int
		body   string
	}
	answers := make(chan answer, 2)
	for _, p := range []*whProcess{a, apiE} {
		go func() {
			st, _, b, err := p.request(http.MethodPost, "/v1/ingest?table="+url.QueryEscape(table), map[string]string{"Content-Type": "application/json"}, `{"event_id":"dup","page":"x"}`)
			assert.NoError(t, err)
			answers <- answer{st, strings.TrimSpace(b)}
		}()
	}
	var ok, dup, inflight int
	for range 2 {
		ans := <-answers
		switch {
		case ans.status == http.StatusOK && strings.Contains(ans.body, `"ok":true`):
			ok++
		case ans.status == http.StatusOK && strings.Contains(ans.body, `"duplicate":true`):
			dup++
		case ans.status == http.StatusServiceUnavailable:
			inflight++ // the other's claim was still pending
		default:
			t.Errorf("unexpected answer %d %s", ans.status, ans.body)
		}
	}
	assert.Equal(t, 1, ok, "exactly one process accepts the id")
	assert.Equal(t, 1, dup+inflight)
	status, resp = apiE.ingest(t, table, "application/json", `{"event_id":"dup","page":"y"}`)
	require.Equal(t, http.StatusOK, status, resp)
	assert.JSONEq(t, `{"duplicate":true}`, resp, "the id is committed for every process")
	status, resp = a.ingest(t, table, "application/json", `{"event_id":"dup","page":"z"}`)
	require.Equal(t, http.StatusOK, status, resp)
	assert.JSONEq(t, `{"duplicate":true}`, resp)
	require.Eventually(t, func() bool { c, _ := count(); return c == n+1 }, 60*time.Second, 200*time.Millisecond)
	assert.Never(t, func() bool { c, _ := count(); return c != n+1 }, 3*time.Second, 500*time.Millisecond, "the duplicate was inserted")

	// The sweeper lease: D holds it; killed without resigning, its
	// replacement takes it once D's term has gone unrenewed for the lease
	// duration.
	holder := sweeperLeaseHolder(t, op)
	require.Eventually(t, func() bool { return holder() == "sweeper-d" }, 30*time.Second, 200*time.Millisecond, "D never took the sweeper lease")
	d := procs[3]
	d.kill(t)
	killed := time.Now()
	d2 := startProcess(t, "sweeper-d2", with("sweeper"))
	d2.awaitLive(t)
	require.Eventually(t, func() bool { return holder() == "sweeper-d2" }, sweeperLeaseDuration+30*time.Second, 200*time.Millisecond, "the replacement never took the lease")
	took := time.Since(killed)
	t.Logf("sweeper lease moved %s after D was killed (lease duration %s)", took.Round(100*time.Millisecond), sweeperLeaseDuration)
	assert.GreaterOrEqual(t, took, sweeperLeaseDuration-time.Second, "the lease moved before D's term could have lapsed")
	assert.Less(t, took, sweeperLeaseDuration+15*time.Second)
}

// TestRoles_BootRefusesWhatTheBackendsCannotServe drives the boot rules 1–5
// through the real binary and its environment: each
// combination exits non-zero before dialing anything, naming the fix.
func TestRoles_BootRefusesWhatTheBackendsCannotServe(t *testing.T) {
	t.Parallel()
	nats := map[string]string{"WH_MQ_BACKEND": "nats", "WH_MQ_NATS_URLS": "nats://127.0.0.1:1", "WH_COORD_BACKEND": "nats"}
	cases := []struct {
		name string
		vars map[string]string
		want string
	}{
		{"1: unknown backend", map[string]string{"WH_CACHE_BACKEND": "memcached"}, "is not a backend this build has; valid: local, redis"},
		{"1: unknown role", map[string]string{"WH_ROLES": "api,reader"}, "is not a role; valid: api,ingest,sweeper"},
		{"1: repeated role", map[string]string{"WH_ROLES": "api,api"}, "api\\\" twice"},
		{"2: a split over the embedded queue", map[string]string{"WH_ROLES": "api,ingest"}, "the embedded MQ lives inside this process"},
		{"3: nats leases without the nats queue", map[string]string{"WH_COORD_BACKEND": "nats"}, "the NATS leases ride mq.nats"},
		{"4: a sweeper on the nats queue with local leases", merge(nats, map[string]string{"WH_COORD_BACKEND": "local", "WH_ROLES": "sweeper"}), "a shared queue needs a shared lease"},
		{"5: api without ingest over a local cache", merge(nats, map[string]string{"WH_ROLES": "api"}), "would never reach the API's cache"},
		{"5: ingest without api over a local cache", merge(nats, map[string]string{"WH_ROLES": "ingest,sweeper"}), "would never reach the API's cache"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := startProcess(t, "refused", merge(tc.vars, map[string]string{"WH_SETTINGS_DIR": t.TempDir()}))
			log := p.awaitExit(t, 30*time.Second)
			assert.NotZero(t, p.cmd.ProcessState.ExitCode())
			assert.Contains(t, log, tc.want)
		})
	}
}

// merge returns a copy of a with b's entries over it.
func merge(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// sweeperLeaseDuration is how long a stalled holder keeps the lease
// (internal/mq's default; a candidate waits it out on its own clock).
const sweeperLeaseDuration = 15 * time.Second

// sweeperLeaseHolder reads, as the operator, which instance holds the
// sweeper lease in the shipped coordination bucket ("" when none does).
func sweeperLeaseHolder(t *testing.T, op *natstest.Operator) func() string {
	t.Helper()
	kv, err := op.JetStream().KeyValue(t.Context(), natstest.CoordBucket)
	require.NoError(t, err)
	return func() string {
		e, err := kv.Get(t.Context(), "lease.sweeper")
		if err != nil {
			return ""
		}
		var v struct {
			Holder string `json:"holder"`
		}
		if json.Unmarshal(e.Value(), &v) != nil {
			return ""
		}
		return v.Holder
	}
}
