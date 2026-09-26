//go:build integration

package cache_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/Wave-RF/WaveHouse/internal/testutil/cachetest"
)

// Pinned: the servers the shared cache is documented to run on.
const (
	redisImage     = "redis:8.10.2-alpine"
	valkeyImage    = "valkey/valkey:8.1.10-alpine"
	dragonflyImage = "docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.0"
)

// maxValue is the stored-size limit the tests run with; small, so the
// oversize case stays cheap.
const maxValue = 64 << 10

type server struct {
	ctr  testcontainers.Container
	addr string
	mode string
}

// noPersistence keeps the image's VOLUME /data off an anonymous volume.
func noPersistence(hc *container.HostConfig) {
	hc.Tmpfs = map[string]string{"/data": ""}
}

func startContainer(t *testing.T, req testcontainers.ContainerRequest, port string) (testcontainers.Container, string) {
	t.Helper()
	ctx := context.Background()
	if req.HostConfigModifier == nil {
		req.HostConfigModifier = noPersistence
	}
	req.WaitingFor = wait.ForListeningPort(port).WithStartupTimeout(90 * time.Second)
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	mapped, err := ctr.MappedPort(ctx, port)
	require.NoError(t, err)
	return ctr, net.JoinHostPort(host, mapped.Port())
}

func startStandalone(t *testing.T, image string, cmd ...string) *server {
	t.Helper()
	ctr, addr := startContainer(t, testcontainers.ContainerRequest{
		Image: image, Cmd: cmd, ExposedPorts: []string{"6379/tcp"},
	}, "6379/tcp")
	s := &server{ctr: ctr, addr: addr, mode: cache.RedisStandalone}
	waitReady(t, s, nil)
	return s
}

func startRedis(t *testing.T) *server {
	return startStandalone(t, redisImage, "redis-server", "--save", "", "--appendonly", "no")
}

func startValkey(t *testing.T) *server {
	return startStandalone(t, valkeyImage, "valkey-server", "--save", "", "--appendonly", "no")
}

func startDragonfly(t *testing.T) *server {
	return startStandalone(t, dragonflyImage, "--proactor_threads=2", "--maxmemory=512mb")
}

// startCluster runs a one-node Redis Cluster owning every slot: enough for
// the server to enforce cluster semantics — CROSSSLOT on a multi-key
// command, MOVED routing through the client — which is what the key schema
// must survive. The node announces 127.0.0.1 on a host port bound to the
// same number, so the address the client learns from CLUSTER SLOTS is
// dialable from the test.
func startCluster(t *testing.T) *server {
	t.Helper()
	port := freePort(t)
	p := network.MustParsePort(port + "/tcp")
	ctr, _ := startContainer(t, testcontainers.ContainerRequest{
		Image: redisImage,
		Cmd: []string{
			"redis-server", "--port", port, "--cluster-enabled", "yes", "--cluster-port", "16379",
			"--cluster-announce-ip", "127.0.0.1", "--save", "", "--appendonly", "no",
		},
		ExposedPorts: []string{port + "/tcp"},
		HostConfigModifier: func(hc *container.HostConfig) {
			noPersistence(hc)
			hc.PortBindings = network.PortMap{p: {{HostPort: port}}}
		},
	}, port+"/tcp")
	code, out, err := ctr.Exec(context.Background(), []string{"redis-cli", "-p", port, "cluster", "addslotsrange", "0", "16383"})
	require.NoError(t, err)
	require.Zero(t, code, "%v", out)
	s := &server{ctr: ctr, addr: net.JoinHostPort("127.0.0.1", port), mode: cache.RedisCluster}
	waitReady(t, s, func(c rueidis.Client) error {
		info, err := c.Do(context.Background(), c.B().ClusterInfo().Build()).ToString()
		if err == nil && !strings.Contains(info, "cluster_state:ok") {
			err = fmt.Errorf("cluster not ready: %q", info)
		}
		return err
	})
	return s
}

func freePort(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	for {
		ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
		require.NoError(t, ln.Close())
		if port != "16379" {
			return port
		}
	}
}

// raw opens a plain client on s, for the test to reach under the cache.
func raw(t *testing.T, s *server) rueidis.Client {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{s.addr}, DisableCache: true, ForceSingleClient: s.mode == cache.RedisStandalone,
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// waitReady blocks until s answers — a listening port is not yet a server
// that takes commands — and, given ready, until ready passes too.
func waitReady(t *testing.T, s *server, ready func(rueidis.Client) error) {
	t.Helper()
	var last error
	require.Eventually(t, func() bool {
		c, err := rueidis.NewClient(rueidis.ClientOption{
			InitAddress: []string{s.addr}, DisableCache: true, ForceSingleClient: true,
		})
		if last = err; err != nil {
			return false
		}
		defer c.Close()
		if last = c.Do(context.Background(), c.B().Ping().Build()).Error(); last != nil {
			return false
		}
		if ready != nil {
			last = ready(c)
		}
		return last == nil
	}, 60*time.Second, 100*time.Millisecond, "server %s not ready: %v", s.addr, last)
}

var prefixes atomic.Uint64

func uniquePrefix() string { return fmt.Sprintf("t%d", prefixes.Add(1)) }

// open builds a RedisCache on s. Its timeout is generous: the suite runs
// in parallel under -race, and a timed-out lookup is a miss the conformance
// cases would read as a wrong answer.
func open(t *testing.T, s *server, prefix string, tune ...func(*cache.RedisConfig)) *cache.RedisCache {
	t.Helper()
	cfg := cache.RedisConfig{
		Addrs: []string{s.addr}, Mode: s.mode, KeyPrefix: prefix,
		Timeout: 5 * time.Second, MaxValueBytes: maxValue, CompressMinBytes: cache.DefaultRedisCompressMinBytes,
	}
	for _, f := range tune {
		f(&cfg)
	}
	c, err := cache.NewRedis(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestRedis_Conformance(t *testing.T) {
	t.Parallel()
	servers := []struct {
		name  string
		start func(*testing.T) *server
	}{
		{"redis", startRedis},
		{"valkey", startValkey},
		{"dragonfly", startDragonfly},
		{"redis cluster", startCluster},
	}
	for _, sv := range servers {
		t.Run(sv.name, func(t *testing.T) {
			t.Parallel()
			s := sv.start(t)
			cachetest.Run(t,
				func(t *testing.T) cache.Cache { return open(t, s, uniquePrefix()) },
				cachetest.Options{
					// The raw-size bound: a value past it is refused before
					// compression, and the suite's oversize value is zeros,
					// which would compress under the stored-size one.
					MaxValueBytes: maxValue * cache.DecodedFactor,
					NewPair: func(t *testing.T) (cache.Cache, cache.Cache) {
						p := uniquePrefix()
						return open(t, s, p), open(t, s, p)
					},
				})
			t.Run("cross-tenant invalidation spans slots", func(t *testing.T) {
				t.Parallel()
				testCrossTenantInvalidate(t, open(t, s, uniquePrefix()))
			})
			t.Run("compressed values round-trip", func(t *testing.T) {
				t.Parallel()
				testCompression(t, s)
			})
		})
	}
}

// The ingest worker's shared-tables fan-out bumps one table under several
// tenants in one call: tokens in as many slots, one pipeline.
func testCrossTenantInvalidate(t *testing.T, c *cache.RedisCache) {
	ctx := context.Background()
	var deps [][]cache.Namespace
	for i := range 20 {
		id := tenantID(i)
		d := []cache.Namespace{{Tenant: id, Table: "events"}}
		deps = append(deps, d)
		_, snap, err := c.Lookup(ctx, id, "q", d)
		require.NoError(t, err)
		require.NoError(t, c.Set(ctx, snap, []byte("rows"), time.Minute))
	}
	var all []cache.Namespace
	for _, d := range deps {
		all = append(all, d...)
	}
	n, err := c.Invalidate(ctx, all)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(all)), n)
	for i, d := range deps {
		e, _, err := c.Lookup(ctx, tenantID(i), "q", d)
		require.NoError(t, err)
		assert.Nil(t, e.Value, tenantID(i))
	}
}

func tenantID(i int) tenant.ID { return tenant.ID(fmt.Sprintf("tenant-%d", i)) }

func testCompression(t *testing.T, s *server) {
	ctx := context.Background()
	prefix := uniquePrefix()
	c := open(t, s, prefix)
	rows := bytes.Repeat([]byte(`{"user_id":"u-1","event":"click","value":42.5},`), 10_000)
	require.Greater(t, len(rows), maxValue, "stored only because it compresses under the limit")
	deps := []cache.Namespace{{Tenant: "acme", Table: "events"}}
	_, snap, err := c.Lookup(ctx, "acme", "big", deps)
	require.NoError(t, err)
	require.NoError(t, c.Set(ctx, snap, rows, time.Minute))
	e, _, err := c.Lookup(ctx, "acme", "big", deps)
	require.NoError(t, err)
	assert.Equal(t, rows, e.Value)

	r := raw(t, s)
	keys, err := r.Do(ctx, r.B().Keys().Pattern(prefix+":q:*").Build()).AsStrSlice()
	require.NoError(t, err)
	require.Len(t, keys, 1)
	stored, err := r.Do(ctx, r.B().Strlen().Key(keys[0]).Build()).AsInt64()
	require.NoError(t, err)
	assert.Less(t, stored, int64(len(rows)/10))
}

// A token that is lost — evicted, expired, flushed, a restart without
// persistence — is recreated fresh, so a value stored under its predecessor
// can only miss. A counter recreated at its initial value would serve the
// value filed at that value again: this test fails for one.
func TestRedis_LostTokensAreMisses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := startRedis(t)
	r := raw(t, s)
	prefix := uniquePrefix()
	c := open(t, s, prefix)
	deps := []cache.Namespace{{Tenant: "acme", Table: "events", Scope: "org_1"}}
	fill := func(t *testing.T, sha string, deps []cache.Namespace) {
		t.Helper()
		_, snap, err := c.Lookup(ctx, "acme", sha, deps)
		require.NoError(t, err)
		require.NoError(t, c.Set(ctx, snap, []byte("rows"), time.Minute))
		e, _, err := c.Lookup(ctx, "acme", sha, deps)
		require.NoError(t, err)
		require.Equal(t, "rows", string(e.Value))
	}
	// Twice: the first lookup recreates the lost tokens, and it is the
	// second, reading them back, that a recreated counter would fool.
	requireMiss := func(t *testing.T, sha string, deps []cache.Namespace) {
		t.Helper()
		for range 2 {
			e, _, err := c.Lookup(ctx, "acme", sha, deps)
			require.NoError(t, err)
			require.Nil(t, e.Value)
		}
	}
	keys := func(t *testing.T, pattern string) []string {
		t.Helper()
		keys, err := r.Do(ctx, r.B().Keys().Pattern(pattern).Build()).AsStrSlice()
		require.NoError(t, err)
		return keys
	}

	for _, lost := range []string{"T", "B:events", "S:events:org_1", "*"} {
		fill(t, "q", deps)
		fill(t, "pipe", nil)
		tokens := keys(t, prefix+":{acme}:"+lost)
		require.NotEmpty(t, tokens, lost)
		require.NoError(t, r.Do(ctx, r.B().Del().Key(tokens...).Build()).Error())
		require.Len(t, keys(t, prefix+":q:*"), 2, "lost %s: the values survive, only their tokens are gone", lost)
		requireMiss(t, "q", deps)
		if lost == "T" || lost == "*" {
			requireMiss(t, "pipe", nil)
		}
	}

	fill(t, "q", deps)
	require.NoError(t, r.Do(ctx, r.B().Flushall().Build()).Error())
	requireMiss(t, "q", deps)
	fill(t, "q", deps)
}

// A token or value key holding something else — another program under the
// prefix, a different token size mid-upgrade — is a reply, not a failure:
// it is replaced, which can only cause misses, and the breaker stays closed.
func TestRedis_ForeignTokenIsReplaced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := startRedis(t)
	r := raw(t, s)
	prefix := uniquePrefix()
	c := open(t, s, prefix, func(c *cache.RedisConfig) { c.BreakerThreshold = 1 })
	deps := []cache.Namespace{{Tenant: "acme", Table: "events"}}
	_, snap, err := c.Lookup(ctx, "acme", "q", deps)
	require.NoError(t, err)
	require.NoError(t, c.Set(ctx, snap, []byte("rows"), time.Minute))
	valueKeys, err := r.Do(ctx, r.B().Keys().Pattern(prefix+":q:*").Build()).AsStrSlice()
	require.NoError(t, err)
	require.Len(t, valueKeys, 1)

	for _, plant := range []struct {
		name string
		cmd  rueidis.Completed
	}{
		{"a short string under the table token", r.B().Set().Key(prefix + ":{acme}:B:events").Value("abc").Build()},
		{"", r.B().Del().Key(prefix + ":{acme}:T").Build()},
		{"a list under the tenant token", r.B().Rpush().Key(prefix + ":{acme}:T").Element("x").Build()},
		{"", r.B().Del().Key(valueKeys[0]).Build()},
		{"a hash under the value key", r.B().Hset().Key(valueKeys[0]).FieldValue().FieldValue("f", "v").Build()},
	} {
		require.NoError(t, r.Do(ctx, plant.cmd).Error())
		if plant.name == "" { // the first half of a two-step plant
			continue
		}
		e, snap, err := c.Lookup(ctx, "acme", "q", deps)
		require.NoError(t, err, plant.name)
		assert.Nil(t, e.Value, plant.name)
		assert.False(t, cache.Bypassed(c), plant.name)
		require.NoError(t, c.Set(ctx, snap, []byte("new rows"), time.Minute), plant.name)
		e, _, err = c.Lookup(ctx, "acme", "q", deps)
		require.NoError(t, err, plant.name)
		assert.Equal(t, "new rows", string(e.Value), plant.name)
	}
}

func dockerClient(t *testing.T) *testcontainers.DockerClient {
	t.Helper()
	d, err := testcontainers.NewDockerClientWithOpts(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// A server that stops answering costs a request at most about the op
// timeout, then nothing: the breaker opens and the cache is bypassed.
// Invalidations made meanwhile are kept and land once it answers again, and
// a process that boots while it is down starts bypassed and connects later.
func TestRedis_ServerStopsAnswering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := startRedis(t)
	d := dockerClient(t)
	prefix := uniquePrefix()
	const timeout = 100 * time.Millisecond
	a := open(t, s, prefix, func(c *cache.RedisConfig) {
		c.Timeout, c.BreakerThreshold, c.BreakerOpenFor = timeout, 3, 300*time.Millisecond
	})
	deps := []cache.Namespace{{Tenant: "acme", Table: "events"}}
	_, snap, err := a.Lookup(ctx, "acme", "q", deps)
	require.NoError(t, err)
	require.NoError(t, a.Set(ctx, snap, []byte("pre-write rows"), time.Minute))

	_, err = d.ContainerPause(ctx, s.ctr.GetContainerID(), client.ContainerPauseOptions{})
	require.NoError(t, err)
	paused := true
	unpause := func() {
		if paused {
			paused = false
			_, err := d.ContainerUnpause(ctx, s.ctr.GetContainerID(), client.ContainerUnpauseOptions{})
			require.NoError(t, err)
		}
	}
	t.Cleanup(unpause)

	for i := range 3 {
		start := time.Now()
		e, snap, err := a.Lookup(ctx, "acme", "q", deps)
		require.Error(t, err, "lookup %d", i)
		assert.Nil(t, e.Value)
		assert.Less(t, time.Since(start), 10*timeout, "a lookup costs at most about the timeout")
		require.NoError(t, a.Set(ctx, snap, []byte("rows"), time.Minute), "the failed lookup's snapshot files nothing")
	}
	require.True(t, cache.Bypassed(a), "three timeouts open the breaker")
	start := time.Now()
	e, _, err := a.Lookup(ctx, "acme", "q", deps)
	require.NoError(t, err, "bypassed is a miss, not a failure")
	assert.Nil(t, e.Value)
	assert.Less(t, time.Since(start), timeout/2, "bypassed costs no round trip")

	_, err = a.Invalidate(ctx, deps)
	require.Error(t, err)
	assert.Equal(t, 1, cache.Pending(a))

	bootStart := time.Now()
	late := open(t, s, prefix, func(c *cache.RedisConfig) { c.DialTimeout = 200 * time.Millisecond })
	assert.Less(t, time.Since(bootStart), 5*time.Second, "an unanswering server does not hold boot")
	assert.True(t, cache.Bypassed(late))

	unpause()
	require.Eventually(t, func() bool { return cache.Pending(a) == 0 && !cache.Bypassed(a) }, 15*time.Second, 50*time.Millisecond,
		"the deferred bump lands once the server answers")
	require.Eventually(t, func() bool { return !cache.Bypassed(late) }, 15*time.Second, 50*time.Millisecond,
		"the late process connects")

	b := open(t, s, prefix)
	e, _, err = b.Lookup(ctx, "acme", "q", deps)
	require.NoError(t, err)
	assert.Nil(t, e.Value, "the fill from before the deferred bump is orphaned for every process")
}

// Close makes its last attempt at the pending bumps past the breaker: an
// open one is why they are pending, and the server may be back by now.
func TestRedis_CloseDeliversPastAnOpenBreaker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := startRedis(t)
	d := dockerClient(t)
	prefix := uniquePrefix()
	a := open(t, s, prefix, func(c *cache.RedisConfig) {
		c.Timeout, c.BreakerThreshold, c.BreakerOpenFor = 100*time.Millisecond, 1, time.Hour
	})
	deps := []cache.Namespace{{Tenant: "acme", Table: "events"}}
	_, snap, err := a.Lookup(ctx, "acme", "q", deps)
	require.NoError(t, err)
	require.NoError(t, a.Set(ctx, snap, []byte("pre-write rows"), time.Minute))

	_, err = d.ContainerPause(ctx, s.ctr.GetContainerID(), client.ContainerPauseOptions{})
	require.NoError(t, err)
	_, _, err = a.Lookup(ctx, "acme", "q", deps)
	require.Error(t, err)
	require.True(t, cache.Bypassed(a))
	_, err = a.Invalidate(ctx, deps)
	require.Error(t, err)
	_, err = d.ContainerUnpause(ctx, s.ctr.GetContainerID(), client.ContainerUnpauseOptions{})
	require.NoError(t, err)

	require.True(t, cache.Bypassed(a), "the breaker stays open for its hour")
	require.Equal(t, 1, cache.Pending(a))
	require.NoError(t, a.Close())
	assert.Zero(t, cache.Pending(a))

	e, _, err := open(t, s, prefix).Lookup(ctx, "acme", "q", deps)
	require.NoError(t, err)
	assert.Nil(t, e.Value, "the bump Close delivered orphans the fill")
}

// A cluster client reads the topology after the handshake; a node that
// answers the handshake and then goes quiet held that read, and so boot and
// Close, for rueidis's 10 s default. It is bounded like a dial now. Any
// server serves: what matters is that the read goes unanswered.
func TestRedis_ClusterTopologyReadIsBounded(t *testing.T) {
	t.Parallel()
	s := startRedis(t)
	opt, err := cache.ClientOption(cache.RedisConfig{Addrs: []string{s.addr}, Mode: cache.RedisCluster, DialTimeout: 300 * time.Millisecond})
	require.NoError(t, err)
	opt.DialCtxFn = func(ctx context.Context, addr string, d *net.Dialer, _ *tls.Config) (net.Conn, error) {
		c, err := d.DialContext(ctx, "tcp", addr)
		return unanswered{c}, err
	}
	start := time.Now()
	c, err := rueidis.NewClient(opt)
	if err == nil {
		c.Close()
	}
	require.Error(t, err, "nothing answered the topology read")
	assert.Less(t, time.Since(start), 3*time.Second)
}

// unanswered drops CLUSTER commands unsent, as a node gone quiet would
// leave them unanswered.
type unanswered struct{ net.Conn }

func (c unanswered) Write(b []byte) (int, error) {
	if bytes.Contains(b, []byte("CLUSTER")) {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

// command runs cmd on s through c, for the test to reconfigure the server.
func command(t *testing.T, c rueidis.Client, cmd ...string) {
	t.Helper()
	require.NoError(t, c.Do(context.Background(), c.B().Arbitrary(cmd[0]).Args(cmd[1:]...).Build()).Error(), "%q", cmd)
}

// A bump this process owes holds the lookups it would orphan — a bypass
// that files nothing — until it lands, with the breaker closed and every
// other lookup served. The ACL lets the process read the tokens but not
// replace them, so the bump stays owed until the test grants the write.
func TestRedis_OwedBumpHoldsItsLookups(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := startRedis(t)
	r := raw(t, s)
	prefix := uniquePrefix()
	command(t, r, "ACL", "SETUSER", "limited", "on", ">pw", "+@all", "%R~*", "%W~"+prefix+":q:*")

	events := []cache.Namespace{{Tenant: "acme", Table: "events"}}
	orders := []cache.Namespace{{Tenant: "acme", Table: "orders"}}
	seed := open(t, s, prefix)
	for _, deps := range [][]cache.Namespace{events, orders, nil} {
		_, snap, err := seed.Lookup(ctx, "acme", "q", deps)
		require.NoError(t, err)
		require.NoError(t, seed.Set(ctx, snap, []byte("pre-write rows"), time.Minute))
	}
	a := open(t, s, prefix, func(c *cache.RedisConfig) { c.Username, c.Password, c.BreakerThreshold = "limited", "pw", 1000 })
	lookup := func(deps []cache.Namespace) (string, cache.Snapshot) {
		t.Helper()
		e, snap, err := a.Lookup(ctx, "acme", "q", deps)
		require.NoError(t, err)
		return string(e.Value), snap
	}
	got, _ := lookup(events)
	require.Equal(t, "pre-write rows", got)

	_, err := a.Invalidate(ctx, events)
	require.ErrorContains(t, err, "NOPERM")
	require.Equal(t, 1, cache.Pending(a))
	require.False(t, cache.Bypassed(a), "a refused key is not a refusing server")

	got, snap := lookup(events)
	assert.Empty(t, got, "the owed bump would orphan it")
	assert.True(t, cache.ZeroSnapshot(snap), "and a fill under the token it replaces would be orphaned too")
	for _, deps := range [][]cache.Namespace{orders, nil} {
		got, _ = lookup(deps)
		assert.Equal(t, "pre-write rows", got, "%v: lookups the bump does not orphan are served", deps)
	}

	command(t, r, "ACL", "SETUSER", "limited", "~*")
	deadline := time.Now().Add(10 * time.Second)
	for cache.Pending(a) > 0 {
		require.True(t, time.Now().Before(deadline), "the bump lands once the server takes it")
		got, _ = lookup(events)
		require.NotEqual(t, "pre-write rows", got, "served before the owed bump landed")
		time.Sleep(time.Millisecond)
	}
	got, _ = lookup(events)
	assert.Empty(t, got, "the landed bump orphaned the pre-write rows")
}

// A server that answers but refuses writes — a primary demoted to a replica
// (READONLY), memory full under noeviction (OOM) — takes no bump, so its
// first refusal opens the breaker whatever the threshold, and the bump stays
// owed. The probe writes, so it keeps the breaker open until the server
// takes writes again; then the owed bump lands before anything it would
// orphan is served, and a process that only invalidates, whose drain loop
// is all that probes for it, recovers at the probe's cadence too.
func TestRedis_RefusedWrites(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name            string
		refuse, restore [][]string
		reply           string
	}{
		{
			"demoted to a replica",
			[][]string{{"REPLICAOF", "127.0.0.1", "1"}}, // nothing listens: it stays a replica, serving reads
			[][]string{{"REPLICAOF", "NO", "ONE"}},
			"READONLY",
		},
		{
			"full under noeviction",
			[][]string{{"CONFIG", "SET", "maxmemory-policy", "noeviction"}, {"CONFIG", "SET", "maxmemory", "1"}},
			[][]string{{"CONFIG", "SET", "maxmemory", "0"}},
			"OOM",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := startRedis(t)
			r := raw(t, s)
			prefix := uniquePrefix()
			const openFor = 200 * time.Millisecond
			tune := func(c *cache.RedisConfig) { c.BreakerThreshold, c.BreakerOpenFor = 1000, openFor }
			a := open(t, s, prefix, tune)
			ingest := open(t, s, prefix, tune)
			reader := open(t, s, prefix, tune)
			events := []cache.Namespace{{Tenant: "acme", Table: "events"}}
			orders := []cache.Namespace{{Tenant: "acme", Table: "orders"}}
			for _, deps := range [][]cache.Namespace{events, orders} {
				_, snap, err := a.Lookup(ctx, "acme", "q", deps)
				require.NoError(t, err)
				require.NoError(t, a.Set(ctx, snap, []byte("pre-write rows"), time.Minute))
			}
			lookup := func(c *cache.RedisCache, deps []cache.Namespace) string {
				t.Helper()
				e, _, err := c.Lookup(ctx, "acme", "q", deps)
				require.NoError(t, err)
				return string(e.Value)
			}

			for _, cmd := range tt.refuse {
				command(t, r, cmd...)
			}
			_, err := a.Invalidate(ctx, events)
			require.ErrorContains(t, err, tt.reply)
			assert.Equal(t, 1, cache.Pending(a))
			assert.True(t, cache.Bypassed(a), "the first refusal opens the breaker")
			wide := slices.Clone(orders) // past one batch: the rest go unsent after the refusal, and drain in two
			for i := range 1500 {
				wide = append(wide, cache.Namespace{Tenant: "acme", Table: fmt.Sprintf("t%d", i)})
			}
			_, err = ingest.Invalidate(ctx, wide)
			require.ErrorContains(t, err, tt.reply)
			assert.True(t, cache.Bypassed(ingest))
			assert.Equal(t, len(wide), cache.Pending(ingest))
			_, snap, err := reader.Lookup(ctx, "acme", "another query", orders)
			require.NoError(t, err)
			require.ErrorContains(t, reader.Set(ctx, snap, []byte("rows"), time.Minute), tt.reply)
			assert.True(t, cache.Bypassed(reader), "a refused fill opens the breaker too")

			// Long enough for many probes to be refused, and for a drain
			// backing off unchecked to be seconds from its next attempt.
			time.Sleep(3500 * time.Millisecond)
			assert.True(t, cache.Bypassed(reader), "owing nothing, it is held open by probes that write and are refused")
			assert.True(t, cache.Bypassed(a))
			assert.Empty(t, lookup(a, events))
			assert.Equal(t, 1, cache.Pending(a))
			assert.Equal(t, len(wide), cache.Pending(ingest))

			for _, cmd := range tt.restore {
				command(t, r, cmd...)
			}
			restored := time.Now()
			for cache.Pending(a) > 0 {
				require.Less(t, time.Since(restored), 10*time.Second, "the bump lands once the server takes writes")
				require.NotEqual(t, "pre-write rows", lookup(a, events), "served before the owed bump landed")
				time.Sleep(time.Millisecond)
			}
			require.Eventually(t, func() bool { return cache.Pending(ingest) == 0 }, 10*time.Second, 5*time.Millisecond)
			assert.Less(t, time.Since(restored), openFor+time.Second, "an ingest-only process probes when due, not at the drain's backoff")
			require.Eventually(t, func() bool { return !cache.Bypassed(reader) }, 10*time.Second, 5*time.Millisecond)

			fresh := open(t, s, prefix)
			for _, deps := range [][]cache.Namespace{events, orders} {
				assert.Empty(t, lookup(fresh, deps), "%v: the landed bumps orphaned the pre-write rows", deps)
			}
		})
	}
}
