package cache

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

func TestRedisConfig_Validation(t *testing.T) {
	t.Parallel()
	ok := RedisConfig{Addrs: []string{"redis:6379"}}
	tests := []struct {
		name    string
		mutate  func(c *RedisConfig)
		wantErr string
	}{
		{"no address", func(c *RedisConfig) { c.Addrs = nil }, "at least one address"},
		{"address without a port", func(c *RedisConfig) { c.Addrs = []string{"redis"} }, `address "redis"`},
		{"unknown mode", func(c *RedisConfig) { c.Mode = "ring" }, `mode "ring"`},
		{"cluster with a db", func(c *RedisConfig) { c.Mode, c.DB = RedisCluster, 1 }, "only database 0"},
		{"sentinel without a master", func(c *RedisConfig) { c.Mode = RedisSentinel }, "master set name"},
		{"negative db", func(c *RedisConfig) { c.DB = -1 }, "negative"},
		{"hash tag in the prefix", func(c *RedisConfig) { c.KeyPrefix = "{wh}" }, "brace"},
		{"negative timeout", func(c *RedisConfig) { c.Timeout = -time.Second }, "timeout is negative"},
		{"negative size", func(c *RedisConfig) { c.MaxValueBytes = -1 }, "max value bytes is negative"},
		{"version ttl under EX's resolution", func(c *RedisConfig) { c.VersionTTL = 1500 * time.Millisecond }, "under 2s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := ok
			tt.mutate(&c)
			_, err := NewRedis(c)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRedisConfig_Defaults(t *testing.T) {
	t.Parallel()
	c, err := RedisConfig{Addrs: []string{"redis:6379"}}.withDefaults()
	require.NoError(t, err)
	assert.Equal(t, RedisStandalone, c.Mode)
	assert.Equal(t, DefaultRedisKeyPrefix, c.KeyPrefix)
	assert.Equal(t, DefaultRedisTimeout, c.Timeout)
	assert.Equal(t, DefaultRedisMaxValueBytes, c.MaxValueBytes)
	assert.Equal(t, DefaultRedisVersionTTL, c.VersionTTL)
	assert.Zero(t, c.CompressMinBytes, "0 means never compress, not the default")
	assert.True(t, c.clientOption().ForceSingleClient)
	assert.Equal(t, DefaultRedisDialTimeout, c.clientOption().ConnWriteTimeout)
	slow := c
	slow.Timeout = 5 * time.Second
	assert.Equal(t, slow.Timeout, slow.clientOption().ConnWriteTimeout, "never under the op timeout")

	c.Mode, c.SentinelMaster = RedisSentinel, "mymaster"
	assert.Equal(t, "mymaster", c.clientOption().Sentinel.MasterSet)
	c.Mode = RedisCluster
	assert.False(t, c.clientOption().ForceSingleClient)
}

// closedAddr is an address nothing listens on: dials are refused at once.
func closedAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// An unreachable server does not fail construction: the cache is bypassed
// — a miss that files nothing, a no-op fill, a deferred invalidation — and
// keeps dialing.
func TestRedis_UnreachableIsBypassed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r, err := NewRedis(RedisConfig{Addrs: []string{closedAddr(t)}, DialTimeout: 100 * time.Millisecond, PendingMax: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	assert.True(t, r.bypassed())

	deps := []Namespace{{Tenant: "acme", Table: "events"}}
	e, snap, err := r.Lookup(ctx, "acme", "q", deps)
	require.NoError(t, err)
	assert.Nil(t, e.Value)
	assert.Empty(t, snap.key, "nothing to file under")

	_, _, err = r.Lookup(ctx, "acme", "q", []Namespace{{Tenant: "globex", Table: "events"}})
	require.ErrorIs(t, err, ErrForeignDependency)

	require.NoError(t, r.Set(ctx, Snapshot{key: "k"}, []byte("rows"), time.Minute))

	n, err := r.Invalidate(ctx, []Namespace{{Tenant: "acme", Table: "events", Scope: "org_1"}})
	require.ErrorIs(t, err, errBypassed)
	assert.Equal(t, uint64(1), n)
	assert.Equal(t, 2, r.pending.len())
	require.ErrorIs(t, r.InvalidateTenant(ctx, "globex"), errBypassed)
	owed := r.pending.snapshot()
	assert.Len(t, owed, 2, "past PendingMax: one bump per tenant")
	assert.Contains(t, owed, "wh:{acme}:T")
	assert.Contains(t, owed, "wh:{globex}:T")

	require.NoError(t, r.Close())
	require.NoError(t, r.Close(), "idempotent")
}

func TestRedis_SetDeclinesWithoutTouchingTheServer(t *testing.T) {
	t.Parallel()
	r, err := NewRedis(RedisConfig{Addrs: []string{closedAddr(t)}, DialTimeout: 100 * time.Millisecond, MaxValueBytes: 64})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	ctx := context.Background()
	snap := Snapshot{key: "k", tokens: newToken()}
	for _, tt := range []struct {
		name  string
		snap  Snapshot
		value []byte
		ttl   time.Duration
	}{
		{"zero snapshot", Snapshot{}, []byte("rows"), time.Minute},
		{"zero ttl", snap, []byte("rows"), 0},
		{"over the stored limit", snap, randomBytes(100), time.Minute},
		{"over the decoded limit", snap, make([]byte, 64*decodedFactor+1), time.Minute},
	} {
		require.NoError(t, r.Set(ctx, tt.snap, tt.value, tt.ttl), tt.name)
	}
}

func TestRedis_Record(t *testing.T) {
	t.Parallel()
	r := &RedisCache{breaker: newBreaker(1, time.Hour, time.Now)}
	live, cancelled := context.Background(), cancelledCtx()

	r.record(live, rueidis.Nil)
	r.record(live, &rueidis.RedisError{})
	r.record(cancelled, context.Canceled)
	r.record(live, fmt.Errorf("%w: token is 3 bytes", errMalformedReply))
	assert.False(t, r.breaker.isOpen(), "a reply, even an error reply, or the caller giving up says nothing against the server")

	r.record(live, context.DeadlineExceeded)
	assert.True(t, r.breaker.isOpen())
}

func TestRefusesWork(t *testing.T) {
	t.Parallel()
	for _, msg := range []string{
		"READONLY You can't write against a read only replica.",
		"OOM command not allowed when used memory > 'maxmemory'.",
		"MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
		"NOREPLICAS Not enough good replicas to write.",
		"MISCONF Errors writing to the AOF file: No space left on device",
		"LOADING Redis is loading the dataset in memory",
		"BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
		"CLUSTERDOWN The cluster is down",
	} {
		assert.True(t, refusesWork(msg), msg)
	}
	for _, msg := range []string{
		"WRONGTYPE Operation against a key holding the wrong kind of value",
		"NOPERM this user has no permissions to access one of the keys used as arguments",
		"TRYAGAIN Multiple keys request during rehashing of slot",
		"BUSYKEY Target key name already exists.",
		"ERR unknown command",
		"",
	} {
		assert.False(t, refusesWork(msg), msg)
	}
}

// The first bump owed wakes the drain at once; later ones ride the retry
// already under way rather than resetting its backoff.
func TestRedis_FirstDeferralWakesTheDrain(t *testing.T) {
	t.Parallel()
	r := &RedisCache{breaker: newBreaker(1, time.Hour, time.Now), pending: newPendingBumps("wh", 10), wake: make(chan struct{}, 1)}
	var err error
	r.metrics, err = newMetrics("redis", r.bypassed, r.pending.len)
	require.NoError(t, err)
	t.Cleanup(r.metrics.close)

	r.deferBumps(map[string]tenant.ID{"wh:{acme}:B:events": "acme"})
	assert.Len(t, r.wake, 1)
	<-r.wake
	r.deferBumps(map[string]tenant.ID{"wh:{acme}:B:orders": "acme"})
	assert.Empty(t, r.wake)
}

func cancelledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestSetFailureReason(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "timeout", setFailureReason(context.DeadlineExceeded))
	assert.Equal(t, "other", setFailureReason(errors.New("broken pipe")))
}

func TestIsAuthError(t *testing.T) {
	t.Parallel()
	assert.True(t, isAuthError(errors.New("WRONGPASS invalid username-password pair")))
	assert.True(t, isAuthError(errors.New("NOAUTH authentication required")))
	assert.False(t, isAuthError(errors.New("dial tcp: connection refused")))
}

func TestReadTokens_NotAnArrayIsMalformed(t *testing.T) {
	t.Parallel()
	_, _, _, err := readTokens(rueidis.RedisResult{})
	require.ErrorIs(t, err, errMalformedReply)
}

func TestRedisMetrics(t *testing.T) {
	// No t.Parallel(): swaps the global meter provider.
	saved := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
		otel.SetMeterProvider(saved)
	})

	r, err := NewRedis(RedisConfig{Addrs: []string{closedAddr(t)}, DialTimeout: 100 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	ctx := context.Background()
	_, _, _ = r.Lookup(ctx, "acme", "q", nil)
	_, _ = r.Invalidate(ctx, []Namespace{{Tenant: "acme", Table: "events"}})
	r.metrics.op("set", time.Now())
	r.metrics.stored(10)
	r.metrics.tooLarge()
	r.metrics.setFailed("oom")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	got := map[string]metricdata.Aggregation{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = m.Data
		}
	}
	sumOf := func(name, key, value string) int64 {
		t.Helper()
		var n int64
		switch d := got[name].(type) {
		case metricdata.Sum[int64]:
			for _, dp := range d.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(key)); key == "" || ok && v.AsString() == value {
					n += dp.Value
				}
			}
		case metricdata.Gauge[int64]:
			for _, dp := range d.DataPoints {
				n += dp.Value
			}
		default:
			t.Fatalf("%s: %T", name, got[name])
		}
		return n
	}
	assert.Equal(t, int64(1), sumOf("wavehouse_cache_lookups_total", "result", resultBypass))
	assert.Equal(t, int64(1), sumOf("wavehouse_cache_invalidations_total", "result", "deferred"))
	assert.Equal(t, int64(1), sumOf("wavehouse_cache_invalidations_pending", "", ""))
	assert.Equal(t, int64(1), sumOf("wavehouse_cache_breaker_open", "", ""))
	assert.Equal(t, int64(1), sumOf("wavehouse_cache_oversize_total", "", ""))
	assert.Equal(t, int64(1), sumOf("wavehouse_cache_set_failures_total", "reason", "oom"))
	assert.Contains(t, got, "wavehouse_cache_op_duration_seconds")
	assert.Contains(t, got, "wavehouse_cache_value_bytes")
}
