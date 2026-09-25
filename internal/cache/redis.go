package cache

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// Redis deployment modes for RedisConfig.Mode.
const (
	RedisStandalone = "standalone"
	RedisCluster    = "cluster"
	RedisSentinel   = "sentinel"
)

// Defaults for RedisConfig's zero values.
const (
	DefaultRedisKeyPrefix        = "wh"
	DefaultRedisTimeout          = 100 * time.Millisecond
	DefaultRedisDialTimeout      = time.Second
	DefaultRedisMaxValueBytes    = 1 << 20
	DefaultRedisCompressMinBytes = 1 << 10
	DefaultRedisVersionTTL       = 7 * 24 * time.Hour
	DefaultRedisPendingMax       = 100_000
	defaultBreakerThreshold      = 5
	defaultBreakerOpenFor        = 5 * time.Second
)

const (
	drainMinBackoff  = 100 * time.Millisecond
	drainMaxBackoff  = 10 * time.Second
	drainIdle        = time.Second
	drainBatch       = 1000
	dialMaxBackoff   = 30 * time.Second
	closeDrainBudget = time.Second
)

var errBypassed = errors.New("cache: redis unavailable")

// RedisConfig configures a RedisCache. A zero field takes its Default*
// value, except CompressMinBytes, where 0 means never compress.
type RedisConfig struct {
	Addrs          []string // host:port; several are seeds (cluster) or sentinels
	Mode           string   // RedisStandalone (""), RedisCluster or RedisSentinel
	SentinelMaster string   // the master set name, Mode RedisSentinel
	Username       string
	Password       string
	DB             int         // standalone and sentinel only
	TLS            *tls.Config // nil: plaintext

	KeyPrefix        string        // leads every key; separates deployments sharing a server
	Timeout          time.Duration // per operation
	DialTimeout      time.Duration
	MaxValueBytes    int           // largest value stored, after compression
	CompressMinBytes int           // zstd-compress values at least this large
	VersionTTL       time.Duration // idle lifetime of a version token, jittered ±10%
	PendingMax       int           // undelivered bumps kept before collapsing to tenant bumps

	BreakerThreshold int           // consecutive failures that open the breaker
	BreakerOpenFor   time.Duration // how long it stays open before a probe
}

func (c RedisConfig) withDefaults() (RedisConfig, error) {
	if len(c.Addrs) == 0 {
		return c, errors.New("cache: redis needs at least one address")
	}
	for _, a := range c.Addrs {
		if _, _, err := net.SplitHostPort(a); err != nil {
			return c, fmt.Errorf("cache: redis address %q: %w", a, err)
		}
	}
	switch c.Mode {
	case "":
		c.Mode = RedisStandalone
	case RedisStandalone, RedisCluster, RedisSentinel:
	default:
		return c, fmt.Errorf("cache: redis mode %q: want %s, %s or %s", c.Mode, RedisStandalone, RedisCluster, RedisSentinel)
	}
	if c.Mode == RedisCluster && c.DB != 0 {
		return c, errors.New("cache: redis cluster has only database 0")
	}
	if c.Mode == RedisSentinel && c.SentinelMaster == "" {
		return c, errors.New("cache: redis sentinel mode needs the master set name")
	}
	if c.DB < 0 {
		return c, fmt.Errorf("cache: redis db %d is negative", c.DB)
	}
	if strings.ContainsAny(c.KeyPrefix, "{}") {
		return c, fmt.Errorf("cache: redis key prefix %q may not contain a hash tag brace", c.KeyPrefix)
	}
	for _, v := range []struct {
		name string
		n    int64
	}{
		{"timeout", int64(c.Timeout)},
		{"dial timeout", int64(c.DialTimeout)},
		{"max value bytes", int64(c.MaxValueBytes)},
		{"compress min bytes", int64(c.CompressMinBytes)},
		{"version ttl", int64(c.VersionTTL)},
		{"pending max", int64(c.PendingMax)},
		{"breaker threshold", int64(c.BreakerThreshold)},
		{"breaker open for", int64(c.BreakerOpenFor)},
	} {
		if v.n < 0 {
			return c, fmt.Errorf("cache: redis %s is negative", v.name)
		}
	}
	c.KeyPrefix = cmpOr(c.KeyPrefix, DefaultRedisKeyPrefix)
	c.Timeout = cmpOr(c.Timeout, DefaultRedisTimeout)
	c.DialTimeout = cmpOr(c.DialTimeout, DefaultRedisDialTimeout)
	c.MaxValueBytes = cmpOr(c.MaxValueBytes, DefaultRedisMaxValueBytes)
	c.VersionTTL = cmpOr(c.VersionTTL, DefaultRedisVersionTTL)
	c.PendingMax = cmpOr(c.PendingMax, DefaultRedisPendingMax)
	c.BreakerThreshold = cmpOr(c.BreakerThreshold, defaultBreakerThreshold)
	c.BreakerOpenFor = cmpOr(c.BreakerOpenFor, defaultBreakerOpenFor)
	return c, nil
}

func cmpOr[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}

func (c RedisConfig) clientOption() rueidis.ClientOption {
	opt := rueidis.ClientOption{
		InitAddress:       c.Addrs,
		Username:          c.Username,
		Password:          c.Password,
		SelectDB:          c.DB,
		TLSConfig:         c.TLS,
		Dialer:            net.Dialer{Timeout: c.DialTimeout},
		ClientName:        "wavehouse",
		DisableCache:      true, // no client-side caching until the near-cache (E5)
		ForceSingleClient: c.Mode == RedisStandalone,
	}
	if c.Mode == RedisSentinel {
		opt.Sentinel = rueidis.SentinelOption{MasterSet: c.SentinelMaster, TLSConfig: c.TLS, Dialer: opt.Dialer}
	}
	return opt
}

// RedisCache is a Cache shared by every process pointed at one Redis —
// or Valkey, Dragonfly, ElastiCache, MemoryDB: it uses only GET, SET and
// MGET, no scripts and no client tracking.
//
// Versions are random tokens, one per tenant, per table and per scope,
// under the tenant's hash tag; a bump sets a fresh one. A value carries the
// tokens it was computed under and is a hit only while they are all still
// current, so a lost token (eviction, expiry, a restart) can only cause
// misses. A lookup is one round trip. The server failing or timing out is
// a miss, a skipped fill and a deferred invalidation — never a failed query.
type RedisCache struct {
	cfg        RedisConfig
	opt        rueidis.ClientOption
	maxDecoded int

	client  atomic.Pointer[rueidis.Client] // nil until the first connection
	codec   *codec
	breaker *breaker
	pending *pendingBumps
	metrics *metrics

	ctx       context.Context // cancelled by Close
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	wake      chan struct{}
	closeOnce sync.Once
}

var _ Cache = (*RedisCache)(nil)

// NewRedis builds a RedisCache. A malformed cfg is an error; a server that
// cannot be reached within the dial timeout is not — the cache starts
// bypassed and keeps dialing in the background.
func NewRedis(cfg RedisConfig) (*RedisCache, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	maxDecoded := cfg.MaxValueBytes * decodedFactor
	cd, err := newCodec(cfg.CompressMinBytes, maxDecoded)
	if err != nil {
		return nil, fmt.Errorf("cache: zstd: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &RedisCache{
		cfg:        cfg,
		opt:        cfg.clientOption(),
		maxDecoded: maxDecoded,
		codec:      cd,
		breaker:    newBreaker(cfg.BreakerThreshold, cfg.BreakerOpenFor, time.Now),
		pending:    newPendingBumps(cfg.KeyPrefix, cfg.PendingMax),
		ctx:        ctx,
		cancel:     cancel,
		wake:       make(chan struct{}, 1),
	}
	if r.metrics, err = newMetrics("redis", r.bypassed, r.pending.len); err != nil {
		cancel()
		cd.close()
		return nil, fmt.Errorf("cache: metrics: %w", err)
	}
	r.wg.Add(1)
	go r.drainLoop()
	if err := r.dial(); err != nil {
		r.wg.Add(1)
		go r.dialLoop()
	}
	return r, nil
}

func (r *RedisCache) dial() error {
	c, err := rueidis.NewClient(r.opt)
	if err != nil {
		level := slog.LevelWarn
		if isAuthError(err) {
			level = slog.LevelError
		}
		slog.Log(r.ctx, level, "cache: redis unreachable; bypassing the cache and retrying",
			"addrs", r.cfg.Addrs, "error", err)
		return err
	}
	if r.ctx.Err() != nil {
		c.Close()
		return r.ctx.Err()
	}
	r.client.Store(&c)
	r.nudge()
	return nil
}

func (r *RedisCache) dialLoop() {
	defer r.wg.Done()
	backoff := time.Second
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(backoff):
		}
		if r.dial() == nil {
			slog.InfoContext(r.ctx, "cache: redis connected", "addrs", r.cfg.Addrs)
			return
		}
		backoff = min(backoff*2, dialMaxBackoff)
	}
}

func isAuthError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "WRONGPASS") || strings.Contains(msg, "NOAUTH") || strings.Contains(msg, "NOPERM")
}

// conn returns the client to send to, or nil when the server is to be
// skipped: not connected yet, or the breaker open. The caller that finds an
// open breaker due for a probe starts it.
func (r *RedisCache) conn() rueidis.Client {
	cp := r.client.Load()
	if cp == nil {
		return nil
	}
	ok, probe := r.breaker.allow()
	if probe {
		go r.probe(*cp)
	}
	if !ok {
		return nil
	}
	return *cp
}

func (r *RedisCache) probe(c rueidis.Client) {
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Timeout)
	defer cancel()
	if err := c.Do(ctx, c.B().Ping().Build()).Error(); err != nil {
		r.breaker.failure()
		return
	}
	r.breaker.success()
	slog.InfoContext(ctx, "cache: redis reachable again; cache back in use")
	r.nudge()
}

// bypassed reports whether operations are skipping the server.
func (r *RedisCache) bypassed() bool {
	return r.client.Load() == nil || r.breaker.isOpen()
}

// record feeds an operation's outcome to the breaker. A reply from the
// server, even an error reply, shows it is up; a caller that gave up first
// shows nothing about it.
func (r *RedisCache) record(parent context.Context, err error) {
	if err == nil || rueidis.IsRedisNil(err) {
		r.breaker.success()
		return
	}
	if _, ok := rueidis.IsRedisErr(err); ok {
		r.breaker.success()
		return
	}
	if parent.Err() != nil {
		return
	}
	r.breaker.failure()
}

// Lookup reads the tokens deps fold and the entry for sha in one pipelined
// round trip. A token that does not exist yet is created, never read as a
// value, so its first use is a miss.
func (r *RedisCache) Lookup(ctx context.Context, id tenant.ID, sha string, deps []Namespace) (Entry, Snapshot, error) {
	for _, d := range deps {
		if d.Tenant != id {
			return Entry{}, Snapshot{}, fmt.Errorf("%w: %q under %q", ErrForeignDependency, d.Tenant, id)
		}
	}
	keys := tokenKeys(r.cfg.KeyPrefix, id, deps)
	if len(keys) > maxTokenKeys {
		return Entry{}, Snapshot{}, fmt.Errorf("cache: %d dependencies is more than a value can record", len(deps))
	}
	c := r.conn()
	if c == nil {
		r.metrics.lookup(resultBypass)
		return Entry{}, Snapshot{}, nil
	}
	defer r.metrics.op("lookup", time.Now())
	opCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()

	vkey := valueKey(r.cfg.KeyPrefix, id, sha, deps)
	res := c.DoMulti(opCtx, c.B().Mget().Key(keys...).Build(), c.B().Get().Key(vkey).Build())
	tokens, missing, err := readTokens(res[0])
	if err != nil {
		return r.lookupFailed(ctx, err)
	}
	val, err := res[1].AsBytes()
	if err != nil && !rueidis.IsRedisNil(err) {
		return r.lookupFailed(ctx, err)
	}
	r.record(ctx, nil)

	if len(missing) > 0 {
		if tokens, err = r.createTokens(opCtx, c, keys, missing); err != nil {
			return r.lookupFailed(ctx, err)
		}
		r.metrics.lookup(resultMiss)
		return Entry{}, Snapshot{key: vkey, tokens: tokens}, nil
	}
	snap := Snapshot{key: vkey, tokens: tokens}
	if val == nil {
		r.metrics.lookup(resultMiss)
		return Entry{}, snap, nil
	}
	stored, expiresAt, payload, err := r.codec.decode(val)
	if err != nil {
		slog.DebugContext(ctx, "cache: unreadable value; treating as a miss", "key", vkey, "error", err)
		r.metrics.lookup(resultMiss)
		return Entry{}, snap, nil
	}
	remaining := time.Until(expiresAt)
	if !bytes.Equal(stored, tokens) || remaining <= 0 {
		r.metrics.lookup(resultStale)
		return Entry{}, snap, nil
	}
	r.metrics.lookup(resultHit)
	return Entry{Value: payload, TTL: remaining}, snap, nil
}

func (r *RedisCache) lookupFailed(ctx context.Context, err error) (Entry, Snapshot, error) {
	r.record(ctx, err)
	r.metrics.lookup(resultError)
	return Entry{}, Snapshot{}, fmt.Errorf("cache: redis lookup: %w", err)
}

// readTokens concatenates an MGET reply's tokens, listing the indexes of the
// keys that do not exist.
func readTokens(res rueidis.RedisResult) (tokens []byte, missing []int, err error) {
	msgs, err := res.ToArray()
	if err != nil {
		return nil, nil, err
	}
	tokens = make([]byte, 0, len(msgs)*tokenLen)
	for i := range msgs {
		if msgs[i].IsNil() {
			missing = append(missing, i)
			tokens = append(tokens, make([]byte, tokenLen)...)
			continue
		}
		s, err := msgs[i].ToString()
		if err != nil {
			return nil, nil, err
		}
		if len(s) != tokenLen {
			return nil, nil, fmt.Errorf("version token is %d bytes, not %d: is another program writing under this key prefix?", len(s), tokenLen)
		}
		tokens = append(tokens, s...)
	}
	return tokens, missing, nil
}

// createTokens sets each missing token — only if still missing, as another
// process may create it first — and reads them all back, in one round trip:
// the tokens share a slot, so the pipeline runs in order on one node.
func (r *RedisCache) createTokens(ctx context.Context, c rueidis.Client, keys []string, missing []int) ([]byte, error) {
	cmds := make(rueidis.Commands, 0, len(missing)+1)
	for _, i := range missing {
		tok := newToken()
		cmds = append(cmds, c.B().Set().Key(keys[i]).Value(rueidis.BinaryString(tok)).Nx().Ex(jitter(r.cfg.VersionTTL, tok)).Build())
	}
	cmds = append(cmds, c.B().Mget().Key(keys...).Build())
	res := c.DoMulti(ctx, cmds...)
	for _, rr := range res[:len(missing)] {
		if err := rr.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return nil, err
		}
	}
	tokens, still, err := readTokens(res[len(missing)])
	if err != nil {
		return nil, err
	}
	if len(still) > 0 {
		return nil, errors.New("version token vanished as it was created: is the server evicting everything?")
	}
	return tokens, nil
}

// Set stores value with the tokens snap read, for ttl. A value over the size
// limit is not stored; neither is anything while the server is bypassed.
func (r *RedisCache) Set(ctx context.Context, snap Snapshot, value []byte, ttl time.Duration) error {
	if snap.key == "" || ttl <= 0 {
		return nil
	}
	if len(value) > r.maxDecoded {
		r.metrics.tooLarge()
		return nil
	}
	b := r.codec.encode(snap.tokens, time.Now().Add(ttl), value)
	if len(b) > r.cfg.MaxValueBytes {
		r.metrics.tooLarge()
		return nil
	}
	c := r.conn()
	if c == nil {
		return nil
	}
	defer r.metrics.op("set", time.Now())
	opCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	err := c.Do(opCtx, c.B().Set().Key(snap.key).Value(rueidis.BinaryString(b)).Px(max(ttl, time.Millisecond)).Build()).Error()
	r.record(ctx, err)
	if err != nil {
		r.metrics.setFailed(setFailureReason(err))
		return fmt.Errorf("cache: redis set: %w", err)
	}
	r.metrics.stored(len(b))
	return nil
}

func setFailureReason(err error) string {
	if re, ok := rueidis.IsRedisErr(err); ok && strings.HasPrefix(re.Error(), "OOM") {
		return "oom"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "other"
}

// Invalidate sets a fresh token for every token the namespaces' writes
// reach, in one pipelined round trip. Bumps the server does not take are
// kept and retried until it does, and reported as an error meanwhile.
func (r *RedisCache) Invalidate(ctx context.Context, namespaces []Namespace) (uint64, error) {
	owner := map[string]tenant.ID{}
	for _, ns := range namespaces {
		for _, k := range bumpKeys(r.cfg.KeyPrefix, ns) {
			owner[k] = ns.Tenant
		}
	}
	return uint64(len(namespaces)), r.bump(ctx, owner)
}

// InvalidateTenant sets a fresh tenant token, orphaning every entry of id.
func (r *RedisCache) InvalidateTenant(ctx context.Context, id tenant.ID) error {
	return r.bump(ctx, map[string]tenant.ID{tenantTokenKey(r.cfg.KeyPrefix, id): id})
}

func (r *RedisCache) bump(ctx context.Context, owner map[string]tenant.ID) error {
	if len(owner) == 0 {
		return nil
	}
	c := r.conn()
	if c == nil {
		r.deferBumps(owner)
		return fmt.Errorf("%w: %d invalidations deferred", errBypassed, len(owner))
	}
	defer r.metrics.op("invalidate", time.Now())
	opCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	keys := make([]string, 0, len(owner))
	cmds := make(rueidis.Commands, 0, len(owner))
	for k := range owner {
		keys = append(keys, k)
		cmds = append(cmds, r.bumpCmd(c, k))
	}
	failed := map[string]tenant.ID{}
	var firstErr error
	for i, rr := range c.DoMulti(opCtx, cmds...) {
		if err := rr.Error(); err != nil {
			failed[keys[i]] = owner[keys[i]]
			firstErr = cmpOr(firstErr, err)
		}
	}
	r.record(ctx, firstErr)
	r.metrics.invalidated("ok", len(owner)-len(failed))
	if len(failed) > 0 {
		r.deferBumps(failed)
		return fmt.Errorf("cache: redis invalidate (%d deferred): %w", len(failed), firstErr)
	}
	return nil
}

func (r *RedisCache) bumpCmd(c rueidis.Client, key string) rueidis.Completed {
	tok := newToken()
	return c.B().Set().Key(key).Value(rueidis.BinaryString(tok)).Ex(jitter(r.cfg.VersionTTL, tok)).Build()
}

func (r *RedisCache) deferBumps(owner map[string]tenant.ID) {
	for k, id := range owner {
		r.pending.add(id, k)
	}
	r.metrics.invalidated("deferred", len(owner))
}

// nudge wakes the drain loop now, rather than at its next tick.
func (r *RedisCache) nudge() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *RedisCache) drainLoop() {
	defer r.wg.Done()
	backoff := drainMinBackoff
	timer := time.NewTimer(drainIdle)
	defer timer.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.wake:
			backoff = drainMinBackoff
		case <-timer.C:
		}
		wait := drainIdle
		if r.breaker.isOpen() {
			r.conn() // starts the probe when due, so an idle process recovers too
		}
		if r.pending.len() > 0 {
			if r.drain(r.ctx) {
				backoff = drainMinBackoff
			} else {
				wait, backoff = backoff, min(backoff*2, drainMaxBackoff)
			}
		}
		timer.Reset(wait)
	}
}

// drain delivers the pending bumps, reporting whether none remain.
func (r *RedisCache) drain(ctx context.Context) bool {
	owed := r.pending.snapshot()
	keys := make([]string, 0, len(owed))
	for k := range owed {
		keys = append(keys, k)
	}
	for len(keys) > 0 {
		batch := keys[:min(drainBatch, len(keys))]
		keys = keys[len(batch):]
		c := r.conn()
		if c == nil {
			return false
		}
		cmds := make(rueidis.Commands, 0, len(batch))
		for _, k := range batch {
			cmds = append(cmds, r.bumpCmd(c, k))
		}
		opCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
		landed := map[string]uint64{}
		var firstErr error
		for i, rr := range c.DoMulti(opCtx, cmds...) {
			if err := rr.Error(); err != nil {
				firstErr = cmpOr(firstErr, err)
				continue
			}
			landed[batch[i]] = owed[batch[i]]
		}
		cancel()
		r.record(ctx, firstErr)
		r.pending.done(landed)
		r.metrics.invalidated("ok", len(landed))
		if firstErr != nil {
			return false
		}
	}
	return r.pending.len() == 0
}

// Close stops the background loops, makes one last attempt at the pending
// bumps, and closes the connection. Bumps still undelivered are lost: the
// entries they would orphan are served until their TTL.
func (r *RedisCache) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		r.wg.Wait()
		if r.pending.len() > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), closeDrainBudget)
			r.drain(ctx)
			cancel()
			if n := r.pending.len(); n > 0 {
				slog.Warn("cache: closing with undelivered invalidations; entries they orphan stay cached until their TTL", "pending", n)
			}
		}
		if cp := r.client.Load(); cp != nil {
			(*cp).Close()
		}
		r.codec.close()
		r.metrics.close()
	})
	return nil
}
