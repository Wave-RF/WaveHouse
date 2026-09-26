package cache

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
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
	defaultConnLifetime          = time.Minute
)

const (
	drainMinBackoff  = 100 * time.Millisecond
	drainMaxBackoff  = 10 * time.Second
	drainIdle        = time.Second
	drainBatch       = 1000
	dialMaxBackoff   = 30 * time.Second
	closeDrainBudget = time.Second
)

var (
	errBypassed = errors.New("cache: redis unavailable")
	// errMalformedReply is a reply the server sent that this code cannot
	// use: the server is up, so it never counts against the breaker.
	errMalformedReply = errors.New("cache: malformed reply")
)

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
	VersionTTL       time.Duration // a token's lifetime from its last bump, jittered ±10%; reads do not extend it
	PendingMax       int           // undelivered bumps kept before collapsing to tenant bumps

	BreakerThreshold int           // consecutive failures that open the breaker
	BreakerOpenFor   time.Duration // how long it stays open before a probe

	connLifetime time.Duration // defaultConnLifetime; tests shorten it
	onePipe      bool          // one connection per node, whatever GOMAXPROCS; tests only
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
	if c.VersionTTL != 0 && c.VersionTTL < 2*time.Second {
		return c, fmt.Errorf("cache: redis version ttl %s is under 2s: EX has one-second resolution, and jittered below ~1.1s it could be EX 0", c.VersionTTL)
	}
	c.KeyPrefix = cmpOr(c.KeyPrefix, DefaultRedisKeyPrefix)
	c.Timeout = cmpOr(c.Timeout, DefaultRedisTimeout)
	c.DialTimeout = cmpOr(c.DialTimeout, DefaultRedisDialTimeout)
	c.MaxValueBytes = cmpOr(c.MaxValueBytes, DefaultRedisMaxValueBytes)
	c.VersionTTL = cmpOr(c.VersionTTL, DefaultRedisVersionTTL)
	c.PendingMax = cmpOr(c.PendingMax, DefaultRedisPendingMax)
	c.BreakerThreshold = cmpOr(c.BreakerThreshold, defaultBreakerThreshold)
	c.BreakerOpenFor = cmpOr(c.BreakerOpenFor, defaultBreakerOpenFor)
	c.connLifetime = cmpOr(c.connLifetime, defaultConnLifetime)
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
		DisableCache:      true, // no client-side caching until a local near-cache in front of Redis exists
		ForceSingleClient: c.Mode == RedisStandalone,
	}
	// How long a connection waits on a silent server, 10 s unset. A cluster
	// client reads the topology under it after the handshake, which boot and
	// Close would wait out; never under Timeout, so no connection is cut
	// while an operation may still wait on it.
	opt.ConnWriteTimeout = max(c.DialTimeout, c.Timeout)
	// A connection outlives a failover behind a stable address: the demoted
	// node still answers, refusing writes, and rueidis does not redial on
	// READONLY. Replacing each connection this often (rueidis retries what
	// was in flight) re-resolves the address, so a process the refusals
	// bypass reaches the new primary, and delivers its owed bumps, within
	// about this long.
	opt.ConnLifetime = c.connLifetime
	if c.onePipe {
		opt.PipelineMultiplex = -1
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
// current, so a lost token (eviction, expiry, a restart without persistence)
// can only cause misses. Restoring a snapshot is not a loss but a rollback —
// a restart after a crash that reloads the server's last save included,
// which stock Redis and Valkey make by default: the old tokens return with
// their values. A lookup is one round trip. The server failing or timing
// out is a miss, a skipped fill and a deferred invalidation — never a
// failed query.
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
	if probe && r.ctx.Err() == nil {
		go r.probe(*cp)
	}
	if !ok {
		return nil
	}
	return *cp
}

// probe decides whether an open breaker closes. It writes: a server that
// answers but refuses writes (refusesWork) would take no bump either.
// rueidis redials under the calling operation's context, bounding the dial
// (TLS included) by DialTimeout and then the handshake by DialTimeout again,
// so the first write has twice DialTimeout for a reconnect on top of
// Timeout: a reconnect slower than Timeout fails the operations waiting on
// it, but not the probe. The allowance is for a reconnect only: a first
// write slower than Timeout is repeated under Timeout, and the repeat
// decides, so a server answering slower than Timeout stays bypassed.
func (r *RedisCache) probe(c rueidis.Client) {
	set := func(budget time.Duration) error {
		ctx, cancel := context.WithTimeout(r.ctx, budget)
		defer cancel()
		return c.Do(ctx, c.B().Set().Key(r.cfg.KeyPrefix+":probe").Value("1").Ex(time.Minute).Build()).Error()
	}
	start := time.Now()
	err := set(2*r.cfg.DialTimeout + r.cfg.Timeout)
	if time.Since(start) > r.cfg.Timeout {
		err = set(r.cfg.Timeout)
	}
	r.record(r.ctx, err)
	if r.breaker.isOpen() {
		return
	}
	slog.InfoContext(r.ctx, "cache: redis reachable again; cache back in use")
	r.nudge()
}

// bypassed reports whether operations are skipping the server.
func (r *RedisCache) bypassed() bool {
	return r.client.Load() == nil || r.breaker.isOpen()
}

// record feeds an operation's outcome to the breaker. A reply from the
// server, even an error reply or one this code cannot use, shows it is up —
// unless it refuses the work outright, which opens the breaker at once. A
// caller that gave up first shows nothing about it.
func (r *RedisCache) record(parent context.Context, err error) {
	if err == nil || rueidis.IsRedisNil(err) {
		r.breaker.success()
		return
	}
	if re, ok := rueidis.IsRedisErr(err); ok {
		r.recordReply(parent, re.Error())
		return
	}
	if errors.Is(err, errMalformedReply) {
		r.breaker.success()
		return
	}
	if parent.Err() != nil {
		return
	}
	if r.breaker.failure() {
		slog.WarnContext(parent, "cache: redis not answering; bypassing the cache",
			"addrs", r.cfg.Addrs, "error", err)
	}
}

// recordReply is record for an error reply. The breaker opening is logged
// once, not once per operation in flight.
func (r *RedisCache) recordReply(parent context.Context, msg string) {
	switch {
	case rejectsCredentials(msg):
		if r.breaker.trip() {
			slog.ErrorContext(parent, "cache: redis rejected the credentials; bypassing the cache until they work",
				"addrs", r.cfg.Addrs, "error", msg)
		}
	case refusesWork(msg):
		if r.breaker.trip() {
			slog.WarnContext(parent, "cache: redis refusing writes; bypassing the cache",
				"addrs", r.cfg.Addrs, "reply", msg)
		}
	default:
		r.breaker.success()
	}
}

// refusesWork reports whether an error reply says the server takes no
// writes from anyone right now, so no bump can land: a replica (READONLY,
// or MASTERDOWN, which refuses reads too), memory full under noeviction
// (OOM), writes stopped by min-replicas-to-write (NOREPLICAS) or a failed
// snapshot (MISCONF), a dataset still loading (LOADING), a script holding
// the server (BUSY), a cluster not serving the slot (CLUSTERDOWN). Replies
// about one key or one moment — WRONGTYPE, NOPERM, TRYAGAIN during a slot
// migration — are not.
func refusesWork(msg string) bool {
	code, _, _ := strings.Cut(msg, " ")
	switch code {
	case "READONLY", "MASTERDOWN", "OOM", "NOREPLICAS", "MISCONF", "LOADING", "BUSY", "CLUSTERDOWN":
		return true
	}
	return false
}

// rejectsCredentials reports whether an error reply refuses this process's
// credentials, as a connection's handshake after a password rotation does:
// every operation meets it, so it refuses all work. NOPERM is not one: it
// names a key or a command, which the rest of the work may not touch.
func rejectsCredentials(msg string) bool {
	code, _, _ := strings.Cut(msg, " ")
	return code == "WRONGPASS" || code == "NOAUTH"
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
	// A bump this process owes would orphan what a lookup reading its key
	// finds, so that lookup is a bypass: no hit, and a zero snapshot, so no
	// fill either. A lookup's token keys are exactly those whose bumps orphan
	// its entry, and include the tenant token a set past PendingMax collapses
	// to. Other processes cannot know what this one owes, and serve those
	// entries until the bump lands.
	if r.pending.owesAny(keys) {
		r.metrics.lookup(resultBypass)
		return Entry{}, Snapshot{}, nil
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
	if err := res[0].Error(); err != nil {
		return r.lookupFailed(ctx, err)
	}
	// An error reply such as WRONGTYPE: the value key holds something else,
	// which the fill's plain SET replaces, so it is a miss to fill.
	valErr := res[1].Error()
	_, valReplied := rueidis.IsRedisErr(valErr)
	if valErr != nil && !rueidis.IsRedisNil(valErr) && !valReplied {
		return r.lookupFailed(ctx, valErr)
	}
	r.record(ctx, nil)
	tokens, missing, foreign, err := readTokens(res[0])
	if err != nil {
		return r.lookupFailed(ctx, err)
	}
	var val []byte
	if valErr == nil {
		if val, err = res[1].AsBytes(); err != nil {
			return r.lookupFailed(ctx, fmt.Errorf("%w: %w", errMalformedReply, err))
		}
	}

	if len(missing) > 0 || len(foreign) > 0 {
		if tokens, err = r.createTokens(opCtx, c, keys, missing, foreign); err != nil {
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
// keys that do not exist and of those holding something that is not a token.
func readTokens(res rueidis.RedisResult) (tokens []byte, missing, foreign []int, err error) {
	msgs, err := res.ToArray()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: %w", errMalformedReply, err)
	}
	tokens = make([]byte, 0, len(msgs)*tokenLen)
	for i := range msgs {
		if msgs[i].IsNil() {
			missing = append(missing, i)
			tokens = append(tokens, make([]byte, tokenLen)...)
			continue
		}
		s, err := msgs[i].ToString()
		if err != nil || len(s) != tokenLen {
			foreign = append(foreign, i)
			tokens = append(tokens, make([]byte, tokenLen)...)
			continue
		}
		tokens = append(tokens, s...)
	}
	return tokens, missing, foreign, nil
}

// createTokens sets each missing token — only if still missing, as another
// process may create it first — replaces each foreign one (a string that is
// not a token, or, on a second round trip, a key of another type), and reads
// them all back, in one round trip: the tokens share a slot, so the pipeline runs
// in order on one node. A fresh token can only cause misses, so replacing
// whatever held a token key is safe.
func (r *RedisCache) createTokens(ctx context.Context, c rueidis.Client, keys []string, missing, foreign []int) ([]byte, error) {
	if len(foreign) > 0 {
		slog.WarnContext(ctx, "cache: replacing values that are not version tokens; is another program writing under this key prefix?",
			"keys", len(foreign), "prefix", r.cfg.KeyPrefix)
	}
	cmds := make(rueidis.Commands, 0, len(missing)+len(foreign)+1)
	for _, i := range missing {
		tok := newToken()
		cmds = append(cmds, c.B().Set().Key(keys[i]).Value(rueidis.BinaryString(tok)).Nx().Ex(jitter(r.cfg.VersionTTL, tok)).Build())
	}
	for _, i := range foreign {
		cmds = append(cmds, r.bumpCmd(c, keys[i]))
	}
	cmds = append(cmds, c.B().Mget().Key(keys...).Build())
	res := c.DoMulti(ctx, cmds...)
	for _, rr := range res {
		if err := rr.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return nil, err
		}
	}
	tokens, still, bad, err := readTokens(res[len(res)-1])
	if err != nil {
		return nil, err
	}
	if len(still) == 0 && len(bad) == 0 {
		return tokens, nil
	}
	// Still nil after SET NX: the key holds a list, hash or other non-string,
	// which MGET reads as nil and NX will not overwrite. Replace it too.
	if len(foreign) == 0 && len(still) > 0 {
		return r.createTokens(ctx, c, keys, nil, still)
	}
	return nil, fmt.Errorf("%w: version token gone or replaced as it was written", errMalformedReply)
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
// reach, in pipelined batches of drainBatch, one round trip each. Bumps the server does not take are
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
	failed := map[string]tenant.ID{}
	var firstErr error
	// In batches, so a wide fan-out is several round trips each within the
	// timeout; past a failure the rest are deferred unsent.
	for batch := range slices.Chunk(slices.Collect(maps.Keys(owner)), drainBatch) {
		var landed []bool
		if firstErr == nil {
			landed, firstErr = r.sendBumps(ctx, c, batch)
		}
		for i, k := range batch {
			if landed == nil || !landed[i] {
				failed[k] = owner[k]
			}
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

// sendBumps sets a fresh token under each key in one pipeline, within the
// op timeout, reporting which landed and the first error.
func (r *RedisCache) sendBumps(ctx context.Context, c rueidis.Client, keys []string) (landed []bool, firstErr error) {
	cmds := make(rueidis.Commands, 0, len(keys))
	for _, k := range keys {
		cmds = append(cmds, r.bumpCmd(c, k))
	}
	opCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	landed = make([]bool, len(keys))
	for i, rr := range c.DoMulti(opCtx, cmds...) {
		err := rr.Error()
		landed[i] = err == nil
		firstErr = cmpOr(firstErr, err)
	}
	return landed, firstErr
}

func (r *RedisCache) deferBumps(owner map[string]tenant.ID) {
	first := r.pending.len() == 0
	for k, id := range owner {
		r.pending.add(id, k)
	}
	r.metrics.invalidated("deferred", len(owner))
	// The first bump owed wakes the drain now, not at its next tick: it is
	// retried at once, or, past an open breaker, when the probe is due.
	// Later ones join the retry already backing off.
	if first {
		r.nudge()
	}
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
			r.conn() // starts the probe when due
		}
		if r.pending.len() > 0 {
			if r.drain(r.ctx, r.conn) {
				backoff = drainMinBackoff
			} else {
				wait, backoff = backoff, min(backoff*2, drainMaxBackoff)
			}
		}
		// Lookups start the probe that closes an open breaker; a process
		// with none (ingest only) has this loop, which wakes when the probe
		// is due rather than at the drain's backoff, so it recovers as soon.
		if d, open := r.breaker.untilProbe(); open {
			wait = min(wait, d)
		}
		timer.Reset(wait)
	}
}

// drain delivers the pending bumps through the client conn returns,
// reporting whether none remain.
func (r *RedisCache) drain(ctx context.Context, conn func() rueidis.Client) bool {
	owed := r.pending.snapshot()
	keys := make([]string, 0, len(owed))
	for k := range owed {
		keys = append(keys, k)
	}
	for batch := range slices.Chunk(keys, drainBatch) {
		c := conn()
		if c == nil {
			return false
		}
		sent, err := r.sendBumps(ctx, c, batch)
		landed := map[string]uint64{}
		for i, k := range batch {
			if sent[i] {
				landed[k] = owed[k]
			}
		}
		r.record(ctx, err)
		r.pending.done(landed)
		r.metrics.invalidated("ok", len(landed))
		if err != nil {
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
			// Past the breaker: an open one is why bumps are pending, and this
			// is the last chance to deliver them.
			r.drain(ctx, func() rueidis.Client {
				if cp := r.client.Load(); cp != nil {
					return *cp
				}
				return nil
			})
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
