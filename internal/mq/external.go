package mq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// NATSConfig is how ExternalNATS reaches an operator-owned NATS cluster and
// what topology it expects there. Secrets are file paths only.
type NATSConfig struct {
	URLs []string
	// Name is the connection name the server reports; default
	// wavehouse-<hostname>.
	Name string
	// CredsFile (a user JWT and nkey seed) and NKeySeedFile are exclusive
	// with each other and with User.
	CredsFile    string
	NKeySeedFile string
	User         string
	PasswordFile string
	TLS          NATSTLS
	// JSDomain is the JetStream domain, for a leafnode or hub-and-spoke
	// deployment.
	JSDomain string
	// Topology is what the operator must have created (see NATSTopology).
	// Its PublishTimeout also bounds each publish attempt.
	Topology NATSTopology
	// ConnectTimeout bounds one dial; default 5s.
	ConnectTimeout time.Duration
	// TopologyWait is how long boot waits for the operator's topology;
	// default 60s.
	TopologyWait time.Duration

	// recheckEvery overrides the periodic check's interval (tests), and
	// activeWithin how recently a unit's durable must have delivered or had an
	// ack to count as someone's (default its pinned_ttl).
	recheckEvery, activeWithin time.Duration
}

// NATSTLS is the client side of TLS to the NATS servers.
type NATSTLS struct {
	CAFile         string
	CertFile       string
	KeyFile        string
	ServerName     string
	HandshakeFirst bool
}

const (
	defaultNATSConnectTimeout = 5 * time.Second
	defaultNATSTopologyWait   = 60 * time.Second
	// topologyRecheck is how often the topology is checked again after boot,
	// and the history's max_age read with it.
	topologyRecheck = 5 * time.Minute
	recheckTimeout  = 30 * time.Second
	// publishRetries is how many times a publish that got no answer is sent
	// again, with the same Nats-Msg-Id, so the stream stores it once.
	publishRetries   = 2
	publishRetryWait = 250 * time.Millisecond
	natsDrainTimeout = 5 * time.Second
	// workerDurable is the ingest worker's durable name
	// (ingest.BufferConsumerName), which maps to the operator's durable.
	workerDurable = "buffer-consumer"
	// jsErrStreamNotMatch is the server's answer to a publish whose subject
	// is held by a stream other than the one it expected.
	jsErrStreamNotMatch jetstream.ErrorCode = 10060
)

// ExternalNATS is the Broker over an operator-owned NATS cluster (see
// NATSTopology): every tenant shares N work-queue ingest partitions, each
// republishing every row it stores to the live subject, a history stream the
// ingest workers record every written row on, and one dead-letter stream. It
// never creates, changes, purges or deletes a stream or a durable. The only
// JetStream objects it creates are auto-expiring consumers on the history
// stream, for the length of a replay.
type ExternalNATS struct {
	topo NATSTopology
	nc   *nats.Conn
	js   jetstream.JetStream
	// hist records written rows on the history stream (Record).
	hist historyWriter
	// apiPrefix is the JetStream API's subject prefix, the domain's if set.
	apiPrefix string
	// activeWithin overrides each durable's pinned_ttl as the window
	// recentlyActive reads (tests).
	activeWithin time.Duration

	// partitions is partition p's stream name, dlq the dead-letter stream's,
	// both found by subject at boot; units is every partition's shard
	// durables, partition by partition. extras is the units outside them
	// still draining, as last read (at boot and every re-check).
	partitions []string
	dlq        string
	units      []natsUnit
	extras     atomic.Pointer[[]natsUnit]

	budgets    sync.Map // tenant.ID → int64
	budgetNote sync.Once
	// replayWindows holds each tenant's replay window as last set, and
	// warnedGap, per tenant warned about, the window it was warned for.
	replayWindows sync.Map // tenant.ID → time.Duration
	warnedGap     sync.Map // tenant.ID → time.Duration
	// historyMaxAge is the history stream's max_age as last read: the windows
	// are checked again when a re-check reads a different one.
	historyMaxAge atomic.Int64

	// perms is every publish the server refused the connection, which the
	// topology check probes the user's shard durable permissions against.
	perms      *natsPermissionWatch
	connected  atomic.Bool
	topologyOK atomic.Bool
	// closing is set by Close, whose own disconnect is not worth a warning.
	closing atomic.Bool
	gauges  metric.Registration

	mu       sync.Mutex
	nextID   int
	stoppers map[int]func()
	// feeds is every live subscription (Subscribe), told when the connection
	// is lost and when it is back.
	feeds map[*liveFeed]struct{}

	// rechecks asks the watch loop for a topology check now: a reconnect
	// sends one, since the cluster may have come back without a stream.
	rechecks chan struct{}
	// stopping ends the watch loop's checks at Close, which waits for
	// loopDone; connClosed is closed by the client's closed callback.
	stopping             context.Context
	stop                 context.CancelFunc
	loopDone, connClosed chan struct{}
	closeOnce            sync.Once
}

var (
	_ Broker  = (*ExternalNATS)(nil)
	_ Sharded = (*ExternalNATS)(nil)
)

// NewNATS connects to the cluster, waits up to cfg.TopologyWait for the
// operator's topology to pass verifyNATSTopology, and returns the broker.
// Recommended findings are logged; a required one still missing when the
// wait runs out is a *TopologyError listing every finding. A cluster not
// reached in that time is ErrUnavailable. The topology is checked again every
// five minutes, reported on wavehouse_mq_topology_ok and in the log, and
// never repaired.
func NewNATS(ctx context.Context, cfg NATSConfig) (*ExternalNATS, error) {
	topo := cfg.Topology.withDefaults()
	if err := topo.validate(); err != nil {
		return nil, err
	}
	if len(cfg.URLs) == 0 {
		return nil, errors.New("nats: no server URLs")
	}
	e := &ExternalNATS{
		topo:         topo,
		apiPrefix:    jetstream.DefaultAPIPrefix,
		activeWithin: cfg.activeWithin,
		perms:        newNATSPermissionWatch(),
		stoppers:     map[int]func(){},
		feeds:        map[*liveFeed]struct{}{},
		rechecks:     make(chan struct{}, 1),
		loopDone:     make(chan struct{}),
		connClosed:   make(chan struct{}),
	}
	opts, err := e.connectOptions(cfg)
	if err != nil {
		return nil, err
	}
	e.nc, err = nats.Connect(strings.Join(cfg.URLs, ","), opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: connect: %w", ErrUnavailable, err)
	}
	e.connected.Store(e.nc.IsConnected())
	if cfg.JSDomain != "" {
		e.apiPrefix = "$JS." + cfg.JSDomain + ".API."
		e.js, err = jetstream.NewWithDomain(e.nc, cfg.JSDomain)
	} else {
		e.js, err = jetstream.New(e.nc)
	}
	if err == nil {
		e.hist, err = newHistoryWriter(e.nc, cfg.JSDomain, topo.PublishTimeout)
	}
	if err != nil {
		e.nc.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	wait := cfg.TopologyWait
	if wait == 0 {
		wait = defaultNATSTopologyWait
	}
	// Each check's requests end with the wait, not the client's API timeout.
	bootCtx, cancel := context.WithTimeout(ctx, wait+time.Second)
	defer cancel()
	findings, err := awaitNATSTopology(bootCtx, e.js, topo, e.perms, wait)
	if err == nil {
		err = e.resolveStreams(bootCtx)
	}
	if err != nil {
		if !e.nc.IsConnected() {
			err = fmt.Errorf("%w: not connected to %s: %w", ErrUnavailable, redactURLs(cfg.URLs), errors.Join(e.nc.LastError(), err))
		}
		// A refused request only times out; name what the server refused,
		// unless the findings already do.
		if denied := e.perms.list(); len(denied) > 0 && !errors.Is(err, ErrTopology) {
			err = fmt.Errorf("%w (the server refused publishes to %s: regenerate the user's permissions with wavehouse mq permissions)", err, strings.Join(denied, ", "))
		}
		e.nc.Close()
		return nil, err
	}
	for _, f := range findings {
		slog.Warn("mq: nats topology: "+f.String(), "component", "nats")
	}
	e.topologyOK.Store(true)
	if err := e.readHistory(ctx); err != nil {
		e.nc.Close()
		return nil, err
	}
	if e.gauges, err = e.registerGauges(); err != nil {
		e.nc.Close()
		return nil, fmt.Errorf("register mq gauges: %w", err)
	}
	e.stopping, e.stop = context.WithCancel(context.Background()) //nolint:gosec // G118: Close calls it
	go func() {
		defer observability.Crash("mq_external")
		e.watch(orDefault(cfg.recheckEvery, topologyRecheck))
		close(e.loopDone)
	}()
	return e, nil
}

// redactURLs lists urls with any password in them masked.
func redactURLs(urls []string) string {
	out := make([]string, len(urls))
	for i, raw := range urls {
		if u, err := url.Parse(raw); err == nil {
			raw = u.Redacted()
		}
		out[i] = raw
	}
	return strings.Join(out, ",")
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// connectOptions is the connection's auth, TLS and reconnect behavior. It
// reconnects forever: only Close ends the connection, or the server ending
// it for good (auth revoked), which fails every consumer through failed.
func (e *ExternalNATS) connectOptions(cfg NATSConfig) ([]nats.Option, error) {
	name := cfg.Name
	if name == "" {
		host, _ := os.Hostname()
		name = "wavehouse-" + host
	}
	timeout := cfg.ConnectTimeout
	if timeout == 0 {
		timeout = defaultNATSConnectTimeout
	}
	opts := []nats.Option{
		nats.Name(name),
		nats.Timeout(timeout),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 2*time.Second),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
		nats.CustomInboxPrefix(natsInboxPrefix(e.topo.Prefix)),
		// A subscription the server refuses says so, which the topology check
		// probes the live subject with.
		nats.PermissionErrOnSubscribe(true),
		nats.DrainTimeout(natsDrainTimeout),
		nats.ConnectHandler(func(*nats.Conn) { e.connected.Store(true) }),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			e.connected.Store(false)
			if !e.closing.Load() {
				slog.Warn("mq: disconnected from nats; reconnecting", "component", "nats", "error", err)
			}
			// Whatever the partitions store from here on never reaches the
			// live subscriptions.
			for _, f := range e.liveFeeds() {
				f.disconnected()
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			e.connected.Store(true)
			slog.Info("mq: reconnected to nats", "component", "nats", "url", nc.ConnectedUrlRedacted())
			for _, f := range e.liveFeeds() {
				f.reconnected()
			}
			select {
			case e.rechecks <- struct{}{}:
			default: // one is already due
			}
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			e.connected.Store(false)
			close(e.connClosed)
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			// A refused consumer request is a topology fault the next check
			// names; the others (a refused publish) surface where they fail.
			if subj, first, ok := e.perms.record(err); ok && isConsumerAPI(subj) {
				e.topologyOK.Store(false)
				if !first {
					return
				}
				slog.Error("mq: nats refused a consumer request: the wavehouse user's permissions do not match the topology; regenerate them with wavehouse mq permissions",
					"component", "nats", "subject", subj)
				return
			}
			subj := ""
			if sub != nil {
				subj = sub.Subject
			}
			slog.Warn("mq: nats error", "component", "nats", "subject", subj, "error", err)
		}),
	}

	auth := 0
	if cfg.CredsFile != "" {
		auth++
		opts = append(opts, nats.UserCredentials(cfg.CredsFile))
	}
	if cfg.NKeySeedFile != "" {
		auth++
		opt, err := nats.NkeyOptionFromSeed(cfg.NKeySeedFile)
		if err != nil {
			return nil, fmt.Errorf("nats nkey seed: %w", err)
		}
		opts = append(opts, opt)
	}
	if cfg.User != "" {
		auth++
		password := ""
		if cfg.PasswordFile != "" {
			raw, err := os.ReadFile(cfg.PasswordFile)
			if err != nil {
				return nil, fmt.Errorf("nats password: %w", err)
			}
			password = strings.TrimRight(string(raw), "\r\n")
		}
		opts = append(opts, nats.UserInfo(cfg.User, password))
	}
	if auth > 1 {
		return nil, errors.New("nats: set one of creds file, nkey seed file, or user")
	}

	t := cfg.TLS
	if (t.CertFile == "") != (t.KeyFile == "") {
		return nil, errors.New("nats tls: cert and key files come as a pair")
	}
	if t.CAFile != "" {
		opts = append(opts, nats.RootCAs(t.CAFile))
	}
	if t.CertFile != "" {
		opts = append(opts, nats.ClientCert(t.CertFile, t.KeyFile))
	}
	if t.ServerName != "" {
		opts = append(opts, nats.Secure(&tls.Config{ServerName: t.ServerName, MinVersion: tls.VersionTLS12}))
	}
	if t.HandshakeFirst {
		opts = append(opts, nats.TLSHandshakeFirst())
	}
	return opts, nil
}

// resolveStreams finds each partition's stream and the dead-letter stream by
// subject, once the verifier has found exactly one of each.
func (e *ExternalNATS) resolveStreams(ctx context.Context) error {
	e.partitions = make([]string, e.topo.Partitions)
	for p := range e.partitions {
		name, err := e.js.StreamNameBySubject(ctx, fmt.Sprintf("%s.ingest.%d.x", e.topo.Prefix, p))
		if err != nil {
			return fmt.Errorf("find partition %d: %w", p, err)
		}
		e.partitions[p] = name
	}
	name, err := e.js.StreamNameBySubject(ctx, e.topo.Prefix+".dlq.x")
	if err != nil {
		return fmt.Errorf("find dead-letter stream: %w", err)
	}
	e.dlq = name
	e.units = e.units[:0]
	for _, stream := range e.partitions {
		for s := range e.topo.Shards {
			e.units = append(e.units, natsUnit{stream: stream, durable: natsShardDurable(e.topo.IngestConsumer, s)})
		}
	}
	return e.readExtras(ctx)
}

// readExtras reads the shard durables outside the configured units.
func (e *ExternalNATS) readExtras(ctx context.Context) error {
	extras, err := findExtraUnits(ctx, e.js, e.topo, e.partitions)
	if err != nil {
		return e.apiError(err)
	}
	extras = slices.DeleteFunc(extras, func(u natsUnit) bool { return u.durable == "" })
	e.extras.Store(&extras)
	return nil
}

// watch re-checks the topology until Close.
func (e *ExternalNATS) watch(recheck time.Duration) {
	topology := time.NewTicker(recheck)
	defer topology.Stop()
	for {
		select {
		case <-e.stopping.Done():
			return
		case <-topology.C:
			e.recheck()
		case <-e.rechecks:
			e.recheck()
		}
	}
}

// recheck verifies the topology again, reporting the outcome on the gauge
// and in the log. It skips a check while disconnected, which has a gauge of
// its own: the server version reads as empty then, a false fault.
func (e *ExternalNATS) recheck() {
	if !e.nc.IsConnected() {
		return
	}
	ctx, cancel := context.WithTimeout(e.stopping, recheckTimeout)
	defer cancel()
	findings, err := verifyNATSTopology(ctx, e.js, e.topo, e.perms)
	if !e.nc.IsConnected() {
		return
	}
	if err != nil {
		slog.Warn("mq: nats topology re-check could not run", "component", "nats", "error", err)
		return
	}
	var faults []string
	for _, f := range findings {
		if f.Severity == FindingRequired {
			faults = append(faults, f.String())
		}
	}
	if err := e.readExtras(ctx); err != nil {
		slog.Warn("mq: could not list the shard durables outside the configured ones", "component", "nats", "error", err)
	}
	if err := e.readHistory(ctx); err != nil {
		slog.Warn("mq: read the nats history stream", "component", "nats", "error", err)
	}
	was := e.topologyOK.Swap(len(faults) == 0)
	switch {
	case len(faults) > 0:
		slog.Error("mq: nats topology no longer matches what WaveHouse needs", "component", "nats", "findings", faults)
	case !was:
		slog.Info("mq: nats topology matches again", "component", "nats")
	}
}

// readHistory reads the history stream's max_age, and checks every tenant's
// replay window against it again when it has changed.
func (e *ExternalNATS) readHistory(ctx context.Context) error {
	s, err := e.js.Stream(ctx, e.topo.HistoryStream)
	if err != nil {
		return fmt.Errorf("history stream %s: %w", e.topo.HistoryStream, err)
	}
	maxAge := int64(s.CachedInfo().Config.MaxAge)
	if prev := e.historyMaxAge.Swap(maxAge); prev != maxAge {
		e.replayWindows.Range(func(id, window any) bool {
			e.checkReplayWindow(id.(tenant.ID), window.(time.Duration))
			return true
		})
	}
	return nil
}

// registerGauges reports the connection and the topology check.
func (e *ExternalNATS) registerGauges() (metric.Registration, error) {
	meter := otel.Meter("wavehouse-mq")
	connected, err := meter.Int64ObservableGauge("wavehouse_mq_connected",
		metric.WithDescription("1 while connected to the external NATS cluster, else 0"))
	if err != nil {
		return nil, err
	}
	topologyOK, err := meter.Int64ObservableGauge("wavehouse_mq_topology_ok",
		metric.WithDescription("1 while the external NATS topology passed its last check, else 0"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(connected, boolGauge(e.connected.Load()))
		o.ObserveInt64(topologyOK, boolGauge(e.topologyOK.Load()))
		return nil
	}, connected, topologyOK)
}

func boolGauge(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// Ready reports whether a publish can get through right now: ErrUnavailable
// while the connection is down (the client reconnects by itself, so this is
// the gap between a disconnect and the reconnect, or a server that is gone
// for good), ErrTopology while the topology failed its last check — run
// every topologyRecheck, and again as soon as the client reconnects, so a
// cluster that came back without a stream is not ready for long. The
// readiness probe reads it. Under coord.backend=nats the lease bucket rides
// this connection and is part of the check, so it is covered too.
func (e *ExternalNATS) Ready() error {
	if !e.connected.Load() {
		return fmt.Errorf("%w: not connected to nats", ErrUnavailable)
	}
	if !e.topologyOK.Load() {
		return fmt.Errorf("%w: the topology failed its last check", ErrTopology)
	}
	return nil
}

// track registers stop to run at Close, returning its unregistration.
func (e *ExternalNATS) track(stop func()) (untrack func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := e.nextID
	e.nextID++
	e.stoppers[id] = stop
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		delete(e.stoppers, id)
	}
}

// Publish stores data on topic's subject in its table's partition and shard, bounded
// by the topology's PublishTimeout per attempt. A publish that gets no answer
// is sent again up to twice with the same Nats-Msg-Id — the caller's
// WithIdempotencyKey when given — which the partition's duplicate window
// stores once. A partition at max_bytes, or a topic at its
// max_msgs_per_subject, is ErrQueueFull; no answer, a lost connection, or a
// partition stream that is gone is ErrUnavailable. It never creates anything.
func (e *ExternalNATS) Publish(ctx context.Context, topic Topic, data []byte, opts ...PublishOpt) error {
	subj, p, err := natsIngestSubject(e.topo.Prefix, e.topo.Partitions, e.topo.Shards, topic)
	if err != nil {
		return err
	}
	return e.publish(ctx, publishKindIngest, subj, e.partitions[p], false, data, opts)
}

// DuplicateWindow is the shortest duplicate_window among the partitions: how
// long an idempotency key is remembered everywhere a tenant may publish.
func (e *ExternalNATS) DuplicateWindow(ctx context.Context) (time.Duration, error) {
	var shortest time.Duration
	for _, name := range e.partitions {
		s, err := e.js.Stream(ctx, name)
		if err != nil {
			return 0, fmt.Errorf("stream %s: %w", name, err)
		}
		if d := s.CachedInfo().Config.Duplicates; shortest == 0 || d < shortest {
			shortest = d
		}
	}
	return shortest, nil
}

// DeadLetter parks msg's data on the shared dead-letter stream under its
// topic, with a fresh Nats-Msg-Id. It does not ack msg.
func (e *ExternalNATS) DeadLetter(ctx context.Context, msg *Message, opts ...PublishOpt) error {
	return e.publish(ctx, publishKindDLQ, e.topo.Prefix+".dlq."+msg.topicKey, e.dlq, true, msg.Data, opts)
}

// publish stores data on subj, which stream should hold. With expect the
// server refuses a publish another stream holds. Without it the row is
// stored wherever the subject leads, and a mismatch the ack names is
// reported as a topology fault and ErrUnavailable, the row left on the other
// stream. An ingest publish goes without: the partition republishes each row
// with its headers, and the expectation would ride every copy to every
// process reading the live feed. The publish, retries included, is one
// mq.publish span of kind.
func (e *ExternalNATS) publish(ctx context.Context, kind, subj, stream string, expect bool, data []byte, opts []PublishOpt) (err error) {
	spanCtx, end := beginPublish(ctx, kind)
	attempts := 0
	defer func() { end(attempts, err) }()
	msg := nats.NewMsg(subj)
	msg.Data = data
	headers := Headers{}
	for _, opt := range opts {
		opt(headers)
	}
	observability.InjectHeaders(spanCtx, headers)
	msg.Header = nats.Header(headers)
	// WithMsgID overwrites the header, so a caller's idempotency key must be
	// the id itself; otherwise a fresh one keeps this publish's retries one.
	id := headers.Get(idempotencyHeader)
	if id == "" {
		id = nuid.Next()
	}
	pubOpts := []jetstream.PublishOpt{
		jetstream.WithMsgID(id),
		jetstream.WithRetryAttempts(0),
	}
	if expect {
		pubOpts = append(pubOpts, jetstream.WithExpectStream(stream))
	}

	for attempt := 0; ; attempt++ {
		attempts++
		actx, cancel := context.WithTimeout(spanCtx, e.topo.PublishTimeout)
		var ack *jetstream.PubAck
		ack, err = e.js.PublishMsg(actx, msg, pubOpts...)
		cancel()
		if err == nil && ack.Stream != stream {
			e.lostTopology("the subject is held by another stream than " + stream)
			return fmt.Errorf("%w: %s was stored by stream %s, not %s", ErrUnavailable, subj, ack.Stream, stream)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("publish: %w", ctx.Err())
		}
		if !noAnswer(err) || attempt == publishRetries {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("publish: %w", ctx.Err())
		case <-time.After(publishRetryWait):
		}
	}
	return e.publishError(stream, err)
}

// noAnswer reports a publish that got no answer: it may or may not have been
// stored, so it is sent again with the same id.
func noAnswer(err error) bool {
	return errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout)
}

// publishError maps a failed publish to the sentinel the API answers.
func (e *ExternalNATS) publishError(stream string, err error) error {
	// The server names a full store only in the error's text.
	if msg := err.Error(); strings.Contains(msg, "maximum bytes exceeded") || strings.Contains(msg, "maximum messages per subject exceeded") {
		return fmt.Errorf("%w: %w", ErrQueueFull, err)
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode == jsErrStreamNotMatch {
		e.lostTopology("the subject is held by another stream than " + stream)
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if errors.Is(err, jetstream.ErrNoStreamResponse) && e.nc.IsConnected() {
		ctx, cancel := context.WithTimeout(context.Background(), e.topo.PublishTimeout)
		defer cancel()
		if _, serr := e.js.Stream(ctx, stream); errors.Is(serr, jetstream.ErrStreamNotFound) {
			e.lostTopology("stream " + stream + " does not exist")
			return fmt.Errorf("%w: stream %s does not exist: %w", ErrUnavailable, stream, err)
		}
	}
	if noAnswer(err) || errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionDraining) ||
		errors.Is(err, nats.ErrReconnectBufExceeded) || errors.Is(err, nats.ErrDisconnected) {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}

// lostTopology records a topology fault found between checks.
func (e *ExternalNATS) lostTopology(problem string) {
	e.topologyOK.Store(false)
	slog.Error("mq: nats topology no longer matches what WaveHouse needs", "component", "nats", "problem", problem)
}

// wrapMsg adapts a message a shard durable delivered: its topic key is the
// subject with the prefix, partition and shard stripped, and it was accepted
// when its partition stored it.
func (e *ExternalNATS) wrapMsg(ctx context.Context, m jetstream.Msg) *Message {
	key, ok := natsTopicKey(e.topo.Prefix, m.Subject())
	if !ok {
		key = m.Subject()
	}
	msg := newMessage(ctx, key, m.Data(), time.Now(), m.DoubleAck, m.Ack, m.Nak, WithNakDelay(m.NakWithDelay))
	if meta, err := m.Metadata(); err == nil {
		msg.accepted = meta.Timestamp
	}
	return msg
}

// Subscribe delivers every row a partition stores from now on to handler, one
// at a time, until ctx is done or Close: a plain subscription to the subject
// the partitions republish to, which every pod's hub holds for itself. A
// lost connection skips what the partitions stored meanwhile, and a pod that
// falls far enough behind has rows dropped: either way lost is called before
// a later row is delivered, and once the connection is back (liveFeed).
func (e *ExternalNATS) Subscribe(ctx context.Context, handler func(msg *Message), lost func()) error {
	feed := newLiveFeed(func(m *nats.Msg) {
		key, ok := natsTopicKey(e.topo.Prefix, m.Subject)
		if !ok {
			key = m.Subject
		}
		handler(newMessage(observability.ExtractHeaders(ctx, Headers(m.Header)), key, m.Data, time.Now(), nil, nil, nil))
	}, lost, e.nc.IsConnected)
	stop, stopped, err := subscribeLive(ctx, e.nc, natsLiveSubjects(e.topo.Prefix), feed)
	if err != nil {
		return fmt.Errorf("live subscription: %w", e.apiError(err))
	}
	untrack := e.track(stop)
	e.mu.Lock()
	e.feeds[feed] = struct{}{}
	e.mu.Unlock()
	go func() {
		defer observability.Crash("mq_external")
		<-stopped
		untrack()
		e.mu.Lock()
		delete(e.feeds, feed)
		e.mu.Unlock()
	}()
	return nil
}

// liveFeeds is every live subscription there is.
func (e *ExternalNATS) liveFeeds() []*liveFeed {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Collect(maps.Keys(e.feeds))
}

// apiError marks a JetStream request that got no answer as ErrUnavailable.
func (e *ExternalNATS) apiError(err error) error {
	if err == nil {
		return nil
	}
	if noAnswer(err) || errors.Is(err, nats.ErrConnectionClosed) {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}

// durable maps the durable a caller names to the operator's shard durables'
// prefix: the ingest worker's name, or the operator's own.
func (e *ExternalNATS) durable(name string) (string, bool) {
	if name == workerDurable || name == e.topo.IngestConsumer {
		return e.topo.IngestConsumer, true
	}
	return "", false
}

// IngestUnits implements Sharded: every partition's shard durables, then the
// ones outside them still draining. A unit's name is <stream>/<durable>.
func (e *ExternalNATS) IngestUnits() (configured, extra []string) {
	for _, u := range e.units {
		configured = append(configured, u.id())
	}
	if xs := e.extras.Load(); xs != nil {
		for _, u := range *xs {
			extra = append(extra, u.id())
		}
	}
	return configured, extra
}

// unit resolves a unit's name, and whether it is an extra.
func (e *ExternalNATS) unit(name string) (natsUnit, bool, bool) {
	for _, u := range e.units {
		if u.id() == name {
			return u, false, true
		}
	}
	if xs := e.extras.Load(); xs != nil {
		for _, u := range *xs {
			if u.id() == name {
				return u, true, true
			}
		}
	}
	return natsUnit{}, false, false
}

// ResetOrphaned implements Sharded. A pinned client holds the unit, and one
// that delivered or had an ack within the durable's pinned_ttl may still:
// its pin can be gone for a moment (a consumer leader change releases it)
// while it lives. Either is ErrUnitHeld. Otherwise, with rows awaiting an
// ack, whoever received them is gone: the reset redelivers them from the ack
// floor at once, where the server would wait out ack_wait. Rows already
// acked are not delivered again.
func (e *ExternalNATS) ResetOrphaned(ctx context.Context, name string) (bool, error) {
	u, _, ok := e.unit(name)
	if !ok {
		return false, fmt.Errorf("unit %q: %w", name, ErrUnitsUnsupported)
	}
	c, err := e.js.Consumer(ctx, u.stream, u.durable)
	if err != nil {
		return false, fmt.Errorf("consumer %s: %w", name, e.apiError(err))
	}
	info := c.CachedInfo()
	if pinnedClient(info) != "" {
		return false, fmt.Errorf("unit %s: %w", name, ErrUnitHeld)
	}
	if info.NumAckPending == 0 {
		return false, nil
	}
	if e.recentlyActive(info) {
		return false, fmt.Errorf("unit %s: %w: it delivered or had an ack within %s", name, ErrUnitHeld, e.activeWindow(info))
	}
	if _, err := e.js.ResetConsumer(ctx, u.stream, u.durable); err != nil {
		return false, fmt.Errorf("reset %s: %w", name, e.apiError(err))
	}
	return true, nil
}

// UnitStates implements Sharded: each durable's pending, ack-pending and
// redelivered counts, its max_ack_pending, and whether a client holds its
// pin or was active on it within its pinned_ttl. A live owner keeps its pin
// by pulling, even with nothing more to fetch, so a unit with rows and no
// holder has nobody writing it.
func (e *ExternalNATS) UnitStates(ctx context.Context) ([]UnitState, error) {
	units := slices.Clone(e.units)
	if xs := e.extras.Load(); xs != nil {
		units = append(units, *xs...)
	}
	states := make([]UnitState, 0, len(units))
	for _, u := range units {
		c, err := e.js.Consumer(ctx, u.stream, u.durable)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			continue // the topology check names it
		}
		if err != nil {
			return nil, fmt.Errorf("consumer %s: %w", u.id(), e.apiError(err))
		}
		info := c.CachedInfo()
		states = append(states, UnitState{
			Unit:          u.id(),
			Pending:       int(min(info.NumPending, uint64(math.MaxInt))), //nolint:gosec // G115: clamped
			AckPending:    info.NumAckPending,
			MaxAckPending: info.Config.MaxAckPending,
			Redelivered:   info.NumRedelivered,
			Held:          pinnedClient(info) != "" || e.recentlyActive(info),
		})
	}
	return states, nil
}

// activeWindow is how recently a durable must have delivered or had an ack
// to count as someone's: its pinned_ttl, the time the server gives a silent
// pinned client before it unpins it.
func (e *ExternalNATS) activeWindow(info *jetstream.ConsumerInfo) time.Duration {
	if e.activeWithin > 0 {
		return e.activeWithin
	}
	return max(info.Config.PinnedTTL, minPinnedTTL)
}

// recentlyActive reports whether a durable delivered a row, or its ack
// floor moved, within its activeWindow, by the server's own clock.
func (e *ExternalNATS) recentlyActive(info *jetstream.ConsumerInfo) bool {
	window := e.activeWindow(info)
	for _, last := range []*time.Time{info.Delivered.Last, info.AckFloor.Last} {
		if last != nil && info.TimeStamp.Sub(*last) < window {
			return true
		}
	}
	return false
}

// pinnedClient is the pin id the server holds for WaveHouse's priority group
// on a consumer, "" when no client is pinned.
func pinnedClient(info *jetstream.ConsumerInfo) string {
	for _, g := range info.PriorityGroups {
		if g.Group == natsPriorityGroup {
			return g.PinnedClientID
		}
	}
	return ""
}

// DeadLetterCounts counts tenant id's parked messages on the shared
// dead-letter stream, by a subject filter on its tenant: one call. Per-table
// counts are keyed by deadLetterTables, whose table filter matches every
// scope of that table. A tenant with nothing parked has zero counts; there is
// no queue of its own whose absence could mean anything.
func (e *ExternalNATS) DeadLetterCounts(ctx context.Context, id tenant.ID, table string) (DeadLetterCounts, error) {
	if _, err := tenant.Parse(string(id)); err != nil {
		return DeadLetterCounts{}, fmt.Errorf("tenant: %w", err)
	}
	s, err := e.js.Stream(ctx, e.dlq)
	if err != nil {
		return DeadLetterCounts{}, fmt.Errorf("dead-letter stream %s: %w", e.dlq, e.apiError(err))
	}
	prefix := e.topo.Prefix + ".dlq."
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(prefix+string(id)+".>"))
	if err != nil {
		return DeadLetterCounts{}, fmt.Errorf("dead-letter stream %s: %w", e.dlq, e.apiError(err))
	}
	var total uint64
	for _, n := range info.State.Subjects {
		total += n
	}
	return DeadLetterCounts{Tables: deadLetterTables(info.State.Subjects, prefix, table), Total: total}, nil
}

// SetReplayWindow records tenant id's replay window and enforces nothing: the
// history stream's max_age is the operator's, and every tenant's rows share
// it. It warns when the window is longer than the history keeps, once per
// tenant and window, so calling it after every settings reload repeats
// nothing until a window changes; a re-check that reads a different max_age
// checks every recorded window again, a tenant's no longer served included.
// No I/O: max_age is read at boot and with each topology re-check.
func (e *ExternalNATS) SetReplayWindow(_ context.Context, id tenant.ID, window time.Duration) error {
	if _, err := tenant.Parse(string(id)); err != nil {
		return fmt.Errorf("tenant: %w", err)
	}
	e.replayWindows.Store(id, window)
	e.checkReplayWindow(id, window)
	return nil
}

// checkReplayWindow warns when tenant id's window is longer than the history
// stream keeps a row after it is written: a replay serves only the last
// max_age of it.
func (e *ExternalNATS) checkReplayWindow(id tenant.ID, window time.Duration) {
	maxAge := time.Duration(e.historyMaxAge.Load())
	if maxAge <= 0 {
		return
	}
	if window <= maxAge {
		e.warnedGap.Delete(id)
		return
	}
	if prev, ok := e.warnedGap.Swap(id, window); ok && prev.(time.Duration) == window {
		return
	}
	slog.Warn("mq: the nats history keeps less than this tenant's gap window; SSE replay serves only the rows written within the last max_age",
		"component", "nats", "tenant", id, "history_stream", e.topo.HistoryStream, "max_age", maxAge, "gap_window", window)
}

// Record stores each written row on the history stream under its topic, with
// the time its partition accepted it, in order, within the topology's
// PublishTimeout for the batch. It never creates anything: a history stream
// that is gone, full and refusing, or without a leader costs the batch its
// replay, and the error says how many rows.
func (e *ExternalNATS) Record(ctx context.Context, msgs []*Message) error {
	prefix := e.topo.Prefix + ".hist."
	return e.hist.record(ctx, msgs, func(m *Message) (string, bool) { return prefix + m.topicKey, true },
		jetstream.WithExpectStream(e.topo.HistoryStream))
}

// ReplaySince sends topic's events accepted at or after since: the written
// ones from the history stream, as long as its max_age keeps them, then the
// ones still waiting wherever the topic's table has queued rows, then the
// ones written while those were read (replaySource). Waiting rows can be on
// the partition that holds the table now; on a partition that held it under
// a smaller partition count, until the rows queued before the count was
// raised are written (natsEarlierPartitions); and on a stream a lower count
// left behind, which the operator deletes once it has drained, so one that is
// gone is skipped. The history is read through ack-less consumers of its own,
// in memory, that expire once idle; the partitions by direct get. A read that
// fails before the replay catches up is an error; a done ctx returns ctx's
// error.
func (e *ExternalNATS) ReplaySince(ctx context.Context, topic Topic, since time.Time, send func(data []byte) bool) error {
	historySubject, err := natsHistorySubject(e.topo.Prefix, topic)
	if err != nil {
		return err
	}
	p, _ := natsRoute(topic, e.topo.Partitions, e.topo.Shards)
	var queues []replayQueue
	for _, held := range append(natsEarlierPartitions(topic, e.topo.Partitions), p) {
		subj, err := natsWaitingSubject(e.topo.Prefix, held, topic)
		if err != nil {
			return err
		}
		queues = append(queues, replayQueue{stream: e.partitions[held], subject: subj})
	}
	leftover, err := natsWaitingSubject(e.topo.Prefix, -1, topic)
	if err != nil {
		return err
	}
	if xs := e.extras.Load(); xs != nil {
		for _, u := range *xs {
			if !slices.Contains(e.partitions, u.stream) && !slices.ContainsFunc(queues, func(q replayQueue) bool { return q.stream == u.stream }) {
				queues = append(queues, replayQueue{stream: u.stream, subject: leftover, optional: true})
			}
		}
	}
	src := replaySource{
		nc: e.nc, js: e.js, apiPrefix: e.apiPrefix,
		history: e.topo.HistoryStream, historySubject: historySubject,
		queues: queues,
		consumer: jetstream.ConsumerConfig{
			AckPolicy:         jetstream.AckNonePolicy,
			InactiveThreshold: replayInactiveThreshold,
			MemoryStorage:     true,
			Replicas:          1,
		},
	}
	return e.apiError(src.replay(ctx, since, send))
}

// SetMaxBytes records tenant id's budget and enforces nothing: the tenants of
// a partition share its max_bytes, and max_msgs_per_subject caps each topic.
// The first call says so in the log.
func (e *ExternalNATS) SetMaxBytes(_ context.Context, id tenant.ID, maxBytes int64) error {
	if _, err := tenant.Parse(string(id)); err != nil {
		return fmt.Errorf("tenant: %w", err)
	}
	e.budgetNote.Do(func() {
		slog.Info("mq: tenant byte budgets (mq.max_bytes_gb) are recorded, not enforced, on external NATS; a partition's max_bytes is shared by its tenants",
			"component", "nats", "partitions", e.topo.Partitions)
	})
	e.budgets.Store(id, maxBytes)
	return nil
}

// MaxBytes reports the budget last recorded for id.
func (e *ExternalNATS) MaxBytes(id tenant.ID) int64 {
	if v, ok := e.budgets.Load(id); ok {
		return v.(int64)
	}
	return 0
}

// Stats reports this process's client connection: whether it is connected,
// and its cumulative message, byte and reconnect counts.
func (e *ExternalNATS) Stats() (observability.MQStats, error) {
	s := e.nc.Stats()
	return observability.MQStats{
		Connections: boolGauge(e.nc.IsConnected()),
		InMsgs:      capInt64(s.InMsgs),
		OutMsgs:     capInt64(s.OutMsgs),
		InBytes:     capInt64(s.InBytes),
		OutBytes:    capInt64(s.OutBytes),
		Reconnects:  capInt64(s.Reconnects),
	}, nil
}

// capInt64 converts a cumulative count for an int64 instrument, capped far
// above anything a process reaches.
func capInt64(n uint64) int64 {
	return int64(min(n, uint64(1<<62))) //nolint:gosec // capped
}

// Close stops every consumer, so none reports failed, then drains the
// connection so pending acks are flushed. Safe to call more than once.
func (e *ExternalNATS) Close() error {
	e.closeOnce.Do(func() {
		e.closing.Store(true)
		e.stop()
		<-e.loopDone
		e.mu.Lock()
		stops := make([]func(), 0, len(e.stoppers))
		for _, stop := range e.stoppers {
			stops = append(stops, stop)
		}
		clear(e.stoppers)
		e.mu.Unlock()
		for _, stop := range stops {
			stop()
		}
		if e.gauges != nil {
			_ = e.gauges.Unregister()
		}
		if err := e.nc.Drain(); err != nil {
			e.nc.Close()
		}
		select {
		case <-e.connClosed:
		case <-time.After(natsDrainTimeout + time.Second):
			e.nc.Close()
		}
	})
	return nil
}
