package mq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

	// recheckEvery and sourcesEvery override the periodic checks' intervals
	// (tests).
	recheckEvery, sourcesEvery time.Duration
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
	// and sourcesPoll how often the history's sources are read for the lag
	// gauges: one stream-info call.
	topologyRecheck = 5 * time.Minute
	sourcesPoll     = 30 * time.Second
	recheckTimeout  = 30 * time.Second
	// publishRetries is how many times a publish that got no answer is sent
	// again, with the same Nats-Msg-Id, so the stream stores it once.
	publishRetries   = 2
	publishRetryWait = 250 * time.Millisecond
	natsDrainTimeout = 5 * time.Second
	// hubInactiveThreshold and replayInactiveThreshold are how long the
	// server keeps the history consumers of a pod that went away. A replay's
	// also has to outlast sending one fetched batch to a slow SSE client,
	// since no pull is waiting meanwhile; a finished replay deletes its own.
	hubInactiveThreshold    = time.Minute
	replayInactiveThreshold = time.Minute
	// replayPullWait bounds one pull of a replay whose remaining events the
	// server has already counted, and replayBatch is how many one pull asks
	// for: a replay is a round trip per batch, not per event.
	replayPullWait = 2 * time.Second
	replayBatch    = 256
	// workerDurable is the ingest worker's durable name
	// (ingest.BufferConsumerName), which maps to the operator's durable.
	workerDurable = "buffer-consumer"
	// jsErrStreamNotMatch is the server's answer to a publish whose subject
	// is held by a stream other than the one it expected.
	jsErrStreamNotMatch jetstream.ErrorCode = 10060
)

// ExternalNATS is the Broker over an operator-owned NATS cluster (see
// NATSTopology): every tenant shares N interest-retention ingest partitions,
// a history stream that sources them, and one dead-letter stream. It never
// creates, changes, purges or deletes a stream or a durable. The only
// JetStream objects it creates are auto-expiring consumers on the history
// stream: one per Subscribe (the hub bridge) and one per replay.
type ExternalNATS struct {
	topo NATSTopology
	nc   *nats.Conn
	js   jetstream.JetStream

	// partitions is partition p's stream name, dlq the dead-letter stream's,
	// both found by subject at boot.
	partitions []string
	dlq        string

	budgets    sync.Map // tenant.ID → int64
	budgetNote sync.Once
	// warnedGap holds the tenants PurgeAcked has warned about.
	warnedGap sync.Map // tenant.ID → struct{}
	// historyMaxAge is the history stream's max_age as last read.
	historyMaxAge atomic.Int64

	connected  atomic.Bool
	topologyOK atomic.Bool
	sources    atomic.Pointer[[]sourceState]
	gauges     metric.Registration

	mu       sync.Mutex
	nextID   int
	stoppers map[int]func()

	// stopping ends the watch loop's checks at Close, which waits for
	// loopDone; connClosed is closed by the client's closed callback.
	stopping             context.Context
	stop                 context.CancelFunc
	loopDone, connClosed chan struct{}
	closeOnce            sync.Once
}

// sourceState is one history source as last read. active is the time since
// it last heard from its partition, negative when it never attached: after
// a NATS restart it keeps counting up until the source re-attaches (~10s),
// rather than reading as detached.
type sourceState struct {
	name   string
	active time.Duration
	lag    uint64
}

var _ Broker = (*ExternalNATS)(nil)

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
		topo:       topo,
		stoppers:   map[int]func(){},
		loopDone:   make(chan struct{}),
		connClosed: make(chan struct{}),
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
		e.js, err = jetstream.NewWithDomain(e.nc, cfg.JSDomain)
	} else {
		e.js, err = jetstream.New(e.nc)
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
	findings, err := awaitNATSTopology(bootCtx, e.js, topo, wait)
	if err == nil {
		err = e.resolveStreams(bootCtx)
	}
	if err != nil {
		if !e.nc.IsConnected() {
			err = fmt.Errorf("%w: not connected to %s: %w", ErrUnavailable, redactURLs(cfg.URLs), errors.Join(e.nc.LastError(), err))
		}
		e.nc.Close()
		return nil, err
	}
	for _, f := range findings {
		slog.Warn("mq: nats topology: "+f.String(), "component", "nats")
	}
	e.topologyOK.Store(true)
	if err := e.readSources(ctx); err != nil {
		e.nc.Close()
		return nil, err
	}
	if e.gauges, err = e.registerGauges(); err != nil {
		e.nc.Close()
		return nil, fmt.Errorf("register mq gauges: %w", err)
	}
	e.stopping, e.stop = context.WithCancel(context.Background()) //nolint:gosec // G118: Close calls it
	go e.watch(orDefault(cfg.recheckEvery, topologyRecheck), orDefault(cfg.sourcesEvery, sourcesPoll))
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
		nats.DrainTimeout(natsDrainTimeout),
		nats.ConnectHandler(func(*nats.Conn) { e.connected.Store(true) }),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			e.connected.Store(false)
			slog.Warn("mq: disconnected from nats; reconnecting", "component", "nats", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			e.connected.Store(true)
			slog.Info("mq: reconnected to nats", "component", "nats", "url", nc.ConnectedUrlRedacted())
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			e.connected.Store(false)
			close(e.connClosed)
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
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
	return nil
}

// watch re-checks the topology and polls the history's sources until Close.
func (e *ExternalNATS) watch(recheck, poll time.Duration) {
	defer close(e.loopDone)
	topology := time.NewTicker(recheck)
	defer topology.Stop()
	sources := time.NewTicker(poll)
	defer sources.Stop()
	for {
		select {
		case <-e.stopping.Done():
			return
		case <-topology.C:
			e.recheck()
		case <-sources.C:
			ctx, cancel := context.WithTimeout(e.stopping, recheckTimeout)
			if err := e.readSources(ctx); err != nil {
				slog.Warn("mq: read the nats history stream", "component", "nats", "error", err)
			}
			cancel()
		}
	}
}

// recheck verifies the topology again, reporting the outcome on the gauge
// and in the log. A history source that has not attached yet is transient,
// as is one re-attaching after a NATS restart (~10s): both show on the source
// gauges instead. It skips a check while disconnected, which has a gauge of
// its own: the server version reads as empty then, a false fault.
func (e *ExternalNATS) recheck() {
	if !e.nc.IsConnected() {
		return
	}
	ctx, cancel := context.WithTimeout(e.stopping, recheckTimeout)
	defer cancel()
	findings, err := verifyNATSTopology(ctx, e.js, e.topo)
	if !e.nc.IsConnected() {
		return
	}
	if err != nil {
		slog.Warn("mq: nats topology re-check could not run", "component", "nats", "error", err)
		return
	}
	var faults []string
	for _, f := range findings {
		if f.Severity == FindingRequired && !f.transient {
			faults = append(faults, f.String())
		}
	}
	was := e.topologyOK.Swap(len(faults) == 0)
	switch {
	case len(faults) > 0:
		slog.Error("mq: nats topology no longer matches what WaveHouse needs", "component", "nats", "findings", faults)
	case !was:
		slog.Info("mq: nats topology matches again", "component", "nats")
	}
}

// readSources reads the history stream's max_age and the state of its
// sources.
func (e *ExternalNATS) readSources(ctx context.Context) error {
	s, err := e.js.Stream(ctx, e.topo.HistoryStream)
	if err != nil {
		return fmt.Errorf("history stream %s: %w", e.topo.HistoryStream, err)
	}
	info := s.CachedInfo()
	e.historyMaxAge.Store(int64(info.Config.MaxAge))
	states := make([]sourceState, 0, len(info.Sources))
	for _, src := range info.Sources {
		states = append(states, sourceState{name: src.Name, active: src.Active, lag: src.Lag})
	}
	e.sources.Store(&states)
	return nil
}

// registerGauges reports the connection, the topology check and the history
// sources. The source lag matters beyond SSE: a source holds each row on its
// partition until the history has it, so a history that stops copying fills
// the partitions and refuses ingest.
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
	active, err := meter.Float64ObservableGauge("wavehouse_mq_history_source_last_active_seconds",
		metric.WithDescription("Seconds since the history stream's source last heard from an ingest partition; -1 if it never attached"))
	if err != nil {
		return nil, err
	}
	lag, err := meter.Int64ObservableGauge("wavehouse_mq_history_source_lag",
		metric.WithDescription("Messages on an ingest partition the history stream has yet to copy"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(connected, boolGauge(e.connected.Load()))
		o.ObserveInt64(topologyOK, boolGauge(e.topologyOK.Load()))
		if states := e.sources.Load(); states != nil {
			for _, s := range *states {
				set := metric.WithAttributes(attribute.String("source", s.name))
				o.ObserveFloat64(active, activeSeconds(s.active), set)
				o.ObserveInt64(lag, int64(min(s.lag, uint64(1<<62))), set) //nolint:gosec // capped
			}
		}
		return nil
	}, connected, topologyOK, active, lag)
}

// activeSeconds is a source's time since last contact as the gauge reports
// it: -1 for a source that never attached, which the server reports as -1ns.
func activeSeconds(d time.Duration) float64 {
	if d < 0 {
		return -1
	}
	return d.Seconds()
}

func boolGauge(b bool) int64 {
	if b {
		return 1
	}
	return 0
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

// Publish stores data on topic's subject in its tenant's partition, bounded
// by the topology's PublishTimeout per attempt. A publish that gets no answer
// is sent again up to twice with the same Nats-Msg-Id, which the partition's
// duplicate window stores once. A partition at max_bytes, or a topic at its
// max_msgs_per_subject, is ErrQueueFull; no answer, a lost connection, or a
// partition stream that is gone is ErrUnavailable. It never creates anything.
func (e *ExternalNATS) Publish(ctx context.Context, topic Topic, data []byte, opts ...PublishOpt) error {
	subj, err := natsIngestSubject(e.topo.Prefix, e.topo.Partitions, topic)
	if err != nil {
		return err
	}
	return e.publish(ctx, subj, e.partitions[partitionOf(topic.Tenant, e.topo.Partitions)], data, opts)
}

// DeadLetter parks msg's data on the shared dead-letter stream under its
// topic, with a fresh Nats-Msg-Id. It does not ack msg.
func (e *ExternalNATS) DeadLetter(ctx context.Context, msg *Message, opts ...PublishOpt) error {
	return e.publish(ctx, e.topo.Prefix+".dlq."+msg.topicKey, e.dlq, msg.Data, opts)
}

func (e *ExternalNATS) publish(ctx context.Context, subj, stream string, data []byte, opts []PublishOpt) error {
	msg := nats.NewMsg(subj)
	msg.Data = data
	headers := Headers{}
	for _, opt := range opts {
		opt(headers)
	}
	observability.InjectHeaders(ctx, headers)
	msg.Header = nats.Header(headers)
	pubOpts := []jetstream.PublishOpt{
		jetstream.WithMsgID(nuid.Next()),
		jetstream.WithExpectStream(stream),
		jetstream.WithRetryAttempts(0),
	}

	var err error
	for attempt := 0; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, e.topo.PublishTimeout)
		_, err = e.js.PublishMsg(actx, msg, pubOpts...)
		cancel()
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

// wrapMsg adapts a delivered message: its topic key is the subject with the
// prefix and partition stripped.
func (e *ExternalNATS) wrapMsg(ctx context.Context, m jetstream.Msg, acks bool) *Message {
	key, ok := natsTopicKey(e.topo.Prefix, m.Subject())
	if !ok {
		key = m.Subject()
	}
	if !acks {
		return newMessage(ctx, key, m.Data(), time.Now(), nil, nil, nil)
	}
	return newMessage(ctx, key, m.Data(), time.Now(), m.DoubleAck, m.Ack, m.Nak)
}

// Subscribe delivers every ingest event stored on the history stream from
// now on to handler, on one goroutine, until ctx is done or Close. It reads
// through an ordered ack-less consumer of its own, which skips nothing across
// reconnects and expires once this process is gone: every pod's hub needs
// every event, which one shared durable would split between them. So
// consumerName names nothing here, and a handler error has no redelivery to
// ask for: it is logged.
func (e *ExternalNATS) Subscribe(ctx context.Context, consumerName string, handler func(msg *Message) error) error {
	cons, err := e.js.OrderedConsumer(ctx, e.topo.HistoryStream, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{e.topo.Prefix + ".ingest.>"},
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		InactiveThreshold: hubInactiveThreshold,
	})
	if err != nil {
		return fmt.Errorf("history consumer: %w", e.apiError(err))
	}
	cc, err := cons.Consume(func(m jetstream.Msg) {
		msg := e.wrapMsg(observability.ExtractHeaders(ctx, m.Headers()), m, false)
		if err := handler(msg); err != nil {
			slog.Warn("mq: subscriber could not handle an event", "component", "nats", "consumer", consumerName, "topic", msg.TopicKey(), "error", err)
		}
	}, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		slog.Warn("mq: history consumer reported an error", "component", "nats", "consumer", consumerName, "error", err)
	}))
	if err != nil {
		return fmt.Errorf("consume history: %w", err)
	}
	untrack := e.track(cc.Stop)
	go func() {
		select {
		case <-ctx.Done():
		case <-cc.Closed():
		}
		untrack()
		cc.Stop()
	}()
	return nil
}

// apiError marks a JetStream request that got no answer as ErrUnavailable.
func (e *ExternalNATS) apiError(err error) error {
	if noAnswer(err) || errors.Is(err, nats.ErrConnectionClosed) {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}

// durable maps the durable a caller names to the operator's: the ingest
// worker's name, or the operator's own.
func (e *ExternalNATS) durable(name string) (string, bool) {
	if name == workerDurable || name == e.topo.IngestConsumer {
		return e.topo.IngestConsumer, true
	}
	return "", false
}

// CreateConsumer finds the operator's durable on every partition — it never
// creates one — and checks it against cfg: its ack_wait must cover
// cfg.AckWait and its max_ack_pending must be set. A durable name that does
// not map to the operator's is ErrConsumerNotFound.
func (e *ExternalNATS) CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	name, ok := e.durable(cfg.Durable)
	if !ok {
		return nil, fmt.Errorf("consumer %q: %w: the ingest durable is %q", cfg.Durable, ErrConsumerNotFound, e.topo.IngestConsumer)
	}
	c := &externalConsumer{e: e, ctx: ctx, failed: make(chan error, 1)}
	for p, stream := range e.partitions {
		h, err := e.js.Consumer(ctx, stream, name)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return nil, fmt.Errorf("partition %d: consumer %s/%s: %w", p, stream, name, ErrConsumerNotFound)
		}
		if err != nil {
			return nil, fmt.Errorf("partition %d: consumer %s/%s: %w", p, stream, name, e.apiError(err))
		}
		have := h.CachedInfo().Config
		if have.AckWait < cfg.AckWait {
			return nil, fmt.Errorf("consumer %s/%s: ack_wait %s is shorter than the %s asked for", stream, name, have.AckWait, cfg.AckWait)
		}
		if have.MaxAckPending <= 0 {
			return nil, fmt.Errorf("consumer %s/%s: max_ack_pending must be set", stream, name)
		}
		c.handles = append(c.handles, h)
	}
	return c, nil
}

// externalConsumer is the operator's durable on every partition.
type externalConsumer struct {
	e       *ExternalNATS
	ctx     context.Context
	handles []jetstream.Consumer
	failed  chan error
	// reported and stopped keep failed to one error, none after stop.
	reported, stopped atomic.Bool
}

// Consume pulls from every partition, each on its own delivery goroutine,
// splitting prefetch between them (at least one each). A partition's
// delivery that the client ends on its own — the durable deleted, the
// connection closed for good — is reported on failed.
func (c *externalConsumer) Consume(handler func(msg *Message), prefetch int) (func(), <-chan error, error) {
	var (
		mu      sync.Mutex
		running []jetstream.ConsumeContext
	)
	stopAll := func() {
		c.stopped.Store(true)
		mu.Lock()
		defer mu.Unlock()
		for _, cc := range running {
			cc.Stop()
		}
	}
	for p, h := range c.handles {
		// The client calls this for passing conditions too, and stops the
		// subscription itself on a terminal one: closing without our stop is
		// what terminal means (see fanIn.run).
		var lastErr atomic.Pointer[error]
		opts := []jetstream.PullConsumeOpt{
			jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
				lastErr.Store(&err)
				slog.Warn("mq: consumer reported an error", "component", "nats", "partition", p, "error", err)
			}),
		}
		if prefetch > 0 {
			opts = append(opts, jetstream.PullMaxMessages(max(1, prefetch/len(c.handles))))
		}
		cc, err := h.Consume(func(m jetstream.Msg) { handler(c.e.wrapMsg(c.ctx, m, true)) }, opts...)
		if err != nil {
			stopAll()
			return nil, nil, fmt.Errorf("consume partition %d: %w", p, err)
		}
		mu.Lock()
		running = append(running, cc)
		mu.Unlock()
		go func() {
			<-cc.Closed()
			if c.stopped.Load() {
				return
			}
			reason := ErrDeliveryEnded
			if r := lastErr.Load(); r != nil {
				reason = fmt.Errorf("%w: %w", ErrDeliveryEnded, *r)
			}
			c.fail(fmt.Errorf("partition %d: %w", p, reason))
		}()
	}
	untrack := c.e.track(stopAll)
	return func() {
		untrack()
		stopAll()
	}, c.failed, nil
}

func (c *externalConsumer) fail(err error) {
	if c.stopped.Load() || !c.reported.CompareAndSwap(false, true) {
		return
	}
	c.failed <- err
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

// PurgeAcked removes nothing: the partitions delete each row once it is
// acknowledged, and the history keeps what its max_age allows, both the
// operator's. It warns once per tenant whose cutoff is older than the history
// keeps — a gap window SSE replay cannot serve in full. No I/O: max_age is
// read by the periodic source poll.
func (e *ExternalNATS) PurgeAcked(_ context.Context, consumer string, olderThan map[tenant.ID]time.Time) (bool, error) {
	if _, ok := e.durable(consumer); !ok {
		return false, fmt.Errorf("consumer %q: %w", consumer, ErrConsumerNotFound)
	}
	maxAge := time.Duration(e.historyMaxAge.Load())
	if maxAge <= 0 {
		return false, nil
	}
	floor := time.Now().Add(-maxAge)
	for id, cutoff := range olderThan {
		if !cutoff.Before(floor) {
			continue
		}
		if _, warned := e.warnedGap.LoadOrStore(id, struct{}{}); !warned {
			slog.Warn("mq: the nats history keeps less than this tenant's gap window; SSE replay serves only the last max_age",
				"component", "nats", "tenant", id, "history_stream", e.topo.HistoryStream, "max_age", maxAge, "gap_window", time.Since(cutoff).Round(time.Second))
		}
	}
	return false, nil
}

// ReplaySince reads topic's events from the history stream, stored at or
// after since, through an ack-less consumer of its own that expires once
// idle, in batches, until send returns false or the events the server
// counted when the replay began are sent. Anything older than the history's max_age is gone.
// A pull that fails before then is an error; a done ctx returns ctx's error.
func (e *ExternalNATS) ReplaySince(ctx context.Context, topic Topic, since time.Time, send func(data []byte) bool) error {
	subj, err := natsIngestSubject(e.topo.Prefix, e.topo.Partitions, topic)
	if err != nil {
		return err
	}
	cons, err := e.js.CreateConsumer(ctx, e.topo.HistoryStream, jetstream.ConsumerConfig{
		FilterSubject:     subj,
		DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
		OptStartTime:      &since,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: replayInactiveThreshold,
		MemoryStorage:     true,
		Replicas:          1,
	})
	if err != nil {
		return fmt.Errorf("replay consumer: %w", e.apiError(err))
	}
	defer e.dropConsumer(cons.CachedInfo().Name)

	// Counted once: events published during the replay reach the SSE client
	// through the live subscription it registered first, so chasing them here
	// would only send duplicates.
	remaining := cons.CachedInfo().NumPending
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := cons.Fetch(int(min(remaining, replayBatch)), jetstream.FetchMaxWait(replayPullWait)) //nolint:gosec // capped
		if err != nil {
			return fmt.Errorf("replay fetch: %w", err)
		}
		got := 0
		for msg := range batch.Messages() {
			got++
			remaining--
			if !send(msg.Data()) {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if e.nc.IsClosed() || e.nc.IsDraining() {
				return fmt.Errorf("replay: %w", nats.ErrConnectionClosed)
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return fmt.Errorf("replay fetch: %w", err)
		}
		if got == 0 {
			// The history dropped what was left (max_age) while connected:
			// that is caught up. A pull that raced the connection closing
			// can end the same way, and that is not.
			if e.nc.IsConnected() {
				return nil
			}
			return fmt.Errorf("replay fetch: %w", nats.ErrConnectionClosed)
		}
	}
	return nil
}

// dropConsumer deletes a finished replay's consumer rather than leaving it to
// expire, best effort.
func (e *ExternalNATS) dropConsumer(name string) {
	if !e.nc.IsConnected() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = e.js.DeleteConsumer(ctx, e.topo.HistoryStream, name)
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

// Stats reports this process's connection and its inbound message count.
func (e *ExternalNATS) Stats() (observability.MQStats, error) {
	s := e.nc.Stats()
	return observability.MQStats{
		Connections: boolGauge(e.nc.IsConnected()),
		InMsgs:      int64(min(s.InMsgs, uint64(1<<62))), //nolint:gosec // capped
	}, nil
}

// Close stops every consumer, so none reports failed, then drains the
// connection so pending acks are flushed. Safe to call more than once.
func (e *ExternalNATS) Close() error {
	e.closeOnce.Do(func() {
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
