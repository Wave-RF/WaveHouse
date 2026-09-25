package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// The external-NATS fixture: an in-process server listening on TCP whose
// accounts, users and permissions are the shipped Helm values' config.merge
// block, verbatim, and whose streams and consumers are the shipped nack
// manifests. So the tests exercise what an operator deploys, not a copy of it.

const (
	shippedManifests = "../../deployments/nats/jetstream.yaml"
	shippedValues    = "../../deployments/nats/values.yaml"
)

// fixtureUser is the password every fixture user gets in place of the Helm
// values' secret reference.
func fixturePassword(user string) string { return "pw-" + user }

type natsFixture struct {
	server *natsserver.Server
	// opts is what server was started with, for a restart on the same port
	// and store.
	opts *natsserver.Options
	// admin is nack's stand-in: the operator's user, with full access.
	admin jetstream.JetStream
}

// helmVariable matches the chart's `<< $VAR >>` unquoted config variable.
var helmVariable = regexp.MustCompile(`^<< *\$[A-Za-z0-9_]+ *>>$`)

// newNATSFixture starts a server configured from the shipped Helm values,
// shut down by the test framework.
func newNATSFixture(t *testing.T) *natsFixture {
	t.Helper()
	raw, err := os.ReadFile(shippedValues)
	require.NoError(t, err)
	var values struct {
		Config struct {
			Merge map[string]any `yaml:"merge"`
		} `yaml:"config"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &values))
	merge := values.Config.Merge
	require.NotEmpty(t, merge, "values.yaml has no config.merge")

	// The chart writes nats.conf as JSON; the users' passwords are Secret
	// references resolved at runtime, which the fixture fills in.
	accounts, _ := merge["accounts"].(map[string]any)
	require.NotEmpty(t, accounts, "values.yaml config.merge has no accounts")
	for _, acc := range accounts {
		users, _ := acc.(map[string]any)["users"].([]any)
		for _, u := range users {
			user := u.(map[string]any)
			if pw, _ := user["password"].(string); helmVariable.MatchString(pw) {
				user["password"] = fixturePassword(user["user"].(string))
			}
		}
	}
	dir := t.TempDir()
	conf := map[string]any{"jetstream": map[string]any{"store_dir": dir}}
	for k, v := range merge {
		conf[k] = v
	}
	// NATS config strings take no \u escapes, which json.Marshal writes for
	// the '>' of every wildcard.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	require.NoError(t, enc.Encode(conf))
	confPath := filepath.Join(dir, "nats.conf")
	require.NoError(t, os.WriteFile(confPath, buf.Bytes(), 0o600))
	opts, err := natsserver.ProcessConfigFile(confPath)
	require.NoError(t, err)
	opts.Host, opts.Port, opts.NoSigs, opts.NoLog = "127.0.0.1", -1, true, true
	// The manifests' byte caps are reserved against these; a test machine has
	// less disk (and memory, for the storage mutations) than a cluster.
	opts.JetStreamMaxStore, opts.JetStreamMaxMemory = 1<<50, 1<<50

	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	s.Start()
	require.True(t, s.ReadyForConnections(10*time.Second), "nats server not ready")
	t.Cleanup(s.Shutdown)
	f := &natsFixture{server: s, opts: opts}
	f.admin = f.connect(t, "nack")
	return f
}

// connect opens a JetStream context as user, closed by the test framework.
// The wavehouse user gets the inbox prefix its permissions allow.
func (f *natsFixture) connect(t *testing.T, user string, opts ...nats.Option) jetstream.JetStream {
	t.Helper()
	opts = append([]nats.Option{nats.UserInfo(user, fixturePassword(user))}, opts...)
	if user == "wavehouse" {
		opts = append(opts, nats.CustomInboxPrefix(natsInboxPrefix(DefaultNATSSubjectPrefix)))
	}
	nc, err := nats.Connect(f.server.ClientURL(), opts...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

// fixtureTopology is a set of stream and consumer configs to create, in
// order: each stream, then its consumers.
type fixtureTopology struct {
	streams   []jetstream.StreamConfig
	consumers map[string][]jetstream.ConsumerConfig // by stream name
}

// shippedTopology is the shipped manifests (N=4) at one replica, which is
// all a single server can hold.
func shippedTopology(t *testing.T) *fixtureTopology {
	t.Helper()
	tp := loadNATSManifests(t, shippedManifests)
	for i := range tp.streams {
		tp.streams[i].Replicas = 1
	}
	return tp
}

// loadNATSManifests parses nack Stream and Consumer resources into the
// JetStream configs nack would create from them.
func loadNATSManifests(t *testing.T, path string) *fixtureTopology {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // G304: a shipped manifest or one the test wrote
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	tp := &fixtureTopology{consumers: map[string][]jetstream.ConsumerConfig{}}
	dec := yaml.NewDecoder(f)
	for {
		var doc struct {
			Kind string    `yaml:"kind"`
			Spec yaml.Node `yaml:"spec"`
		}
		if err := dec.Decode(&doc); err != nil {
			require.ErrorContains(t, err, "EOF")
			break
		}
		switch doc.Kind {
		case "Stream":
			var s nackStream
			require.NoError(t, doc.Spec.Decode(&s))
			tp.streams = append(tp.streams, streamFromNack(t, s))
		case "Consumer":
			var c nackConsumer
			require.NoError(t, doc.Spec.Decode(&c))
			tp.consumers[c.StreamName] = append(tp.consumers[c.StreamName], consumerFromNack(t, c))
		default:
			t.Fatalf("%s: unexpected kind %q", path, doc.Kind)
		}
	}
	return tp
}

func fixtureDuration(t *testing.T, s string) time.Duration {
	t.Helper()
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	require.NoError(t, err)
	return d
}

func fixtureEnum[T any](t *testing.T, field, value string, values map[string]T) T {
	t.Helper()
	v, ok := values[value]
	require.True(t, ok, "%s: unknown value %q", field, value)
	return v
}

func streamFromNack(t *testing.T, s nackStream) jetstream.StreamConfig {
	t.Helper()
	cfg := jetstream.StreamConfig{
		Name:     s.Name,
		Subjects: s.Subjects,
		Retention: fixtureEnum(t, "retention", s.Retention, map[string]jetstream.RetentionPolicy{
			"limits": jetstream.LimitsPolicy, "interest": jetstream.InterestPolicy, "workqueue": jetstream.WorkQueuePolicy,
		}),
		Discard: fixtureEnum(t, "discard", s.Discard, map[string]jetstream.DiscardPolicy{
			"old": jetstream.DiscardOld, "new": jetstream.DiscardNew,
		}),
		DiscardNewPerSubject: s.DiscardPerSubject,
		MaxBytes:             s.MaxBytes,
		MaxAge:               fixtureDuration(t, s.MaxAge),
		MaxMsgsPerSubject:    s.MaxMsgsPerSubject,
		Storage: fixtureEnum(t, "storage", s.Storage, map[string]jetstream.StorageType{
			"file": jetstream.FileStorage, "memory": jetstream.MemoryStorage,
		}),
		Replicas:   s.Replicas,
		Duplicates: fixtureDuration(t, s.DuplicateWindow),
		DenyPurge:  s.DenyPurge,
		DenyDelete: s.DenyDelete,
		Metadata:   s.Metadata,
	}
	for _, src := range s.Sources {
		cfg.Sources = append(cfg.Sources, &jetstream.StreamSource{Name: src.Name})
	}
	return cfg
}

func consumerFromNack(t *testing.T, c nackConsumer) jetstream.ConsumerConfig {
	t.Helper()
	return jetstream.ConsumerConfig{
		Durable: c.DurableName,
		DeliverPolicy: fixtureEnum(t, "deliverPolicy", c.DeliverPolicy, map[string]jetstream.DeliverPolicy{
			"all": jetstream.DeliverAllPolicy, "last": jetstream.DeliverLastPolicy, "new": jetstream.DeliverNewPolicy,
		}),
		AckPolicy: fixtureEnum(t, "ackPolicy", c.AckPolicy, map[string]jetstream.AckPolicy{
			"none": jetstream.AckNonePolicy, "all": jetstream.AckAllPolicy, "explicit": jetstream.AckExplicitPolicy,
		}),
		AckWait:       fixtureDuration(t, c.AckWait),
		MaxDeliver:    c.MaxDeliver,
		MaxAckPending: c.MaxAckPending,
		FilterSubject: c.FilterSubject,
	}
}

// stream is the named stream's config, to mutate before apply.
func (tp *fixtureTopology) stream(t *testing.T, name string) *jetstream.StreamConfig {
	t.Helper()
	i := slices.IndexFunc(tp.streams, func(s jetstream.StreamConfig) bool { return s.Name == name })
	require.GreaterOrEqual(t, i, 0, "no stream %s in the fixture", name)
	return &tp.streams[i]
}

// consumer is the one consumer on the named stream, to mutate before apply.
func (tp *fixtureTopology) consumer(t *testing.T, stream string) *jetstream.ConsumerConfig {
	t.Helper()
	require.Len(t, tp.consumers[stream], 1, "consumers on %s", stream)
	return &tp.consumers[stream][0]
}

// drop removes the named stream and its consumers.
func (tp *fixtureTopology) drop(name string) {
	tp.streams = slices.DeleteFunc(tp.streams, func(s jetstream.StreamConfig) bool { return s.Name == name })
	delete(tp.consumers, name)
}

// apply creates tp as the operator would, and waits for every history source
// to attach: a row acked on a partition before its source exists never
// reaches the history.
func (f *natsFixture) apply(t *testing.T, tp *fixtureTopology) {
	t.Helper()
	require.NoError(t, f.create(t.Context(), tp))
	for _, cfg := range tp.streams {
		for _, src := range cfg.Sources {
			f.awaitSource(t, cfg.Name, src.Name, tp.consumers[src.Name])
		}
	}
}

// reset deletes every stream, and with them their consumers.
func (f *natsFixture) reset(t *testing.T) {
	t.Helper()
	names := f.admin.StreamNames(t.Context())
	var all []string
	for name := range names.Name() {
		all = append(all, name)
	}
	require.NoError(t, names.Err())
	for _, name := range all {
		require.NoError(t, f.admin.DeleteStream(t.Context(), name))
	}
}

// create creates tp's streams and consumers without waiting for anything, so
// a goroutine can call it.
func (f *natsFixture) create(ctx context.Context, tp *fixtureTopology) error {
	for _, cfg := range tp.streams {
		s, err := f.admin.CreateStream(ctx, cfg)
		if err != nil {
			return fmt.Errorf("create stream %s: %w", cfg.Name, err)
		}
		for _, c := range tp.consumers[cfg.Name] {
			if _, err := s.CreateConsumer(ctx, c); err != nil {
				return fmt.Errorf("create consumer %s/%s: %w", cfg.Name, c.Durable, err)
			}
		}
	}
	return nil
}

// awaitSource waits for stream's source consumer on origin to appear beside
// origin's own consumers. Only an interest-retention origin lists it; there it
// is what keeps an acked row until the history has copied it.
func (f *natsFixture) awaitSource(t *testing.T, stream, origin string, own []jetstream.ConsumerConfig) {
	t.Helper()
	ctx := t.Context()
	s, err := f.admin.Stream(ctx, origin)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return // a source the fixture left out on purpose
	}
	require.NoError(t, err)
	cfg := s.CachedInfo().Config
	if cfg.Retention != jetstream.InterestPolicy {
		return
	}
	if cfg.MaxConsumers > 0 && cfg.MaxConsumers <= len(own) {
		return // a source the fixture keeps out on purpose
	}
	require.Eventually(t, func() bool {
		n := 0
		for range s.ListConsumers(ctx).Info() {
			n++
		}
		return n > len(own)
	}, 10*time.Second, 10*time.Millisecond, "%s's source on %s never attached", stream, origin)
}
