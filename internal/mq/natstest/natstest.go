// Package natstest stands up NATS the way an operator deploys it for
// mq.backend: nats: the shipped Helm values' accounts, users and permissions
// (deployments/nats/values.yaml) and the shipped nack manifests
// (deployments/nats/jetstream.yaml). internal/mq's own fixture builds on it,
// and so do tests outside internal/mq, which may not import NATS themselves
// (depguard's mq boundary): they get a server URL and the two users'
// passwords, and act on the topology only through this package.
//
// It is test code that lives outside *_test.go so those tests can import it,
// like mqtest; nothing in the binary imports it.
package natstest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

// The shipped values' two users: nack, the operator's JetStream controller
// with full access, and wavehouse, WaveHouse with exactly the permissions it
// needs.
const (
	OperatorUser  = "nack"
	WaveHouseUser = "wavehouse"
)

// Password is the password every user gets in place of the Helm values'
// Secret reference.
func Password(user string) string { return "pw-" + user }

// repoFile is path under the repository root.
func repoFile(path string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", path)
}

// ShippedValues and ShippedManifests are the files an operator deploys.
func ShippedValues() string    { return repoFile("deployments/nats/values.yaml") }
func ShippedManifests() string { return repoFile("deployments/nats/jetstream.yaml") }

// helmVariable matches the chart's `<< $VAR >>` unquoted config variable.
var helmVariable = regexp.MustCompile(`^<< *\$[A-Za-z0-9_]+ *>>$`)

// ServerConfig renders the Helm values' config.merge block as a nats.conf
// (the chart writes it as JSON too), each Secret-referenced password set to
// Password(user), and JetStream on file storage under storeDir. The store's
// limits are lifted: the manifests reserve a cluster's worth of bytes, which
// a test machine does not have.
func ServerConfig(valuesPath, storeDir string) ([]byte, error) {
	raw, err := os.ReadFile(valuesPath) //nolint:gosec // G304: a shipped file or one a test wrote
	if err != nil {
		return nil, err
	}
	var values struct {
		Config struct {
			Merge map[string]any `yaml:"merge"`
		} `yaml:"config"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("%s: %w", valuesPath, err)
	}
	merge := values.Config.Merge
	accounts, _ := merge["accounts"].(map[string]any)
	if len(accounts) == 0 {
		return nil, fmt.Errorf("%s: config.merge has no accounts", valuesPath)
	}
	for _, acc := range accounts {
		users, _ := acc.(map[string]any)["users"].([]any)
		for _, u := range users {
			user, _ := u.(map[string]any)
			if pw, _ := user["password"].(string); helmVariable.MatchString(pw) {
				name, _ := user["user"].(string)
				user["password"] = Password(name)
			}
		}
	}
	conf := map[string]any{"jetstream": map[string]any{
		"store_dir": storeDir, "max_file_store": int64(1) << 50, "max_memory_store": int64(1) << 40,
	}}
	for k, v := range merge {
		conf[k] = v
	}
	// NATS config strings take no \u escapes, which json.Marshal writes for
	// the '>' of every wildcard.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(conf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Manifests is a set of nack Stream and Consumer resources as the JetStream
// configs nack would create from them, in manifest order.
type Manifests struct {
	Streams   []jetstream.StreamConfig
	Consumers map[string][]jetstream.ConsumerConfig // by stream name
}

// The nack (jetstream.nats.io/v1beta2) fields the shipped manifests use.
// Decoding is strict, so a field the generator starts writing fails here
// rather than being dropped from every fixture.
type nackStream struct {
	Name              string                  `yaml:"name"`
	Subjects          []string                `yaml:"subjects"`
	Sources           []struct{ Name string } `yaml:"sources"`
	Retention         string                  `yaml:"retention"`
	Discard           string                  `yaml:"discard"`
	DiscardPerSubject bool                    `yaml:"discardPerSubject"`
	MaxBytes          int64                   `yaml:"maxBytes"`
	MaxAge            string                  `yaml:"maxAge"`
	MaxMsgsPerSubject int64                   `yaml:"maxMsgsPerSubject"`
	Storage           string                  `yaml:"storage"`
	Replicas          int                     `yaml:"replicas"`
	DuplicateWindow   string                  `yaml:"duplicateWindow"`
	DenyPurge         bool                    `yaml:"denyPurge"`
	DenyDelete        bool                    `yaml:"denyDelete"`
	Metadata          map[string]string       `yaml:"metadata"`
	PreventDelete     bool                    `yaml:"preventDelete"`
}

type nackConsumer struct {
	StreamName    string `yaml:"streamName"`
	DurableName   string `yaml:"durableName"`
	DeliverPolicy string `yaml:"deliverPolicy"`
	AckPolicy     string `yaml:"ackPolicy"`
	AckWait       string `yaml:"ackWait"`
	MaxDeliver    int    `yaml:"maxDeliver"`
	MaxAckPending int    `yaml:"maxAckPending"`
	FilterSubject string `yaml:"filterSubject"`
	PreventDelete bool   `yaml:"preventDelete"`
}

// LoadManifests parses the nack resources at path.
func LoadManifests(path string) (*Manifests, error) {
	f, err := os.Open(path) //nolint:gosec // G304: a shipped manifest or one a test wrote
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	m := &Manifests{Consumers: map[string][]jetstream.ConsumerConfig{}}
	dec := yaml.NewDecoder(f)
	for {
		var doc struct {
			Kind string    `yaml:"kind"`
			Spec yaml.Node `yaml:"spec"`
		}
		if err := dec.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				return m, nil
			}
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		switch doc.Kind {
		case "Stream":
			var s nackStream
			if err := decodeStrict(&doc.Spec, &s); err != nil {
				return nil, fmt.Errorf("%s: stream: %w", path, err)
			}
			cfg, err := streamConfig(s)
			if err != nil {
				return nil, fmt.Errorf("%s: stream %s: %w", path, s.Name, err)
			}
			m.Streams = append(m.Streams, cfg)
		case "Consumer":
			var c nackConsumer
			if err := decodeStrict(&doc.Spec, &c); err != nil {
				return nil, fmt.Errorf("%s: consumer: %w", path, err)
			}
			cfg, err := consumerConfig(c)
			if err != nil {
				return nil, fmt.Errorf("%s: consumer %s/%s: %w", path, c.StreamName, c.DurableName, err)
			}
			m.Consumers[c.StreamName] = append(m.Consumers[c.StreamName], cfg)
		default:
			return nil, fmt.Errorf("%s: unexpected kind %q", path, doc.Kind)
		}
	}
}

// decodeStrict decodes node into v, refusing a field v does not declare.
func decodeStrict(node *yaml.Node, v any) error {
	raw, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	return dec.Decode(v)
}

func duration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func enum[T any](field, value string, values map[string]T) (T, error) {
	v, ok := values[value]
	if !ok {
		return v, fmt.Errorf("%s: unknown value %q", field, value)
	}
	return v, nil
}

func streamConfig(s nackStream) (jetstream.StreamConfig, error) {
	cfg := jetstream.StreamConfig{
		Name:                 s.Name,
		Subjects:             s.Subjects,
		DiscardNewPerSubject: s.DiscardPerSubject,
		MaxBytes:             s.MaxBytes,
		MaxMsgsPerSubject:    s.MaxMsgsPerSubject,
		Replicas:             s.Replicas,
		DenyPurge:            s.DenyPurge,
		DenyDelete:           s.DenyDelete,
		Metadata:             s.Metadata,
	}
	var err error
	var errs []error
	cfg.Retention, err = enum("retention", s.Retention, map[string]jetstream.RetentionPolicy{
		"limits": jetstream.LimitsPolicy, "interest": jetstream.InterestPolicy, "workqueue": jetstream.WorkQueuePolicy,
	})
	errs = append(errs, err)
	cfg.Discard, err = enum("discard", s.Discard, map[string]jetstream.DiscardPolicy{
		"old": jetstream.DiscardOld, "new": jetstream.DiscardNew,
	})
	errs = append(errs, err)
	cfg.Storage, err = enum("storage", s.Storage, map[string]jetstream.StorageType{
		"file": jetstream.FileStorage, "memory": jetstream.MemoryStorage,
	})
	errs = append(errs, err)
	cfg.MaxAge, err = duration(s.MaxAge)
	errs = append(errs, err)
	cfg.Duplicates, err = duration(s.DuplicateWindow)
	errs = append(errs, err)
	for _, src := range s.Sources {
		cfg.Sources = append(cfg.Sources, &jetstream.StreamSource{Name: src.Name})
	}
	return cfg, errors.Join(errs...)
}

func consumerConfig(c nackConsumer) (jetstream.ConsumerConfig, error) {
	cfg := jetstream.ConsumerConfig{
		Durable:       c.DurableName,
		MaxDeliver:    c.MaxDeliver,
		MaxAckPending: c.MaxAckPending,
		FilterSubject: c.FilterSubject,
	}
	var err error
	var errs []error
	cfg.DeliverPolicy, err = enum("deliverPolicy", c.DeliverPolicy, map[string]jetstream.DeliverPolicy{
		"all": jetstream.DeliverAllPolicy, "last": jetstream.DeliverLastPolicy, "new": jetstream.DeliverNewPolicy,
	})
	errs = append(errs, err)
	cfg.AckPolicy, err = enum("ackPolicy", c.AckPolicy, map[string]jetstream.AckPolicy{
		"none": jetstream.AckNonePolicy, "all": jetstream.AckAllPolicy, "explicit": jetstream.AckExplicitPolicy,
	})
	errs = append(errs, err)
	cfg.AckWait, err = duration(c.AckWait)
	errs = append(errs, err)
	return cfg, errors.Join(errs...)
}

// SingleReplica sets every stream to one replica, which is all a single
// server can hold.
func (m *Manifests) SingleReplica() {
	for i := range m.Streams {
		m.Streams[i].Replicas = 1
	}
}

// Create creates m's streams and each one's consumers, in order, without
// waiting for anything.
func (m *Manifests) Create(ctx context.Context, js jetstream.JetStream) error {
	for _, cfg := range m.Streams {
		s, err := js.CreateStream(ctx, cfg)
		if err != nil {
			return fmt.Errorf("create stream %s: %w", cfg.Name, err)
		}
		for _, c := range m.Consumers[cfg.Name] {
			if _, err := s.CreateConsumer(ctx, c); err != nil {
				return fmt.Errorf("create consumer %s/%s: %w", cfg.Name, c.Durable, err)
			}
		}
	}
	return nil
}

// Apply creates or updates m's streams and each one's consumers, in order,
// as nack applying changed manifests would. What m leaves out is kept: the
// generated resources set preventDelete.
func (m *Manifests) Apply(ctx context.Context, js jetstream.JetStream) error {
	for _, cfg := range m.Streams {
		s, err := js.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			return fmt.Errorf("apply stream %s: %w", cfg.Name, err)
		}
		for _, c := range m.Consumers[cfg.Name] {
			if _, err := s.CreateOrUpdateConsumer(ctx, c); err != nil {
				return fmt.Errorf("apply consumer %s/%s: %w", cfg.Name, c.Durable, err)
			}
		}
	}
	return nil
}

// AwaitSources waits for every sourcing stream's source consumer to appear
// beside its origin's own consumers, until ctx ends. The server creates it
// asynchronously, and a row acked on an interest partition before it exists
// never reaches the history. Only an interest-retention origin lists it; an
// origin m leaves out, or keeps from gaining one (max_consumers), is skipped.
func (m *Manifests) AwaitSources(ctx context.Context, js jetstream.JetStream) error {
	for _, cfg := range m.Streams {
		for _, src := range cfg.Sources {
			if err := awaitSource(ctx, js, src.Name, len(m.Consumers[src.Name])); err != nil {
				return fmt.Errorf("%s's source on %s never attached: %w", cfg.Name, src.Name, err)
			}
		}
	}
	return nil
}

func awaitSource(ctx context.Context, js jetstream.JetStream, origin string, own int) error {
	s, err := js.Stream(ctx, origin)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	cfg := s.CachedInfo().Config
	if cfg.Retention != jetstream.InterestPolicy || (cfg.MaxConsumers > 0 && cfg.MaxConsumers <= own) {
		return nil
	}
	for {
		n := 0
		for range s.ListConsumers(ctx).Info() {
			n++
		}
		if n > own {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Operator is the operator's hand on a running server: nack's user.
type Operator struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// Connect connects to url as OperatorUser.
func Connect(url string) (*Operator, error) {
	nc, err := nats.Connect(url, nats.UserInfo(OperatorUser, Password(OperatorUser)))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Operator{nc: nc, js: js}, nil
}

// JetStream is the operator's JetStream context.
func (o *Operator) JetStream() jetstream.JetStream { return o.js }

// Close closes the connection.
func (o *Operator) Close() { o.nc.Close() }

// ApplyShipped creates the shipped manifests at one replica, as nack would,
// and waits up to a minute for the history's sources to attach.
func (o *Operator) ApplyShipped(ctx context.Context) error {
	m, err := LoadManifests(ShippedManifests())
	if err != nil {
		return err
	}
	m.SingleReplica()
	if err := m.Create(ctx, o.js); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return m.AwaitSources(ctx, o.js)
}

// DeleteDurable deletes the durable on every stream that has it in the
// shipped manifests, as an operator could while WaveHouse consumes it.
func (o *Operator) DeleteDurable(ctx context.Context, durable string) error {
	m, err := LoadManifests(ShippedManifests())
	if err != nil {
		return err
	}
	for stream, consumers := range m.Consumers {
		for _, c := range consumers {
			if c.Durable != durable {
				continue
			}
			if err := o.js.DeleteConsumer(ctx, stream, durable); err != nil {
				return fmt.Errorf("delete %s/%s: %w", stream, durable, err)
			}
		}
	}
	return nil
}

// StreamMsgs is how many messages the named stream holds.
func (o *Operator) StreamMsgs(ctx context.Context, stream string) (uint64, error) {
	s, err := o.js.Stream(ctx, stream)
	if err != nil {
		return 0, err
	}
	return s.CachedInfo().State.Msgs, nil
}

// Server is an in-process NATS server configured from the shipped Helm
// values, listening on TCP, with the shipped manifests applied.
type Server struct {
	s *natsserver.Server
	// Operator is connected as OperatorUser, closed with the test.
	Operator *Operator
}

// Start starts a Server, shut down by the test framework.
func Start(t testing.TB) *Server {
	t.Helper()
	dir := t.TempDir()
	conf, err := ServerConfig(ShippedValues(), dir)
	if err != nil {
		t.Fatal(err)
	}
	confPath := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(confPath, conf, 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	opts.Host, opts.Port, opts.NoSigs, opts.NoLog = "127.0.0.1", -1, true, true
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	t.Cleanup(s.Shutdown)
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	op, err := Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(op.Close)
	if err := op.ApplyShipped(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &Server{s: s, Operator: op}
}

// URL is the server's client URL.
func (s *Server) URL() string { return s.s.ClientURL() }
