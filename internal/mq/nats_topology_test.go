package mq

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// shippedSpec is the topology the shipped manifests are generated for, and
// coordSpec the same for a process holding its leases there.
var (
	shippedSpec = NATSTopology{Partitions: 4}
	coordSpec   = NATSTopology{Partitions: 4, CoordBucket: natstest.CoordBucket}
)

// replicaWarnings are what the shipped manifests at one replica leave: one
// num_replicas recommendation per partition, the history and the DLQ.
func replicaWarnings(findings []Finding) bool {
	for _, f := range findings {
		if f.Severity != FindingRecommended || f.Field != "num_replicas" {
			return false
		}
	}
	return len(findings) == shippedSpec.Partitions+2
}

// The shipped manifests pass the verifier, run as the wavehouse user with
// exactly the shipped permissions.
func TestVerifyNATSTopology_ShippedManifestsPass(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	f.apply(t, shippedTopology(t))
	js := f.connect(t, "wavehouse")
	findings, err := verifyNATSTopology(t.Context(), js, shippedSpec)
	require.NoError(t, err)
	assert.True(t, replicaWarnings(findings), "findings: %v", findings)

	// With the lease bucket checked too: one more replica warning, its own.
	findings, err = verifyNATSTopology(t.Context(), js, coordSpec)
	require.NoError(t, err)
	require.Len(t, findings, shippedSpec.Partitions+3, "findings: %v", findings)
	last := findings[len(findings)-1]
	assert.Equal(t, "kv bucket wh_coord", last.Object)
	assert.Equal(t, "num_replicas", last.Field)
}

// Every rule the verifier holds the operator to, one mutation each.
func TestVerifyNATSTopology_Findings(t *testing.T) { //nolint:tparallel // its cases share one server, so they run in turn
	t.Parallel()
	const (
		p0      = "WH_INGEST_0"
		history = "WH_HISTORY"
		dlq     = "WH_DLQ"
	)
	type want struct {
		sev    FindingSeverity
		object string // a substring of Finding.Object
		field  string
		// problem, when set, is a substring of Finding.Problem: the history
		// cases share the "sources" field with a source still attaching.
		problem string
	}
	req := func(object, field string) want { return want{FindingRequired, object, field, ""} }
	rec := func(object, field string) want { return want{FindingRecommended, object, field, ""} }
	stream := func(name string, mut func(*jetstream.StreamConfig)) func(*testing.T, *fixtureTopology) {
		return func(t *testing.T, tp *fixtureTopology) { mut(tp.stream(t, name)) }
	}
	durable := func(mut func(*jetstream.ConsumerConfig)) func(*testing.T, *fixtureTopology) {
		return func(t *testing.T, tp *fixtureTopology) { mut(tp.consumer(t, p0)) }
	}

	cases := []struct {
		name   string
		mutate func(*testing.T, *fixtureTopology)
		spec   NATSTopology
		want   want
	}{
		// Ingest partitions.
		{"partition missing", func(_ *testing.T, tp *fixtureTopology) { tp.drop(p0) }, shippedSpec, req("ingest partition 0", "subjects")},
		{"partition subjects", stream(p0, func(s *jetstream.StreamConfig) { s.Subjects = []string{"wh.ingest.0.*"} }), shippedSpec, req(p0, "subjects")},
		{"partition retention", stream(p0, func(s *jetstream.StreamConfig) { s.Retention = jetstream.LimitsPolicy }), shippedSpec, req(p0, "retention")},
		{"partition discard", stream(p0, func(s *jetstream.StreamConfig) {
			s.Discard, s.DiscardNewPerSubject = jetstream.DiscardOld, false
		}), shippedSpec, req(p0, "discard")},
		{"partition max_bytes", stream(p0, func(s *jetstream.StreamConfig) { s.MaxBytes = -1 }), shippedSpec, req(p0, "max_bytes")},
		{"partition max_age", stream(p0, func(s *jetstream.StreamConfig) { s.MaxAge = time.Hour }), shippedSpec, req(p0, "max_age")},
		{"partition storage", stream(p0, func(s *jetstream.StreamConfig) { s.Storage = jetstream.MemoryStorage }), shippedSpec, req(p0, "storage")},
		{"partition duplicate_window", stream(p0, func(s *jetstream.StreamConfig) { s.Duplicates = time.Second }), shippedSpec, req(p0, "duplicate_window")},
		{"duplicate window against the publish timeout", nil, NATSTopology{Partitions: 4, PublishTimeout: 2 * time.Minute}, req(p0, "duplicate_window")},
		{"partition no_ack", stream(p0, func(s *jetstream.StreamConfig) { s.NoAck = true }), shippedSpec, req(p0, "no_ack")},
		{"partition per-subject cap", stream(p0, func(s *jetstream.StreamConfig) {
			s.MaxMsgsPerSubject, s.DiscardNewPerSubject = 0, false
		}), shippedSpec, rec(p0, "max_msgs_per_subject")},
		{"partition per-subject cap evicting", stream(p0, func(s *jetstream.StreamConfig) {
			s.MaxMsgsPerSubject, s.DiscardNewPerSubject = 1000, false
		}), shippedSpec, req(p0, "discard_new_per_subject")},
		{"one stream for two partitions", func(t *testing.T, tp *fixtureTopology) {
			tp.drop("WH_INGEST_1")
			s := tp.stream(t, p0)
			s.Subjects = append(s.Subjects, "wh.ingest.1.>")
			s.Metadata = nil
			tp.consumer(t, p0).FilterSubject = ""
		}, shippedSpec, req(p0, "subjects")},
		{"partition deny_purge", stream(p0, func(s *jetstream.StreamConfig) { s.DenyPurge = false }), shippedSpec, rec(p0, "deny_purge")},
		{"partition at one replica", nil, shippedSpec, want{FindingRecommended, p0, "num_replicas", "sync_interval"}},
		{"partition persist_mode async", stream(p0, func(s *jetstream.StreamConfig) { s.PersistMode = jetstream.AsyncPersistMode }), shippedSpec, req(p0, "persist_mode")},
		{"partition metadata missing", stream(p0, func(s *jetstream.StreamConfig) { s.Metadata = nil }), shippedSpec, rec(p0, "metadata")},
		{"partition metadata mismatch", stream(p0, func(s *jetstream.StreamConfig) {
			s.Metadata = map[string]string{"wavehouse.dev/partition": "3", "wavehouse.dev/partitions": "4"}
		}), shippedSpec, req(p0, "metadata")},
		{"partition count mismatch caught by metadata", nil, NATSTopology{Partitions: 2}, req(p0, "metadata")},
		{"partitions beyond N", nil, NATSTopology{Partitions: 2}, rec("WH_INGEST_3", "subjects")},

		// The wh-ingest durable.
		{"durable missing", func(_ *testing.T, tp *fixtureTopology) { delete(tp.Consumers, p0) }, shippedSpec, req(p0+"/wh-ingest", "durable_name")},
		{"durable is push", durable(func(c *jetstream.ConsumerConfig) {
			c.DeliverSubject = "deliver.here"
			c.MaxAckPending = 0
		}), shippedSpec, req(p0+"/wh-ingest", "deliver_subject")},
		{"durable ack_policy", durable(func(c *jetstream.ConsumerConfig) {
			c.AckPolicy, c.MaxAckPending = jetstream.AckNonePolicy, 0
		}), shippedSpec, req(p0+"/wh-ingest", "ack_policy")},
		{"durable ack_wait", durable(func(c *jetstream.ConsumerConfig) { c.AckWait = 30 * time.Second }), shippedSpec, req(p0+"/wh-ingest", "ack_wait")},
		{"durable max_deliver", durable(func(c *jetstream.ConsumerConfig) { c.MaxDeliver = 5 }), shippedSpec, req(p0+"/wh-ingest", "max_deliver")},
		{"durable max_ack_pending unlimited", durable(func(c *jetstream.ConsumerConfig) { c.MaxAckPending = -1 }), shippedSpec, req(p0+"/wh-ingest", "max_ack_pending")},
		{"durable max_ack_pending low", durable(func(c *jetstream.ConsumerConfig) { c.MaxAckPending = 100 }), shippedSpec, rec(p0+"/wh-ingest", "max_ack_pending")},
		{"durable deliver_policy", durable(func(c *jetstream.ConsumerConfig) { c.DeliverPolicy = jetstream.DeliverNewPolicy }), shippedSpec, req(p0+"/wh-ingest", "deliver_policy")},
		{"durable filter", durable(func(c *jetstream.ConsumerConfig) { c.FilterSubject = "wh.ingest.0.acme.>" }), shippedSpec, req(p0+"/wh-ingest", "filter_subject")},
		{"durable headers_only", durable(func(c *jetstream.ConsumerConfig) { c.HeadersOnly = true }), shippedSpec, req(p0+"/wh-ingest", "headers_only")},
		{"durable replay_policy", durable(func(c *jetstream.ConsumerConfig) { c.ReplayPolicy = jetstream.ReplayOriginalPolicy }), shippedSpec, req(p0+"/wh-ingest", "replay_policy")},
		{"durable inactive_threshold", durable(func(c *jetstream.ConsumerConfig) { c.InactiveThreshold = time.Hour }), shippedSpec, req(p0+"/wh-ingest", "inactive_threshold")},
		{"durable max_request_batch", durable(func(c *jetstream.ConsumerConfig) { c.MaxRequestBatch = 10 }), shippedSpec, req(p0+"/wh-ingest", "max_request_batch")},
		{"durable priority_policy", durable(func(c *jetstream.ConsumerConfig) {
			c.PriorityPolicy, c.PriorityGroups, c.PinnedTTL = jetstream.PriorityPolicyPinned, []string{"workers"}, time.Minute
		}), shippedSpec, req(p0+"/wh-ingest", "priority_policy")},

		// The history.
		{"history missing", func(_ *testing.T, tp *fixtureTopology) { tp.drop(history) }, shippedSpec, req(history, "name")},
		{"history has subjects", stream(history, func(s *jetstream.StreamConfig) { s.Subjects = []string{"history.>"} }), shippedSpec, req(history, "subjects")},
		{"history misses a partition", stream(history, func(s *jetstream.StreamConfig) { s.Sources = s.Sources[1:] }), shippedSpec, want{FindingRequired, history, "sources", "do not include WH_INGEST_0"}},
		{"history filters a partition", stream(history, func(s *jetstream.StreamConfig) {
			s.Sources[0].FilterSubject = "wh.ingest.0.acme.>"
		}), shippedSpec, want{FindingRequired, history, "sources", "filter WH_INGEST_0"}},
		{"history source cannot attach", stream(p0, func(s *jetstream.StreamConfig) { s.MaxConsumers = 1 }), shippedSpec, want{FindingRequired, history, "sources", "WH_INGEST_0 is not attached"}},
		{"history retention", stream(history, func(s *jetstream.StreamConfig) { s.Retention = jetstream.InterestPolicy }), shippedSpec, req(history, "retention")},
		{"history discard", stream(history, func(s *jetstream.StreamConfig) { s.Discard = jetstream.DiscardNew }), shippedSpec, req(history, "discard")},
		{"history max_age", stream(history, func(s *jetstream.StreamConfig) { s.MaxAge = 0 }), shippedSpec, req(history, "max_age")},
		{"history max_bytes", stream(history, func(s *jetstream.StreamConfig) { s.MaxBytes = -1 }), shippedSpec, rec(history, "max_bytes")},
		{"history at one replica", nil, shippedSpec, want{FindingRecommended, history, "num_replicas", "sync_interval"}},
		{"history named elsewhere", nil, NATSTopology{Partitions: 4, HistoryStream: "OTHER"}, req("OTHER", "name")},

		// The dead-letter stream.
		{"dlq missing", func(_ *testing.T, tp *fixtureTopology) { tp.drop(dlq) }, shippedSpec, req("dead-letter stream", "subjects")},
		{"dlq subjects", stream(dlq, func(s *jetstream.StreamConfig) { s.Subjects = []string{"wh.dlq.x", "wh.dlq.acme.>"} }), shippedSpec, req(dlq, "subjects")},
		{"dlq retention", stream(dlq, func(s *jetstream.StreamConfig) { s.Retention = jetstream.InterestPolicy }), shippedSpec, req(dlq, "retention")},
		{"dlq discard", stream(dlq, func(s *jetstream.StreamConfig) { s.Discard = jetstream.DiscardNew }), shippedSpec, req(dlq, "discard")},
		{"dlq storage", stream(dlq, func(s *jetstream.StreamConfig) { s.Storage = jetstream.MemoryStorage }), shippedSpec, req(dlq, "storage")},
		{"dlq max_bytes", stream(dlq, func(s *jetstream.StreamConfig) { s.MaxBytes = -1 }), shippedSpec, req(dlq, "max_bytes")},
		{"dlq at one replica", nil, shippedSpec, want{FindingRecommended, dlq, "num_replicas", "sync_interval"}},
		{"dlq persist_mode async", stream(dlq, func(s *jetstream.StreamConfig) { s.PersistMode = jetstream.AsyncPersistMode }), shippedSpec, rec(dlq, "persist_mode")},
		{"dlq per-subject cap", stream(dlq, func(s *jetstream.StreamConfig) { s.MaxMsgsPerSubject = 0 }), shippedSpec, rec(dlq, "max_msgs_per_subject")},
	}
	// One server for every case, emptied between them: a server per case
	// costs more than the unit suite's per-package timeout can spare.
	f := newNATSFixture(t)
	js := f.connect(t, "wavehouse")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.reset(t)
			tp := shippedTopology(t)
			tp.KeyValues = nil // no case here checks the lease bucket
			if tc.mutate != nil {
				tc.mutate(t, tp)
			}
			// No wait for the sources: an unattached one is one more finding, and the
			// case only looks for its own.
			require.NoError(t, f.create(t.Context(), tp))
			findings, err := verifyNATSTopology(t.Context(), js, tc.spec)
			require.NoError(t, err)
			found := false
			for _, got := range findings {
				if got.Severity == tc.want.sev && got.Field == tc.want.field && strings.Contains(got.Object, tc.want.object) &&
					strings.Contains(got.Problem, tc.want.problem) {
					found = true
				}
			}
			assert.True(t, found, "want %s %s/%s among %v", tc.want.sev, tc.want.object, tc.want.field, findings)
			if tc.want.sev == FindingRequired {
				assert.Equal(t, FindingRequired, findings[0].Severity, "required findings sort first")
			}
		})
	}
}

// Fewer than 3 replicas is recommended against, never required: a one-server
// dev cluster boots. At one replica nothing but the server's sync interval
// stands behind an ack, since WaveHouse does not require sync_always.
func TestReplicasProblem(t *testing.T) {
	t.Parallel()
	for n, want := range map[int]string{0: "sync_interval", 1: "sync_interval", 2: "3 across failure domains"} {
		got, ok := replicasProblem(n)
		assert.True(t, ok, "replicas %d", n)
		assert.Contains(t, got, want, "replicas %d", n)
	}
	got, _ := replicasProblem(2)
	assert.NotContains(t, got, "sync_interval", "a quorum of two does not rest on one disk")
	for _, n := range []int{3, 5} {
		_, ok := replicasProblem(n)
		assert.False(t, ok, "replicas %d", n)
	}
}

// WaveHouse does not require sync_always under nats, so the shipped Helm
// values set no sync option anywhere. natstest.ServerConfig reads only
// config.merge, so this reads the file itself.
func TestShippedValues_SetNoSync(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(natstest.ShippedValues())
	require.NoError(t, err)
	var values any
	require.NoError(t, yaml.Unmarshal(raw, &values))
	var walk func(path string, v any)
	walk = func(path string, v any) {
		switch v := v.(type) {
		case map[string]any:
			for k, child := range v {
				assert.NotContains(t, strings.ToLower(k), "sync", "values.yaml sets %s.%s", path, k)
				walk(path+"."+k, child)
			}
		case []any:
			for _, child := range v {
				walk(path, child)
			}
		}
	}
	walk("", values)
}

// Boot waits for the operator's resources, which on Kubernetes roll out with
// the pods, and passes once they are there.
func TestAwaitNATSTopology_WaitsForTheOperator(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	js := f.connect(t, "wavehouse")
	tp := shippedTopology(t)
	created := make(chan error, 1)
	go func() {
		time.Sleep(time.Second)
		created <- f.create(t.Context(), tp)
	}()
	start := time.Now()
	findings, err := awaitNATSTopology(t.Context(), js, shippedSpec, 20*time.Second)
	require.NoError(t, <-created)
	require.NoError(t, err)
	assert.True(t, replicaWarnings(findings), "findings: %v", findings)
	assert.GreaterOrEqual(t, time.Since(start), time.Second)
}

// When the wait runs out, one error lists every finding at once.
func TestAwaitNATSTopology_ListsEveryFinding(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	tp := shippedTopology(t)
	tp.drop("WH_DLQ")
	tp.stream(t, "WH_INGEST_1").Retention = jetstream.LimitsPolicy
	delete(tp.Consumers, "WH_INGEST_2")
	f.apply(t, tp)

	_, err := awaitNATSTopology(t.Context(), f.connect(t, "wavehouse"), shippedSpec, 300*time.Millisecond)
	require.ErrorIs(t, err, ErrTopology)
	var terr *TopologyError
	require.True(t, errors.As(err, &terr))
	msg := err.Error()
	for _, want := range []string{"dead-letter stream", "stream WH_INGEST_1: retention", "consumer WH_INGEST_2/wh-ingest"} {
		assert.Contains(t, msg, want)
	}
	assert.Equal(t, 3, countRequired(terr.Findings), "findings: %v", terr.Findings)
}

func countRequired(findings []Finding) int {
	n := 0
	for _, f := range findings {
		if f.Severity == FindingRequired {
			n++
		}
	}
	return n
}

// A check that cannot run is an error, not a finding, and await gives up on
// its context.
func TestAwaitNATSTopology_ContextEnds(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	js := f.connect(t, "wavehouse")
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := awaitNATSTopology(ctx, js, shippedSpec, time.Minute)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestVerifyNATSTopology_ServerVersion(t *testing.T) {
	t.Parallel()
	cases := map[string]*FindingSeverity{
		"2.14.6":       nil,
		"v2.14.0-beta": nil,
		"2.10.0":       new(FindingRecommended),
		"2.15.1":       new(FindingRecommended),
		"2.9.25":       new(FindingRequired),
		"1.4.1":        new(FindingRequired),
		"garbage":      new(FindingRequired),
	}
	for version, want := range cases {
		v := &topologyVerifier{}
		v.serverVersion(version)
		if want == nil {
			assert.Empty(t, v.findings, version)
			continue
		}
		require.Len(t, v.findings, 1, version)
		assert.Equal(t, *want, v.findings[0].Severity, version)
	}
}

func TestVerifyNATSTopology_RefusesAnImpossibleSpec(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	js := f.connect(t, "wavehouse")
	_, err := verifyNATSTopology(t.Context(), js, NATSTopology{Prefix: "Bad.Prefix"})
	require.Error(t, err)
	_, err = verifyNATSTopology(t.Context(), js, NATSTopology{Partitions: -1})
	require.Error(t, err)
}

// The shipped Helm values give the wavehouse user exactly natsPermissions.
func TestNATSPermissions_MatchShippedValues(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(natstest.ShippedValues())
	require.NoError(t, err)
	var values struct {
		Config struct {
			Merge struct {
				Accounts map[string]struct {
					Users []struct {
						User        string `yaml:"user"`
						Permissions struct {
							Publish   struct{ Allow, Deny []string } `yaml:"publish"`
							Subscribe struct{ Allow []string }       `yaml:"subscribe"`
						} `yaml:"permissions"`
					} `yaml:"users"`
				} `yaml:"accounts"`
			} `yaml:"merge"`
		} `yaml:"config"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &values))
	want := natsPermissions(NATSTopology{})
	found := false
	for _, acc := range values.Config.Merge.Accounts {
		for _, u := range acc.Users {
			if u.User != "wavehouse" {
				continue
			}
			found = true
			assert.Equal(t, want.PublishAllow, u.Permissions.Publish.Allow)
			assert.Equal(t, want.PublishDeny, u.Permissions.Publish.Deny)
			assert.Equal(t, want.SubscribeAllow, u.Permissions.Subscribe.Allow)
		}
	}
	assert.True(t, found, "no wavehouse user in %s", natstest.ShippedValues())
}

// The generated manifests round-trip through the fixture's parser into the
// configs the verifier accepts, at any N and prefix.
func TestWriteNATSManifests_RoundTrip(t *testing.T) {
	t.Parallel()
	spec := NATSTopology{Prefix: "acme-wh", Partitions: 3}
	path := t.TempDir() + "/m.yaml"
	out, err := os.Create(path) //nolint:gosec // G304: path is rooted in t.TempDir()
	require.NoError(t, err)
	require.NoError(t, WriteNATSManifests(out, NATSManifestOptions{Topology: spec, Replicas: 1}))
	require.NoError(t, out.Close())

	f := newNATSFixture(t)
	f.apply(t, loadNATSManifests(t, path))
	spec.CoordBucket = DefaultNATSCoordBucket(spec.Prefix)
	findings, err := verifyNATSTopology(t.Context(), f.admin, spec)
	require.NoError(t, err)
	for _, got := range findings {
		assert.Equal(t, "num_replicas", got.Field, "unexpected finding %v", got)
	}
}

func TestWriteNATSManifests_RefusesAnImpossibleSpec(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	require.Error(t, WriteNATSManifests(&b, NATSManifestOptions{Topology: NATSTopology{Prefix: "a.b"}}))
	require.Error(t, WriteNATSManifests(&b, NATSManifestOptions{Topology: NATSTopology{Partitions: -2}}))
	require.Error(t, WriteNATSManifests(&b, NATSManifestOptions{Topology: NATSTopology{CoordBucket: "a.b"}}))
}
