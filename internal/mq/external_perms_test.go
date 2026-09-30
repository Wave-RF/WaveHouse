//go:build integration

package mq

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/Wave-RF/WaveHouse/internal/coord"
	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// Boot refuses a permission set narrower than the shard count, the
// validator's case: the shipped values name 8 shards' durables, and the
// topology has 16. Without the check the broker boots, publishes to shards
// 8-15 succeed, and nothing ever pulls them.
func TestNewNATS_RefusesPermissionsNarrowerThanTheShards(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	f.apply(t, generatedTopology(t, 4, 16))
	_, err := NewNATS(t.Context(), NATSConfig{
		URLs: []string{f.server.ClientURL()}, User: "wavehouse", PasswordFile: writeSecret(t, fixturePassword("wavehouse")),
		Topology: NATSTopology{Partitions: 4, Shards: 16}, TopologyWait: time.Second,
	})
	require.ErrorIs(t, err, ErrTopology)
	var te *TopologyError
	require.ErrorAs(t, err, &te)
	assert.Contains(t, err.Error(), "required: consumer WH_INGEST_3/wh-ingest-15: permissions: the connecting user may not publish to $JS.API.CONSUMER.")
	assert.Contains(t, err.Error(), "wavehouse mq permissions --shards 16")
	assert.NotContains(t, err.Error(), "wh-ingest-7: permissions")
	assert.Equal(t, 4*8, countRequired(te.Findings))
}

// The same under a JetStream domain, on a server in it: the probe goes out
// as $JS.hub.API.…, and the server refuses it under the plain form it maps
// that to.
func TestNewNATS_RefusesNarrowPermissionsUnderAJSDomain(t *testing.T) {
	t.Parallel()
	f := newNATSFixtureFrom(t, valuesWithPermissions(t, 8, "hub"), func(o *natsserver.Options) { o.JetStreamDomain = "hub" })
	f.apply(t, generatedTopology(t, 4, 16))
	_, err := NewNATS(t.Context(), NATSConfig{
		URLs: []string{f.server.ClientURL()}, User: "wavehouse", PasswordFile: writeSecret(t, fixturePassword("wavehouse")),
		Topology: NATSTopology{Partitions: 4, Shards: 16}, JSDomain: "hub", TopologyWait: time.Second,
	})
	require.ErrorIs(t, err, ErrTopology)
	var te *TopologyError
	require.ErrorAs(t, err, &te)
	assert.Contains(t, err.Error(), "consumer WH_INGEST_3/wh-ingest-15: permissions")
	assert.Contains(t, err.Error(), "wavehouse mq permissions --shards 16 --js-domain hub")
	assert.Equal(t, 4*8, countRequired(te.Findings))
}

// A consumer request the server refuses after boot marks the topology
// faulty at once, before the next re-check names it.
func TestExternalNATS_RefusedConsumerRequestIsATopologyFault(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	require.True(t, e.topologyOK.Load())
	require.NoError(t, e.nc.Publish("$JS.API.CONSUMER.MSG.NEXT.WH_INGEST_0.wh-ingest-9", nil))
	require.Eventually(t, func() bool { return !e.topologyOK.Load() }, 5*time.Second, 10*time.Millisecond)

	// A refused request to another API is not a topology fault.
	g := shippedFixture(t)
	e2 := g.broker(t, nil)
	require.NoError(t, e2.nc.Publish("$JS.API.STREAM.DELETE.WH_DLQ", nil))
	require.NoError(t, e2.nc.Flush())
	require.Eventually(t, func() bool { return e2.perms.deniedSince("$JS.API.STREAM.DELETE.WH_DLQ", 0) }, 5*time.Second, 10*time.Millisecond)
	assert.True(t, e2.topologyOK.Load())
}

// The shipped permissions let the wavehouse user ack only what its shard
// durables deliver: an ack for a consumer of its own on the history (which
// it may create and pull) is refused, and the row stays pending.
func TestNATSPermissions_RefuseAcksOutsideTheShards(t *testing.T) {
	t.Parallel()
	f := shippedFixture(t)
	e := f.broker(t, nil)
	require.NoError(t, e.Publish(t.Context(), Topic{Tenant: "acme", Table: "t"}, []byte("row")))

	js := f.connect(t, "wavehouse", nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	c, err := js.CreateConsumer(t.Context(), "WH_HISTORY", jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, InactiveThreshold: time.Minute})
	require.NoError(t, err)
	msg, err := c.Next(jetstream.FetchMaxWait(5 * time.Second))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(msg.Reply(), "$JS.ACK."), msg.Reply())
	require.Error(t, call(t.Context(), msg.DoubleAck), "an ack outside the shard durables is refused")
	info, err := f.admin.Consumer(t.Context(), "WH_HISTORY", c.CachedInfo().Name)
	require.NoError(t, err)
	assert.Equal(t, 1, info.CachedInfo().NumAckPending)
}

// Rows are acked under either ack subject layout the server may use: the
// default (v1) and the js_ack_fc_v2 feature flag's, which puts the domain and
// the account ahead of the stream and consumer.
func TestExternalNATS_AcksUnderBothAckSubjectLayouts(t *testing.T) {
	t.Parallel()
	for _, v2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("v2=%t", v2), func(t *testing.T) {
			t.Parallel()
			f := newNATSFixtureFrom(t, natstest.ShippedValues(), func(o *natsserver.Options) {
				o.FeatureFlags = map[string]bool{"js_ack_fc_v2": v2}
			})
			f.apply(t, shippedTopology(t))
			drainsWithAcks(t, f, f.broker(t, nil))

			// The layout the server used, by token count.
			require.NoError(t, f.broker(t, nil).Publish(t.Context(), Topic{Tenant: "acme", Table: "t"}, []byte("row")))
			c, err := f.admin.CreateConsumer(t.Context(), "WH_HISTORY", jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
			require.NoError(t, err)
			msg, err := c.Next(jetstream.FetchMaxWait(5 * time.Second))
			require.NoError(t, err)
			tokens := map[bool]int{false: 9, true: 11}[v2]
			assert.Len(t, strings.Split(msg.Reply(), "."), tokens, msg.Reply())
		})
	}
}

// drainsWithAcks publishes a row, consumes and acks it through e, and
// requires the work-queue partition to have dropped it: the ack was taken.
func drainsWithAcks(t *testing.T, f *natsFixture, e *ExternalNATS) {
	t.Helper()
	topic := Topic{Tenant: "acme", Table: "t"}
	require.NoError(t, e.Publish(t.Context(), topic, []byte("row")))
	cons, err := e.CreateConsumer(t.Context(), ConsumerConfig{Durable: workerDurable})
	require.NoError(t, err)
	got := make(chan string, 4)
	stop, _, err := cons.Consume(func(m *Message) {
		assert.NoError(t, m.DoubleAck(m.Ctx))
		got <- string(m.Data)
	}, 4)
	require.NoError(t, err)
	t.Cleanup(stop)
	require.Equal(t, "row", receive(t, got))
	partition := shippedPartition(mustPartition(topic))
	require.Eventually(t, func() bool { return f.streamMsgs(t, partition) == 0 }, 5*time.Second, 10*time.Millisecond)
}

// Under mq.nats.js_domain, WaveHouse connected to a server in the domain
// boots, consumes, acks and holds a lease with `wavehouse mq permissions
// --js-domain`: that server maps $JS.<domain>.API.… to $JS.API.… before it
// checks permissions.
func TestNewNATS_JSDomainOnAServerInTheDomain(t *testing.T) {
	t.Parallel()
	f := newNATSFixtureFrom(t, valuesWithPermissions(t, 8, "hub"), func(o *natsserver.Options) { o.JetStreamDomain = "hub" })
	f.apply(t, shippedTopology(t))
	e := f.broker(t, func(c *NATSConfig) { c.JSDomain = "hub" })
	drainsWithAcks(t, f, e)
	holdsALease(t, e)
}

// Through a leafnode outside the domain, the permissions are checked on the
// subjects as sent, $JS.<domain>.API.…, which a set without --js-domain does
// not allow.
func TestNewNATS_JSDomainThroughALeafnode(t *testing.T) {
	t.Parallel()
	hub := domainHub(t)
	l := domainLeaf(t, hub, "hub")
	e := l.broker(t, func(c *NATSConfig) { c.JSDomain = "hub" })
	drainsWithAcks(t, l, e)
	holdsALease(t, e)

	plain := domainLeaf(t, hub, "")
	_, err := NewNATS(t.Context(), NATSConfig{
		URLs: []string{plain.server.ClientURL()}, User: "wavehouse", PasswordFile: writeSecret(t, fixturePassword("wavehouse")),
		Topology: NATSTopology{Partitions: 4, Shards: 8}, JSDomain: "hub", TopologyWait: time.Second,
	})
	require.ErrorContains(t, err, "the server refused publishes to $JS.hub.API.", "a set without the domain's subjects lets nothing through the leafnode")
}

// A halted shard keeps its pin with raw pulls sent under the domain's API
// prefix. The generated permissions must allow them in both forms: a server
// in the domain checks the plain one, a leafnode the one as sent. A refused
// renewal would let the pin lapse after its 10s TTL and be recorded as a
// refusal.
func TestExternalNATS_PinRenewalUnderAJSDomain(t *testing.T) {
	t.Parallel()
	renews := func(t *testing.T, f *natsFixture) {
		t.Helper()
		e := f.broker(t, func(c *NATSConfig) { c.JSDomain = "hub" })
		topic := Topic{Tenant: "acme", Table: "events"}
		p, s := natsRoute(topic, 4, 8)
		c, stop, got := unitConsumer(t, e, shippedPartition(p)+"/"+natsShardDurable("wh-ingest", s), nil)
		require.NoError(t, e.Publish(t.Context(), topic, []byte("1")))
		require.Equal(t, "1", receive(t, got))
		pin := unitPin(t, f, topic)
		require.NotEmpty(t, pin)
		c.(Halter).Halt()
		time.Sleep(15 * time.Second) // past the pinned TTL: only the renewals keep it
		assert.Equal(t, pin, unitPin(t, f, topic), "the renewals kept the pin")
		assert.Empty(t, e.perms.list(), "the server refused no request")
		stop()
	}
	t.Run("server in the domain", func(t *testing.T) {
		t.Parallel()
		f := newNATSFixtureFrom(t, valuesWithPermissions(t, 8, "hub"), func(o *natsserver.Options) { o.JetStreamDomain = "hub" })
		f.apply(t, shippedTopology(t))
		renews(t, f)
	})
	t.Run("leafnode outside it", func(t *testing.T) {
		t.Parallel()
		renews(t, domainLeaf(t, domainHub(t), "hub"))
	})
}

// domainHub is a server in JetStream domain hub holding the shipped
// topology, with a leafnode port.
func domainHub(t *testing.T) *natsFixture {
	t.Helper()
	hub := newNATSFixtureFrom(t, natstest.ShippedValues(), func(o *natsserver.Options) {
		o.JetStreamDomain = "hub"
		o.LeafNode.Host, o.LeafNode.Port = "127.0.0.1", -1
	})
	hub.apply(t, shippedTopology(t))
	return hub
}

// domainLeaf is a leafnode of hub without JetStream, whose wavehouse user has
// the permissions generated for domain.
func domainLeaf(t *testing.T, hub *natsFixture, domain string) *natsFixture {
	t.Helper()
	varz, err := hub.server.Varz(nil)
	require.NoError(t, err)
	remote, err := url.Parse(fmt.Sprintf("nats-leaf://%s:%s@127.0.0.1:%d", natstest.OperatorUser, fixturePassword(natstest.OperatorUser), varz.LeafNode.Port))
	require.NoError(t, err)
	l := newNATSFixtureFrom(t, valuesWithPermissions(t, 8, domain), func(o *natsserver.Options) {
		o.JetStream = false
		o.LeafNode.Remotes = []*natsserver.RemoteLeafOpts{{URLs: []*url.URL{remote}, LocalAccount: "WAVEHOUSE"}}
	})
	require.Eventually(t, func() bool { return l.server.NumLeafNodes() == 1 }, 10*time.Second, 10*time.Millisecond)
	l.admin = hub.admin // the topology lives on the hub
	return l
}

// holdsALease acquires a lease in the shipped lease bucket through e, and
// a second holder then reads it as held: a KV write and a direct get.
func holdsALease(t *testing.T, e *ExternalNATS) {
	t.Helper()
	coordinator := func(holder string) coord.Coordinator {
		c, err := e.Leases(t.Context(), natstest.CoordBucket, holder)
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c
	}
	_, err := coordinator("holder-a").TryAcquire(t.Context(), "probe")
	require.NoError(t, err)
	_, err = coordinator("holder-b").TryAcquire(t.Context(), "probe")
	require.ErrorIs(t, err, coord.ErrHeld)
}

// valuesWithPermissions writes the shipped Helm values with the wavehouse
// user's permissions generated for shards and domain.
func valuesWithPermissions(t *testing.T, shards int, domain string) string {
	t.Helper()
	raw, err := os.ReadFile(natstest.ShippedValues())
	require.NoError(t, err)
	var values map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &values))
	p := natsPermissions(NATSTopology{Shards: shards}, domain)
	accounts := values["config"].(map[string]any)["merge"].(map[string]any)["accounts"].(map[string]any)
	for _, acc := range accounts {
		for _, u := range acc.(map[string]any)["users"].([]any) {
			if user := u.(map[string]any); user["user"] == natstest.WaveHouseUser {
				user["permissions"] = map[string]any{
					"publish":   map[string][]string{"allow": p.PublishAllow, "deny": p.PublishDeny},
					"subscribe": map[string][]string{"allow": p.SubscribeAllow},
				}
			}
		}
	}
	out, err := yaml.Marshal(values)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "values.yaml")
	require.NoError(t, os.WriteFile(path, out, 0o600))
	return path
}
