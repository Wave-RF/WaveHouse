//go:build integration

package mq

import (
	"strconv"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/require"
)

// ExternalFixture is the external-NATS fixture as the conformance run (in
// mq_test, since mqtest imports mq) sees it: a server with the shipped
// topology, and the operator's hand on it.
type ExternalFixture struct {
	f *natsFixture
}

// NewExternalFixture starts a server from the shipped Helm values and applies
// the shipped manifests to it.
func NewExternalFixture(t *testing.T) *ExternalFixture {
	t.Helper()
	f := newNATSFixture(t)
	f.apply(t, shippedTopology(t))
	return &ExternalFixture{f: f}
}

// Broker connects an ExternalNATS as the restricted wavehouse user, closed
// by the test framework.
func (x *ExternalFixture) Broker(t *testing.T) Broker {
	t.Helper()
	return x.f.broker(t, nil)
}

// DeleteIngestDurable deletes every shard durable of every partition, as the
// operator could.
func (x *ExternalFixture) DeleteIngestDurable(t *testing.T) {
	t.Helper()
	for p := range 4 {
		for s := range 8 {
			require.NoError(t, x.f.admin.DeleteConsumer(t.Context(), shippedPartition(p), natsShardDurable(DefaultNATSIngestConsumer, s)))
		}
	}
}

// FillPartition shrinks every partition to a few KiB and publishes id's
// events into each until it refuses even the smallest: a tenant's tables
// spread over the partitions, so any of them then finds its own full.
func (x *ExternalFixture) FillPartition(t *testing.T, b Broker, id tenant.ID) {
	t.Helper()
	for p := range 4 {
		x.f.shrink(t, shippedPartition(p), 4<<10)
		topic := topicIn(t, id, p, 4, 8)
		for _, size := range []int{1 << 10, 1} {
			payload := make([]byte, size)
			for i := 0; ; i++ {
				require.Less(t, i, 1<<10, "partition %d never filled", p)
				err := b.Publish(t.Context(), topic, payload)
				if err != nil {
					require.ErrorIs(t, err, ErrQueueFull)
					break
				}
			}
		}
	}
}

// topicIn is a table of id's whose events go to partition p of n (v shards).
func topicIn(t *testing.T, id tenant.ID, p, n, v int) Topic {
	t.Helper()
	for i := range 1000 {
		topic := Topic{Tenant: id, Table: "t" + strconv.Itoa(i)}
		if got, _ := natsRoute(topic, n, v); got == p {
			return topic
		}
	}
	t.Fatalf("no table of %s in partition %d of %d", id, p, n)
	return Topic{}
}

// shippedPartition is partition p's stream in the shipped manifests.
func shippedPartition(p int) string { return "WH_INGEST_" + strconv.Itoa(p) }

// broker connects an ExternalNATS to f as the wavehouse user, with cfg's
// fields over the fixture's, closed by the test framework.
func (f *natsFixture) broker(t *testing.T, edit func(*NATSConfig)) *ExternalNATS {
	t.Helper()
	cfg := NATSConfig{
		URLs:         []string{f.server.ClientURL()},
		User:         "wavehouse",
		PasswordFile: writeSecret(t, fixturePassword("wavehouse")),
		Topology:     NATSTopology{Partitions: 4, Shards: 8},
		TopologyWait: 10 * time.Second,
	}
	if edit != nil {
		edit(&cfg)
	}
	e, err := NewNATS(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// shrink sets a stream's max_bytes, as the operator could.
func (f *natsFixture) shrink(t *testing.T, stream string, maxBytes int64) {
	t.Helper()
	s, err := f.admin.Stream(t.Context(), stream)
	require.NoError(t, err)
	cfg := s.CachedInfo().Config
	cfg.MaxBytes = maxBytes
	_, err = f.admin.UpdateStream(t.Context(), cfg)
	require.NoError(t, err)
}

// streamMsgs is how many messages a stream holds.
func (f *natsFixture) streamMsgs(t *testing.T, stream string) uint64 {
	t.Helper()
	s, err := f.admin.Stream(t.Context(), stream)
	require.NoError(t, err)
	return s.CachedInfo().State.Msgs
}

// mustPartition is the shipped partition topic's table lives in.
func mustPartition(topic Topic) int {
	p, _ := natsRoute(topic, 4, 8)
	return p
}
