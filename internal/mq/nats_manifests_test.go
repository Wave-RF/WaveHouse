package mq

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/mq/natstest"
)

// The shipped manifests reserve at most three quarters of the file store the
// NATS chart gives each server from the shipped values, the rest being
// headroom for what else the store holds.
func TestShippedManifests_FitTheShippedFileStore(t *testing.T) {
	t.Parallel()
	m, err := natstest.LoadManifests(natstest.ShippedManifests())
	require.NoError(t, err)
	var reserved int64
	for _, s := range m.Streams {
		require.Positive(t, s.MaxBytes, "stream %s reserves nothing", s.Name)
		reserved += s.MaxBytes
	}
	store := shippedFileStore(t)
	assert.LessOrEqual(t, reserved, store/4*3, "the shipped streams reserve %s of a %s file store", FormatStoreSize(reserved), FormatStoreSize(store))
}

// Streams that together reserve more than the file store are refused, as
// JetStream would refuse one of them; partitions are sized independently of
// their count, so lowering N never needs more store.
func TestWriteNATSManifests_RefusesStreamsOverTheFileStore(t *testing.T) {
	t.Parallel()
	var b bytes.Buffer
	require.NoError(t, WriteNATSManifests(&b, NATSManifestOptions{Topology: NATSTopology{Partitions: 5}}))
	err := WriteNATSManifests(&b, NATSManifestOptions{Topology: NATSTopology{Partitions: 6}})
	require.ErrorContains(t, err, "reserve 105Gi (6 partitions of 15Gi, history 10Gi, dlq 5Gi), more than the 100Gi file store")
	require.NoError(t, WriteNATSManifests(&b, NATSManifestOptions{Topology: NATSTopology{Partitions: 6}, PartitionMaxBytes: 10 << 30}))
	err = WriteNATSManifests(&b, NATSManifestOptions{Topology: NATSTopology{Partitions: 1}, PartitionMaxBytes: 86 << 30})
	require.ErrorContains(t, err, "more than the 100Gi file store")

	one, two := NATSManifestOptions{Topology: NATSTopology{Partitions: 1}}.withDefaults(), NATSManifestOptions{Topology: NATSTopology{Partitions: 2}}.withDefaults()
	assert.Equal(t, one.PartitionMaxBytes, two.PartitionMaxBytes)
}

// The fixture's server reserves streams against the file store the chart
// would give it, so a topology that does not fit the shipped volume does not
// fit the tests either.
func TestNATSFixture_ReservesAgainstTheShippedFileStore(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	_, err := f.admin.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "TOO_BIG", Subjects: []string{"too.big.>"}, Storage: jetstream.FileStorage, MaxBytes: shippedFileStore(t) + 1,
	})
	var apiErr *jetstream.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, jetstream.ErrorCode(10047), apiErr.ErrorCode, "insufficient storage resources: %v", err)
}

func TestStoreSize(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]int64{"100Gi": 100 << 30, "15Gi": 15 << 30, "1024": 1024, "2Ti": 2 << 40, "5G": 5e9, "64Mi": 64 << 20, "3k": 3000} {
		got, err := ParseStoreSize(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "Gi", "1.5Gi", "-1", "10GB", "10 Gi", "9999999999Ti"} {
		_, err := ParseStoreSize(in)
		assert.Error(t, err, in)
	}
	for in, want := range map[int64]string{100 << 30: "100Gi", 75 << 30: "75Gi", 1536 << 20: "1536Mi", 1000: "1000", 2 << 40: "2Ti"} {
		assert.Equal(t, want, FormatStoreSize(in))
	}
}

// Under a JetStream domain every API request and KV write is allowed and
// denied both as sent ($JS.<domain>.API.…, what a leafnode checks) and as a
// server in the domain maps it ($JS.API.…).
func TestNATSPermissions_JSDomain(t *testing.T) {
	t.Parallel()
	plain := natsPermissions(NATSTopology{Shards: 2}, "")
	hub := natsPermissions(NATSTopology{Shards: 2}, "hub")
	for _, s := range plain.PublishAllow {
		assert.Contains(t, hub.PublishAllow, s)
		if rest, ok := strings.CutPrefix(s, "$JS.API."); ok {
			assert.Contains(t, hub.PublishAllow, "$JS.hub.API."+rest)
		}
	}
	for _, s := range plain.PublishDeny {
		assert.Contains(t, hub.PublishDeny, s)
		if rest, ok := strings.CutPrefix(s, "$JS.API."); ok {
			assert.Contains(t, hub.PublishDeny, "$JS.hub.API."+rest)
		}
	}
	assert.Contains(t, hub.PublishAllow, "$JS.hub.API.$KV.wh_coord.lease.>")
	assert.NotContains(t, plain.PublishAllow, "$JS.ACK.>")
	var b bytes.Buffer
	require.Error(t, WriteNATSPermissions(&b, NATSTopology{}, "a.b"))
}

// The validator's case: the shipped permissions, which name 8 shards'
// durables, against a topology of 16. The verifier probes each durable as
// the connecting user and names every shard the user may not pull.
func TestVerifyNATSTopology_ProbesTheShardPermissions(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	var buf bytes.Buffer
	require.NoError(t, WriteNATSManifests(&buf, NATSManifestOptions{Topology: NATSTopology{Partitions: 4, Shards: 16}, Replicas: 1}))
	path := filepath.Join(t.TempDir(), "manifests.yaml")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
	f.apply(t, loadNATSManifests(t, path))

	perms := newNATSPermissionWatch()
	js := f.connect(t, natstest.WaveHouseUser, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { perms.record(err) }))
	findings, err := verifyNATSTopology(t.Context(), js, NATSTopology{Partitions: 4, Shards: 16}, perms)
	require.NoError(t, err)
	var denied []string
	for _, got := range findings {
		if got.Field == "permissions" {
			require.Equal(t, FindingRequired, got.Severity)
			denied = append(denied, strings.TrimPrefix(got.Object, "consumer "))
			assert.Contains(t, got.Problem, "wavehouse mq permissions --shards 16")
		}
	}
	var want []string
	for p := range 4 {
		for s := 8; s < 16; s++ {
			want = append(want, "WH_INGEST_"+strconv.Itoa(p)+"/"+natsShardDurable(DefaultNATSIngestConsumer, s))
		}
	}
	assert.ElementsMatch(t, want, denied)
}

// The probe changes nothing it touches: a durable the user may pull keeps
// its pending rows and delivers them to the next pull.
func TestVerifyNATSTopology_ProbeLeavesTheDurablesAlone(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	f.apply(t, shippedTopology(t))
	_, err := f.admin.Publish(t.Context(), "wh.ingest.0.0.acme.t", []byte("row"))
	require.NoError(t, err)
	perms := newNATSPermissionWatch()
	js := f.connect(t, natstest.WaveHouseUser, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { perms.record(err) }))
	findings, err := verifyNATSTopology(t.Context(), js, shippedSpec, perms)
	require.NoError(t, err)
	assert.True(t, replicaWarnings(findings), "findings: %v", findings)

	c, err := f.admin.Consumer(t.Context(), "WH_INGEST_0", "wh-ingest-0")
	require.NoError(t, err)
	info := c.CachedInfo()
	assert.Equal(t, uint64(1), info.NumPending)
	assert.Zero(t, info.NumAckPending)
	assert.Zero(t, info.Delivered.Stream)
	assert.Empty(t, info.PriorityGroups[0].PinnedClientID, "a probe pinned no one")
}

// A refusal seen before a probe says nothing about it: permissions change,
// and a durable the user may now pull is not reported for an old refusal.
func TestVerifyNATSTopology_ProbeIgnoresAnEarlierRefusal(t *testing.T) {
	t.Parallel()
	f := newNATSFixture(t)
	f.apply(t, shippedTopology(t))
	perms := newNATSPermissionWatch()
	_, _, ok := perms.record(fmt.Errorf(`%w: Permissions Violation for Publish to "$JS.API.CONSUMER.MSG.NEXT.WH_INGEST_0.wh-ingest-0"`, nats.ErrPermissionViolation))
	require.True(t, ok)
	js := f.connect(t, natstest.WaveHouseUser, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { perms.record(err) }))
	findings, err := verifyNATSTopology(t.Context(), js, shippedSpec, perms)
	require.NoError(t, err)
	assert.True(t, replicaWarnings(findings), "findings: %v", findings)
}
