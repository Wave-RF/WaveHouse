package mq

import (
	"strconv"
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The table-to-(partition, shard) mapping is pinned: a change moves tables
// on upgrade, so it must be deliberate. The values were derived apart from
// this code (the paper's algorithm, FNV-1a 64 and splitmix64's finalizer,
// in another language), not by running natsRoute.
func TestNATSRoute_Golden(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []int{0, 6, 87, 520}, []int{jumpHash(0, 1), jumpHash(1, 10), jumpHash(0xdeadbeef, 128), jumpHash(256, 1024)})
	assert.Equal(t, []uint64{0, 0x5692161d100b05e5, 0x4e062702ec929eea}, []uint64{mix64(0), mix64(1), mix64(0xdeadbeef)})
	ns, vs := []int{1, 2, 4, 7, 16, 64}, []int{1, 8, 32, 33, 100}
	for _, tc := range []struct {
		topic      Topic
		partitions []int
		shards     []int
	}{
		{Topic{Tenant: "0", Table: "events"}, []int{0, 1, 2, 2, 2, 2}, []int{0, 4, 29, 29, 84}},
		{Topic{Tenant: "acme", Table: "events"}, []int{0, 0, 2, 2, 11, 11}, []int{0, 0, 22, 22, 42}},
		{Topic{Tenant: "acme", Table: "clicks"}, []int{0, 1, 2, 6, 15, 41}, []int{0, 6, 16, 16, 33}},
		{Topic{Tenant: "globex", Table: "a.b *>% c"}, []int{0, 1, 3, 3, 3, 63}, []int{0, 3, 3, 3, 89}},
		{Topic{Tenant: "t-1_x", Table: "users", Scope: "ignored"}, []int{0, 0, 0, 0, 15, 27}, []int{0, 0, 22, 22, 22}},
	} {
		got := make([]int, len(ns))
		for i, n := range ns {
			got[i], _ = natsRoute(tc.topic, n, 1)
		}
		assert.Equal(t, tc.partitions, got, "partitions of %+v", tc.topic)
		got = make([]int, len(vs))
		for i, v := range vs {
			_, got[i] = natsRoute(tc.topic, 1, v)
		}
		assert.Equal(t, tc.shards, got, "shards of %+v", tc.topic)
	}
}

// Consistent hashing, for partitions and shards alike: growing the count by
// one moves a table only into the new bucket, and about 1/(n+1) of the
// tables; the tables spread evenly. The shard does not depend on the
// partition, so every partition's shards fill.
func TestNATSRoute_Consistent(t *testing.T) {
	t.Parallel()
	const tables = 20_000
	topics := make([]Topic, tables)
	for i := range topics {
		topics[i] = Topic{Tenant: tenant.ID("t" + strconv.Itoa(i%97)), Table: "table_" + strconv.Itoa(i)}
	}
	for _, n := range []int{1, 3, 4, 8, 15} {
		movedP, movedS := 0, 0
		countsP, countsS := make([]int, n+1), make([]int, n+1)
		for _, topic := range topics {
			p0, s0 := natsRoute(topic, n, n)
			p1, s1 := natsRoute(topic, n+1, n+1)
			countsP[p1]++
			countsS[s1]++
			if p0 != p1 {
				movedP++
				require.Equal(t, n, p1, "%+v moved to an old partition", topic)
			}
			if s0 != s1 {
				movedS++
				require.Equal(t, n, s1, "%+v moved to an old shard", topic)
			}
		}
		want := float64(tables) / float64(n+1)
		assert.InDelta(t, want, float64(movedP), want*0.1, "partitions %d→%d", n, n+1)
		assert.InDelta(t, want, float64(movedS), want*0.1, "shards %d→%d", n, n+1)
		for b := range n + 1 {
			assert.InDelta(t, want, float64(countsP[b]), want*0.1, "partition %d of %d", b, n+1)
			assert.InDelta(t, want, float64(countsS[b]), want*0.1, "shard %d of %d", b, n+1)
		}
	}
	// Four shards in each of two partitions: every one of the eight holds tables.
	cells := map[[2]int]int{}
	for _, topic := range topics {
		p, s := natsRoute(topic, 2, 4)
		cells[[2]int{p, s}]++
	}
	assert.Len(t, cells, 8)
	for cell, n := range cells {
		assert.InDelta(t, tables/8, n, tables/8*0.1, "cell %v", cell)
	}
}

func TestNATSShardDurable(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "wh-ingest-0", natsShardDurable("wh-ingest", 0))
	assert.Equal(t, "wh-ingest-31", natsShardDurable("wh-ingest", 31))
	for name, want := range map[string]int{"wh-ingest-0": 0, "wh-ingest-7": 7, "wh-ingest-255": 255} {
		got, ok := parseShardDurable("wh-ingest", name)
		assert.True(t, ok, name)
		assert.Equal(t, want, got, name)
	}
	for _, name := range []string{"wh-ingest", "wh-ingest-", "wh-ingest-07", "wh-ingest-x", "wh-ingest-1-2", "other-1", "wh-ingest--1"} {
		_, ok := parseShardDurable("wh-ingest", name)
		assert.False(t, ok, name)
	}
}

func TestNATSSubjects_RoundTrip(t *testing.T) {
	t.Parallel()
	topics := []Topic{
		{Tenant: "acme", Table: "events"},
		{Tenant: "globex", Table: "a.b *>% c", Scope: "s.1"},
	}
	for _, topic := range topics {
		subj, p, err := natsIngestSubject("wh", 4, 8, topic)
		require.NoError(t, err)
		wantP, wantS := natsRoute(topic, 4, 8)
		assert.Equal(t, wantP, p)
		assert.True(t, strings.HasPrefix(subj, "wh.ingest."+strconv.Itoa(p)+"."+strconv.Itoa(wantS)+"."+string(topic.Tenant)+"."), subj)
		key, ok := natsTopicKey("wh", subj)
		require.True(t, ok, subj)
		assert.Equal(t, topic, parseTopicKey(key))

		hist, err := natsHistorySubject("wh", topic)
		require.NoError(t, err)
		assert.Equal(t, "wh.hist."+topic.key(), hist)
		key, ok = natsTopicKey("wh", hist)
		require.True(t, ok, hist)
		assert.Equal(t, topic, parseTopicKey(key))

		dlq, err := natsDLQSubject("wh", topic)
		require.NoError(t, err)
		assert.Equal(t, "wh.dlq."+topic.key(), dlq)
		key, ok = natsTopicKey("wh", dlq)
		require.True(t, ok, dlq)
		assert.Equal(t, topic, parseTopicKey(key))
	}
}

func TestNATSSubjects_RefuseATopicWithoutATenant(t *testing.T) {
	t.Parallel()
	_, _, err := natsIngestSubject("wh", 4, 8, Topic{Table: "events"})
	require.Error(t, err)
	_, err = natsDLQSubject("wh", Topic{Tenant: "a.b", Table: "events"})
	require.Error(t, err)
}

func TestNATSTopicKey_OtherSubjects(t *testing.T) {
	t.Parallel()
	for _, subj := range []string{
		"other.ingest.0.1.acme.events", "wh.ingest.acme.events", "wh.ingest.x.1.acme.events", "wh.ingest.0.x.acme.events",
		"wh.ingest.0.1.", "wh.ingest.0.1", "wh.ingest.0", "wh.dlq.", "wh.hist.", "wh.history.acme.events", "wh", "",
	} {
		_, ok := natsTopicKey("wh", subj)
		assert.False(t, ok, subj)
	}
}

func TestValidSubjectPrefix(t *testing.T) {
	t.Parallel()
	for _, ok := range []string{"wh", "acme-wh", "wh_2"} {
		require.NoError(t, validSubjectPrefix(ok), ok)
	}
	for _, bad := range []string{"", "Wh", "a.b", "a*", "a>", "a b"} {
		require.Error(t, validSubjectPrefix(bad), bad)
	}
}
