package mq

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// partitionOf agrees with the server's own {{partition(n,…)}} mapping, so a
// later move to a server-side mapping keeps every tenant in its partition.
func TestPartitionOf_MatchesServerMapping(t *testing.T) {
	const n, tenants = 8, 10_000
	s, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true})
	require.NoError(t, err)
	s.Start()
	require.True(t, s.ReadyForConnections(10*time.Second))
	t.Cleanup(s.Shutdown)
	require.NoError(t, s.GlobalAccount().AddMapping("x.*", "x.{{partition("+strconv.Itoa(n)+",1)}}.{{wildcard(1)}}"))

	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	got := make(chan string, tenants)
	_, err = nc.Subscribe("x.>", func(m *nats.Msg) { got <- m.Subject })
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404: reproducible test tenants, not secrets
	for range tenants {
		b := make([]byte, 1+rng.IntN(tenant.MaxLen))
		for i := range b {
			b[i] = alphabet[rng.IntN(len(alphabet))]
		}
		require.NoError(t, nc.Publish("x."+string(b), nil))
	}
	require.NoError(t, nc.Flush())
	for range tenants {
		select {
		case subj := <-got:
			parts := strings.SplitN(subj, ".", 3)
			require.Len(t, parts, 3, subj)
			id, err := tenant.Parse(parts[2])
			require.NoError(t, err)
			assert.Equal(t, parts[1], strconv.Itoa(partitionOf(id, n)), "tenant %s", id)
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for mapped messages")
		}
	}
}

func TestNATSSubjects_RoundTrip(t *testing.T) {
	topics := []Topic{
		{Tenant: "acme", Table: "events"},
		{Tenant: "globex", Table: "a.b *>% c", Scope: "s.1"},
	}
	for _, topic := range topics {
		subj, err := natsIngestSubject("wh", 4, topic)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(subj, "wh.ingest."+strconv.Itoa(partitionOf(topic.Tenant, 4))+"."+string(topic.Tenant)+"."), subj)
		key, ok := natsTopicKey("wh", subj)
		require.True(t, ok, subj)
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
	_, err := natsIngestSubject("wh", 4, Topic{Table: "events"})
	require.Error(t, err)
	_, err = natsDLQSubject("wh", Topic{Tenant: "a.b", Table: "events"})
	require.Error(t, err)
}

func TestNATSTopicKey_OtherSubjects(t *testing.T) {
	for _, subj := range []string{
		"other.ingest.0.acme.events", "wh.ingest.acme.events", "wh.ingest.x.acme.events",
		"wh.ingest.0.", "wh.ingest.0", "wh.dlq.", "wh.history.acme.events", "wh", "",
	} {
		_, ok := natsTopicKey("wh", subj)
		assert.False(t, ok, subj)
	}
}

func TestValidSubjectPrefix(t *testing.T) {
	for _, ok := range []string{"wh", "acme-wh", "wh_2"} {
		require.NoError(t, validSubjectPrefix(ok), ok)
	}
	for _, bad := range []string{"", "Wh", "a.b", "a*", "a>", "a b"} {
		require.Error(t, validSubjectPrefix(bad), bad)
	}
}
