package mq

import (
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The subjects are pinned byte for byte: an embedded broker holds messages
// under them across an upgrade.
func TestSubject_Golden(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		topic Topic
		want  string
	}{
		{Topic{Tenant: "0", Table: "events"}, "0.events"},
		{Topic{Tenant: "acme-co", Table: "default.clicks", Scope: "org_1"}, "acme-co.default%2Eclicks.org_1"},
		{Topic{Tenant: "a", Table: "a.*.>", Scope: "*"}, "a.a%2E%2A%2E%3E.%2A"},
		{Topic{Tenant: "a", Table: "my table", Scope: "tab\there"}, "a.my%20table.tab%09here"},
		{Topic{Tenant: "a", Table: "table-with-dashes", Scope: "org-1"}, "a.table-with-dashes.org-1"},
		{Topic{Tenant: "a", Table: "100%", Scope: "a/b"}, "a.100%25.a%2Fb"},
		{Topic{Tenant: "a", Table: "{acme}:x"}, "a.%7Bacme%7D%3Ax"},
		{Topic{Tenant: "a", Table: "nul\x00", Scope: "\xff"}, "a.nul%00.%FF"},
		{Topic{Tenant: "a", Table: "caf\u00e9", Scope: "\u65e5"}, "a.caf%C3%A9.%E6%97%A5"},
		{Topic{Tenant: "a", Table: ""}, "a."},
		{Topic{Tenant: "a", Table: "t", Scope: ""}, "a.t"},
	} {
		assert.Equal(t, tt.want, tt.topic.key(), "%+v", tt.topic)
		for _, prefix := range []string{ingestPrefix, dlqPrefix} {
			subj, err := subject(prefix, tt.topic)
			require.NoError(t, err)
			assert.Equal(t, prefix+tt.want, subj)
		}
	}
}

// A token another writer left partly unescaped, or escaped in lowercase,
// still reads as it always did — and so does an earlier build's %2D for '-',
// so a message it queued reads as the same topic.
func TestParseTopicKey_LenientTokens(t *testing.T) {
	t.Parallel()
	assert.Equal(t, Topic{Tenant: "a", Table: "b~c", Scope: "d.e"}, parseTopicKey("a.b~c.d%2ee"))
	assert.Equal(t, parseTopicKey("a.table-with-dashes.org-1"), parseTopicKey("a.table%2Dwith%2Ddashes.org%2D1"))
}

func TestSubject_RoundTripsEveryTopic(t *testing.T) {
	t.Parallel()
	topics := []Topic{
		{Tenant: "0", Table: "normal_table"},
		{Tenant: "acme-co", Table: "db.schema.table"},
		{Tenant: "Acme_42", Table: "table-with-dashes", Scope: "org-1"},
		{Tenant: "9223372036854775807", Table: "table with spaces and / slashes !!", Scope: "a.b"},
		{Tenant: "0", Table: "~weird_chars_@#$%", Scope: "*.>"},
	}

	for _, topic := range topics {
		t.Run(topic.key(), func(t *testing.T) {
			t.Parallel()
			for _, prefix := range []string{ingestPrefix, dlqPrefix} {
				subj, err := subject(prefix, topic)
				require.NoError(t, err)
				assert.NotContains(t, subj[len(prefix):], "*")
				assert.NotContains(t, subj[len(prefix):], ">")
				assert.Equal(t, topic.key(), topicKey(prefix, subj), "a subject tail is the topic key")
				assert.Equal(t, topic, parseTopicKey(topicKey(prefix, subj)))
			}
		})
	}
}

func TestSubject_Shape(t *testing.T) {
	t.Parallel()
	subj, err := subject(ingestPrefix, Topic{Tenant: "0", Table: "events"})
	require.NoError(t, err)
	assert.Equal(t, "ingest.0.events", subj)
	// The tenant leads, verbatim — a dash is not encoded — so one wildcard
	// selects a tenant's traffic; the table and scope are encoded tokens.
	subj, err = subject(ingestPrefix, Topic{Tenant: "acme-co", Table: "default.clicks", Scope: "org_1"})
	require.NoError(t, err)
	assert.Equal(t, "ingest.acme-co.default%2Eclicks.org_1", subj)
	// The same topic has the same tail on the DLQ: parking is a prefix swap.
	subj, err = subject(dlqPrefix, Topic{Tenant: "acme-co", Table: "default.clicks", Scope: "org_1"})
	require.NoError(t, err)
	assert.Equal(t, "dlq.acme-co.default%2Eclicks.org_1", subj)
}

// A topic reaches the wire only with a tenant that satisfies the grammar: an
// empty one is a caller that never set it, and one with a dot or a wildcard
// would split or widen the subject.
func TestSubject_RefusesATenantOutsideTheGrammar(t *testing.T) {
	t.Parallel()
	for _, topic := range []Topic{
		{Table: "events"},
		{Tenant: "a.b", Table: "events"},
		{Tenant: "*", Table: "events"},
		{Tenant: ">", Table: "events"},
		{Tenant: "a b", Table: "events"},
	} {
		_, err := subject(ingestPrefix, topic)
		assert.Error(t, err, "%+v", topic)
	}
}

func TestTopicKey_IsInjective(t *testing.T) {
	t.Parallel()
	// A dotted table must not collide with a table + scope pair, and a
	// tenant's table must not collide with another tenant's.
	assert.NotEqual(t, Topic{Tenant: "0", Table: "a.b"}.key(), Topic{Tenant: "0", Table: "a", Scope: "b"}.key())
	assert.NotEqual(t, Topic{Tenant: "a", Table: "b"}.key(), Topic{Tenant: "b", Table: "a"}.key())
	assert.Equal(t, Topic{Tenant: "0", Table: "a", Scope: "b"}.key(), Topic{Tenant: "0", Table: "a", Scope: "b"}.key())
}

func TestParseTopicKey_ForeignTailKeepsItself(t *testing.T) {
	t.Parallel()
	// Subjects this package did not write still yield one usable topic, of
	// no tenant, rather than being dropped.
	for _, tail := range []string{
		"a.b.c.d",       // more tokens than any topic renders
		"0.bad%2Gtoken", // a token that does not decode
		"a%2Eb.events",  // a tenant outside the grammar
		"a%2Db.events",  // a tenant token is read verbatim, never decoded
		".events",       // a topic whose tenant was never set
		"events",        // one token: no tenant leads it
		"bad%2G",        // one token that does not decode
	} {
		assert.Equal(t, Topic{Table: tail}, parseTopicKey(tail), tail)
	}
	assert.Equal(t, Topic{}, parseTopicKey(""))
}

// The tenant a key leads with picks the stream its subject lands in, so it
// is read off the first token whatever the rest of the key holds.
func TestKeyTenant(t *testing.T) {
	t.Parallel()
	for key, want := range map[string]tenant.ID{"acme.t": "acme", "a.b.c.d": "a", "0.bad%2G": "0"} {
		id, ok := keyTenant(key)
		assert.True(t, ok, key)
		assert.Equal(t, want, id, key)
	}
	for _, key := range []string{"", ".events", "a%2Eb.events"} {
		_, ok := keyTenant(key)
		assert.False(t, ok, key)
	}
}

// Every tenant's two streams have names of their own: no id makes one
// kind's name another stream's, none is a stream an earlier build shared,
// and each name gives its tenant back.
func TestStreamNames_NeverCollide(t *testing.T) {
	t.Parallel()
	ids := []tenant.ID{"0", "acme", "DLQ", "DLQ_acme", "INGEST", "INGEST_acme", "_", "-", "WAVEHOUSE", tenant.ID(strings.Repeat("a", tenant.MaxLen))}
	seen := map[string]tenant.ID{legacyIngestStream: "", legacyDLQStream: ""}
	for _, id := range ids {
		for prefix, name := range map[string]string{ingestStreamPrefix: ingestStreamName(id), dlqStreamPrefix: dlqStreamName(id)} {
			other, dup := seen[name]
			assert.False(t, dup, "%s names a stream of %q's too", name, other)
			seen[name] = id
			back, ok := streamTenant(prefix, name)
			assert.True(t, ok, name)
			assert.Equal(t, id, back, name)
		}
	}
	for _, name := range []string{legacyIngestStream, legacyDLQStream, "INGEST_a.b", "DLQ_"} {
		for _, prefix := range []string{ingestStreamPrefix, dlqStreamPrefix} {
			_, ok := streamTenant(prefix, name)
			assert.False(t, ok, "%s is no tenant's %s stream", name, prefix)
		}
	}
}
