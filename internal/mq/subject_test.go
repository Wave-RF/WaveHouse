package mq

import (
	"strings"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		raw      string
		expected string
	}{
		{"safe string", "my_table123", "my_table123"},
		{"with dots", "default.clicks", "default%2Eclicks"},
		{"with spaces", "my table", "my%20table"},
		{"with dashes and slashes", "a-b/c", "a%2Db%2Fc"},
		{"wildcards cannot survive", "a.*.>", "a%2E%2A%2E%3E"},
		{"empty string", "", ""},
		{"only safe characters", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, encodeToken(tt.raw))
		})
	}
}

func TestDecodeToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		safe     string
		expected string
		wantErr  bool
	}{
		{"safe string", "my_table123", "my_table123", false},
		{"encoded dots", "default%2Eclicks", "default.clicks", false},
		{"encoded spaces", "my%20table", "my table", false},
		{"invalid percent encoding", "default%2Gclicks", "", true}, // %2G is not valid hex
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeToken(tt.safe)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
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
