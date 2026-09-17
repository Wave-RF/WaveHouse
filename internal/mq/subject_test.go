package mq

import (
	"testing"

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
		{Table: "normal_table"},
		{Table: "db.schema.table"},
		{Table: "table-with-dashes", Scope: "org-1"},
		{Table: "table with spaces and / slashes !!", Scope: "a.b"},
		{Table: "~weird_chars_@#$%", Scope: "*.>"},
	}

	for _, topic := range topics {
		t.Run(topic.Key(), func(t *testing.T) {
			t.Parallel()
			for _, prefix := range []string{ingestPrefix, dlqPrefix} {
				subj := subject(prefix, topic)
				assert.NotContains(t, subj[len(prefix):], "*")
				assert.NotContains(t, subj[len(prefix):], ">")
				assert.Equal(t, topic.Key(), topicKey(prefix, subj), "a subject tail is the topic key")
				assert.Equal(t, topic, parseTopicKey(topicKey(prefix, subj)))
			}
		})
	}
}

func TestSubject_Shape(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "ingest.events", subject(ingestPrefix, Topic{Table: "events"}))
	assert.Equal(t, "ingest.default%2Eclicks.org_1", subject(ingestPrefix, Topic{Table: "default.clicks", Scope: "org_1"}))
	// The same topic has the same tail on the DLQ: parking is a prefix swap.
	assert.Equal(t, "dlq.default%2Eclicks.org_1", subject(dlqPrefix, Topic{Table: "default.clicks", Scope: "org_1"}))
}

func TestTopicKey_IsInjective(t *testing.T) {
	t.Parallel()
	// A dotted table must not collide with a table + scope pair.
	assert.NotEqual(t, Topic{Table: "a.b"}.Key(), Topic{Table: "a", Scope: "b"}.Key())
	assert.Equal(t, Topic{Table: "a", Scope: "b"}.Key(), Topic{Table: "a", Scope: "b"}.Key())
}

func TestParseTopicKey_ForeignTailKeepsItself(t *testing.T) {
	t.Parallel()
	// Subjects this package did not write still yield one usable topic rather
	// than being dropped.
	assert.Equal(t, Topic{Table: "a.b.c"}, parseTopicKey("a.b.c"))
	assert.Equal(t, Topic{Table: "bad%2Gtoken"}, parseTopicKey("bad%2Gtoken"))
}
