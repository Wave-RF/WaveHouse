package mq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeadLetterTables(t *testing.T) {
	t.Parallel()
	subjects := map[string]uint64{
		"dlq.0.a%2Eb":         1, // table "a.b"
		"dlq.0.a.b":           2, // table "a", scope "b"
		"dlq.0.a":             4, // table "a", unscoped
		"dlq.0.my-t":          8,
		"dlq.0.my%2Dt.org-1":  16, // v0.1.0's escaping of '-', scoped
		"dlq.0.clicks.org%2E": 32,
	}
	assert.Equal(t, map[string]uint64{"a.b": 1, "a": 6, "my-t": 24, "clicks": 32},
		deadLetterTables(subjects, dlqPrefix, ""), "a dotted table never shares a count with a table + scope")
	assert.Equal(t, map[string]uint64{"a": 6}, deadLetterTables(subjects, dlqPrefix, "a"), "the filter keeps every scope of its table")
	assert.Equal(t, map[string]uint64{"a.b": 1}, deadLetterTables(subjects, dlqPrefix, "a.b"))
	assert.Empty(t, deadLetterTables(subjects, dlqPrefix, "never_failed"))
}
