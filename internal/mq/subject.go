package mq

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// The embedded broker's naming. Private to this package: everything else
// addresses events by Topic.
const (
	// ingestStream / dlqStream are the JetStream stream names. Hardcoded — the
	// embedded NATS server is private to the WaveHouse process, so there is
	// nothing to namespace against.
	ingestStream = "WAVEHOUSE"
	dlqStream    = "WAVEHOUSE_DLQ"

	// A topic's subject is <prefix><tenant>.<table>[.<scope>]: the tenant id
	// verbatim — its grammar (tenant.Parse) admits only letters, digits, '_'
	// and '-', so it is one token as it is — then the table and scope each
	// as one encoded token. Tenant first so one wildcard selects a tenant's
	// traffic (ingest.acme.>). The same topic has the same tail on both
	// streams, so parking a message on the DLQ is a prefix swap.
	ingestPrefix = "ingest."
	dlqPrefix    = "dlq."

	ingestAll = ingestPrefix + ">" // every topic on the ingest stream
	dlqAll    = dlqPrefix + ">"    // every topic on the DLQ stream
)

// encodeToken converts any table or scope name into a safe, single NATS
// subject token. It preserves alphanumerics and underscores, but
// percent-encodes everything else (so '.', ' ', '*' and '>' can never split
// or wildcard a subject).
func encodeToken(raw string) string {
	var buf bytes.Buffer
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' {
			buf.WriteByte(b)
		} else {
			fmt.Fprintf(&buf, "%%%02X", b)
		}
	}
	return buf.String()
}

// decodeToken reverses encodeToken. url.PathUnescape handles exactly the %XX
// form encodeToken writes.
func decodeToken(safe string) (string, error) {
	return url.PathUnescape(safe)
}

// subject renders a caller's topic under prefix. The tenant is checked
// against its grammar here, on the way to the wire: an empty one — a caller
// that never set it — must not become a subject of some other tenant's, and
// one with a dot or a wildcard would split or widen the subject.
func subject(prefix string, t Topic) (string, error) {
	if _, err := tenant.Parse(string(t.Tenant)); err != nil {
		return "", fmt.Errorf("topic tenant: %w", err)
	}
	return prefix + t.key(), nil
}

// topicKey is the tail of a subject carrying prefix — by construction the
// Key() of the topic it was published on. A trim, no decoding.
func topicKey(prefix, subj string) string {
	return strings.TrimPrefix(subj, prefix)
}

// parseTopicKey recovers the Topic from a subject tail. Three tokens are
// tenant, table and scope; two are tenant and table. One token is the form
// this package wrote before the tenant led the subject (#583 story 5) and
// reads as tenant.Default's table: every event of that era was the default
// tenant's, and the durable consumers still deliver them after the upgrade,
// as the dead-letter queue still holds them. A tail this package could not
// have written — more tokens, a token that does not decode, a tenant outside
// the grammar — cannot be split reliably, so the whole of it becomes the
// table of no tenant rather than being dropped.
func parseTopicKey(tail string) Topic {
	parts := strings.Split(tail, ".")
	switch len(parts) {
	case 1:
		if table, err := decodeToken(parts[0]); err == nil && table != "" {
			return Topic{Tenant: tenant.Default, Table: table}
		}
	case 2, 3:
		id, idErr := tenant.Parse(parts[0])
		table, tableErr := decodeToken(parts[1])
		scope, scopeErr := "", error(nil)
		if len(parts) == 3 {
			scope, scopeErr = decodeToken(parts[2])
		}
		if idErr == nil && tableErr == nil && scopeErr == nil {
			return Topic{Tenant: id, Table: table, Scope: scope}
		}
	}
	return Topic{Table: tail}
}
