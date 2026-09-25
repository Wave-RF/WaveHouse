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
	// Each tenant's queue is a pair of JetStream streams named after it:
	// INGEST_<tenant> and DLQ_<tenant>. The prefixes differ in their first
	// letter, so no tenant id makes one kind's name the other's, and the
	// tenant grammar (tenant.Parse: letters, digits, '_' and '-', at most
	// tenant.MaxLen bytes) keeps every name inside JetStream's. No namespacing
	// beyond that: the embedded server is private to the WaveHouse process.
	ingestStreamPrefix = "INGEST_"
	dlqStreamPrefix    = "DLQ_"

	// legacyIngestStream / legacyDLQStream are the one pair an earlier build
	// kept for every tenant. Their subjects (ingest.> and dlq.>) overlap every
	// tenant's, and JetStream refuses a stream whose subjects overlap
	// another's, so NewEmbedded deletes them.
	legacyIngestStream = "WAVEHOUSE"
	legacyDLQStream    = "WAVEHOUSE_DLQ"

	// A topic's subject is <prefix><tenant>.<table>[.<scope>]: the tenant id
	// verbatim — its grammar (tenant.Parse) admits only letters, digits, '_'
	// and '-', so it is one token as it is — then the table and scope each
	// as one encoded token. Tenant first so one wildcard selects a tenant's
	// traffic (ingest.acme.>), which is what the tenant's streams hold. The
	// same topic has the same tail on both kinds, so parking a message on the
	// dead-letter queue is a prefix swap.
	ingestPrefix = "ingest."
	dlqPrefix    = "dlq."
)

// ingestStreamName / dlqStreamName name tenant id's two streams.
func ingestStreamName(id tenant.ID) string { return ingestStreamPrefix + string(id) }
func dlqStreamName(id tenant.ID) string    { return dlqStreamPrefix + string(id) }

// tenantSubjects is every subject of tenant id's under prefix: what its
// stream of that kind holds.
func tenantSubjects(prefix string, id tenant.ID) string { return prefix + string(id) + ".>" }

// streamTenant recovers the tenant a stream name carries under prefix, false
// for any other name: a stream of the other kind, a legacy one, or a name no
// tenant id could have produced.
func streamTenant(prefix, name string) (tenant.ID, bool) {
	rest, ok := strings.CutPrefix(name, prefix)
	if !ok {
		return "", false
	}
	id, err := tenant.Parse(rest)
	return id, err == nil
}

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

// topicKey is the tail of a subject carrying prefix — the key() of the topic
// it was published on. A trim, no decoding.
func topicKey(prefix, subj string) string {
	return strings.TrimPrefix(subj, prefix)
}

// keyTenant is the tenant a topic key leads with — the token that decides
// which tenant's stream its subject lands in — whether or not the rest of
// the key parses.
func keyTenant(key string) (tenant.ID, bool) {
	first, _, _ := strings.Cut(key, ".")
	id, err := tenant.Parse(first)
	return id, err == nil
}

// parseTopicKey recovers the Topic from a subject tail. Three tokens are
// tenant, table and scope; two are tenant and table. A tail this package
// could not have written — one token, more than three, a token that does not
// decode, a tenant outside the grammar — cannot be split reliably, so the
// whole of it becomes the table of no tenant rather than being dropped.
func parseTopicKey(tail string) Topic {
	parts := strings.Split(tail, ".")
	switch len(parts) {
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
