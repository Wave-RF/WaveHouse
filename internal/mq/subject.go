package mq

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"
)

// The embedded broker's naming. Private to this package: everything else
// addresses events by Topic.
const (
	// ingestStream / dlqStream are the JetStream stream names. Hardcoded — the
	// embedded NATS server is private to the WaveHouse process, so there is
	// nothing to namespace against.
	ingestStream = "WAVEHOUSE"
	dlqStream    = "WAVEHOUSE_DLQ"

	// A topic's subject is <prefix><table>[.<scope>], each part one encoded
	// token. The same topic has the same tail on both streams, so parking a
	// message on the DLQ is a prefix swap.
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

// subject renders topic under prefix.
func subject(prefix string, t Topic) string {
	return prefix + t.Key()
}

// topicKey is the tail of a subject carrying prefix — by construction the
// Key() of the topic it was published on. A trim, no decoding.
func topicKey(prefix, subj string) string {
	return strings.TrimPrefix(subj, prefix)
}

// parseTopicKey recovers the Topic from a subject tail. A tail this package
// did not write (extra tokens, a token that does not decode) cannot be split
// reliably, so the whole of it becomes the table rather than being dropped.
func parseTopicKey(tail string) Topic {
	parts := strings.Split(tail, ".")
	if len(parts) <= 2 {
		table, tableErr := decodeToken(parts[0])
		scope, scopeErr := "", error(nil)
		if len(parts) == 2 {
			scope, scopeErr = decodeToken(parts[1])
		}
		if tableErr == nil && scopeErr == nil {
			return Topic{Table: table, Scope: scope}
		}
	}
	return Topic{Table: tail}
}
