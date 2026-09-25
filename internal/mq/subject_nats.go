package mq

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// The external broker's subjects, on streams every tenant shares:
//
//	<prefix>.ingest.<p>.<tenant>.<table>[.<scope>]   p = partitionOf(tenant, N)
//	<prefix>.dlq.<tenant>.<table>[.<scope>]          not partitioned (low volume)
//
// The tail after the partition (or after dlq) is the topic key, the same one
// the embedded broker writes, so parking a message is still a prefix swap.

// subjectPrefixPattern is the grammar of a subject prefix: one lowercase
// token, so it can neither split nor wildcard a subject, and uppercased it is
// still a valid stream name.
var subjectPrefixPattern = regexp.MustCompile(`^[a-z0-9_-]+$`)

// validSubjectPrefix reports why prefix cannot lead the external subjects.
func validSubjectPrefix(prefix string) error {
	if !subjectPrefixPattern.MatchString(prefix) {
		return fmt.Errorf("subject prefix %q must be one token of [a-z0-9_-]", prefix)
	}
	return nil
}

// partitionOf is the ingest partition that holds tenant id's events among n:
// FNV-1a 32 of the tenant id, mod n. It is the hash NATS's own
// {{partition(n,…)}} subject mapping uses, so moving the partitioning into a
// server-side mapping later keeps every tenant where it is.
func partitionOf(id tenant.ID, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(n)) //nolint:gosec // n is a small positive partition count
}

// natsIngestPartition is every subject of ingest partition p: what its
// stream holds, and what its wh-ingest durable filters on.
func natsIngestPartition(prefix string, p int) string {
	return prefix + ".ingest." + strconv.Itoa(p) + ".>"
}

// natsDLQSubjects is every subject of the shared dead-letter stream.
func natsDLQSubjects(prefix string) string { return prefix + ".dlq.>" }

// natsIngestSubject is where topic t is published among n partitions.
func natsIngestSubject(prefix string, n int, t Topic) (string, error) {
	return subject(prefix+".ingest."+strconv.Itoa(partitionOf(t.Tenant, n))+".", t)
}

// natsDLQSubject is where a message on topic t is parked.
func natsDLQSubject(prefix string, t Topic) (string, error) {
	return subject(prefix+".dlq.", t)
}

// natsTopicKey is the topic key a subject under prefix carries — the ingest
// partition or the dlq token stripped — false for a subject of neither kind.
func natsTopicKey(prefix, subj string) (string, bool) {
	rest, ok := strings.CutPrefix(subj, prefix+".")
	if !ok {
		return "", false
	}
	if key, ok := strings.CutPrefix(rest, "dlq."); ok {
		return key, key != ""
	}
	rest, ok = strings.CutPrefix(rest, "ingest.")
	if !ok {
		return "", false
	}
	p, key, ok := strings.Cut(rest, ".")
	if !ok || key == "" {
		return "", false
	}
	if _, err := strconv.ParseUint(p, 10, 31); err != nil {
		return "", false
	}
	return key, true
}
