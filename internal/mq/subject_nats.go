package mq

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/keyenc"
	"github.com/Wave-RF/WaveHouse/pkg/shard"
	"github.com/nats-io/nats.go/jetstream"
)

// The external broker's subjects, on streams every tenant shares:
//
//	<prefix>.ingest.<p>.<s>.<tenant>.<table>[.<scope>]   (p, s) = natsRoute(topic, N, V)
//	<prefix>.live.<tenant>.<table>[.<scope>]             republished by each partition; no stream holds it
//	<prefix>.hist.<tenant>.<table>[.<scope>]             written by the ingest worker
//	<prefix>.dlq.<tenant>.<table>[.<scope>]              not partitioned (low volume)
//
// The tail after the shard (or after live, hist or dlq) is the topic key, the
// same one the embedded broker writes, so parking or recording a message is
// still a prefix swap.

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

// natsRoute is where topic t's table lives among n partitions of v shards
// each: shard.Jump of FNV-1a 64 over the tenant id and the table's subject
// token for the partition, and of the same hash through shard.Mix for the
// shard, which has to be independent of the partition. The scope is left
// out, so a table's scopes share one shard and one owner. Consistent: growing
// n (or v) by one moves about 1/(n+1) of the tables, each into the new
// partition (or shard). This is the one place a table is mapped.
func natsRoute(t Topic, n, v int) (p, s int) {
	key := natsRouteKey(t)
	return shard.Jump(key, n), shard.Jump(shard.Mix(key), v)
}

// natsRouteKey is the hash a table is placed by.
func natsRouteKey(t Topic) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(t.Tenant))
	_, _ = h.Write([]byte{'.'})
	_, _ = h.Write([]byte(keyenc.Escape(t.Table)))
	return h.Sum64()
}

// natsEarlierPartitions is the partitions topic t's table lived in under
// every partition count below n, in the order it moved through them, without
// the one it lives in under n. A jump hash only ever moves a key into the
// newest bucket, so these are the only partitions among the n that can still
// hold rows the table queued before the count was raised: about ln(n) of
// them, none for most tables at a small n.
func natsEarlierPartitions(t Topic, n int) []int {
	key := natsRouteKey(t)
	now := shard.Jump(key, n)
	var earlier []int
	for k := 1; k < n; k++ {
		if p := shard.Jump(key, k); p != now && !slices.Contains(earlier, p) {
			earlier = append(earlier, p)
		}
	}
	return earlier
}

// natsIngestPartition is every subject of ingest partition p: what its
// stream holds.
func natsIngestPartition(prefix string, p int) string {
	return prefix + ".ingest." + strconv.Itoa(p) + ".>"
}

// natsShardFilter is every subject of shard s of partition p: what its
// durable filters on.
func natsShardFilter(prefix string, p, s int) string {
	return prefix + ".ingest." + strconv.Itoa(p) + "." + strconv.Itoa(s) + ".>"
}

// natsShardDurable is shard s's durable on every partition. It is not
// zero-padded, so a durable keeps its name when the shard count grows.
func natsShardDurable(consumerPrefix string, s int) string {
	return consumerPrefix + "-" + strconv.Itoa(s)
}

// parseShardDurable is the shard a durable named by natsShardDurable
// serves, false for any other name.
func parseShardDurable(consumerPrefix, name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, consumerPrefix+"-")
	if !ok || rest == "" || (len(rest) > 1 && rest[0] == '0') {
		return 0, false
	}
	s, err := strconv.ParseUint(rest, 10, 31)
	return int(s), err == nil
}

// natsHistorySubjects is every subject of the history stream.
func natsHistorySubjects(prefix string) string { return prefix + ".hist.>" }

// natsHistorySubject is where topic t's rows are recorded once written.
func natsHistorySubject(prefix string, t Topic) (string, error) {
	return subject(prefix+".hist.", t)
}

// natsLiveSubjects is every subject of the live feed, which no stream holds.
func natsLiveSubjects(prefix string) string { return prefix + ".live.>" }

// natsRepublish is how partition p republishes each row it stores to the
// live feed: the same subject with the partition and shard tokens dropped.
func natsRepublish(prefix string, p int) jetstream.RePublish {
	return jetstream.RePublish{Source: prefix + ".ingest." + strconv.Itoa(p) + ".*.>", Destination: natsLiveSubjects(prefix)}
}

// natsWaitingSubject is every subject topic t's rows can be waiting on in a
// stream holding ingest partition p — whatever shard a process on another
// shard count published them under — or, for p below zero, in a stream a
// lower partition count left behind, whatever its partition was.
func natsWaitingSubject(prefix string, p int, t Topic) (string, error) {
	partition := "*"
	if p >= 0 {
		partition = strconv.Itoa(p)
	}
	return subject(prefix+".ingest."+partition+".*.", t)
}

// natsDLQSubjects is every subject of the shared dead-letter stream.
func natsDLQSubjects(prefix string) string { return prefix + ".dlq.>" }

// natsIngestSubject is where topic t is published among n partitions of v
// shards, and the partition that holds it.
func natsIngestSubject(prefix string, n, v int, t Topic) (string, int, error) {
	p, s := natsRoute(t, n, v)
	subj, err := subject(prefix+".ingest."+strconv.Itoa(p)+"."+strconv.Itoa(s)+".", t)
	return subj, p, err
}

// natsDLQSubject is where a message on topic t is parked.
func natsDLQSubject(prefix string, t Topic) (string, error) {
	return subject(prefix+".dlq.", t)
}

// natsTopicKey is the topic key a subject under prefix carries — the
// partition and shard, the live, the hist or the dlq token stripped — false
// for a subject of none of those kinds.
func natsTopicKey(prefix, subj string) (string, bool) {
	rest, ok := strings.CutPrefix(subj, prefix+".")
	if !ok {
		return "", false
	}
	if key, ok := strings.CutPrefix(rest, "dlq."); ok {
		return key, key != ""
	}
	if key, ok := strings.CutPrefix(rest, "hist."); ok {
		return key, key != ""
	}
	if key, ok := strings.CutPrefix(rest, "live."); ok {
		return key, key != ""
	}
	rest, ok = strings.CutPrefix(rest, "ingest.")
	if !ok {
		return "", false
	}
	for range 2 { // the partition, then the shard
		var n string
		n, rest, ok = strings.Cut(rest, ".")
		if !ok {
			return "", false
		}
		if _, err := strconv.ParseUint(n, 10, 31); err != nil {
			return "", false
		}
	}
	return rest, rest != ""
}
