package mq

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// NATSTopology is what WaveHouse needs of an operator-owned JetStream: N
// ingest partition streams with work-queue retention, each split into V
// shards by a subject token, with one pinned-client durable per shard, and
// each republishing every row it stores to the history
// stream (limits retention, for SSE replay and the live hub) under the
// row's subject without the partition; one dead-letter stream; and,
// for coord.backend=nats, a KV bucket holding the leases (Leases). The
// operator creates all of it (WriteNATSManifests renders it as nack CRs);
// WaveHouse only checks it (verifyNATSTopology) and never repairs it.
type NATSTopology struct {
	// Prefix leads every subject: <prefix>.ingest.<p>.…, <prefix>.hist.… and
	// <prefix>.dlq.….
	Prefix string
	// Partitions is N, the number of ingest partition streams.
	Partitions int
	// Shards is V, the shards of every partition: a table's rows are all in
	// one shard, and each shard has a durable of its own.
	Shards int
	// IngestConsumer names the shard durables: shard s's on every partition
	// is <IngestConsumer>-<s> (natsShardDurable).
	IngestConsumer string
	// HistoryStream names the history stream, which holds <prefix>.hist.>.
	HistoryStream string
	// PublishTimeout bounds one publish attempt; a partition's duplicate
	// window must cover every attempt (minDuplicateWindow), so a retried
	// publish is not stored twice.
	PublishTimeout time.Duration
	// DedupeLease is how long a dedupe claim stays pending (dedupe.lease); 0
	// skips its rule. A publish whose outcome was unknown keeps its claim
	// until the lease lapses, and the client's retry is published under the
	// same idempotency key, so a partition's duplicate window must still hold
	// the first copy then (dedupeDuplicateWindow).
	DedupeLease time.Duration
	// CoordBucket is the KV bucket this process holds its leases in; empty
	// when it holds none there, and then the bucket is not checked.
	CoordBucket string
	// AckWait, MaxAckPending and Prefetch are what the ingest worker asks of
	// the durable (internal/ingest/worker.go, which imports this package).
	AckWait       time.Duration
	MaxAckPending int
	Prefetch      int
}

// Defaults for a NATSTopology's zero fields.
const (
	DefaultNATSSubjectPrefix = "wh"
	DefaultNATSPartitions    = 1
	DefaultNATSShards        = 32
	// MaxNATSShards bounds V: the wavehouse user's permissions name every
	// shard's durable, and each shard is a consumer (a Raft group at R3).
	MaxNATSShards             = 256
	DefaultNATSIngestConsumer = "wh-ingest"
	defaultNATSPublishTimeout = 5 * time.Second
	defaultNATSAckWait        = 60 * time.Second
	defaultNATSMaxAckPending  = 10_000
	defaultNATSPrefetch       = 500
)

// withDefaults fills t's zero fields.
func (t NATSTopology) withDefaults() NATSTopology {
	if t.Prefix == "" {
		t.Prefix = DefaultNATSSubjectPrefix
	}
	if t.Partitions == 0 {
		t.Partitions = DefaultNATSPartitions
	}
	if t.Shards == 0 {
		t.Shards = DefaultNATSShards
	}
	if t.IngestConsumer == "" {
		t.IngestConsumer = DefaultNATSIngestConsumer
	}
	if t.HistoryStream == "" {
		t.HistoryStream = t.streamName("HISTORY")
	}
	if t.PublishTimeout == 0 {
		t.PublishTimeout = defaultNATSPublishTimeout
	}
	if t.AckWait == 0 {
		t.AckWait = defaultNATSAckWait
	}
	if t.MaxAckPending == 0 {
		t.MaxAckPending = defaultNATSMaxAckPending
	}
	if t.Prefetch == 0 {
		t.Prefetch = defaultNATSPrefetch
	}
	return t
}

// validate reports a topology no deployment could satisfy.
func (t NATSTopology) validate() error {
	if err := validSubjectPrefix(t.Prefix); err != nil {
		return err
	}
	if t.Partitions < 1 {
		return fmt.Errorf("partitions must be at least 1, got %d", t.Partitions)
	}
	if t.Shards < 1 || t.Shards > MaxNATSShards {
		return fmt.Errorf("shards must be from 1 to %d, got %d", MaxNATSShards, t.Shards)
	}
	if !natsBucketName.MatchString(t.coordBucket()) {
		return fmt.Errorf("coord bucket %q must be a KV bucket name of [a-zA-Z0-9_-]", t.coordBucket())
	}
	return nil
}

// natsBucketName is JetStream's grammar for a KV bucket name.
var natsBucketName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// DefaultNATSCoordBucket is the lease bucket's name for a subject prefix, as
// the generated manifests name it: one per prefix, so deployments sharing a
// NATS account under different prefixes never contend for one lease.
func DefaultNATSCoordBucket(prefix string) string { return prefix + "_coord" }

// coordBucket is the lease bucket: the configured one, or the prefix's.
func (t NATSTopology) coordBucket() string {
	if t.CoordBucket != "" {
		return t.CoordBucket
	}
	return DefaultNATSCoordBucket(t.Prefix)
}

// streamName is the name the generated manifests give a stream of kind. Only
// the history's is binding; the others are found by subject.
func (t NATSTopology) streamName(kind string) string {
	return strings.ToUpper(t.Prefix) + "_" + kind
}

// minDuplicateWindow is the shortest duplicate window that stores a publish
// once however many of its attempts were stored: ExternalNATS sends the last
// retry this long after the first attempt.
func (t NATSTopology) minDuplicateWindow() time.Duration {
	return (publishRetries+1)*t.PublishTimeout + publishRetries*publishRetryWait
}

// dedupeDuplicateWindow is how long after a claim its record's retry can
// still be published under the same idempotency key: the lease, the
// Retry-After a client obeys (the lease rounded up to a second), and the
// second a claim can outlive its lease — config's rule for the embedded
// queue's window, applied here to the operator's.
func (t NATSTopology) dedupeDuplicateWindow() time.Duration {
	ceil := t.DedupeLease
	if r := ceil % time.Second; r != 0 {
		ceil += time.Second - r
	}
	return t.DedupeLease + ceil + time.Second
}

// natsPriorityGroup is the one priority group of every shard durable: a
// worker pulls a shard in it, and the server delivers to the one pinned
// puller.
const natsPriorityGroup = "wavehouse"

// natsPullExpiry is the most time between two pulls of a shard that is at
// its cap or halted, which only renew its pin (every pull itself waits a
// second at most); boot also requires max_expires to allow a pull this long. The server renews a pin only when its holder sends a new
// pull, so a live owner keeps its pin only while this is well under the
// durable's pinned TTL.
const natsPullExpiry = 5 * time.Second

// minPinnedTTL is the shortest pinned TTL a shard durable may have: twice
// the time between pulls, so a live owner pulls at least once before its pin
// lapses.
const minPinnedTTL = 2 * natsPullExpiry

// FindingSeverity says whether a finding stops WaveHouse from serving.
type FindingSeverity int

const (
	// FindingRequired refuses boot.
	FindingRequired FindingSeverity = iota
	// FindingRecommended is logged as a warning.
	FindingRecommended
)

func (s FindingSeverity) String() string {
	if s == FindingRequired {
		return "required"
	}
	return "recommended"
}

// Finding is one way the operator's topology differs from what WaveHouse
// needs.
type Finding struct {
	Severity FindingSeverity
	// Object is what the finding is about, e.g. "stream WH_INGEST_0".
	Object string
	// Field is the setting, e.g. "retention", in the stream or consumer
	// config's own (JSON) names.
	Field string
	// Problem says what is wrong and what is needed.
	Problem string
}

func (f Finding) String() string {
	return fmt.Sprintf("%s: %s: %s: %s", f.Severity, f.Object, f.Field, f.Problem)
}

// ErrTopology is what a TopologyError matches.
var ErrTopology = errors.New("nats topology does not match what WaveHouse needs")

// TopologyError lists every finding from the last check, required ones first.
type TopologyError struct {
	Findings []Finding
}

func (e *TopologyError) Error() string {
	var b strings.Builder
	b.WriteString(ErrTopology.Error())
	for _, f := range e.Findings {
		b.WriteString("\n  - ")
		b.WriteString(f.String())
	}
	return b.String()
}

func (e *TopologyError) Unwrap() error { return ErrTopology }

// hasRequired reports whether any finding refuses boot.
func hasRequired(findings []Finding) bool {
	return slices.ContainsFunc(findings, func(f Finding) bool { return f.Severity == FindingRequired })
}

// minNATSServer is the oldest server whose features the topology relies on:
// pinned-client priority groups and unpinning (2.11) and resetting a consumer
// to its ack floor (2.14), besides stream metadata, subject-filtered stream
// info and discard_new_per_subject.
var minNATSServer = [3]int{2, 14, 0}

// recommendedNATSMinor is the server line the embedded broker runs.
const recommendedNATSMinor = "2.14."

// topologyVerifier accumulates the findings of one check.
type topologyVerifier struct {
	js       jetstream.JetStream
	t        NATSTopology
	findings []Finding
}

func (v *topologyVerifier) add(sev FindingSeverity, object, field, format string, args ...any) {
	v.findings = append(v.findings, Finding{Severity: sev, Object: object, Field: field, Problem: fmt.Sprintf(format, args...)})
}

// verifyNATSTopology checks the operator's JetStream against t and returns
// every finding at once. The error is for a check that could not run (the
// server unreachable, a request refused); a missing stream or consumer is a
// finding.
func verifyNATSTopology(ctx context.Context, js jetstream.JetStream, t NATSTopology) ([]Finding, error) {
	t = t.withDefaults()
	if err := t.validate(); err != nil {
		return nil, err
	}
	v := &topologyVerifier{js: js, t: t}
	v.serverVersion(js.Conn().ConnectedServerVersion())

	partitions := make([]string, t.Partitions)
	for p := range t.Partitions {
		name, err := v.partition(ctx, p)
		if err != nil {
			return nil, err
		}
		if q := slices.Index(partitions[:p], name); name != "" && q >= 0 {
			v.add(FindingRequired, "stream "+name, "subjects", "holds partitions %d and %d; each partition needs a stream of its own", q, p)
		}
		partitions[p] = name
	}
	if err := v.extras(ctx, partitions); err != nil {
		return nil, err
	}
	if err := v.history(ctx); err != nil {
		return nil, err
	}
	if err := v.dlq(ctx); err != nil {
		return nil, err
	}
	if t.CoordBucket != "" {
		if err := v.coordBucket(ctx); err != nil {
			return nil, err
		}
	}
	slices.SortStableFunc(v.findings, func(a, b Finding) int { return int(a.Severity) - int(b.Severity) })
	return v.findings, nil
}

// awaitNATSTopology repeats the check until nothing required is missing or
// wait runs out — on Kubernetes the operator's CRs roll out with the pods —
// and then returns the warnings, or a *TopologyError with every finding of
// the last check. A check that could not run is retried the same way.
func awaitNATSTopology(ctx context.Context, js jetstream.JetStream, t NATSTopology, wait time.Duration) ([]Finding, error) {
	deadline := time.Now().Add(wait)
	backoff := 250 * time.Millisecond
	for {
		findings, err := verifyNATSTopology(ctx, js, t)
		if err == nil && !hasRequired(findings) {
			return findings, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if err != nil {
				return nil, fmt.Errorf("check nats topology: %w", err)
			}
			return nil, &TopologyError{Findings: findings}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(backoff, remaining)):
		}
		backoff = min(2*backoff, 5*time.Second)
	}
}

func (v *topologyVerifier) serverVersion(version string) {
	const object, field = "server", "version"
	var got [3]int
	parts := strings.SplitN(strings.TrimPrefix(version, "v"), ".", 3)
	parsed := len(parts) == 3
	for i := 0; parsed && i < 3; i++ {
		// Only the leading digits: a pre-release suffix rides on the patch.
		end := strings.IndexFunc(parts[i], func(r rune) bool { return r < '0' || r > '9' })
		if end < 0 {
			end = len(parts[i])
		}
		n, err := strconv.Atoi(parts[i][:end])
		parsed = err == nil
		got[i] = n
	}
	switch {
	case !parsed:
		v.add(FindingRequired, object, field, "cannot read server version %q; need at least %d.%d.%d", version, minNATSServer[0], minNATSServer[1], minNATSServer[2])
	case slices.Compare(got[:], minNATSServer[:]) < 0:
		v.add(FindingRequired, object, field, "is %s; need at least %d.%d.%d", version, minNATSServer[0], minNATSServer[1], minNATSServer[2])
	case !strings.HasPrefix(strings.TrimPrefix(version, "v"), recommendedNATSMinor):
		v.add(FindingRecommended, object, field, "is %s; WaveHouse is tested against %sx", version, recommendedNATSMinor)
	}
}

// streamsHolding lists the streams whose subjects match subject.
func (v *topologyVerifier) streamsHolding(ctx context.Context, subject string) ([]string, error) {
	lister := v.js.StreamNames(ctx, jetstream.WithStreamListSubject(subject))
	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("list streams holding %s: %w", subject, err)
	}
	slices.Sort(names)
	return names, nil
}

// findBySubject finds the one stream holding probe, or adds a finding and
// returns nil.
func (v *topologyVerifier) findBySubject(ctx context.Context, object, probe string) (jetstream.Stream, error) {
	names, err := v.streamsHolding(ctx, probe)
	if err != nil {
		return nil, err
	}
	switch len(names) {
	case 0:
		v.add(FindingRequired, object, "subjects", "no stream holds %s", probe)
		return nil, nil
	case 1:
	default:
		v.add(FindingRequired, object, "subjects", "streams %s all hold %s; exactly one may", strings.Join(names, ", "), probe)
		return nil, nil
	}
	return v.stream(ctx, object, names[0])
}

// stream looks a stream up by name, adding a finding when it does not exist.
func (v *topologyVerifier) stream(ctx context.Context, object, name string) (jetstream.Stream, error) {
	s, err := v.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		v.add(FindingRequired, object, "name", "stream %s does not exist", name)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", name, err)
	}
	return s, nil
}

// partition checks ingest partition p and its durable, returning the stream's
// name ("" when there is none to check).
func (v *topologyVerifier) partition(ctx context.Context, p int) (string, error) {
	t := v.t
	filter := natsIngestPartition(t.Prefix, p)
	s, err := v.findBySubject(ctx, fmt.Sprintf("ingest partition %d", p), fmt.Sprintf("%s.ingest.%d.x", t.Prefix, p))
	if s == nil || err != nil {
		return "", err
	}
	cfg := s.CachedInfo().Config
	obj := "stream " + cfg.Name
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }
	rec := func(field, format string, args ...any) { v.add(FindingRecommended, obj, field, format, args...) }

	if !slices.Contains(cfg.Subjects, filter) {
		req("subjects", "are %q; must include %q", cfg.Subjects, filter)
	}
	// Work queue, not interest: a row no consumer's filter covers yet is
	// kept rather than dropped, and a row leaves once the worker acks it.
	if cfg.Retention != jetstream.WorkQueuePolicy {
		req("retention", "is %s; must be workqueue, so a row is kept until the ingest worker acks it, even while no consumer covers it", cfg.Retention)
	}
	v.republish(obj, cfg.RePublish, p)
	if cfg.Discard != jetstream.DiscardNew {
		req("discard", "is %s; must be new, so a full partition refuses rather than dropping unwritten rows", cfg.Discard)
	}
	if cfg.MaxBytes <= 0 {
		req("max_bytes", "is unlimited; must be set, to bound the disk and signal backpressure")
	}
	if cfg.MaxAge != 0 {
		req("max_age", "is %s; must be unset, since an age limit drops unwritten rows", cfg.MaxAge)
	}
	if cfg.Storage != jetstream.FileStorage {
		req("storage", "is %s; must be file", cfg.Storage)
	}
	if cfg.Duplicates < t.minDuplicateWindow() {
		req("duplicate_window", "is %s; must be at least %s (every attempt of a retried publish), so it is stored once", cfg.Duplicates, t.minDuplicateWindow())
	}
	if t.DedupeLease > 0 && cfg.Duplicates < t.dedupeDuplicateWindow() {
		req("duplicate_window", "is %s; must be at least %s for dedupe.lease %s (the lease, a Retry-After of it rounded up, and a second), so the retry of a publish whose outcome was unknown is stored once", cfg.Duplicates, t.dedupeDuplicateWindow(), t.DedupeLease)
	}
	if cfg.NoAck {
		req("no_ack", "is set; publishes must be acknowledged")
	}
	if cfg.Sealed {
		req("sealed", "is set; the partition must take publishes")
	}
	if cfg.Mirror != nil {
		req("mirror", "is set; a partition must not be a mirror")
	}
	switch {
	case cfg.MaxMsgsPerSubject > 0 && !cfg.DiscardNewPerSubject:
		// Without it the server keeps the cap by evicting the topic's oldest rows.
		req("discard_new_per_subject", "is unset while max_msgs_per_subject is %d; must be set, or a topic at its cap loses its oldest unwritten rows", cfg.MaxMsgsPerSubject)
	case cfg.MaxMsgsPerSubject <= 0:
		rec("max_msgs_per_subject", "set it with discard_new_per_subject, so one topic cannot fill the partition for every tenant in it")
	}
	if !cfg.DenyPurge || !cfg.DenyDelete {
		rec("deny_purge", "set deny_purge and deny_delete; nothing should remove unwritten rows")
	}
	if cfg.PersistMode == jetstream.AsyncPersistMode {
		req("persist_mode", "is async; must be default, or an ack precedes the write and a crash of the server process loses unwritten rows")
	}
	v.replicas(obj, cfg.Replicas)
	gotP, hasP := cfg.Metadata["wavehouse.dev/partition"]
	gotN, hasN := cfg.Metadata["wavehouse.dev/partitions"]
	switch {
	case !hasP || !hasN:
		rec("metadata", "set wavehouse.dev/partition and wavehouse.dev/partitions, so a partition count mismatch is caught by name")
	case gotP != strconv.Itoa(p) || gotN != strconv.Itoa(t.Partitions):
		req("metadata", "says partition %s of %s; WaveHouse is configured for partition %d of %d (mq.nats.partitions must match the operator's)", gotP, gotN, p, t.Partitions)
	}

	shards, hasV := cfg.Metadata["wavehouse.dev/shards"]
	switch {
	case !hasV:
		rec("metadata", "set wavehouse.dev/shards, so a shard count mismatch is caught by name")
	case shards != strconv.Itoa(t.Shards):
		req("metadata", "says %s shards; WaveHouse is configured for %d (mq.nats.shards must match the operator's)", shards, t.Shards)
	}
	for sh := range t.Shards {
		if err := v.durable(ctx, s, natsShardDurable(t.IngestConsumer, sh), natsShardFilter(t.Prefix, p, sh)); err != nil {
			return "", err
		}
	}
	return cfg.Name, nil
}

// durable checks shard durable name on stream s, filtering on filter.
func (v *topologyVerifier) durable(ctx context.Context, s jetstream.Stream, name, filter string) error {
	t := v.t
	stream := s.CachedInfo().Config.Name
	obj := "consumer " + stream + "/" + name
	c, err := s.Consumer(ctx, name)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		v.add(FindingRequired, obj, "durable_name", "does not exist")
		return nil
	}
	if errors.Is(err, jetstream.ErrNotPullConsumer) {
		v.add(FindingRequired, obj, "deliver_subject", "is set; must be a pull consumer")
		return nil
	}
	if err != nil {
		return fmt.Errorf("consumer %s/%s: %w", stream, name, err)
	}
	cfg := c.CachedInfo().Config
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }

	if cfg.AckPolicy != jetstream.AckExplicitPolicy {
		req("ack_policy", "is %s; must be explicit", cfg.AckPolicy)
	}
	if cfg.AckWait < t.AckWait {
		req("ack_wait", "is %s; must be at least %s, the ingest worker's", cfg.AckWait, t.AckWait)
	}
	if cfg.MaxDeliver != -1 {
		req("max_deliver", "is %d; must be -1, or a row that is never written stays in the partition undelivered", cfg.MaxDeliver)
	}
	switch {
	case cfg.MaxAckPending <= 0:
		req("max_ack_pending", "is %d; must be set", cfg.MaxAckPending)
	case cfg.MaxAckPending < t.MaxAckPending:
		v.add(FindingRecommended, obj, "max_ack_pending", "is %d; the ingest worker expects %d", cfg.MaxAckPending, t.MaxAckPending)
	}
	if cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
		req("deliver_policy", "is %s; must be all", cfg.DeliverPolicy)
	}
	filters := cfg.FilterSubjects
	if cfg.FilterSubject != "" {
		filters = append(filters, cfg.FilterSubject)
	}
	if !slices.Equal(filters, []string{filter}) {
		req("filter_subject", "is %q; must be %q, the shard's subjects", filters, filter)
	}
	if cfg.HeadersOnly {
		req("headers_only", "is set; the worker needs the bodies, and acking an empty one deletes the row")
	}
	if cfg.ReplayPolicy != jetstream.ReplayInstantPolicy {
		req("replay_policy", "is %s; must be instant, or a backlog drains at the rate it arrived", cfg.ReplayPolicy)
	}
	if cfg.InactiveThreshold != 0 {
		req("inactive_threshold", "is %s; a durable must not expire", cfg.InactiveThreshold)
	}
	if cfg.MaxRequestExpires != 0 && cfg.MaxRequestExpires < natsPullExpiry {
		req("max_expires", "is %s; must be unset or at least %s, or the server may refuse WaveHouse's pulls", cfg.MaxRequestExpires, natsPullExpiry)
	}
	if cfg.MaxRequestBatch != 0 && cfg.MaxRequestBatch < t.Prefetch {
		req("max_request_batch", "is %d; must be 0 or at least %d, the prefetch of a worker that owns this one shard", cfg.MaxRequestBatch, t.Prefetch)
	}
	// One puller receives at a time: the one the server pinned.
	if cfg.PriorityPolicy != jetstream.PriorityPolicyPinned {
		req("priority_policy", "is %s; must be pinned_client, so one worker at a time receives the shard's rows", priorityPolicyName(cfg.PriorityPolicy))
	}
	if !slices.Equal(cfg.PriorityGroups, []string{natsPriorityGroup}) {
		req("priority_groups", "are %q; must be exactly [%q]", cfg.PriorityGroups, natsPriorityGroup)
	}
	switch {
	case cfg.PriorityPolicy != jetstream.PriorityPolicyPinned:
	case cfg.PinnedTTL < minPinnedTTL:
		// The server renews a pin only on a new pull from its holder.
		req("priority_timeout", "is %s; must be at least %s, twice the %s between WaveHouse's pulls of a shard at its cap, or a live owner loses its pin", cfg.PinnedTTL, minPinnedTTL, natsPullExpiry)
	case cfg.PinnedTTL >= defaultLeaseDuration:
		v.add(FindingRecommended, obj, "priority_timeout", "is %s; keep it under %s, the lease after which the other processes count a dead holder gone, so its pin has lapsed by then", cfg.PinnedTTL, defaultLeaseDuration)
	}
	return nil
}

// priorityPolicyName is p as the consumer config spells it.
func priorityPolicyName(p jetstream.PriorityPolicy) string {
	b, err := p.MarshalJSON()
	if err != nil {
		return strconv.Itoa(int(p))
	}
	if name := strings.Trim(string(b), `"`); name != "" {
		return name
	}
	return "none"
}

// extras warns about shard durables outside the N×V the topology names: on
// a stream a lower partition count left behind, or for a shard past a lower
// shard count. The ingest workers drain each one (ExternalNATS.IngestUnits)
// until the operator deletes it; a stream without one has nothing to drain it.
func (v *topologyVerifier) extras(ctx context.Context, partitions []string) error {
	units, err := findExtraUnits(ctx, v.js, v.t, partitions)
	if err != nil {
		return err
	}
	for _, u := range units {
		if u.durable == "" {
			v.add(FindingRecommended, "stream "+u.stream, "subjects",
				"holds %s.ingest subjects outside partitions 0-%d and has no shard durable %s-<s>, so nothing drains its %d rows; delete it", v.t.Prefix, v.t.Partitions-1, v.t.IngestConsumer, u.rows)
			continue
		}
		v.add(FindingRecommended, "consumer "+u.id(), "durable_name",
			"is outside the %d partition(s) of %d shard(s) WaveHouse is configured for; the ingest workers drain its %d rows: delete it once it holds none and no process runs the old counts", v.t.Partitions, v.t.Shards, u.rows)
	}
	return nil
}

// natsUnit is one shard durable on one stream: what one worker at a time
// consumes.
type natsUnit struct {
	stream, durable string
	// rows is what the unit still holds: pending delivery or unacked, as
	// last read (extras only).
	rows uint64
}

// id is the unit's name, the same in every process.
func (u natsUnit) id() string { return u.stream + "/" + u.durable }

// findExtraUnits lists the shard durables outside the topology's N×V, on any
// stream holding ingest subjects, and each such stream with none at all
// (durable empty). partitions is the configured partitions' streams.
func findExtraUnits(ctx context.Context, js jetstream.JetStream, t NATSTopology, partitions []string) ([]natsUnit, error) {
	v := &topologyVerifier{js: js, t: t}
	names, err := v.streamsHolding(ctx, t.Prefix+".ingest.>")
	if err != nil {
		return nil, err
	}
	var out []natsUnit
	for _, name := range names {
		configured := slices.Contains(partitions, name)
		s, err := js.Stream(ctx, name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("stream %s: %w", name, err)
		}
		lister := s.ConsumerNames(ctx)
		var shards []int
		for c := range lister.Name() {
			if sh, ok := parseShardDurable(t.IngestConsumer, c); ok && (!configured || sh >= t.Shards) {
				shards = append(shards, sh)
			}
		}
		if err := lister.Err(); err != nil {
			return nil, fmt.Errorf("list consumers of %s: %w", name, err)
		}
		slices.Sort(shards)
		if len(shards) == 0 && !configured {
			out = append(out, natsUnit{stream: name, rows: s.CachedInfo().State.Msgs})
		}
		for _, sh := range shards {
			durable := natsShardDurable(t.IngestConsumer, sh)
			c, err := s.Consumer(ctx, durable)
			if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrNotPullConsumer) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("consumer %s/%s: %w", name, durable, err)
			}
			info := c.CachedInfo()
			out = append(out, natsUnit{stream: name, durable: durable, rows: info.NumPending + uint64(max(0, info.NumAckPending))})
		}
	}
	return out, nil
}

func (v *topologyVerifier) history(ctx context.Context) error {
	t := v.t
	obj := "stream " + t.HistoryStream
	s, err := v.stream(ctx, obj, t.HistoryStream)
	if s == nil || err != nil {
		return err
	}
	info := s.CachedInfo()
	cfg := info.Config
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }

	if subjects := natsHistorySubjects(t.Prefix); !slices.Equal(cfg.Subjects, []string{subjects}) {
		req("subjects", "are %q; must be exactly %q, where the partitions republish every row", cfg.Subjects, subjects)
	}
	if len(cfg.Sources) > 0 || cfg.Mirror != nil {
		req("sources", "are set; the history must have no sources or mirror, since the partitions' republish already stores every row there")
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		req("retention", "is %s; must be limits", cfg.Retention)
	}
	// A full history never holds up ingest (republish does not wait for it),
	// but with discard new it stops taking the newest rows SSE wants.
	if cfg.Discard != jetstream.DiscardOld {
		v.add(FindingRecommended, obj, "discard", "is %s; old keeps the newest rows for SSE when the history is full", cfg.Discard)
	}
	if cfg.MaxAge <= 0 {
		req("max_age", "is unlimited; must be set, at least the longest gap window a tenant replays")
	}
	if cfg.MaxBytes <= 0 {
		v.add(FindingRecommended, obj, "max_bytes", "is unlimited; set it to bound the disk")
	}
	v.replicas(obj, cfg.Replicas)
	return nil
}

// republish checks that partition p republishes every row it stores to the
// history, with the partition token dropped from the subject.
func (v *topologyVerifier) republish(obj string, rp *jetstream.RePublish, p int) {
	want := natsRepublish(v.t.Prefix, p)
	switch {
	case rp == nil:
		v.add(FindingRequired, obj, "republish", "is unset; must be {src: %q, dest: %q}, or the history (SSE replay and the live hub) gets nothing", want.Source, want.Destination)
	case rp.Source != want.Source || rp.Destination != want.Destination:
		v.add(FindingRequired, obj, "republish", "is {src: %q, dest: %q}; must be {src: %q, dest: %q}", rp.Source, rp.Destination, want.Source, want.Destination)
	case rp.HeadersOnly:
		v.add(FindingRequired, obj, "republish", "is headers_only; the history needs the bodies")
	}
}

func (v *topologyVerifier) dlq(ctx context.Context) error {
	s, err := v.findBySubject(ctx, "dead-letter stream", v.t.Prefix+".dlq.x")
	if s == nil || err != nil {
		return err
	}
	cfg := s.CachedInfo().Config
	obj := "stream " + cfg.Name
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }

	if filter := natsDLQSubjects(v.t.Prefix); !slices.Contains(cfg.Subjects, filter) {
		req("subjects", "are %q; must include %q", cfg.Subjects, filter)
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		req("retention", "is %s; must be limits", cfg.Retention)
	}
	if cfg.Discard != jetstream.DiscardOld {
		req("discard", "is %s; must be old", cfg.Discard)
	}
	if cfg.Storage != jetstream.FileStorage {
		req("storage", "is %s; must be file", cfg.Storage)
	}
	if cfg.MaxBytes <= 0 {
		req("max_bytes", "is unlimited; must be set")
	}
	if cfg.MaxMsgsPerSubject <= 0 {
		v.add(FindingRecommended, obj, "max_msgs_per_subject", "set it, so one topic's parked rows evict only its own")
	}
	if cfg.PersistMode == jetstream.AsyncPersistMode {
		v.add(FindingRecommended, obj, "persist_mode", "is async; a crash of the server process loses parked rows it acked")
	}
	v.replicas(obj, cfg.Replicas)
	return nil
}

// replicas recommends 3 replicas for a stream holding rows. WaveHouse does
// not require sync_always under nats: an R3 publish is acked once a quorum
// has stored it, so with one replica an ack rests on one server's disk.
func (v *topologyVerifier) replicas(obj string, n int) {
	if problem, ok := replicasProblem(n); ok {
		v.add(FindingRecommended, obj, "num_replicas", "%s", problem)
	}
}

func replicasProblem(n int) (string, bool) {
	switch {
	case n <= 1:
		return fmt.Sprintf("is %d; an ack then rests on one server's disk, and a crash loses what it stored since its last sync (sync_interval); 3 across failure domains survives losing a server", n), true
	case n < 3:
		return fmt.Sprintf("is %d; 3 across failure domains survives losing a server", n), true
	}
	return "", false
}

// coordBucket checks the KV bucket the leases live in (Leases). Its stream
// is KV_<bucket>, which is how JetStream stores a bucket.
func (v *topologyVerifier) coordBucket(ctx context.Context) error {
	name := v.t.CoordBucket
	obj := "kv bucket " + name
	s, err := v.js.Stream(ctx, "KV_"+name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		v.add(FindingRequired, obj, "bucket", "does not exist; coord.backend=nats holds its leases there")
		return nil
	}
	if err != nil {
		return fmt.Errorf("kv bucket %s: %w", name, err)
	}
	cfg := s.CachedInfo().Config
	req := func(field, format string, args ...any) { v.add(FindingRequired, obj, field, format, args...) }

	if cfg.MaxMsgsPerSubject < 1 {
		req("history", "is unset; a KV bucket keeps at least one value per key")
	}
	// The wavehouse user may read a key only by direct get.
	if !cfg.AllowDirect {
		req("allow_direct", "is unset; WaveHouse reads leases by direct get (a bucket nack or `nats kv add` creates has it)")
	}
	// A candidate judges expiry on its own clock; a key the server expires
	// would end a live lease early.
	if cfg.MaxAge != 0 {
		req("ttl", "is %s; must be unset, or a live lease expires under its holder", cfg.MaxAge)
	}
	if cfg.Storage != jetstream.FileStorage {
		v.add(FindingRecommended, obj, "storage", "is %s; file survives a server restart without every lease starting over", cfg.Storage)
	}
	if cfg.Replicas < 3 {
		v.add(FindingRecommended, obj, "num_replicas", "is %d; 3 survives losing a server", cfg.Replicas)
	}
	return nil
}
